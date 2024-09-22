package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/jetstream/pkg/consumer"
	"github.com/bluesky-social/jetstream/pkg/models"
	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/semaphore"
	"golang.org/x/time/rate"
)

var (
	upgrader = websocket.Upgrader{}
)

type WantedCollections struct {
	Prefixes  []string
	FullPaths map[string]struct{}
}

type Subscriber struct {
	ws               *websocket.Conn
	realIP           string
	seq              int64
	buf              chan *[]byte
	id               int64
	cLk              sync.Mutex
	cursor           *int64
	compress         bool
	deliveredCounter prometheus.Counter
	bytesCounter     prometheus.Counter
	// wantedCollections is nil if the subscriber wants all collections
	wantedCollections *WantedCollections
	wantedDids        map[string]struct{}
	rl                *rate.Limiter
}

type Server struct {
	Subscribers map[int64]*Subscriber
	lk          sync.RWMutex
	nextSub     int64
	Consumer    *consumer.Consumer
	maxSubRate  float64
	seq         int64
}

func NewServer(maxSubRate float64) (*Server, error) {
	s := Server{
		Subscribers: make(map[int64]*Subscriber),
		maxSubRate:  maxSubRate,
	}

	return &s, nil
}

var maxConcurrentEmits = int64(100)
var cutoverThresholdUS = int64(1_000_000)

func (s *Server) Emit(ctx context.Context, e *models.Event, asJSON, compBytes []byte) error {
	ctx, span := tracer.Start(ctx, "Emit")
	defer span.End()

	log := slog.With("source", "server_emit")

	s.lk.RLock()
	defer s.lk.RUnlock()

	eventsEmitted.Inc()
	evtSize := float64(len(asJSON))
	bytesEmitted.Add(evtSize)

	collection := ""
	if e.EventType == models.EventCommit && e.Commit != nil {
		collection = e.Commit.Collection
	}

	getJSONEvent := func() []byte { return asJSON }
	getCompressedEvent := func() []byte { return compBytes }

	sem := semaphore.NewWeighted(maxConcurrentEmits)
	for _, sub := range s.Subscribers {
		if err := sem.Acquire(ctx, 1); err != nil {
			log.Error("failed to acquire semaphore", "error", err)
			return fmt.Errorf("failed to acquire semaphore: %w", err)
		}
		go func(sub *Subscriber) {
			defer sem.Release(1)
			sub.cLk.Lock()
			defer sub.cLk.Unlock()

			// Don't emit events to subscribers that are replaying and are too far behind
			if sub.cursor != nil && sub.seq < e.TimeUS-cutoverThresholdUS {
				return
			}

			// Pick the event valuer for the subscriber
			getEventBytes := getJSONEvent
			if sub.compress {
				getEventBytes = getCompressedEvent
			}

			emitToSubscriber(ctx, log, sub, e.TimeUS, e.Did, collection, false, getEventBytes)
		}(sub)
	}

	if err := sem.Acquire(ctx, maxConcurrentEmits); err != nil {
		log.Error("failed to acquire semaphore", "error", err)
		return fmt.Errorf("failed to acquire semaphore: %w", err)
	}

	s.seq = e.TimeUS

	return nil
}

func emitToSubscriber(ctx context.Context, log *slog.Logger, sub *Subscriber, timeUS int64, did, collection string, playback bool, getEventBytes func() []byte) error {
	if !sub.WantsCollection(collection) {
		return nil
	}

	if len(sub.wantedDids) > 0 {
		if _, ok := sub.wantedDids[did]; !ok {
			return nil
		}
	}

	// Skip events that are older than the subscriber's last seen event
	if timeUS <= sub.seq {
		return nil
	}

	evtBytes := getEventBytes()
	if playback {
		// Copy the event bytes so the playback iterator can reuse the buffer
		evtBytes = append([]byte{}, evtBytes...)
		select {
		case <-ctx.Done():
			log.Error("failed to send event to subscriber", "error", ctx.Err(), "subscriber", sub.id)
			// If we failed to send to a subscriber, close the connection
			err := sub.ws.Close()
			if err != nil {
				log.Error("failed to close subscriber connection", "error", err)
			}
			return ctx.Err()
		case sub.buf <- &evtBytes:
			sub.seq = timeUS
			sub.deliveredCounter.Inc()
			sub.bytesCounter.Add(float64(len(evtBytes)))
		}
	} else {
		select {
		case <-ctx.Done():
			log.Error("failed to send event to subscriber", "error", ctx.Err(), "subscriber", sub.id)
			// If we failed to send to a subscriber, close the connection
			err := sub.ws.Close()
			if err != nil {
				log.Error("failed to close subscriber connection", "error", err)
			}
			return ctx.Err()
		case sub.buf <- &evtBytes:
			sub.seq = timeUS
			sub.deliveredCounter.Inc()
			sub.bytesCounter.Add(float64(len(evtBytes)))
		default:
			// Drop slow subscribers if they're live tailing and fall too far behind
			log.Error("failed to send event to subscriber, dropping", "error", "buffer full", "subscriber", sub.id)
			err := sub.ws.Close()
			if err != nil {
				log.Error("failed to close subscriber connection", "error", err)
			}
		}
	}

	return nil
}

func (s *Server) GetSeq() int64 {
	s.lk.RLock()
	defer s.lk.RUnlock()
	return s.seq
}

func (s *Server) AddSubscriber(ws *websocket.Conn, realIP string, compress bool, wantedCollectionPrefixes []string, wantedCollections []string, wantedDids []string, cursor *int64) (*Subscriber, error) {
	s.lk.Lock()
	defer s.lk.Unlock()

	didMap := make(map[string]struct{})
	for _, d := range wantedDids {
		didMap[d] = struct{}{}
	}

	// Build the WantedCollections struct
	var wantedCol *WantedCollections
	if len(wantedCollections) > 0 || len(wantedCollectionPrefixes) > 0 {
		wantedCol = &WantedCollections{
			Prefixes:  wantedCollectionPrefixes,
			FullPaths: make(map[string]struct{}),
		}

		// Sort the prefixes by length so we test the shortest prefixes first
		slices.SortFunc(wantedCol.Prefixes, func(a, b string) int {
			return len(a) - len(b)
		})

		// Add the full paths to the map
		for _, c := range wantedCollections {
			wantedCol.FullPaths[c] = struct{}{}
		}
	}

	sub := Subscriber{
		ws:                ws,
		realIP:            realIP,
		buf:               make(chan *[]byte, 10_000),
		id:                s.nextSub,
		wantedCollections: wantedCol,
		wantedDids:        didMap,
		cursor:            cursor,
		compress:          compress,
		deliveredCounter:  eventsDelivered.WithLabelValues(realIP),
		bytesCounter:      bytesDelivered.WithLabelValues(realIP),
		rl:                rate.NewLimiter(rate.Limit(s.maxSubRate), int(s.maxSubRate)),
	}

	s.Subscribers[s.nextSub] = &sub
	s.nextSub++

	subscribersConnected.WithLabelValues(realIP).Inc()

	slog.Info("adding subscriber",
		"real_ip", realIP,
		"id", sub.id,
		"wantedCollections", wantedCol,
		"wantedDids", wantedDids,
		"cursor", cursor,
		"compress", compress,
	)

	return &sub, nil
}

func (s *Server) RemoveSubscriber(num int64) {
	s.lk.Lock()
	defer s.lk.Unlock()

	slog.Info("removing subscriber", "id", num, "real_ip", s.Subscribers[num].realIP)

	subscribersConnected.WithLabelValues(s.Subscribers[num].realIP).Dec()

	delete(s.Subscribers, num)
}

func (s *Server) HandleSubscribe(c echo.Context) error {
	ctx, cancel := context.WithCancel(c.Request().Context())
	defer cancel()

	wantedCollections := []string{}
	wantedCollectionPrefixes := []string{}
	qWantedCollections := c.Request().URL.Query()["wantedCollections"]
	if len(qWantedCollections) > 0 {
		for _, wantedCol := range qWantedCollections {
			if strings.HasSuffix(wantedCol, ".*") {
				wantedCollectionPrefixes = append(wantedCollectionPrefixes, strings.TrimSuffix(wantedCol, "*"))
				continue
			}

			col, err := syntax.ParseNSID(wantedCol)
			if err != nil {
				c.String(http.StatusBadRequest, fmt.Sprintf("invalid collection: %s", wantedCol))
				return fmt.Errorf("invalid collection: %s", wantedCol)
			}
			wantedCollections = append(wantedCollections, col.String())
		}
	}

	// Reject requests with too many wanted collections
	if len(wantedCollections)+len(wantedCollectionPrefixes) > 100 {
		c.String(http.StatusBadRequest, "too many wanted collections")
		return fmt.Errorf("too many wanted collections")
	}

	wantedDids := []string{}
	qWantedDids := c.Request().URL.Query()["wantedDids"]
	if len(qWantedDids) > 0 {
		for _, d := range qWantedDids {
			did, err := syntax.ParseDID(d)
			if err != nil {
				c.String(http.StatusBadRequest, fmt.Sprintf("invalid did: %s", d))
				return fmt.Errorf("invalid did: %s", d)
			}
			wantedDids = append(wantedDids, did.String())
		}
	}

	// Reject requests with too many wanted DIDs
	if len(wantedDids) > 10_000 {
		c.String(http.StatusBadRequest, "too many wanted DIDs")
		return fmt.Errorf("too many wanted DIDs")
	}

	// Check if the user wants zstd compression
	acceptEncoding := c.Request().Header.Get("Accept-Encoding")
	compress := strings.Contains(acceptEncoding, "zstd")

	var cursor *int64
	var err error
	qCursor := c.Request().URL.Query().Get("cursor")
	if qCursor != "" {
		cursor = new(int64)
		*cursor, err = strconv.ParseInt(qCursor, 10, 64)
		if err != nil {
			c.String(http.StatusBadRequest, fmt.Sprintf("invalid cursor: %s", qCursor))
			return fmt.Errorf("invalid cursor: %s", qCursor)
		}

		// If given a future cursor, just live tail
		if *cursor > time.Now().UnixMicro() {
			cursor = nil
		}
	}

	ws, err := upgrader.Upgrade(c.Response(), c.Request(), nil)
	if err != nil {
		return err
	}
	defer ws.Close()

	log := slog.With("source", "server_handle_subscribe", "socket_addr", ws.RemoteAddr().String(), "real_ip", c.RealIP())

	go func() {
		for {
			_, _, err := ws.ReadMessage()
			if err != nil {
				log.Error("failed to read message from websocket", "error", err)
				cancel()
				return
			}
		}
	}()

	sub, err := s.AddSubscriber(ws, c.RealIP(), compress, wantedCollectionPrefixes, wantedCollections, wantedDids, cursor)
	if err != nil {
		log.Error("failed to add subscriber", "error", err)
		return err
	}
	defer s.RemoveSubscriber(sub.id)

	if cursor != nil {
		log.Info("replaying events", "cursor", *cursor)
		playbackRateLimit := s.maxSubRate * 10

		go func() {
			for {
				lastSeq, err := s.Consumer.ReplayEvents(ctx, sub.compress, *cursor, playbackRateLimit, func(ctx context.Context, timeUS int64, did, collection string, getEventBytes func() []byte) error {
					return emitToSubscriber(ctx, log, sub, timeUS, did, collection, true, getEventBytes)
				})
				if err != nil {
					log.Error("failed to replay events", "error", err)
					cancel()
					return
				}
				serverLastSeq := s.GetSeq()
				log.Info("finished replaying events", "replay_last_time", time.UnixMicro(lastSeq), "server_last_time", time.UnixMicro(serverLastSeq))

				// If last event replayed is close enough to the last live event, start live tailing
				if lastSeq > serverLastSeq-(cutoverThresholdUS/2) {
					break
				}

				// Otherwise, update the cursor and replay again
				lastSeq++
				sub.cLk.Lock()
				cursor = &lastSeq
				sub.cLk.Unlock()
			}
			log.Info("finished replaying events, starting live tail")
			sub.cLk.Lock()
			defer sub.cLk.Unlock()
			sub.cursor = nil
		}()
	}

	for {
		select {
		case <-ctx.Done():
			log.Info("shutting down subscriber")
			return nil
		case msg := <-sub.buf:
			err := sub.rl.Wait(ctx)
			if err != nil {
				log.Error("failed to wait for rate limiter", "error", err)
				return fmt.Errorf("failed to wait for rate limiter: %w", err)
			}

			// When compression is enabled, the buffer contains the compressed message
			if compress {
				if err := ws.WriteMessage(websocket.BinaryMessage, *msg); err != nil {
					log.Error("failed to write message to websocket", "error", err)
					return nil
				}
				continue
			}

			if err := ws.WriteMessage(websocket.TextMessage, *msg); err != nil {
				log.Error("failed to write message to websocket", "error", err)
				return nil
			}
		}
	}
}

// WantsCollection returns true if the subscriber wants the given collection
func (sub *Subscriber) WantsCollection(collection string) bool {
	if sub.wantedCollections == nil {
		return true
	}

	// Start with the full paths for fast lookup
	if len(sub.wantedCollections.FullPaths) > 0 {
		if _, match := sub.wantedCollections.FullPaths[collection]; match {
			return true
		}
	}

	// Check the prefixes (shortest first)
	for _, prefix := range sub.wantedCollections.Prefixes {
		if strings.HasPrefix(collection, prefix) {
			return true
		}
	}

	return false
}
