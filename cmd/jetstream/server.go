package main

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/ericvolp12/jetstream/pkg/consumer"
	"github.com/goccy/go-json"
	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/semaphore"
	"golang.org/x/time/rate"
)

var (
	upgrader = websocket.Upgrader{}
)

type Subscriber struct {
	ws                *websocket.Conn
	realIP            string
	seq               int64
	buf               chan *[]byte
	id                int64
	cLk               sync.Mutex
	cursor            *int64
	deliveredCounter  prometheus.Counter
	bytesCounter      prometheus.Counter
	wantedCollections map[string]struct{}
	wantedDids        map[string]struct{}
	rl                *rate.Limiter
}

type Server struct {
	Subscribers map[int64]*Subscriber
	lk          sync.RWMutex
	nextSub     int64
	Consumer    *consumer.Consumer
	maxSubRate  float64
}

func NewServer(maxSubRate float64) (*Server, error) {
	s := Server{
		Subscribers: make(map[int64]*Subscriber),
		maxSubRate:  maxSubRate,
	}

	return &s, nil
}

var maxConcurrentEmits = 100

func (s *Server) Emit(ctx context.Context, e consumer.Event) error {
	ctx, span := tracer.Start(ctx, "Emit")
	defer span.End()

	log := slog.With("source", "server_emit")

	s.lk.RLock()
	defer s.lk.RUnlock()

	eventsEmitted.Inc()

	asJSON := &bytes.Buffer{}
	err := json.NewEncoder(asJSON).Encode(e)
	if err != nil {
		return fmt.Errorf("failed to encode event as json: %w", err)
	}
	b := asJSON.Bytes()

	evtSize := float64(len(b))
	bytesEmitted.Add(evtSize)

	collection := ""
	if e.EventType == consumer.EventCommit && e.Commit != nil {
		collection = e.Commit.Collection
	}

	getEncodedEvent := func() []byte { return b }

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	sem := semaphore.NewWeighted(int64(maxConcurrentEmits))
	for _, sub := range s.Subscribers {
		if err := sem.Acquire(ctx, 1); err != nil {
			log.Error("failed to acquire semaphore", "error", err)
			return fmt.Errorf("failed to acquire semaphore: %w", err)
		}
		go func(sub *Subscriber) {
			defer sem.Release(1)
			sub.cLk.Lock()
			defer sub.cLk.Unlock()

			// Don't emit to subscribers that are playing catch-up
			if sub.cursor != nil {
				return
			}
			emitToSubscriber(ctx, log, sub, e.Did, collection, false, getEncodedEvent)
		}(sub)
	}

	if err := sem.Acquire(ctx, int64(maxConcurrentEmits)); err != nil {
		log.Error("failed to acquire semaphore", "error", err)
		return fmt.Errorf("failed to acquire semaphore: %w", err)
	}

	return nil
}

func emitToSubscriber(ctx context.Context, log *slog.Logger, sub *Subscriber, did, collection string, copyOnWrite bool, getEncodedEvent func() []byte) error {
	if len(sub.wantedCollections) > 0 && collection != "" {
		if _, ok := sub.wantedCollections[collection]; !ok {
			return nil
		}
	}

	if len(sub.wantedDids) > 0 {
		if _, ok := sub.wantedDids[did]; !ok {
			return nil
		}
	}

	evtBytes := getEncodedEvent()
	if copyOnWrite {
		evtBytes = append([]byte{}, evtBytes...)
	}

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
		sub.seq++
		sub.deliveredCounter.Inc()
		sub.bytesCounter.Add(float64(len(evtBytes)))
	}

	return nil
}

func (s *Server) AddSubscriber(ws *websocket.Conn, realIP string, wantedCollections []string, wantedDids []string, cursor *int64) *Subscriber {
	s.lk.Lock()
	defer s.lk.Unlock()

	colMap := make(map[string]struct{})
	for _, c := range wantedCollections {
		colMap[c] = struct{}{}
	}

	didMap := make(map[string]struct{})
	for _, d := range wantedDids {
		didMap[d] = struct{}{}
	}

	sub := Subscriber{
		ws:                ws,
		realIP:            realIP,
		buf:               make(chan *[]byte, 100),
		id:                s.nextSub,
		wantedCollections: colMap,
		wantedDids:        didMap,
		cursor:            cursor,
		deliveredCounter:  eventsDelivered.WithLabelValues(realIP),
		bytesCounter:      bytesDelivered.WithLabelValues(realIP),
		rl:                rate.NewLimiter(rate.Limit(s.maxSubRate), 1000),
	}

	s.Subscribers[s.nextSub] = &sub
	s.nextSub++

	subscribersConnected.WithLabelValues(realIP).Inc()

	slog.Info("adding subscriber",
		"real_ip", realIP,
		"id", sub.id,
		"wantedCollections", wantedCollections,
		"wantedDids", wantedDids,
	)

	return &sub
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

	ws, err := upgrader.Upgrade(c.Response(), c.Request(), nil)
	if err != nil {
		return err
	}
	defer ws.Close()

	wantedCollections := []string{}
	qWantedCollections := c.Request().URL.Query()["wantedCollections"]
	if len(qWantedCollections) > 0 {
		for _, c := range qWantedCollections {
			col, err := syntax.ParseNSID(c)
			if err != nil {
				return fmt.Errorf("invalid collection: %s", c)
			}
			wantedCollections = append(wantedCollections, col.String())
		}
	}

	wantedDids := []string{}
	qWantedDids := c.Request().URL.Query()["wantedDids"]
	if len(qWantedDids) > 0 {
		for _, d := range qWantedDids {
			did, err := syntax.ParseDID(d)
			if err != nil {
				return fmt.Errorf("invalid did: %s", d)
			}
			wantedDids = append(wantedDids, did.String())
		}
	}

	var cursor *int64
	qCursor := c.Request().URL.Query().Get("cursor")
	if qCursor != "" {
		cursor = new(int64)
		*cursor, err = strconv.ParseInt(qCursor, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid cursor: %s", qCursor)
		}

		// If given a future cursor, just live tail
		if *cursor > time.Now().UnixMicro() {
			cursor = nil
		}
	}

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

	sub := s.AddSubscriber(ws, c.RealIP(), wantedCollections, wantedDids, cursor)
	defer s.RemoveSubscriber(sub.id)

	if cursor != nil {
		log.Info("replaying events", "cursor", *cursor)
		playbackRateLimit := s.maxSubRate * 10
		go func() {
			err := s.Consumer.ReplayEvents(ctx, *cursor, playbackRateLimit, func(ctx context.Context, did, collection string, getEncodedEvent func() []byte) error {
				ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()

				return emitToSubscriber(ctx, log, sub, did, collection, true, getEncodedEvent)
			})
			if err != nil {
				log.Error("failed to replay events", "error", err)
				cancel()
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
			if err := ws.WriteMessage(websocket.TextMessage, *msg); err != nil {
				log.Error("failed to write message to websocket", "error", err)
				return nil
			}
		}
	}
}
