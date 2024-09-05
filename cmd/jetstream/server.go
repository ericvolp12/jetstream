package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/ericvolp12/jetstream/pkg/consumer"
	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	upgrader = websocket.Upgrader{}
)

type Subscriber struct {
	ws                *websocket.Conn
	seq               int64
	buf               chan *[]byte
	id                int64
	deliveredCounter  prometheus.Counter
	bytesCounter      prometheus.Counter
	wantedCollections []string
	wantedDids        []string
}

type Server struct {
	Subscribers map[int64]*Subscriber
	lk          sync.RWMutex
	nextSub     int64
}

func NewServer() (*Server, error) {
	s := Server{
		Subscribers: make(map[int64]*Subscriber),
	}

	return &s, nil
}

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

	bytesEmitted.Add(float64(len(b)))

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	isCommit := e.EventType == consumer.EventCommit && e.Commit != nil

	wg := sync.WaitGroup{}
	for _, sub := range s.Subscribers {
		wg.Add(1)
		go func(sub *Subscriber) {
			defer wg.Done()
			if len(sub.wantedCollections) > 0 && isCommit {
				if !slices.Contains(sub.wantedCollections, e.Commit.Collection) {
					return
				}
			}

			if len(sub.wantedDids) > 0 {
				if !slices.Contains(sub.wantedDids, e.Did) {
					return
				}
			}

			select {
			case <-ctx.Done():
				log.Error("failed to send event to subscriber", "error", ctx.Err(), "subscriber", sub.id)

				// If we failed to send to a subscriber, close the connection
				err := sub.ws.Close()
				if err != nil {
					log.Error("failed to close subscriber connection", "error", err)
				}
				return
			case sub.buf <- &b:
				sub.seq++
				sub.deliveredCounter.Inc()
				sub.bytesCounter.Add(float64(len(b)))
			}
		}(sub)
	}

	wg.Wait()

	return nil
}

func (s *Server) AddSubscriber(ws *websocket.Conn, wantedCollections []string, wantedDids []string) *Subscriber {
	s.lk.Lock()
	defer s.lk.Unlock()

	sub := Subscriber{
		ws:                ws,
		buf:               make(chan *[]byte, 100),
		id:                s.nextSub,
		wantedCollections: wantedCollections,
		wantedDids:        wantedDids,
		deliveredCounter:  eventsDelivered.WithLabelValues(ws.RemoteAddr().String()),
		bytesCounter:      bytesDelivered.WithLabelValues(ws.RemoteAddr().String()),
	}

	s.Subscribers[s.nextSub] = &sub
	s.nextSub++

	subscribersConnected.WithLabelValues(ws.RemoteAddr().String()).Inc()

	slog.Info("adding subscriber",
		"remote_addr", ws.RemoteAddr().String(),
		"id", sub.id,
		"wantedCollections", wantedCollections,
		"wantedDids", wantedDids,
	)

	return &sub
}

func (s *Server) RemoveSubscriber(num int64) {
	s.lk.Lock()
	defer s.lk.Unlock()

	slog.Info("removing subscriber", "id", num)

	subscribersConnected.WithLabelValues(s.Subscribers[num].ws.RemoteAddr().String()).Dec()

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

	log := slog.With("source", "server_handle_subscribe", "remote_addr", ws.RemoteAddr().String())

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

	sub := s.AddSubscriber(ws, wantedCollections, wantedDids)
	defer s.RemoveSubscriber(sub.id)

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-sub.buf:
			if err := ws.WriteMessage(websocket.TextMessage, *msg); err != nil {
				log.Error("failed to write message to websocket", "error", err)
				return nil
			}
		}
	}
}
