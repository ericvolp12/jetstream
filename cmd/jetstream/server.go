package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/ericvolp12/jetstream/pkg/consumer"
	"github.com/gorilla/websocket"
	"github.com/klauspost/compress/zstd"
	"github.com/labstack/echo/v4"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/exp/slog"
)

var (
	upgrader = websocket.Upgrader{}
)

type Subscriber struct {
	ws               *websocket.Conn
	seq              int64
	buf              chan []byte
	id               int64
	format           string
	deliveredCounter prometheus.Counter
	bytesCounter     prometheus.Counter
}

type Server struct {
	Subscribers map[int64]*Subscriber
	lk          sync.RWMutex
	nextSub     int64
}

func NewServer() *Server {
	return &Server{
		Subscribers: make(map[int64]*Subscriber),
	}
}

var encoder, _ = zstd.NewWriter(nil)

func ZstdCompress(src []byte) []byte {
	return encoder.EncodeAll(src, make([]byte, 0, len(src)))
}

func (s *Server) Emit(ctx context.Context, e consumer.Event) error {
	ctx, span := tracer.Start(ctx, "Emit")
	defer span.End()

	log := slog.With("source", "server_emit")

	asJSON := &bytes.Buffer{}
	err := json.NewEncoder(asJSON).Encode(e)
	if err != nil {
		return fmt.Errorf("failed to encode event as json: %w", err)
	}

	data := asJSON.Bytes()

	s.lk.RLock()
	defer s.lk.RUnlock()

	eventsEmitted.Inc()
	bytesEmitted.Add(float64(len(data)))

	zstdData := []byte{}

	// Check if any subscribers want zstd
	for _, sub := range s.Subscribers {
		if sub.format == "zstd" {
			zstdData = ZstdCompress(data)
			break
		}
	}

	for _, sub := range s.Subscribers {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		msg := data
		if sub.format == "zstd" {
			msg = zstdData
		}

		select {
		case <-ctx.Done():
			log.Error("failed to send event to subscriber", "error", ctx.Err(), "subscriber", sub.id)

			// If we failed to send to a subscriber, close the connection
			err := sub.ws.Close()
			if err != nil {
				log.Error("failed to close subscriber connection", "error", err)
			}
			return nil
		case sub.buf <- msg:
			sub.seq++
			sub.deliveredCounter.Inc()
			sub.bytesCounter.Add(float64(len(msg)))
		}
	}
	return nil
}

func (s *Server) AddSubscriber(ws *websocket.Conn, format string) *Subscriber {
	s.lk.Lock()
	defer s.lk.Unlock()

	sub := Subscriber{
		ws:               ws,
		buf:              make(chan []byte, 100),
		id:               s.nextSub,
		format:           format,
		deliveredCounter: eventsDelivered.WithLabelValues(format, ws.RemoteAddr().String()),
		bytesCounter:     bytesDelivered.WithLabelValues(format, ws.RemoteAddr().String()),
	}

	s.Subscribers[s.nextSub] = &sub
	s.nextSub++

	subscribersConnected.WithLabelValues(format, ws.RemoteAddr().String()).Inc()

	slog.Info("adding subscriber", "remote_addr", ws.RemoteAddr().String(), "id", sub.id)

	return &sub
}

func (s *Server) RemoveSubscriber(num int64) {
	s.lk.Lock()
	defer s.lk.Unlock()

	slog.Info("removing subscriber", "id", num)

	subscribersConnected.WithLabelValues(s.Subscribers[num].format, s.Subscribers[num].ws.RemoteAddr().String()).Dec()

	delete(s.Subscribers, num)
}

var validFormats = []string{"raw", "gzip", "zstd"}

func (s *Server) HandleSubscribe(c echo.Context) error {
	ctx, cancel := context.WithCancel(c.Request().Context())
	defer cancel()

	ws, err := upgrader.Upgrade(c.Response(), c.Request(), nil)
	if err != nil {
		return err
	}
	defer ws.Close()

	format := c.QueryParam("format")
	if format == "" {
		format = "raw"
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

	sub := s.AddSubscriber(ws, format)
	defer s.RemoveSubscriber(sub.id)

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-sub.buf:
			if err := ws.WriteMessage(websocket.BinaryMessage, msg); err != nil {
				log.Error("failed to write message to websocket", "error", err)
				return nil
			}
		}
	}
}
