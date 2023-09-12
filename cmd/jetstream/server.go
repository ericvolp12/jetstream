package main

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/klauspost/compress/zstd"
	"github.com/labstack/echo/v4"
	"golang.org/x/exp/slog"
)

var (
	upgrader = websocket.Upgrader{}
)

type Subscriber struct {
	con    net.Conn
	seq    int64
	buf    chan []byte
	id     int64
	format string
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

func (s *Server) Emit(ctx context.Context, data []byte) error {
	ctx, span := tracer.Start(ctx, "Emit")
	defer span.End()

	log := slog.With("source", "server_emit")

	s.lk.RLock()
	defer s.lk.RUnlock()

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
			return ctx.Err()
		case sub.buf <- msg:
			sub.seq++
		}
	}
	return nil
}

func (s *Server) AddSubscriber(con net.Conn, format string) *Subscriber {
	s.lk.Lock()
	defer s.lk.Unlock()

	sub := Subscriber{
		con:    con,
		buf:    make(chan []byte, 100),
		id:     s.nextSub,
		format: format,
	}

	s.Subscribers[s.nextSub] = &sub
	s.nextSub++

	slog.Info("adding subscriber", "remote_addr", con.RemoteAddr().String(), "id", sub.id)

	return &sub
}

func (s *Server) RemoveSubscriber(num int64) {
	slog.Info("removing subscriber", "id", num)

	s.lk.Lock()
	defer s.lk.Unlock()

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

	sub := s.AddSubscriber(ws.UnderlyingConn(), format)
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
