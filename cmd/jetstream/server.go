package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/ericvolp12/jetstream/pkg/consumer"
	"github.com/fxamacker/cbor/v2"
	"github.com/gorilla/websocket"
	"github.com/klauspost/compress/zstd"
	"github.com/labstack/echo/v4"
	"github.com/prometheus/client_golang/prometheus"
	kgo "github.com/segmentio/kafka-go"
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
	compress         bool
	deliveredCounter prometheus.Counter
	bytesCounter     prometheus.Counter
	wantedTypes      []string
	wantedDids       []string
}

type Server struct {
	Subscribers map[int64]*Subscriber
	lk          sync.RWMutex
	nextSub     int64
	kafkaWriter *kgo.Writer
	topic       string
	seq         int64
	seqlk       sync.Mutex
}

func NewServer(broker, topic string) *Server {
	// Create a Kafka admin client
	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": broker})
	if err != nil {
		log.Fatalf("Failed to create Kafka admin client: %s", err)
	}

	// Check if the topic exists
	metadata, err := adminClient.GetMetadata(&topic, false, 5000)
	if err != nil || metadata.Topics[topic].Error.Code() != kafka.ErrNoError {
		// If the topic does not exist, create it
		topicSpec := kafka.TopicSpecification{
			Topic:             topic,
			NumPartitions:     1, // Set as needed
			ReplicationFactor: 1, // Set as needed
		}
		results, err := adminClient.CreateTopics(context.Background(), []kafka.TopicSpecification{topicSpec})
		if err != nil {
			log.Fatalf("Failed to create Kafka topic: %s", err)
		}
		for _, result := range results {
			if result.Error.Code() != kafka.ErrNoError {
				log.Fatalf("Failed to create topic: %s", result.Error)
			}
		}
	}

	// Close the admin client
	adminClient.Close()

	kafkaWriter := kgo.NewWriter(kgo.WriterConfig{
		Brokers: []string{broker},
		Topic:   topic,
	})

	return &Server{
		Subscribers: make(map[int64]*Subscriber),
		kafkaWriter: kafkaWriter,
		topic:       topic,
	}
}

var encoder, _ = zstd.NewWriter(nil)

func ZstdCompress(src []byte) []byte {
	return encoder.EncodeAll(src, make([]byte, 0, len(src)))
}

func (s *Server) Emit(ctx context.Context, e consumer.Event) error {
	ctx, span := tracer.Start(ctx, "Emit")
	defer span.End()

	s.seqlk.Lock()
	s.seq++
	jsSeq := s.seq
	s.seqlk.Unlock()

	log := slog.With("source", "server_emit")

	s.lk.RLock()
	defer s.lk.RUnlock()

	eventsEmitted.Inc()

	var jsonData *[]byte
	var cborData *[]byte
	var compressedJSON *[]byte
	var compressedCBOR *[]byte

	biggestBufSize := 0

	asJSON := &bytes.Buffer{}
	err := json.NewEncoder(asJSON).Encode(e)
	if err != nil {
		return fmt.Errorf("failed to encode event as json: %w", err)
	}
	b := asJSON.Bytes()
	if len(b) > biggestBufSize {
		biggestBufSize = len(b)
	}
	jsonData = &b

	// Emit to Kafka
	if jsonData != nil {
		err := s.kafkaWriter.WriteMessages(ctx, kafka.Message{
			Key:   []byte(fmt.Sprintf("event-%d", jsSeq)),
			Value: *jsonData,
		})

		if err != nil {
			log.Error("failed to send event to Kafka", "error", err)
			return err
		}
	}

	// Check if any subscribers want zstd
	for _, sub := range s.Subscribers {
		if sub.format == "json" {
			if sub.compress && compressedJSON == nil {
				b := ZstdCompress(*jsonData)
				compressedJSON = &b
			}
		}
		if sub.format == "cbor" {
			if cborData == nil {
				asCbor, err := cbor.Marshal(e)
				if err != nil {
					return fmt.Errorf("failed to encode event as cbor: %w", err)
				}
				b := asCbor
				if len(b) > biggestBufSize {
					biggestBufSize = len(b)
				}
				cborData = &b
			}
			if sub.compress && compressedCBOR == nil {
				b := ZstdCompress(*cborData)
				compressedCBOR = &b
			}
		}
	}

	bytesEmitted.Add(float64(biggestBufSize))

	for _, sub := range s.Subscribers {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		if len(sub.wantedTypes) > 0 {
			if !slices.Contains(sub.wantedTypes, e.RecType) {
				continue
			}
		}

		if len(sub.wantedDids) > 0 {
			if !slices.Contains(sub.wantedDids, e.Did) {
				continue
			}
		}

		msg := jsonData
		switch sub.format {
		case "json":
			if sub.compress {
				msg = compressedJSON
			}
		case "cbor":
			msg = cborData
			if sub.compress {
				msg = compressedCBOR
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
			return nil
		case sub.buf <- *msg:
			sub.seq++
			sub.deliveredCounter.Inc()
			sub.bytesCounter.Add(float64(len(*msg)))
		}
	}
	return nil
}

func (s *Server) AddSubscriber(ws *websocket.Conn, format string, compress bool, wantedTypes []string, wantedDids []string) *Subscriber {
	s.lk.Lock()
	defer s.lk.Unlock()

	sub := Subscriber{
		ws:               ws,
		buf:              make(chan []byte, 100),
		id:               s.nextSub,
		format:           format,
		compress:         compress,
		wantedTypes:      wantedTypes,
		wantedDids:       wantedDids,
		deliveredCounter: eventsDelivered.WithLabelValues(format, ws.RemoteAddr().String()),
		bytesCounter:     bytesDelivered.WithLabelValues(format, ws.RemoteAddr().String()),
	}

	s.Subscribers[s.nextSub] = &sub
	s.nextSub++

	subscribersConnected.WithLabelValues(format, ws.RemoteAddr().String()).Inc()

	slog.Info("adding subscriber",
		"remote_addr", ws.RemoteAddr().String(),
		"id", sub.id,
		"format", format,
		"compress", compress,
		"wantedTypes", wantedTypes,
		"wantedDids", wantedDids,
	)

	return &sub
}

func (s *Server) RemoveSubscriber(num int64) {
	s.lk.Lock()
	defer s.lk.Unlock()

	slog.Info("removing subscriber", "id", num)

	subscribersConnected.WithLabelValues(s.Subscribers[num].format, s.Subscribers[num].ws.RemoteAddr().String()).Dec()

	delete(s.Subscribers, num)
}

var validFormats = []string{"json", "cbor"}
var validRecordTypes = []string{"post", "like", "repost", "follow", "block", "list", "listItem", "feedGenerator", "handle", "profile"}

func (s *Server) HandleSubscribe(c echo.Context) error {
	ctx, cancel := context.WithCancel(c.Request().Context())
	defer cancel()

	ws, err := upgrader.Upgrade(c.Response(), c.Request(), nil)
	if err != nil {
		return err
	}
	defer ws.Close()

	format := "json"
	qFormat := c.QueryParam("format")
	if qFormat != "" {
		for _, f := range validFormats {
			if f == qFormat {
				format = f
				break
			}
		}
	}

	compress := c.QueryParam("compress") == "true"

	wantedTypes := []string{}
	qWantedTypes := c.Request().URL.Query()["wantedTypes"]
	if len(qWantedTypes) > 0 {
		for _, t := range qWantedTypes {
			for _, v := range validRecordTypes {
				if t == v {
					wantedTypes = append(wantedTypes, t)
					break
				}
			}
		}
	}

	wantedDids := []string{}
	qWantedDids := c.Request().URL.Query()["wantedDids"]
	if len(qWantedDids) > 0 {
		wantedDids = qWantedDids
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

	sub := s.AddSubscriber(ws, format, compress, wantedTypes, wantedDids)
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
