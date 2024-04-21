package client

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"

	"github.com/ericvolp12/jetstream/pkg/consumer"
	"github.com/gorilla/websocket"
)

const (
	FormatJSON = "json"
	FormatCBOR = "cbor"
)

type ClientConfig struct {
	WebsocketURL      string
	WantedDids        []string
	WantedCollections []string
	ExtraHeaders      map[string]string
}

type Handler interface {
	OnEvent(ctx context.Context, event *consumer.Event) error
}

type Client struct {
	con     *websocket.Conn
	config  *ClientConfig
	Handler Handler

	logger *slog.Logger

	shutdown chan chan struct{}
}

func DefaultClientConfig() *ClientConfig {
	return &ClientConfig{
		WebsocketURL:      "ws://localhost:6008/subscribe",
		WantedDids:        []string{},
		WantedCollections: []string{},
		ExtraHeaders: map[string]string{
			"User-Agent": "jetstream-client/v0.0.1",
		},
	}
}

func NewClient(config *ClientConfig) (*Client, error) {
	if config == nil {
		config = DefaultClientConfig()
	}

	logger := slog.Default().With("component", "jetstream-client")

	return &Client{
		config:   config,
		shutdown: make(chan chan struct{}),
		logger:   logger,
	}, nil
}

func (c *Client) ConnectAndRead(ctx context.Context) error {
	header := http.Header{}
	for k, v := range c.config.ExtraHeaders {
		header.Add(k, v)
	}

	fullURL := fmt.Sprintf("%s?format=%s&compress=false", c.config.WebsocketURL, FormatJSON)
	for _, did := range c.config.WantedDids {
		fullURL += fmt.Sprintf("&wantedDids=%s", did)
	}

	for _, collection := range c.config.WantedCollections {
		fullURL += fmt.Sprintf("&wantedCollections=%s", collection)
	}

	u, err := url.Parse(fullURL)
	if err != nil {
		return fmt.Errorf("failed to parse connection url %q: %w", c.config.WebsocketURL, err)
	}

	c.logger.Info("connecting to websocket", "url", u.String())
	con, _, err := websocket.DefaultDialer.DialContext(ctx, u.String(), header)
	if err != nil {
		return err
	}

	c.con = con

	if err := c.readLoop(ctx); err != nil {
		c.logger.Error("read loop failed", "error", err)
	} else {
		c.con.Close()
	}

	return nil
}

func (c *Client) readLoop(ctx context.Context) error {
	c.logger.Info("starting read loop")

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("shutting down read loop on context completion")
			return nil
		case s := <-c.shutdown:
			c.logger.Info("shutting down read loop on shutdown signal")
			s <- struct{}{}
			return nil
		default:
			_, msg, err := c.con.ReadMessage()
			if err != nil {
				c.logger.Error("failed to read message from websocket", "error", err)
				return fmt.Errorf("failed to read message from websocket: %w", err)
			}

			// Unpack the message and pass it to the handler
			var event consumer.Event
			if err := json.Unmarshal(msg, &event); err != nil {
				c.logger.Error("failed to unmarshal event", "error", err)
				return fmt.Errorf("failed to unmarshal event: %w", err)
			}

			if err := c.Handler.OnEvent(ctx, &event); err != nil {
				c.logger.Error("failed to handle event", "error", err)
			}
		}
	}
}
