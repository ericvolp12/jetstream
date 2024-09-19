package client

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"

	"github.com/bluesky-social/jetstream/pkg/models"
	"github.com/goccy/go-json"
	"github.com/gorilla/websocket"
)

type ClientConfig struct {
	WebsocketURL      string
	WantedDids        []string
	WantedCollections []string
	ExtraHeaders      map[string]string
}

type Scheduler interface {
	AddWork(ctx context.Context, repo string, evt *models.Event) error
	Shutdown()
}

type Client struct {
	Scheduler Scheduler
	con       *websocket.Conn
	config    *ClientConfig
	logger    *slog.Logger
	shutdown  chan chan struct{}
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

func NewClient(config *ClientConfig, logger *slog.Logger, scheduler Scheduler) (*Client, error) {
	if config == nil {
		config = DefaultClientConfig()
	}

	logger = logger.With("component", "jetstream-client")
	return &Client{
		config:    config,
		shutdown:  make(chan chan struct{}),
		logger:    logger,
		Scheduler: scheduler,
	}, nil
}

func (c *Client) ConnectAndRead(ctx context.Context, cursor *int64) error {
	header := http.Header{}
	for k, v := range c.config.ExtraHeaders {
		header.Add(k, v)
	}

	fullURL := c.config.WebsocketURL
	params := []string{}
	if cursor != nil {
		params = append(params, fmt.Sprintf("cursor=%d", *cursor))
	}

	for _, did := range c.config.WantedDids {
		params = append(params, fmt.Sprintf("wantedDids=%s", did))
	}

	for _, collection := range c.config.WantedCollections {
		params = append(params, fmt.Sprintf("wantedCollections=%s", collection))
	}

	if len(params) > 0 {
		fullURL += "?" + params[0]
		for _, p := range params[1:] {
			fullURL += "&" + p
		}
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
	c.logger.Info("starting websocket read loop")

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
			var event models.Event
			if err := json.Unmarshal(msg, &event); err != nil {
				c.logger.Error("failed to unmarshal event", "error", err)
				return fmt.Errorf("failed to unmarshal event: %w", err)
			}

			if err := c.Scheduler.AddWork(ctx, event.Did, &event); err != nil {
				c.logger.Error("failed to add work to scheduler", "error", err)
				return fmt.Errorf("failed to add work to scheduler: %w", err)
			}
		}
	}
}
