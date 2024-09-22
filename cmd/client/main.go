package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"os"
	"time"

	apibsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/jetstream/pkg/client"
	"github.com/bluesky-social/jetstream/pkg/client/schedulers/sequential"
	"github.com/bluesky-social/jetstream/pkg/models"
)

const (
	serverAddr = "ws://localhost:6008/subscribe"
)

func main() {
	ctx := context.Background()
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level:     slog.LevelInfo,
		AddSource: true,
	})))
	logger := slog.Default()

	config := client.DefaultClientConfig()
	config.WebsocketURL = serverAddr
	config.Compress = true

	h := &handler{
		seenSeqs: make(map[int64]struct{}),
	}

	scheduler := sequential.NewScheduler("jetstream_localdev", logger, h.HandleEvent)

	c, err := client.NewClient(config, logger, scheduler)
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}

	cursor := time.Now().Add(5 * -time.Hour).UnixMicro()

	// Every 5 seconds print the events read and bytes read and average event size
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		for {
			select {
			case <-ticker.C:
				eventsRead := c.EventsRead.Load()
				bytesRead := c.BytesRead.Load()
				avgEventSize := bytesRead / eventsRead
				logger.Info("stats", "events_read", eventsRead, "bytes_read", bytesRead, "avg_event_size", avgEventSize)
			}
		}
	}()

	if err := c.ConnectAndRead(ctx, &cursor); err != nil {
		log.Fatalf("failed to connect: %v", err)
	}

	slog.Info("shutdown")
}

type handler struct {
	seenSeqs  map[int64]struct{}
	highwater int64
}

func (h *handler) HandleEvent(ctx context.Context, event *models.Event) error {
	// fmt.Println("evt")

	// Unmarshal the record if there is one
	if event.Commit != nil && (event.Commit.OpType == models.CommitCreateRecord || event.Commit.OpType == models.CommitUpdateRecord) {
		switch event.Commit.Collection {
		case "app.bsky.feed.post":
			var post apibsky.FeedPost
			if err := json.Unmarshal(event.Commit.Record, &post); err != nil {
				return fmt.Errorf("failed to unmarshal post: %w", err)
			}
			// fmt.Printf("%v |(%s)| %s\n", time.UnixMicro(event.TimeUS).Local().Format("15:04:05"), event.Did, post.Text)
		}
	}

	return nil
}
