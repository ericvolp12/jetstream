package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"time"

	apibsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/ericvolp12/jetstream/pkg/client"
	"github.com/ericvolp12/jetstream/pkg/client/schedulers/sequential"
	"github.com/ericvolp12/jetstream/pkg/models"
	"github.com/goccy/go-json"
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
	config.WantedCollections = []string{"app.bsky.feed.post"}

	h := &handler{
		seenSeqs: make(map[int64]struct{}),
	}

	scheduler := sequential.NewScheduler("jetstream_localdev", logger, h.HandleEvent)

	c, err := client.NewClient(config, logger, scheduler)
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}

	cursor := time.Now().Add(15 * -time.Second).UnixMicro()

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
	_, ok := h.seenSeqs[event.TimeUS]
	if ok {
		log.Fatalf("dupe seq %d", event.TimeUS)
	}
	h.seenSeqs[event.TimeUS] = struct{}{}

	if event.TimeUS > h.highwater {
		h.highwater = event.TimeUS
	} else {
		log.Fatalf("out of order event: %d", event.TimeUS)
	}

	// Unmarshal the record if there is one
	if event.Commit != nil && (event.Commit.OpType == models.CommitCreateRecord || event.Commit.OpType == models.CommitUpdateRecord) {
		switch event.Commit.Collection {
		case "app.bsky.feed.post":
			var post apibsky.FeedPost
			if err := json.Unmarshal(event.Commit.Record, &post); err != nil {
				return fmt.Errorf("failed to unmarshal post: %w", err)
			}
			fmt.Printf("%v |(%s)| %s\n", time.UnixMicro(event.TimeUS).Local().Format("15:04:05"), event.Did, post.Text)
		}
	}

	return nil
}
