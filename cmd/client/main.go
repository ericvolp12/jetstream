package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"time"

	apibsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/ericvolp12/jetstream/pkg/client"
	"github.com/ericvolp12/jetstream/pkg/models"
	"github.com/goccy/go-json"
)

const (
	serverAddr = "ws://localhost:6008/subscribe"
)

func main() {
	config := client.DefaultClientConfig()
	config.WebsocketURL = serverAddr
	config.WantedCollections = []string{"app.bsky.feed.post"}

	c, err := client.NewClient(config)
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}

	cursor := time.Now().Add(15 * -time.Second).UnixMicro()

	c.Handler = &handler{
		seenSeqs: make(map[int64]struct{}),
	}

	ctx := context.Background()

	if err := c.ConnectAndRead(ctx, &cursor); err != nil {
		log.Fatalf("failed to connect: %v", err)
	}

	slog.Info("shutdown")
}

type handler struct {
	seenSeqs  map[int64]struct{}
	highwater int64
}

func (h *handler) OnEvent(ctx context.Context, event *models.Event) error {
	_, ok := h.seenSeqs[event.TimeUS]
	if ok {
		log.Fatalf("dupe seq %d", event.TimeUS)
	}
	h.seenSeqs[event.TimeUS] = struct{}{}

	// If there's a gap of more than 50ms, log it
	if event.TimeUS > h.highwater+50_000 {
		log.Printf("gap of %dus", event.TimeUS-h.highwater)
	}

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
			// fmt.Printf("%v |(%s)| %s\n", time.UnixMicro(event.TimeUS).Local().Format("15:04:05"), event.Did, post.Text)
		}
	}

	return nil
}
