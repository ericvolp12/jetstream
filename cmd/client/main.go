package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"time"

	apibsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/ericvolp12/jetstream/pkg/client"
	"github.com/ericvolp12/jetstream/pkg/models"
)

const (
	serverAddr = "ws://localhost:6008/subscribe"
)

func main() {
	config := client.DefaultClientConfig()
	config.WebsocketURL = serverAddr

	c, err := client.NewClient(config)
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}

	cursor := time.Now().Add(-time.Hour).UnixMicro()

	c.Handler = &handler{}

	ctx := context.Background()

	if err := c.ConnectAndRead(ctx, &cursor); err != nil {
		log.Fatalf("failed to connect: %v", err)
	}

	slog.Info("shutdown")
}

type handler struct{}

func (h *handler) OnEvent(ctx context.Context, event *models.Event) error {
	// Unmarshal the record if there is one
	if event.Commit != nil && (event.Commit.OpType == models.CommitCreateRecord || event.Commit.OpType == models.CommitUpdateRecord) {
		switch event.Commit.Collection {
		case "app.bsky.feed.post":
			var post apibsky.FeedPost
			if err := json.Unmarshal(event.Commit.Record, &post); err != nil {
				return fmt.Errorf("failed to unmarshal post: %w", err)
			}
			fmt.Printf("(%s)| %s\n", event.Did, post.Text)
		}
	}

	return nil
}
