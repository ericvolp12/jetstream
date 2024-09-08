package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/ericvolp12/jetstream/pkg/client"
	"github.com/ericvolp12/jetstream/pkg/client/schedulers/sequential"
	"github.com/ericvolp12/jetstream/pkg/models"
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

	h := &handler{
		seenSeqs: make(map[int64]struct{}),
	}

	scheduler := sequential.NewScheduler("jetstream_localdev", logger, h.HandleEvent)

	c, err := client.NewClient(config, logger, scheduler)
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}

	cursor := time.Now().Add(1 * -time.Hour).UnixMicro()

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
	fmt.Println("evt")

	// Unmarshal the record if there is one
	// if event.Commit != nil && (event.Commit.OpType == models.CommitCreateRecord || event.Commit.OpType == models.CommitUpdateRecord) {
	// 	switch event.Commit.Collection {
	// 	case "app.bsky.feed.post":
	// 		var post apibsky.FeedPost
	// 		if err := json.Unmarshal(event.Commit.Record, &post); err != nil {
	// 			return fmt.Errorf("failed to unmarshal post: %w", err)
	// 		}
	// 		// fmt.Printf("%v |(%s)| %s\n", time.UnixMicro(event.TimeUS).Local().Format("15:04:05"), event.Did, post.Text)
	// 	}
	// }

	return nil
}
