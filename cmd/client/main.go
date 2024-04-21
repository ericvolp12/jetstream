package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"

	"github.com/ericvolp12/jetstream/pkg/client"
	"github.com/ericvolp12/jetstream/pkg/consumer"
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

	c.Handler = &handler{}

	ctx := context.Background()

	if err := c.ConnectAndRead(ctx); err != nil {
		log.Fatalf("failed to connect: %v", err)
	}

	slog.Info("shutdown")
}

type handler struct{}

func (h *handler) OnEvent(ctx context.Context, event *consumer.Event) error {
	fmt.Printf("received event: %+v\n", event)
	return nil
}
