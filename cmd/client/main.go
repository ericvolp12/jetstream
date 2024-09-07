package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"time"

	"github.com/ericvolp12/jetstream/pkg/client"
	"github.com/ericvolp12/jetstream/pkg/consumer"
	"golang.org/x/time/rate"
)

const (
	serverAddr = "ws://localhost:6008/subscribe"
)

var limiter = rate.NewLimiter(rate.Limit(4), 1)

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

func (h *handler) OnEvent(ctx context.Context, event *consumer.Event) error {
	limiter.Wait(ctx)
	fmt.Printf("evt: did=%s, typ=%s, time_us=%d, commit=%#v, account=%#v, identity=%#v\n", event.Did, event.EventType, event.TimeUS, event.Commit, event.Account, event.Identity)
	return nil
}
