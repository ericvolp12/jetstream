package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/schedulers/autoscaling"
	"github.com/ericvolp12/bsky-experiments/pkg/tracing"
	"github.com/ericvolp12/jetstream/pkg/consumer"
	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"

	"github.com/urfave/cli/v2"
)

func main() {
	app := cli.App{
		Name:    "jetstream",
		Usage:   "atproto firehose translation service",
		Version: "0.1.0",
	}

	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:    "ws-url",
			Usage:   "full websocket path to the ATProto SubscribeRepos XRPC endpoint",
			Value:   "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos",
			EnvVars: []string{"WS_URL"},
		},
		&cli.IntFlag{
			Name:    "worker-count",
			Usage:   "number of workers to process events",
			Value:   10,
			EnvVars: []string{"WORKER_COUNT"},
		},
		&cli.IntFlag{
			Name:    "max-queue-size",
			Usage:   "max number of events to queue",
			Value:   10,
			EnvVars: []string{"MAX_QUEUE_SIZE"},
		},
		&cli.StringFlag{
			Name:    "listen-addr",
			Usage:   "addr to serve echo on",
			Value:   ":6008",
			EnvVars: []string{"LISTEN_ADDR"},
		},
		&cli.StringFlag{
			Name:    "cursor-file",
			Usage:   "path to the cursor file",
			Value:   "./cursor.json",
			EnvVars: []string{"CURSOR_FILE"},
		},
		&cli.StringSliceFlag{
			Name:     "kafka-brokers",
			Usage:    "comma separated list of kafka brokers to connect to",
			EnvVars:  []string{"KAFKA_BROKERS"},
			Required: false,
		},
		&cli.StringFlag{
			Name:     "kafka-topic",
			Usage:    "kafka topic to write events to",
			Value:    "jetstream-events",
			EnvVars:  []string{"KAFKA_TOPIC"},
			Required: false,
		},
	}

	app.Action = Jetstream

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

var tracer = otel.Tracer("Jetstream")

// Jetstream is the main function for jetstream
func Jetstream(cctx *cli.Context) error {
	ctx := cctx.Context

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(log)

	log.Info("starting jetstream")

	u, err := url.Parse(cctx.String("ws-url"))
	if err != nil {
		return fmt.Errorf("failed to parse ws-url: %w", err)
	}

	// Registers a tracer Provider globally if the exporter endpoint is set
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
		log.Info("initializing tracer...")
		shutdown, err := tracing.InstallExportPipeline(ctx, "Jetstream", 0.01)
		if err != nil {
			return fmt.Errorf("failed to initialize tracer: %w", err)
		}
		defer func() {
			if err := shutdown(ctx); err != nil {
				log.Error("failed to shutdown tracer", "error", err)
			}
		}()
	}

	s, err := NewServer(cctx.StringSlice("kafka-brokers"), cctx.String("kafka-topic"))
	if err != nil {
		return fmt.Errorf("failed to create server: %w", err)
	}

	c, err := consumer.NewConsumer(
		ctx,
		u.String(),
		cctx.String("cursor-file"),
		s.Emit,
	)
	if err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}

	schedSettings := autoscaling.DefaultAutoscaleSettings()
	scheduler := autoscaling.NewScheduler(schedSettings, "prod-firehose", c.HandleStreamEvent)

	// Start a goroutine to manage the cursor, saving the current cursor every 5 seconds.
	shutdownCursorManager := make(chan struct{})
	cursorManagerShutdown := make(chan struct{})
	go func() {
		ctx := context.Background()
		ticker := time.NewTicker(5 * time.Second)
		backupTicker := time.NewTicker(5 * time.Minute)
		log := log.With("source", "cursor_manager")

		for {
			select {
			case <-shutdownCursorManager:
				log.Info("shutting down cursor manager")
				err := c.WriteCursor(ctx)
				if err != nil {
					log.Error("failed to write cursor", "error", err)
				}
				log.Info("cursor manager shut down successfully")
				close(cursorManagerShutdown)
				return
			case <-ticker.C:
				err := c.WriteCursor(ctx)
				if err != nil {
					log.Error("failed to write cursor", "error", err)
				}
			case <-backupTicker.C:
				backupFileName := getBackupFileName(c.Progress.GetPath())
				err := c.WriteCursorToFile(ctx, backupFileName)
				if err != nil {
					log.Error("failed to create cursor backup", "error", err)
				} else {
					checkAndDeleteOldBackups(c.Progress.GetPath(), *log)
				}
			}
		}
	}()

	// Create a channel that will be closed when we want to stop the application
	// Usually when a critical routine returns an error
	livenessKill := make(chan struct{})

	// Start a goroutine to manage the liveness checker, shutting down if no events are received for 15 seconds
	shutdownLivenessChecker := make(chan struct{})
	livenessCheckerShutdown := make(chan struct{})
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		lastSeq := int64(0)
		log := log.With("source", "liveness_checker")

		for {
			select {
			case <-shutdownLivenessChecker:
				log.Info("shutting down liveness checker")
				close(livenessCheckerShutdown)
				return
			case <-ticker.C:
				seq, _ := c.Progress.Get()
				if seq == lastSeq && seq != 0 {
					log.Error("no new events in last 15 seconds, shutting down for docker to restart me", "seq", seq)
					close(livenessKill)
				} else {
					log.Info("successful liveness check", "seq", seq)
					lastSeq = seq
				}
			}
		}
	}()

	e := echo.New()
	e.GET("/", func(c echo.Context) error {
		return c.String(http.StatusOK, "Hello, World!")
	})
	e.GET("/metrics", echo.WrapHandler(promhttp.Handler()))
	e.GET("/subscribe", s.HandleSubscribe)

	httpServer := &http.Server{
		Addr:    cctx.String("listen-addr"),
		Handler: e,
	}

	// Startup echo server
	shutdownEcho := make(chan struct{})
	echoShutdown := make(chan struct{})
	go func() {
		logger := log.With("source", "echo_server")

		logger.Info("echo server listening", "addr", cctx.String("listen-addr"))

		go func() {
			if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
				logger.Error("failed to start echo server", "error", err)
			}
		}()
		<-shutdownEcho
		if err := httpServer.Shutdown(ctx); err != nil {
			logger.Error("failed to shutdown echo server", "error", err)
		}
		logger.Info("echo server shut down")
		close(echoShutdown)
	}()

	if c.Progress.LastSeq >= 0 {
		u.RawQuery = fmt.Sprintf("cursor=%d", c.Progress.LastSeq)
	}

	log.Info(fmt.Sprintf("connecting to WebSocket at: %s", u.String()))
	con, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Error("failed to connect to websocket", "error", err)
		return err
	}
	defer con.Close()

	// Create a channel that will be closed when we want to stop the application
	// Usually when a critical routine returns an error
	eventsKill := make(chan struct{})

	shutdownRepoStream := make(chan struct{})
	repoStreamShutdown := make(chan struct{})
	go func() {
		ctx := context.Background()
		ctx, cancel := context.WithCancel(ctx)
		go func() {
			err = events.HandleRepoStream(ctx, con, scheduler)
			if !errors.Is(err, context.Canceled) {
				log.Info("HandleRepoStream returned unexpectedly, killing jetstream", "error", err)
				close(eventsKill)
			} else {
				log.Info("HandleRepoStream closed on context cancel")
			}
			close(repoStreamShutdown)
		}()
		<-shutdownRepoStream
		cancel()
	}()

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-signals:
		log.Info("shutting down on signal")
	case <-ctx.Done():
		log.Info("shutting down on context done")
	case <-livenessKill:
		log.Info("shutting down on liveness kill")
	case <-eventsKill:
		log.Info("shutting down on events kill")
	}

	log.Info("shutting down, waiting for workers to clean up...")
	close(shutdownRepoStream)
	close(shutdownLivenessChecker)
	close(shutdownCursorManager)
	close(shutdownEcho)

	<-repoStreamShutdown
	<-livenessCheckerShutdown
	<-cursorManagerShutdown
	<-echoShutdown
	log.Info("shut down successfully")

	return nil
}

func getBackupFileName(cursorFilePath string) string {
	timestamp := time.Now().Format("20060102-150405")
	dir, _ := filepath.Split(cursorFilePath)
	return filepath.Join(dir, fmt.Sprintf("%s-cursor-backup.json", timestamp))
}

func checkAndDeleteOldBackups(cursorFilePath string, log slog.Logger) {
	log.Info("Created cursor backup. Checking old backups.")

	dir := filepath.Dir(cursorFilePath)

	dirEntries, err := os.ReadDir(dir)
	if err != nil {
		log.Error("failed to read directory", "error", err)
		return
	}

	var backups []fs.DirEntry
	for _, v := range dirEntries {
		if v.IsDir() {
			continue
		}
		if strings.Contains(v.Name(), "-cursor-backup.json") {
			backups = append(backups, v)
		}
	}

	sort.Slice(backups, func(i, j int) bool {
		infoI, errI := backups[i].Info()
		infoJ, errJ := backups[j].Info()
		if errI != nil {
			log.Error("failed to get file info", "error", errI, "file", backups[i].Name())
			return false
		}
		if errJ != nil {
			log.Error("failed to get file info", "error", errJ, "file", backups[j].Name())
			return false
		}
		return infoI.ModTime().Before(infoJ.ModTime())
	})

	for len(backups) > 10 {
		err = os.Remove(filepath.Join(dir, backups[0].Name()))
		if err != nil {
			log.Error("failed to remove old backup", "error", err, "file", backups[0].Name())
			return
		}
		backups = backups[1:]
	}
}
