package consumer

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/data"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/repo"
	"github.com/bluesky-social/indigo/repomgr"
	"github.com/cockroachdb/pebble"
	"github.com/ericvolp12/jetstream/pkg/models"
	"github.com/goccy/go-json"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

// Consumer is the consumer of the firehose
type Consumer struct {
	SocketURL         string
	Progress          *Progress
	Emit              func(context.Context, models.Event) error
	DB                *pebble.DB
	EventTTL          time.Duration
	logger            *slog.Logger
	buf               chan *models.Event
	sequencerShutdown chan chan struct{}

	sequenced prometheus.Counter
	persisted prometheus.Counter
	emitted   prometheus.Counter
}

var tracer = otel.Tracer("consumer")

// NewConsumer creates a new consumer
func NewConsumer(
	ctx context.Context,
	logger *slog.Logger,
	socketURL string,
	dataDir string,
	eventTTL time.Duration,
	emit func(context.Context, models.Event) error,
) (*Consumer, error) {
	dbPath := dataDir + "/jetstream.db"
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("failed to open db: %w", err)
	}

	log := logger.With("component", "consumer")

	c := Consumer{
		SocketURL: socketURL,
		Progress: &Progress{
			LastSeq: -1,
		},
		EventTTL:          eventTTL,
		Emit:              emit,
		DB:                db,
		logger:            log,
		buf:               make(chan *models.Event, 10_000),
		sequencerShutdown: make(chan chan struct{}),

		sequenced: eventsSequencedCounter.WithLabelValues(socketURL),
		persisted: eventsPersistedCounter.WithLabelValues(socketURL),
		emitted:   eventsEmittedCounter.WithLabelValues(socketURL),
	}

	// Check to see if the cursor exists
	err = c.ReadCursor(ctx)
	if err != nil {
		log.Warn("previous cursor not found, starting from live", "error", err)
	}

	// Start the sequencer
	if err := c.RunSequencer(ctx); err != nil {
		return nil, fmt.Errorf("failed to start sequencer: %w", err)
	}

	return &c, nil
}

// HandleStreamEvent handles a stream event from the firehose
func (c *Consumer) HandleStreamEvent(ctx context.Context, xe *events.XRPCStreamEvent) error {
	ctx, span := tracer.Start(ctx, "HandleStreamEvent")
	defer span.End()
	switch {
	case xe.RepoCommit != nil:
		eventsProcessedCounter.WithLabelValues("commit", c.SocketURL).Inc()
		if xe.RepoCommit.TooBig {
			c.logger.Warn("repo commit too big", "repo", xe.RepoCommit.Repo, "seq", xe.RepoCommit.Seq, "rev", xe.RepoCommit.Rev)
			return nil
		}
		return c.HandleRepoCommit(ctx, xe.RepoCommit)
	case xe.RepoIdentity != nil:
		eventsProcessedCounter.WithLabelValues("identity", c.SocketURL).Inc()
		now := time.Now()
		c.Progress.Update(xe.RepoIdentity.Seq, now)
		// Parse time from the event time string
		t, err := time.Parse(time.RFC3339, xe.RepoIdentity.Time)
		if err != nil {
			c.logger.Error("error parsing time", "error", err)
			return nil
		}

		// Emit identity update
		e := models.Event{
			Did:       xe.RepoIdentity.Did,
			EventType: models.EventIdentity,
			Identity:  xe.RepoIdentity,
		}
		// Send to the sequencer
		c.buf <- &e
		lastEvtCreatedAtGauge.WithLabelValues(c.SocketURL).Set(float64(t.UnixNano()))
		lastEvtProcessedAtGauge.WithLabelValues(c.SocketURL).Set(float64(now.UnixNano()))
		lastEvtCreatedEvtProcessedGapGauge.WithLabelValues(c.SocketURL).Set(float64(now.Sub(t).Seconds()))
		lastSeqGauge.WithLabelValues(c.SocketURL).Set(float64(xe.RepoIdentity.Seq))
	case xe.RepoAccount != nil:
		eventsProcessedCounter.WithLabelValues("account", c.SocketURL).Inc()
		now := time.Now()
		c.Progress.Update(xe.RepoAccount.Seq, now)
		// Parse time from the event time string
		t, err := time.Parse(time.RFC3339, xe.RepoAccount.Time)
		if err != nil {
			c.logger.Error("error parsing time", "error", err)
			return nil
		}

		// Emit account update
		e := models.Event{
			Did:       xe.RepoAccount.Did,
			EventType: models.EventAccount,
			Account:   xe.RepoAccount,
		}
		// Send to the sequencer
		c.buf <- &e
		lastEvtCreatedAtGauge.WithLabelValues(c.SocketURL).Set(float64(t.UnixNano()))
		lastEvtProcessedAtGauge.WithLabelValues(c.SocketURL).Set(float64(now.UnixNano()))
		lastEvtCreatedEvtProcessedGapGauge.WithLabelValues(c.SocketURL).Set(float64(now.Sub(t).Seconds()))
		lastSeqGauge.WithLabelValues(c.SocketURL).Set(float64(xe.RepoAccount.Seq))
	case xe.Error != nil:
		eventsProcessedCounter.WithLabelValues("error", c.SocketURL).Inc()
		return fmt.Errorf("error from firehose: %s", xe.Error.Message)
	}
	return nil
}

// HandleRepoCommit handles a repo commit event from the firehose and processes the records
func (c *Consumer) HandleRepoCommit(ctx context.Context, evt *comatproto.SyncSubscribeRepos_Commit) error {
	ctx, span := tracer.Start(ctx, "HandleRepoCommit")
	defer span.End()

	processedAt := time.Now()

	c.Progress.Update(evt.Seq, processedAt)

	lastSeqGauge.WithLabelValues(c.SocketURL).Set(float64(evt.Seq))

	log := c.logger.With("repo", evt.Repo, "seq", evt.Seq, "commit", evt.Commit.String())

	span.AddEvent("Read Repo From Car")
	rr, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(evt.Blocks))
	if err != nil {
		log.Error("failed to read repo from car", "error", err)
		return nil
	}

	// Parse time from the event time string
	evtCreatedAt, err := time.Parse(time.RFC3339, evt.Time)
	if err != nil {
		log.Error("error parsing time", "error", err)
		return nil
	}

	lastEvtCreatedAtGauge.WithLabelValues(c.SocketURL).Set(float64(evtCreatedAt.UnixNano()))
	lastEvtProcessedAtGauge.WithLabelValues(c.SocketURL).Set(float64(processedAt.UnixNano()))
	lastEvtCreatedEvtProcessedGapGauge.WithLabelValues(c.SocketURL).Set(float64(processedAt.Sub(evtCreatedAt).Seconds()))

	for _, op := range evt.Ops {
		collection := strings.Split(op.Path, "/")[0]
		rkey := strings.Split(op.Path, "/")[1]

		ek := repomgr.EventKind(op.Action)
		log = log.With("action", op.Action, "collection", collection)

		opsProcessedCounter.WithLabelValues(op.Action, collection, c.SocketURL).Inc()

		// recordURI := "at://" + evt.Repo + "/" + op.Path
		span.SetAttributes(attribute.String("repo", evt.Repo))
		span.SetAttributes(attribute.String("collection", collection))
		span.SetAttributes(attribute.String("rkey", rkey))
		span.SetAttributes(attribute.Int64("seq", evt.Seq))
		span.SetAttributes(attribute.String("event_kind", op.Action))

		e := models.Event{
			Did:       evt.Repo,
			EventType: models.EventCommit,
		}

		switch ek {
		case repomgr.EvtKindCreateRecord:
			if op.Cid == nil {
				log.Error("update record op missing cid")
				break
			}

			rcid, recB, err := rr.GetRecordBytes(ctx, op.Path)
			if err != nil {
				log.Error("failed to get record bytes", "error", err)
				break
			}

			recCid := rcid.String()
			if recCid != op.Cid.String() {
				log.Error("record cid mismatch", "expected", *op.Cid, "actual", rcid)
				break
			}

			rec, err := data.UnmarshalCBOR(*recB)
			if err != nil {
				return fmt.Errorf("failed to unmarshal record: %w", err)
			}

			recJSON, err := json.Marshal(rec)
			if err != nil {
				log.Error("failed to marshal record to json", "error", err)
				break
			}

			e.Commit = &models.Commit{
				Rev:        evt.Rev,
				OpType:     models.CommitCreateRecord,
				Collection: collection,
				RKey:       rkey,
				Record:     recJSON,
				CID:        recCid,
			}
		case repomgr.EvtKindUpdateRecord:
			if op.Cid == nil {
				log.Error("update record op missing cid")
				break
			}

			rcid, recB, err := rr.GetRecordBytes(ctx, op.Path)
			if err != nil {
				log.Error("failed to get record bytes", "error", err)
				break
			}

			recCid := rcid.String()
			if recCid != op.Cid.String() {
				log.Error("record cid mismatch", "expected", *op.Cid, "actual", rcid)
				break
			}

			rec, err := data.UnmarshalCBOR(*recB)
			if err != nil {
				return fmt.Errorf("failed to unmarshal record: %w", err)
			}

			recJSON, err := json.Marshal(rec)
			if err != nil {
				log.Error("failed to marshal record to json", "error", err)
				break
			}

			e.Commit = &models.Commit{
				Rev:        evt.Rev,
				OpType:     models.CommitUpdateRecord,
				Collection: collection,
				RKey:       rkey,
				Record:     recJSON,
				CID:        recCid,
			}
		case repomgr.EvtKindDeleteRecord:
			// Emit the delete
			e.Commit = &models.Commit{
				Rev:        evt.Rev,
				OpType:     models.CommitDeleteRecord,
				Collection: collection,
				RKey:       rkey,
			}
		default:
			log.Warn("unknown event kind from op action", "kind", op.Action)
			continue
		}

		// Send to the sequencer
		c.buf <- &e
	}

	eventProcessingDurationHistogram.WithLabelValues(c.SocketURL).Observe(time.Since(processedAt).Seconds())
	return nil
}

func (c *Consumer) RunSequencer(ctx context.Context) error {
	log := c.logger.With("component", "sequencer")

	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Info("shutting down sequencer on context completion")
				return
			case s := <-c.sequencerShutdown:
				log.Info("shutting down sequencer on shutdown signal")
				s <- struct{}{}
				return
			case e := <-c.buf:
				// Assign a time_us to the event
				e.TimeUS = time.Now().UnixMicro()
				c.sequenced.Inc()
				if err := c.PersistEvent(ctx, e); err != nil {
					log.Error("failed to persist event", "error", err)
					return
				}
				c.persisted.Inc()
				if err := c.Emit(ctx, *e); err != nil {
					log.Error("failed to emit event", "error", err)
				}
				c.emitted.Inc()
			}
		}
	}()

	return nil
}

func (c *Consumer) Shutdown() {
	shutdown := make(chan struct{})
	c.sequencerShutdown <- shutdown
	<-shutdown
}
