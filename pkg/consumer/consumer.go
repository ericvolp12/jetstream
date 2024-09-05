package consumer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/data"
	"github.com/goccy/go-json"

	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/repo"
	"github.com/bluesky-social/indigo/repomgr"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

// Consumer is the consumer of the firehose
type Consumer struct {
	SocketURL string
	Progress  *Progress
	Emit      func(context.Context, Event) error
}

// Progress is the cursor for the consumer
type Progress struct {
	LastSeq            int64     `json:"last_seq"`
	LastSeqProcessedAt time.Time `json:"last_seq_processed_at"`
	lk                 sync.RWMutex
	path               string
}

func (p *Progress) Update(seq int64, processedAt time.Time) {
	p.lk.Lock()
	defer p.lk.Unlock()
	p.LastSeq = seq
	p.LastSeqProcessedAt = processedAt
}

func (p *Progress) Get() (int64, time.Time) {
	p.lk.RLock()
	defer p.lk.RUnlock()
	return p.LastSeq, p.LastSeqProcessedAt
}

var tracer = otel.Tracer("consumer")

// WriteCursor writes the cursor to file
func (c *Consumer) WriteCursor(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "WriteCursor")
	defer span.End()

	// Marshal the cursor JSON
	seq, processedAt := c.Progress.Get()
	p := Progress{
		LastSeq:            seq,
		LastSeqProcessedAt: processedAt,
	}
	data, err := json.Marshal(&p)
	if err != nil {
		return fmt.Errorf("failed to marshal cursor JSON: %+v", err)
	}

	// Write the cursor JSON to disk
	err = os.WriteFile(c.Progress.path, data, 0644)
	if err != nil {
		return fmt.Errorf("failed to write cursor to file: %+v", err)
	}

	return nil
}

// ReadCursor reads the cursor from file
func (c *Consumer) ReadCursor(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "ReadCursor")
	defer span.End()

	// Read the cursor from disk
	data, err := os.ReadFile(c.Progress.path)
	if err != nil {
		return fmt.Errorf("failed to read cursor from file: %w", err)
	}

	// Unmarshal the cursor JSON
	err = json.Unmarshal(data, c.Progress)
	if err != nil {
		return fmt.Errorf("failed to unmarshal cursor JSON: %w", err)
	}

	return nil
}

// NewConsumer creates a new consumer
func NewConsumer(
	ctx context.Context,
	socketURL string,
	progPath string,
	emit func(context.Context, Event) error,
) (*Consumer, error) {
	c := Consumer{
		SocketURL: socketURL,
		Progress: &Progress{
			LastSeq: -1,
			path:    progPath,
		},
		Emit: emit,
	}

	// Check to see if the cursor exists
	err := c.ReadCursor(context.Background())
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("failed to read cursor from file: %+v", err)
		}
		slog.Warn("cursor not found on disk, starting from live")
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
			slog.Warn("repo commit too big", "repo", xe.RepoCommit.Repo, "seq", xe.RepoCommit.Seq, "rev", xe.RepoCommit.Rev)
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
			slog.Error("error parsing time", "error", err)
			return nil
		}

		// Emit identity update
		e := Event{
			Did:       xe.RepoIdentity.Did,
			TimeUS:    now.UnixMicro(),
			EventType: EventIdentity,
			Identity:  xe.RepoIdentity,
		}
		err = c.Emit(ctx, e)
		if err != nil {
			slog.Error("failed to emit json", "error", err)
		}
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
			slog.Error("error parsing time", "error", err)
			return nil
		}

		// Emit account update
		e := Event{
			Did:       xe.RepoAccount.Did,
			TimeUS:    now.UnixMicro(),
			EventType: EventAccount,
			Account:   xe.RepoAccount,
		}
		err = c.Emit(ctx, e)
		if err != nil {
			slog.Error("failed to emit json", "error", err)
		}
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

	log := slog.With("repo", evt.Repo, "seq", evt.Seq, "commit", evt.Commit.String())

	span.AddEvent("Read Repo From Car")
	rr, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(evt.Blocks))
	if err != nil {
		log.Error("failed to read repo from car", "error", err)
		return nil
	}

	if evt.Rebase {
		log.Debug("rebase")
		rebasesProcessedCounter.WithLabelValues(c.SocketURL).Inc()
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

		e := Event{
			Did:       evt.Repo,
			TimeUS:    time.Now().UnixMicro(),
			EventType: EventCommit,
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

			if rcid.String() != op.Cid.String() {
				log.Error("record cid mismatch", "expected", *op.Cid, "actual", rcid)
				break
			}

			rec, err := data.UnmarshalCBOR(*recB)
			if err != nil {
				return fmt.Errorf("failed to unmarshal record: %w", err)
			}

			e.Commit = &Commit{
				Rev:        evt.Rev,
				OpType:     CommitCreateRecord,
				Collection: collection,
				RKey:       rkey,
				Record:     rec,
			}

			err = c.Emit(ctx, e)
			if err != nil {
				log.Error("failed to emit event", "error", err)
				break
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

			if rcid.String() != op.Cid.String() {
				log.Error("record cid mismatch", "expected", *op.Cid, "actual", rcid)
				break
			}

			rec, err := data.UnmarshalCBOR(*recB)
			if err != nil {
				return fmt.Errorf("failed to unmarshal record: %w", err)
			}

			e.Commit = &Commit{
				Rev:        evt.Rev,
				OpType:     CommitUpdateRecord,
				Collection: collection,
				RKey:       rkey,
				Record:     rec,
			}

			err = c.Emit(ctx, e)
			if err != nil {
				log.Error("failed to emit event", "error", err)
				break
			}
		case repomgr.EvtKindDeleteRecord:
			// Emit the delete
			e.Commit = &Commit{
				Rev:        evt.Rev,
				OpType:     CommitDeleteRecord,
				Collection: collection,
				RKey:       rkey,
			}

			err = c.Emit(ctx, e)
			if err != nil {
				log.Error("failed to emit event", "error", err)
				break
			}
		default:
			log.Warn("unknown event kind from op action", "kind", op.Action)
		}
	}

	eventProcessingDurationHistogram.WithLabelValues(c.SocketURL).Observe(time.Since(processedAt).Seconds())
	return nil
}
