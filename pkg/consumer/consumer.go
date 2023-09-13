package consumer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/dgraph-io/badger/v4"
	"github.com/goccy/go-json"
	"github.com/ipfs/go-cid"
	"github.com/labstack/gommon/log"
	"golang.org/x/exp/slog"

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
	DB        *badger.DB
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
	db *badger.DB,
	emit func(context.Context, Event) error,
) (*Consumer, error) {
	c := Consumer{
		SocketURL: socketURL,
		Progress: &Progress{
			LastSeq: -1,
			path:    progPath,
		},
		Emit: emit,
		DB:   db,
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
		eventsProcessedCounter.WithLabelValues("repo_commit", c.SocketURL).Inc()
		return c.HandleRepoCommit(ctx, xe.RepoCommit)
	case xe.RepoHandle != nil:
		eventsProcessedCounter.WithLabelValues("repo_handle", c.SocketURL).Inc()
		now := time.Now()
		c.Progress.Update(xe.RepoHandle.Seq, now)
		// Parse time from the event time string
		t, err := time.Parse(time.RFC3339, xe.RepoHandle.Time)
		if err != nil {
			log.Errorf("error parsing time: %+v", err)
			return nil
		}

		// Emit handle update
		e := Event{
			Did:    xe.RepoHandle.Did,
			Seq:    xe.RepoHandle.Seq,
			OpType: EvtUpdateRecord,
			Handle: xe.RepoHandle.Handle,
		}
		err = c.Emit(ctx, e)
		if err != nil {
			log.Error("failed to emit json", "error", err)
		}
		lastEvtCreatedAtGauge.WithLabelValues(c.SocketURL).Set(float64(t.UnixNano()))
		lastEvtProcessedAtGauge.WithLabelValues(c.SocketURL).Set(float64(now.UnixNano()))
		lastEvtCreatedEvtProcessedGapGauge.WithLabelValues(c.SocketURL).Set(float64(now.Sub(t).Seconds()))
		lastSeqGauge.WithLabelValues(c.SocketURL).Set(float64(xe.RepoHandle.Seq))
	case xe.RepoInfo != nil:
		eventsProcessedCounter.WithLabelValues("repo_info", c.SocketURL).Inc()
	case xe.RepoMigrate != nil:
		eventsProcessedCounter.WithLabelValues("repo_migrate", c.SocketURL).Inc()
		now := time.Now()
		c.Progress.Update(xe.RepoHandle.Seq, now)
		// Parse time from the event time string
		t, err := time.Parse(time.RFC3339, xe.RepoMigrate.Time)
		if err != nil {
			log.Errorf("error parsing time: %+v", err)
			return nil
		}
		lastEvtCreatedAtGauge.WithLabelValues(c.SocketURL).Set(float64(t.UnixNano()))
		lastEvtProcessedAtGauge.WithLabelValues(c.SocketURL).Set(float64(now.UnixNano()))
		lastEvtCreatedEvtProcessedGapGauge.WithLabelValues(c.SocketURL).Set(float64(now.Sub(t).Seconds()))
		lastSeqGauge.WithLabelValues(c.SocketURL).Set(float64(xe.RepoHandle.Seq))
	case xe.RepoTombstone != nil:
		eventsProcessedCounter.WithLabelValues("repo_tombstone", c.SocketURL).Inc()
	case xe.LabelInfo != nil:
		eventsProcessedCounter.WithLabelValues("label_info", c.SocketURL).Inc()
	case xe.LabelLabels != nil:
		eventsProcessedCounter.WithLabelValues("label_labels", c.SocketURL).Inc()
	case xe.Error != nil:
		eventsProcessedCounter.WithLabelValues("error", c.SocketURL).Inc()
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

	log := slog.With("repo", evt.Repo, "seq", evt.Seq, "commit", evt.Commit)

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
		switch ek {
		case repomgr.EvtKindCreateRecord:
			if op.Cid == nil {
				log.Error("update record op missing cid")
				break
			}
			// Grab the record from the merkel tree
			blk, err := rr.Blockstore().Get(ctx, cid.Cid(*op.Cid))
			if err != nil {
				e := fmt.Errorf("getting block %s within seq %d for %s: %w", *op.Cid, evt.Seq, evt.Repo, err)
				log.Error("failed to get a block from the event", "error", e)
				break
			}

			rec, err := lexutil.CborDecodeValue(blk.RawData())
			if err != nil {
				log.Error("failed to decode cbor", "error", err)
				break
			}
			// Emit the create
			e := Event{
				Did:    evt.Repo,
				Seq:    evt.Seq,
				OpType: EvtCreateRecord,
			}
			switch rec := rec.(type) {
			case *bsky.ActorProfile:
				e.Profile = rec
				e.RecType = "profile"
			case *bsky.FeedPost:
				// Filter out empty embeds which get upset when marshalled to json
				if rec.Embed != nil &&
					rec.Embed.EmbedExternal == nil &&
					rec.Embed.EmbedImages == nil &&
					rec.Embed.EmbedRecord == nil {
					rec.Embed = nil
				}
				if rec.Facets != nil {
					facets := []*bsky.RichtextFacet{}
					for i, f := range rec.Facets {
						if f.Features != nil {
							facets = append(facets, rec.Facets[i])
						}
					}
					rec.Facets = facets
				}
				e.Post = rec
				e.RecType = "post"
			case *bsky.FeedLike:
				e.Like = rec
				e.RecType = "like"
				// Track likes in db
				nsid := strings.Split(op.Path, "/")[0]
				key := fmt.Sprintf("%s_%s_%s", evt.Repo, nsid, rkey)
				err = c.DB.Update(func(txn *badger.Txn) error {
					err := txn.Set([]byte(key), []byte(rec.Subject.Uri))
					if err != nil {
						return fmt.Errorf("badger txn.set: %w", err)
					}
					return nil
				})
				if err != nil {
					log.Error("failed to track like in badger", "error", err)
					break
				}
			case *bsky.FeedRepost:
				e.Repost = rec
				e.RecType = "repost"
				// Track reposts in db
				nsid := strings.Split(op.Path, "/")[0]
				key := fmt.Sprintf("%s_%s_%s", evt.Repo, nsid, rkey)
				err = c.DB.Update(func(txn *badger.Txn) error {
					err := txn.Set([]byte(key), []byte(rec.Subject.Uri))
					if err != nil {
						return fmt.Errorf("badger txn.set: %w", err)
					}
					return nil
				})
				if err != nil {
					log.Error("failed to track repost in badger", "error", err)
					break
				}
			case *bsky.GraphFollow:
				e.Follow = rec
				e.RecType = "follow"
				// Track follows in sqlite
				nsid := strings.Split(op.Path, "/")[0]
				key := fmt.Sprintf("%s_%s_%s", evt.Repo, nsid, rkey)
				err = c.DB.Update(func(txn *badger.Txn) error {
					err := txn.Set([]byte(key), []byte(rec.Subject))
					if err != nil {
						return fmt.Errorf("badger txn.set: %w", err)
					}
					return nil
				})
				if err != nil {
					log.Error("failed to track follow in badger", "error", err)
					break
				}
			case *bsky.GraphBlock:
				e.Block = rec
				e.RecType = "block"
				// Track blocks in sqlite
				nsid := strings.Split(op.Path, "/")[0]
				key := fmt.Sprintf("%s_%s_%s", evt.Repo, nsid, rkey)
				err = c.DB.Update(func(txn *badger.Txn) error {
					err := txn.Set([]byte(key), []byte(rec.Subject))
					if err != nil {
						return fmt.Errorf("badger txn.set: %w", err)
					}
					return nil
				})
				if err != nil {
					log.Error("failed to track block in badger", "error", err)
					break
				}
			case *bsky.GraphList:
				e.List = rec
				e.RecType = "list"
			case *bsky.GraphListitem:
				e.ListItem = rec
				e.RecType = "listItem"
			case *bsky.FeedGenerator:
				e.FeedGenerator = rec
				e.RecType = "feedGenerator"
			default:
				log.Warn("unknown record type", "op", op.Path)
				break
			}

			err = c.Emit(ctx, e)
			if err != nil {
				log.Error("failed to emit json", "error", err)
				break
			}
		case repomgr.EvtKindUpdateRecord:
			if op.Cid == nil {
				log.Error("update record op missing cid")
				break
			}
			// Grab the record from the merkel tree
			blk, err := rr.Blockstore().Get(ctx, cid.Cid(*op.Cid))
			if err != nil {
				e := fmt.Errorf("getting block %s within seq %d for %s: %w", *op.Cid, evt.Seq, evt.Repo, err)
				log.Error("failed to get a block from the event", "error", e)
				break
			}

			rec, err := lexutil.CborDecodeValue(blk.RawData())
			if err != nil {
				log.Error("failed to decode cbor", "error", err)
				break
			}

			// Unpack the record and process it
			switch rec := rec.(type) {
			case *bsky.ActorProfile:
				// Process profile updates
				span.SetAttributes(attribute.String("record_type", "actor_profile"))
				recordsProcessedCounter.WithLabelValues("actor_profile", c.SocketURL).Inc()
				// Pack the record into an event
				e := Event{
					Did:     evt.Repo,
					Seq:     evt.Seq,
					OpType:  EvtUpdateRecord,
					RecType: "profile",
					Profile: rec,
				}

				// Emit the event
				err = c.Emit(ctx, e)
				if err != nil {
					log.Error("failed to emit json", "error", err)
					break
				}
			}
		case repomgr.EvtKindDeleteRecord:
			// Emit the delete
			e := Event{
				Did:       evt.Repo,
				Seq:       evt.Seq,
				OpType:    EvtDeleteRecord,
				DeleteRef: op.Path,
			}

			nsid := strings.Split(op.Path, "/")[0]
			switch nsid {
			case "app.bsky.feed.profile":
				e.RecType = "profile"
			case "app.bsky.feed.post":
				e.RecType = "post"
			case "app.bsky.feed.like":
				e.RecType = "like"
				// Lookup in db to find the subject
				nsid := strings.Split(op.Path, "/")[0]
				key := fmt.Sprintf("%s_%s_%s", evt.Repo, nsid, rkey)

				// Fetch the subject from badger
				err = c.DB.View(func(txn *badger.Txn) error {
					item, err := txn.Get([]byte(key))
					if err != nil {
						return fmt.Errorf("badger txn.get: %w", err)
					}
					valCopy, err := item.ValueCopy(nil)
					if err != nil {
						return fmt.Errorf("badger item.valuecopy: %w", err)
					}
					e.Like = &bsky.FeedLike{
						Subject: &comatproto.RepoStrongRef{Uri: string(valCopy)},
					}
					return nil
				})
				if err != nil {
					if errors.Is(err, badger.ErrKeyNotFound) {
						log.Warn("like not found in badger", "key", key)
						break
					}
					log.Error("failed to lookup like in badger", "error", err)
					break
				}

				// Delete from badger
				err = c.DB.Update(func(txn *badger.Txn) error {
					err := txn.Delete([]byte(key))
					if err != nil {
						return fmt.Errorf("badger txn.delete: %w", err)
					}
					return nil
				})
				if err != nil {
					log.Error("failed to delete like from badger", "error", err)
					break
				}
			case "app.bsky.feed.repost":
				e.RecType = "repost"
				// Lookup in sqlite to find the subject
				key := fmt.Sprintf("%s_%s_%s", evt.Repo, nsid, rkey)
				// Fetch the subject from badger
				err = c.DB.View(func(txn *badger.Txn) error {
					item, err := txn.Get([]byte(key))
					if err != nil {
						return fmt.Errorf("badger txn.get: %w", err)
					}
					valCopy, err := item.ValueCopy(nil)
					if err != nil {
						return fmt.Errorf("badger item.valuecopy: %w", err)
					}
					e.Repost = &bsky.FeedRepost{
						Subject: &comatproto.RepoStrongRef{Uri: string(valCopy)},
					}
					return nil
				})
				if err != nil {
					if errors.Is(err, badger.ErrKeyNotFound) {
						log.Warn("repost not found in badger", "key", key)
						break
					}
					log.Error("failed to lookup repost in badger", "error", err)
					break
				}

				// Delete from badger
				err = c.DB.Update(func(txn *badger.Txn) error {
					err := txn.Delete([]byte(key))
					if err != nil {
						return fmt.Errorf("badger txn.delete: %w", err)
					}
					return nil
				})
				if err != nil {
					log.Error("failed to delete repost from badger", "error", err)
					break
				}
			case "app.bsky.graph.follow":
				e.RecType = "follow"
				// Lookup in sqlite to find the subject
				key := fmt.Sprintf("%s_%s_%s", evt.Repo, nsid, rkey)
				// Fetch the subject from badger
				err = c.DB.View(func(txn *badger.Txn) error {
					item, err := txn.Get([]byte(key))
					if err != nil {
						return fmt.Errorf("badger txn.get: %w", err)
					}
					valCopy, err := item.ValueCopy(nil)
					if err != nil {
						return fmt.Errorf("badger item.valuecopy: %w", err)
					}
					e.Follow = &bsky.GraphFollow{
						Subject: string(valCopy),
					}
					return nil
				})
				if err != nil {
					if errors.Is(err, badger.ErrKeyNotFound) {
						log.Warn("follow not found in badger", "key", key)
						break
					}
					log.Error("failed to lookup follow in badger", "error", err)
					break
				}

				// Delete from badger
				err = c.DB.Update(func(txn *badger.Txn) error {
					err := txn.Delete([]byte(key))
					if err != nil {
						return fmt.Errorf("badger txn.delete: %w", err)
					}
					return nil
				})
				if err != nil {
					log.Error("failed to delete follow from badger", "error", err)
					break
				}
			case "app.bsky.graph.block":
				e.RecType = "block"
				// Lookup in sqlite to find the subject
				key := fmt.Sprintf("%s_%s_%s", evt.Repo, nsid, rkey)
				// Fetch the subject from badger
				err = c.DB.View(func(txn *badger.Txn) error {
					item, err := txn.Get([]byte(key))
					if err != nil {
						return fmt.Errorf("badger txn.get: %w", err)
					}
					valCopy, err := item.ValueCopy(nil)
					if err != nil {
						return fmt.Errorf("badger item.valuecopy: %w", err)
					}
					e.Block = &bsky.GraphBlock{
						Subject: string(valCopy),
					}
					return nil
				})
				if err != nil {
					if errors.Is(err, badger.ErrKeyNotFound) {
						log.Warn("block not found in badger", "key", key)
						break
					}
					log.Error("failed to lookup block in badger", "error", err)
					break
				}

				// Delete from badger
				err = c.DB.Update(func(txn *badger.Txn) error {
					err := txn.Delete([]byte(key))
					if err != nil {
						return fmt.Errorf("badger txn.delete: %w", err)
					}
					return nil
				})
				if err != nil {
					log.Error("failed to delete block from badger", "error", err)
					break
				}
			case "app.bsky.graph.list":
				e.RecType = "list"
			case "app.bsky.graph.listitem":
				e.RecType = "listItem"
			case "app.bsky.feed.generator":
				e.RecType = "feedGenerator"
			default:
				log.Warn("unknown record type", "op", op.Path)
				break
			}

			err = c.Emit(ctx, e)
			if err != nil {
				log.Error("failed to emit json", "error", err)
				break
			}
		default:
			log.Warn("unknown event kind from op action", "kind", op.Action)
		}
	}

	eventProcessingDurationHistogram.WithLabelValues(c.SocketURL).Observe(time.Since(processedAt).Seconds())
	return nil
}
