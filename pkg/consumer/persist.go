package consumer

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bluesky-social/jetstream/pkg/models"
	"github.com/cockroachdb/pebble"
	"github.com/goccy/go-json"
	"github.com/labstack/gommon/log"
	"golang.org/x/time/rate"
)

// Progress is the cursor for the consumer
type Progress struct {
	LastSeq            int64     `json:"last_seq"`
	LastSeqProcessedAt time.Time `json:"last_seq_processed_at"`
	lk                 sync.RWMutex
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

var cursorKey = []byte("cursor")

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

	// Write the cursor JSON to pebble
	err = c.UncompressedDB.Set(cursorKey, data, pebble.Sync)
	if err != nil {
		return fmt.Errorf("failed to write cursor to pebble: %w", err)
	}

	return nil
}

// ReadCursor reads the cursor from file
func (c *Consumer) ReadCursor(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "ReadCursor")
	defer span.End()

	// Read the cursor from pebble
	data, closer, err := c.UncompressedDB.Get(cursorKey)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil
		}
		return fmt.Errorf("failed to read cursor from pebble: %w", err)
	}
	defer closer.Close()

	// Unmarshal the cursor JSON
	err = json.Unmarshal(data, c.Progress)
	if err != nil {
		return fmt.Errorf("failed to unmarshal cursor JSON: %w", err)
	}

	return nil
}

// PersistEvent persists an event to PebbleDB
func (c *Consumer) PersistEvent(ctx context.Context, evt *models.Event, asJSON, compBytes []byte) error {
	ctx, span := tracer.Start(ctx, "PersistEvent")
	defer span.End()

	// Key structure for events in PebbleDB
	// {{event_time_us}}_{{repo}}_{{collection}}
	var key []byte
	if evt.EventType == models.EventCommit && evt.Commit != nil {
		key = []byte(fmt.Sprintf("%d_%s_%s", evt.TimeUS, evt.Did, evt.Commit.Collection))
	} else {
		key = []byte(fmt.Sprintf("%d_%s", evt.TimeUS, evt.Did))
	}

	// Write the event to the uncompressed DB
	err := c.UncompressedDB.Set(key, asJSON, pebble.NoSync)
	if err != nil {
		log.Error("failed to write event to pebble", "error", err)
		return fmt.Errorf("failed to write event to pebble: %w", err)
	}

	// Compress the event and write it to the compressed DB
	err = c.CompressedDB.Set(key, compBytes, pebble.NoSync)
	if err != nil {
		log.Error("failed to write compressed event to pebble", "error", err)
		return fmt.Errorf("failed to write compressed event to pebble: %w", err)
	}

	return nil
}

// TrimEvents deletes old events from PebbleDB
func (c *Consumer) TrimEvents(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "TrimEvents")
	defer span.End()

	// Keys are stored as strings of the event time in microseconds
	// We can range delete events older than the event TTL
	trimUntil := time.Now().Add(-c.EventTTL).UnixMicro()
	trimKey := []byte(fmt.Sprintf("%d", trimUntil))

	// Delete all numeric keys older than the trim key
	err := c.UncompressedDB.DeleteRange([]byte("0"), trimKey, pebble.Sync)
	if err != nil {
		log.Error("failed to delete old events", "error", err)
		return fmt.Errorf("failed to delete old events: %w", err)
	}

	// Delete all numeric keys older than the trim key in the compressed DB
	err = c.CompressedDB.DeleteRange([]byte("0"), trimKey, pebble.Sync)
	if err != nil {
		log.Error("failed to delete old compressed events", "error", err)
		return fmt.Errorf("failed to delete old compressed events: %w", err)
	}

	return nil
}

// Saturday, May 19, 2277 12:26:40 PM
var finalKey = []byte("9700000000000000")

// ReplayEvents replays events from PebbleDB
func (c *Consumer) ReplayEvents(ctx context.Context, compressed bool, cursor int64, playbackRateLimit float64, emit func(context.Context, int64, string, string, func() []byte) error) (int64, error) {
	ctx, span := tracer.Start(ctx, "ReplayEvents")
	defer span.End()

	// Limit the playback rate to avoid thrashing the host when replaying events
	// with very specific filters
	limiter := rate.NewLimiter(rate.Limit(playbackRateLimit), int(playbackRateLimit))

	// Iterate over all events starting from the cursor
	var iter *pebble.Iterator
	var err error

	iterOptions := &pebble.IterOptions{
		LowerBound: []byte(fmt.Sprintf("%d", cursor)),
		UpperBound: finalKey,
	}

	if compressed {
		iter, err = c.CompressedDB.NewIterWithContext(ctx, iterOptions)
	} else {
		iter, err = c.UncompressedDB.NewIterWithContext(ctx, iterOptions)
	}
	if err != nil {
		log.Error("failed to create iterator", "error", err)
		return 0, fmt.Errorf("failed to create iterator: %w", err)
	}
	defer iter.Close()

	// This iterator will return events in ascending order of time, starting from the cursor
	// and stopping when it reaches the end of the database
	// if you never reach the end of the database, you'll just keep consuming slower than the events are produced
	lastSeq := int64(0)
	for iter.First(); iter.Valid(); iter.Next() {
		// Wait for the rate limiter
		err := limiter.Wait(ctx)
		if err != nil {
			log.Error("failed to wait for rate limiter", "error", err)
			return 0, fmt.Errorf("failed to wait for rate limiter: %w", err)
		}

		// Unpack the key ({{event_time_us}}_{{repo}}_{{collection}})
		key := string(iter.Key())
		parts := strings.Split(key, "_")
		if len(parts) < 2 {
			log.Error("invalid key format", "key", key)
			return 0, fmt.Errorf("invalid key format: %s", key)
		}

		timeUS, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			log.Error("failed to parse timeUS from event", "error", err)
			return 0, fmt.Errorf("failed to parse timeUS: %w", err)
		}

		collection := ""
		if len(parts) > 2 {
			collection = parts[2]
		}

		// Emit the event
		err = emit(ctx, timeUS, parts[1], collection, iter.Value)
		if err != nil {
			log.Error("failed to emit event", "error", err)
			return 0, fmt.Errorf("failed to emit event: %w", err)
		}

		lastSeq = timeUS
	}

	return lastSeq, nil
}
