package consumer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/goccy/go-json"
	"github.com/labstack/gommon/log"
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
	err = c.DB.Set(cursorKey, data, pebble.Sync)
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
	data, closer, err := c.DB.Get(cursorKey)
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
func (c *Consumer) PersistEvent(ctx context.Context, evt *Event) error {
	ctx, span := tracer.Start(ctx, "PersistEvent")
	defer span.End()

	// Persist the event to PebbleDB
	data, err := json.Marshal(evt)
	if err != nil {
		log.Error("failed to marshal event", "error", err)
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	key := []byte(fmt.Sprintf("%d", evt.TimeUS))

	err = c.DB.Set(key, data, pebble.Sync)
	if err != nil {
		log.Error("failed to write event to pebble", "error", err)
		return fmt.Errorf("failed to write event to pebble: %w", err)
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
	err := c.DB.DeleteRange([]byte("0"), trimKey, pebble.Sync)
	if err != nil {
		log.Error("failed to delete old events", "error", err)
		return fmt.Errorf("failed to delete old events: %w", err)
	}

	return nil
}

// Saturday, May 19, 2277 12:26:40 PM
var finalKey = []byte("9700000000000000")

// ReplayEvents replays events from PebbleDB
func (c *Consumer) ReplayEvents(ctx context.Context, cursor int64, emit func(context.Context, Event) error) error {
	ctx, span := tracer.Start(ctx, "ReplayEvents")
	defer span.End()

	// Iterate over all events starting from the cursor
	iter, err := c.DB.NewIter(&pebble.IterOptions{
		LowerBound: []byte(fmt.Sprintf("%d", cursor)),
		UpperBound: finalKey,
	})
	if err != nil {
		log.Error("failed to create iterator", "error", err)
		return fmt.Errorf("failed to create iterator: %w", err)
	}
	defer iter.Close()

	// This iterator will return events in ascending order of time, starting from the cursor
	// and stopping when it reaches the end of the database
	// if you never reach the end of the database, you'll just keep consuming slower than the events are produced
	for iter.First(); iter.Valid(); iter.Next() {
		data := iter.Value()

		// Unmarshal the event JSON
		var evt Event
		err := json.Unmarshal(data, &evt)
		if err != nil {
			log.Error("failed to unmarshal event", "error", err)
			return fmt.Errorf("failed to unmarshal event: %w", err)
		}

		// Emit the event
		err = emit(ctx, evt)
		if err != nil {
			log.Error("failed to emit event", "error", err)
			return fmt.Errorf("failed to emit event: %w", err)
		}
	}

	return nil
}
