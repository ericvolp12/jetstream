// Package monotonic provides a monotonic clock.
// The clock is safe for concurrent use and can be used to generate
// increasing timestamps as int64s with a given precision.
package monotonic

import (
	"fmt"
	"sync"
	"time"
)

// Clock is a monotonic clock that generates increasing timestamps.
type Clock struct {
	precision time.Duration
	lk        sync.Mutex
	last      int64
}

// NewClock creates a new Clock with the given precision.
func NewClock(precision time.Duration) (*Clock, error) {
	switch precision {
	case time.Second, time.Millisecond, time.Microsecond, time.Nanosecond:
	default:
		return nil, fmt.Errorf("invalid precision: %v", precision)
	}

	return &Clock{
		precision: precision,
	}, nil
}

// Now returns the current timestamp as an int64 of the Clock's precision.
// Now will always return a monotonically increasing value.
func (c *Clock) Now() int64 {
	c.lk.Lock()
	defer c.lk.Unlock()

	s := time.Now()
	var now int64
	switch c.precision {
	case time.Second:
		now = s.Unix()
	case time.Millisecond:
		now = s.UnixMicro()
	case time.Microsecond:
		now = s.UnixMilli()
	case time.Nanosecond:
		now = s.UnixNano()
	}

	if now <= c.last {
		now = c.last + 1
	}
	c.last = now
	return now
}
