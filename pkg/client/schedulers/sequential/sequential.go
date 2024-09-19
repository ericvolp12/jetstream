package sequential

import (
	"context"
	"log/slog"

	"github.com/bluesky-social/jetstream/pkg/client/schedulers"
	"github.com/bluesky-social/jetstream/pkg/models"
	"github.com/prometheus/client_golang/prometheus"
)

// Scheduler is a sequential scheduler that will run work on a single worker
type Scheduler struct {
	handleEvent func(context.Context, *models.Event) error

	ident  string
	logger *slog.Logger

	// metrics
	itemsAdded     prometheus.Counter
	itemsProcessed prometheus.Counter
	itemsActive    prometheus.Counter
	workersActive  prometheus.Gauge
}

func NewScheduler(ident string, logger *slog.Logger, handleEvent func(context.Context, *models.Event) error) *Scheduler {
	logger = logger.With("component", "sequential-scheduler", "ident", ident)
	p := &Scheduler{
		handleEvent: handleEvent,

		ident:  ident,
		logger: logger,

		itemsAdded:     schedulers.WorkItemsAdded.WithLabelValues(ident, "sequential"),
		itemsProcessed: schedulers.WorkItemsProcessed.WithLabelValues(ident, "sequential"),
		itemsActive:    schedulers.WorkItemsActive.WithLabelValues(ident, "sequential"),
		workersActive:  schedulers.WorkersActive.WithLabelValues(ident, "sequential"),
	}

	p.workersActive.Set(1)

	return p
}

func (p *Scheduler) Shutdown() {
	p.workersActive.Set(0)
}

func (s *Scheduler) AddWork(ctx context.Context, repo string, val *models.Event) error {
	s.itemsAdded.Inc()
	s.itemsActive.Inc()
	err := s.handleEvent(ctx, val)
	s.itemsProcessed.Inc()
	return err
}
