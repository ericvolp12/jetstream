// Package parallel provides a parallel scheduler that will run work on a fixed number of workers.
// Events for the same repository will be processed sequentially, but events for different repositories can be processed concurrently.
package parallel

import (
	"context"
	"log/slog"
	"sync"

	"github.com/ericvolp12/jetstream/pkg/client/schedulers"
	"github.com/ericvolp12/jetstream/pkg/models"
	"github.com/prometheus/client_golang/prometheus"
)

// Scheduler is a parallel scheduler that will run work on a fixed number of workers
type Scheduler struct {
	numWorkers  int
	logger      *slog.Logger
	handleEvent func(context.Context, *models.Event) error
	ident       string

	feeder chan *consumerTask
	wg     sync.WaitGroup

	lk     sync.Mutex
	active map[string][]*consumerTask

	// metrics
	itemsAdded     prometheus.Counter
	itemsProcessed prometheus.Counter
	itemsActive    prometheus.Counter
	workersActive  prometheus.Gauge
}

// NewScheduler creates a new parallel scheduler with the given number of workers
func NewScheduler(numWorkers int, ident string, logger *slog.Logger, handleEvent func(context.Context, *models.Event) error) *Scheduler {
	logger = logger.With("component", "parallel-scheduler", "ident", ident)
	p := &Scheduler{
		numWorkers: numWorkers,

		logger: logger,

		handleEvent: handleEvent,

		feeder: make(chan *consumerTask),
		active: make(map[string][]*consumerTask),

		ident: ident,

		itemsAdded:     schedulers.WorkItemsAdded.WithLabelValues(ident, "parallel"),
		itemsActive:    schedulers.WorkItemsActive.WithLabelValues(ident, "parallel"),
		itemsProcessed: schedulers.WorkItemsProcessed.WithLabelValues(ident, "parallel"),
		workersActive:  schedulers.WorkersActive.WithLabelValues(ident, "parallel"),
	}

	p.wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go p.worker()
	}

	p.workersActive.Set(float64(numWorkers))

	return p
}

// Shutdown shuts down the scheduler, waiting for all workers to finish their current work
// The existing work queue will be processed, but no new work will be accepted
func (p *Scheduler) Shutdown() {
	p.logger.Debug("shutting down parallel scheduler", "ident", p.ident)

	for i := 0; i < p.numWorkers; i++ {
		p.feeder <- &consumerTask{
			stop: true,
		}
	}

	close(p.feeder)

	p.wg.Wait()

	p.logger.Debug("parallel scheduler shutdown complete")
}

type consumerTask struct {
	stop bool
	ctx  context.Context
	repo string
	val  *models.Event
}

// AddWork adds work to the scheduler
func (p *Scheduler) AddWork(ctx context.Context, repo string, val *models.Event) error {
	p.itemsAdded.Inc()
	t := &consumerTask{
		ctx:  ctx,
		repo: repo,
		val:  val,
	}

	// Append to the active list if there is already work for this repository
	p.lk.Lock()
	a, ok := p.active[repo]
	if ok {
		p.active[repo] = append(a, t)
		p.lk.Unlock()
		return nil
	}

	// Otherwise, initialize the active list for this repository to catch future work
	// and send this task to a worker
	p.active[repo] = []*consumerTask{}
	p.lk.Unlock()

	select {
	case p.feeder <- t:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Scheduler) worker() {
	defer p.wg.Done()
	for work := range p.feeder {
		for work != nil {
			if work.stop {
				return
			}

			p.itemsActive.Inc()
			if err := p.handleEvent(work.ctx, work.val); err != nil {
				p.logger.Error("event handler failed", "error", err)
			}
			p.itemsProcessed.Inc()

			p.lk.Lock()
			rem, ok := p.active[work.repo]
			if !ok {
				p.logger.Error("worker should always have an 'active' entry if a worker is processing a job")
			}

			if len(rem) == 0 {
				delete(p.active, work.repo)
				work = nil
			} else {
				work = rem[0]
				p.active[work.repo] = rem[1:]
			}
			p.lk.Unlock()
		}
	}
}
