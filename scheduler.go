package runner

import (
	"fmt"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
)

// CancelFunc stops an individual scheduled job.
type CancelFunc func()

// SchedulerService allows plugins to register periodic work without managing
// goroutine lifecycles themselves. All registered jobs are cancelled
// automatically when the runner stops.
type SchedulerService interface {
	// Every fires fn on a fixed interval. The first tick occurs after one
	// full interval has elapsed. Returns a CancelFunc to stop this specific
	// job early; it is safe to call multiple times.
	Every(interval time.Duration, fn func()) CancelFunc

	// Cron fires fn according to a standard 5-field cron expression
	// (minute hour dom month dow). Returns a CancelFunc and an error if the
	// expression cannot be parsed.
	Cron(expr string, fn func()) (CancelFunc, error)
}

// scheduler is the concrete implementation owned by the Runner.
type scheduler struct {
	cron *cron.Cron

	mu   sync.Mutex
	jobs []*tickerJob // Every-based jobs, stopped on shutdown
}

type tickerJob struct {
	ticker *time.Ticker
	stop   chan struct{}
	once   sync.Once
}

func (j *tickerJob) cancel() {
	j.once.Do(func() {
		close(j.stop)
		j.ticker.Stop()
	})
}

func newScheduler() *scheduler {
	return &scheduler{
		cron: cron.New(),
	}
}

// start begins the cron daemon. Must be called before any jobs are registered
// via Cron (Every jobs start their own goroutines immediately).
func (s *scheduler) start() {
	s.cron.Start()
}

// stop cancels all jobs and blocks until the cron daemon has drained.
func (s *scheduler) stop() {
	// Stop cron-expression jobs.
	ctx := s.cron.Stop()
	<-ctx.Done()

	// Stop Every-based ticker jobs.
	s.mu.Lock()
	jobs := s.jobs
	s.jobs = nil
	s.mu.Unlock()
	for _, j := range jobs {
		j.cancel()
	}
}

// Every implements SchedulerService.
func (s *scheduler) Every(interval time.Duration, fn func()) CancelFunc {
	if interval <= 0 {
		panic(fmt.Sprintf("scheduler.Every: interval must be positive, got %v", interval))
	}
	j := &tickerJob{
		ticker: time.NewTicker(interval),
		stop:   make(chan struct{}),
	}
	s.mu.Lock()
	s.jobs = append(s.jobs, j)
	s.mu.Unlock()

	go func() {
		for {
			select {
			case <-j.stop:
				return
			case <-j.ticker.C:
				fn()
			}
		}
	}()

	return j.cancel
}

// Cron implements SchedulerService.
func (s *scheduler) Cron(expr string, fn func()) (CancelFunc, error) {
	id, err := s.cron.AddFunc(expr, fn)
	if err != nil {
		return nil, fmt.Errorf("scheduler.Cron: invalid expression %q: %w", expr, err)
	}
	var once sync.Once
	cancel := func() {
		once.Do(func() { s.cron.Remove(id) })
	}
	return cancel, nil
}
