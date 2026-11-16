package filequeue

import (
	"context"
	"sync"

	"github.com/gosom/scrapemate"
)

// queueImpl holds the in-memory state reconstructed from the log.
type queueImpl struct {
	opts     Options
	registry registry
	log      *wal
	mu       sync.RWMutex

	records  map[string]*jobRecord
	states   map[string]State
	failures []FailureRecord
	pacing   map[string]int64

	// redispatched holds keys that were inflight when the process (re)started
	// and therefore must be leased out exactly once more. It is purely
	// in-memory: durable truth stays in states.
	redispatched map[string]struct{}

	nextSeq int64
	frameNo int
	stats   Stats

	notify           chan struct{}
	closed           bool
	closeOnce        sync.Once
	recoveredCorrupt bool
	corruptOffset    int64
}

// Jobs implements scrapemate.JobProvider on top of Pop. It feeds a channel
// that is closed when the context is cancelled or the queue is closed.
func (q *queueImpl) Jobs(ctx context.Context) (out <-chan scrapemate.IJob, errc <-chan error) {
	jobs := make(chan scrapemate.IJob)
	errCh := make(chan error, 1)

	go func() {
		defer close(jobs)

		for {
			lease, err := q.Pop(ctx)
			if err != nil {
				if ctx.Err() == nil && !q.isClosed() {
					errCh <- err
				}

				return
			}

			select {
			case <-ctx.Done():
				return
			case jobs <- lease.Job:
			}
		}
	}()

	return jobs, errCh
}

// Push implements scrapemate.JobProvider on top of Enqueue.
func (q *queueImpl) Push(ctx context.Context, job scrapemate.IJob) error {
	_, err := q.Enqueue(ctx, job)

	return err
}

func (q *queueImpl) isClosed() bool {
	q.mu.RLock()
	defer q.mu.RUnlock()

	return q.closed
}
