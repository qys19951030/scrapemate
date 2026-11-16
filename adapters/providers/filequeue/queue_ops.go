package filequeue

import (
	"context"
	"errors"
	"fmt"

	"github.com/gosom/scrapemate"
)

// errDirNotWritable is returned when the queue directory cannot be written.
var errDirNotWritable = errors.New("filequeue: directory not writable")

// Enqueue adds a job to the queue unless its normalized key is already known
// in any lifecycle state. It returns false (and bumps the dedup counter) for
// skipped duplicates.
func (q *queueImpl) Enqueue(ctx context.Context, job scrapemate.IJob) (bool, error) {
	if job == nil {
		return false, fmt.Errorf("filequeue: cannot enqueue nil job")
	}

	key, err := durableKey(job)
	if err != nil {
		return false, err
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return false, ErrQueueClosed
	}

	if _, exists := q.records[key]; exists {
		if err := q.writeFrame(frameDedup, dedupFrame{Key: key}); err != nil {
			return false, err
		}

		q.stats.DedupSkipped++

		return false, nil
	}

	if err := ctx.Err(); err != nil {
		return false, err
	}

	seq := q.nextSeq
	rec := encodeJobRecord(seq, key, 0, job)

	if err := q.writeFrame(frameEnqueue, enqueueFrame{Record: rec}); err != nil {
		return false, err
	}

	q.nextSeq++
	stored := rec
	q.records[key] = &stored
	q.states[key] = StatePending
	q.stats.Enqueued++
	q.signal()

	return true, nil
}

// Pop leases the next pending job, blocking until one is available. Inflight
// jobs whose lease was interrupted by a crash are re-dispatched first because
// they never reached a terminal durable state.
func (q *queueImpl) Pop(ctx context.Context) (Lease, error) {
	for {
		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()

			return Lease{}, ErrQueueClosed
		}

		if lease, ok := q.takePendingLocked(); ok {
			q.mu.Unlock()

			return lease, nil
		}

		notify := q.notify
		q.mu.Unlock()

		select {
		case <-ctx.Done():
			return Lease{}, ctx.Err()
		case <-notify:
		}
	}
}

// takePendingLocked finds and leases the oldest pending (or recovered
// inflight) job, ordering by queue sequence number so dispatch is FIFO. The
// caller must hold q.mu.
func (q *queueImpl) takePendingLocked() (Lease, bool) {
	var chosen *jobRecord

	for key, rec := range q.records {
		state := q.states[key]

		eligible := state == StatePending
		if _, recovered := q.redispatched[key]; recovered {
			eligible = true
		}

		if !eligible {
			continue
		}

		if chosen == nil || rec.Seq < chosen.Seq {
			chosen = rec
		}
	}

	if chosen == nil {
		return Lease{}, false
	}

	// A recovered inflight job is handed out exactly once. After that the
	// current run owns the lease and the key behaves like a normal inflight
	// job that cannot be selected again before it reaches a terminal state.
	_, recovered := q.redispatched[chosen.Key]

	if recovered {
		delete(q.redispatched, chosen.Key)
	}

	frame := leaseFrame{Seq: chosen.Seq, Key: chosen.Key, Attempt: chosen.Attempt}

	if err := q.writeFrame(frameInflight, frame); err != nil {
		return Lease{}, false
	}

	q.states[chosen.Key] = StateInflight

	if !recovered {
		q.stats.Inflight++
	}

	job := decodeJobRecord(q.registry, chosen)

	return Lease{
		Seq:      chosen.Seq,
		Key:      chosen.Key,
		Job:      job,
		Attempts: chosen.Attempt,
	}, true
}

// Complete marks a leased job completed. Completed jobs are never dispatched
// again, even after a restart.
func (q *queueImpl) Complete(ctx context.Context, lease Lease) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return ErrQueueClosed
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	rec, ok := q.records[lease.Key]
	if !ok {
		return fmt.Errorf("filequeue: complete unknown lease key %s", lease.Key)
	}

	if err := q.writeFrame(frameComplete, leaseFrame{Seq: rec.Seq, Key: lease.Key}); err != nil {
		return err
	}

	q.applyComplete(lease.Key)
	q.stats.Completed++
	q.stats.Inflight--

	return nil
}

// Requeue returns a leased job to the pending set and records the attempt.
func (q *queueImpl) Requeue(ctx context.Context, lease Lease, reason string) error {
	return q.RequeueWithKind(ctx, lease, reason, 0)
}

// RequeueWithKind works like Requeue but also records the failure class so
// requeued failures are counted per kind and survive a restart.
func (q *queueImpl) RequeueWithKind(ctx context.Context, lease Lease, reason string, kind FailureKind) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return ErrQueueClosed
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	rec, ok := q.records[lease.Key]
	if !ok {
		return fmt.Errorf("filequeue: requeue unknown lease key %s", lease.Key)
	}

	rec.Attempt = lease.Attempts + 1
	q.records[lease.Key] = rec

	frame := leaseFrame{
		Seq:         rec.Seq,
		Key:         lease.Key,
		Attempt:     rec.Attempt,
		Reason:      reason,
		FailureKind: kind,
	}

	if err := q.writeFrame(frameRequeue, frame); err != nil {
		return err
	}

	q.applyRequeue(&frame)
	q.stats.Requeued++
	q.stats.Inflight--
	q.signal()

	return nil
}

// bumpFailureKind increments the matching per-kind failure counter. A zero
// kind means "no classified failure" and is ignored.
func (q *queueImpl) bumpFailureKind(kind FailureKind) {
	switch kind {
	case FailureUnspecified:
		// no classified failure: nothing to count.
	case FailureNetwork:
		q.stats.NetworkFailed++
	case FailureRejected:
		q.stats.RejectedFailed++
	case FailureParse:
		q.stats.ParseFailed++
	}
}

// Abandon moves a leased job to the durable failure list.
func (q *queueImpl) Abandon(ctx context.Context, lease Lease, failure FailureRecord) error { //nolint:gocritic // value kept for stable public-facing internal API symmetry
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return ErrQueueClosed
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	rec, ok := q.records[lease.Key]
	if !ok {
		return fmt.Errorf("filequeue: abandon unknown lease key %s", lease.Key)
	}

	failure.Seq = rec.Seq
	failure.Key = lease.Key
	failure.Attempts = lease.Attempts + 1

	if failure.AtUnix == 0 {
		failure.AtUnix = now().Unix()
	}

	if failure.URL == "" {
		failure.URL = fullURL(&rec.Job)
	}

	frame := leaseFrame{
		Seq:     rec.Seq,
		Key:     lease.Key,
		Attempt: failure.Attempts,
		Failure: failure,
	}

	if err := q.writeFrame(frameAbandon, frame); err != nil {
		return err
	}

	q.applyAbandon(&frame)
	q.stats.Abandoned++
	q.stats.Inflight--

	// Per-kind failure counts classify the terminal failure of each job, so
	// the kind is counted once when the job is given up. Retries are tracked
	// separately by Requeued.
	q.bumpFailureKind(failure.Kind)

	return nil
}

// Failures returns a copy of the durable failure list.
func (q *queueImpl) Failures() []FailureRecord {
	q.mu.RLock()
	defer q.mu.RUnlock()

	out := make([]FailureRecord, len(q.failures))
	copy(out, q.failures)

	return out
}

// Stats returns a copy of the queue counters.
func (q *queueImpl) Stats() Stats {
	q.mu.RLock()
	defer q.mu.RUnlock()

	out := q.stats

	return out
}

// Pending reports how many jobs are still waiting to run or be re-dispatched.
// On a fresh process every durable inflight job is a crashed lease and is
// therefore included; once the current run re-leases it (removing it from the
// recovery set) it is treated as actively running and excluded.
func (q *queueImpl) Pending() int {
	q.mu.RLock()
	defer q.mu.RUnlock()

	count := 0

	for key, state := range q.states {
		switch state {
		case StatePending:
			count++
		case StateInflight:
			if _, recovered := q.redispatched[key]; recovered {
				count++
			}
		case StateCompleted, StateAbandoned:
			// terminal states are never pending.
		}
	}

	return count
}

// RecordPacing persists the last dispatch time for a host.
func (q *queueImpl) RecordPacing(host string, at int64) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return ErrQueueClosed
	}

	if err := q.writeFrame(framePace, paceFrame{Host: host, AtUnixNano: at}); err != nil {
		return err
	}

	q.pacing[host] = at

	return nil
}

// LastPacing returns the persisted last dispatch time for a host.
func (q *queueImpl) LastPacing(host string) (int64, bool) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	at, ok := q.pacing[host]

	return at, ok
}

// Close flushes the log. It is idempotent.
func (q *queueImpl) Close() error {
	var err error

	q.closeOnce.Do(func() {
		q.mu.Lock()
		q.closed = true
		q.signal()
		q.mu.Unlock()

		if q.log != nil {
			err = q.log.closeWAL()
		}
	})

	return err
}
