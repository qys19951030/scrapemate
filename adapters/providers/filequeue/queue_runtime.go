package filequeue

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	logName                 = "queue.wal"
	dirPerm                 = 0o755
	filePerm                = 0o600
	defaultCompactionFrames = 10_000
)

// globalRegistry holds factories registered before the first New call.
var (
	globalRegistryMu sync.RWMutex
	globalRegistry   = newRegistry()
)

func registerFactory(kind Kind, factory Factory) {
	globalRegistryMu.Lock()
	defer globalRegistryMu.Unlock()

	globalRegistry.register(kind, factory)
}

// New creates a durable queue in opts.Dir, replaying any existing log.
func newQueue(opts Options) (*Queue, error) {
	if opts.Dir == "" {
		return nil, fmt.Errorf("filequeue: empty queue directory")
	}

	if err := os.MkdirAll(opts.Dir, dirPerm); err != nil {
		return nil, fmt.Errorf("filequeue: create queue dir %s: %w", opts.Dir, err)
	}

	if !dirWritable(opts.Dir) {
		return nil, fmt.Errorf("%w: %s", errDirNotWritable, opts.Dir)
	}

	if opts.CompactionFrames == 0 {
		opts.CompactionFrames = defaultCompactionFrames
	}

	factories := snapshotRegistry()

	logPath := filepath.Join(opts.Dir, logName)

	walLog, corruptAt, err := openWAL(logPath, opts.Fsync)
	if err != nil {
		return nil, err
	}

	q := &queueImpl{
		opts:         opts,
		registry:     factories,
		log:          walLog,
		records:      make(map[string]*jobRecord),
		states:       make(map[string]State),
		pacing:       make(map[string]int64),
		redispatched: make(map[string]struct{}),
		notify:       make(chan struct{}, 1),
	}

	if err := q.replay(); err != nil {
		_ = walLog.closeWAL()

		return nil, err
	}

	// Every inflight job found in the durable log was owned by a process that
	// is no longer running; mark it for exactly one re-dispatch.
	for key, state := range q.states {
		if state == StateInflight {
			q.redispatched[key] = struct{}{}
		}
	}

	if corruptAt >= 0 {
		q.recoveredCorrupt = true
		q.corruptOffset = corruptAt
	}

	return &Queue{impl: q}, nil
}

// RecoveredCorrupt reports whether the last open discarded a torn tail.
func (q *Queue) RecoveredCorrupt() bool {
	return q.impl.recoveredCorrupt
}

// snapshotRegistry copies the global factory registry so later registrations
// cannot mutate a running queue.
func snapshotRegistry() registry {
	globalRegistryMu.RLock()
	defer globalRegistryMu.RUnlock()

	out := newRegistry()

	for k, v := range globalRegistry {
		out[k] = v
	}

	return out
}

func dirWritable(dir string) bool {
	probe := filepath.Join(dir, ".write-probe")
	if err := os.WriteFile(probe, []byte("x"), filePerm); err != nil {
		return false
	}

	_ = os.Remove(probe)

	return true
}

// replay rebuilds all in-memory state from the log. A snapshot frame resets
// the state; subsequent frames are applied on top of it. Counters are derived
// once at the end from the terminal state and frame history rather than
// accumulated during replay, so replayed transitions can never be double
// counted.
func (q *queueImpl) replay() error {
	var (
		enqueued    int64
		dedup       int64
		requeued    int64
		abandoned   int64
		netFailed   int64
		rejFailed   int64
		parseFailed int64

		haveSnapshot bool
	)

	err := q.log.readFrames(func(typ frameType, payload []byte) error {
		switch typ {
		case frameSnapshot:
			if err := q.applySnapshot(payload); err != nil {
				return err
			}

			haveSnapshot = true
			// Snapshot already carries counters; frame-history accumulation
			// starts again from zero for the frames that follow it.
			enqueued, dedup, requeued, abandoned = 0, 0, 0, 0
			netFailed, rejFailed, parseFailed = 0, 0, 0
		case frameEnqueue:
			var frame enqueueFrame
			if err := json.Unmarshal(payload, &frame); err != nil {
				return fmt.Errorf("filequeue: decode enqueue frame: %w", err)
			}

			if err := q.applyEnqueue(&frame.Record); err != nil {
				return err
			}

			enqueued++
		case frameDedup:
			dedup++
		case frameInflight:
			var frame leaseFrame
			if err := json.Unmarshal(payload, &frame); err != nil {
				return fmt.Errorf("filequeue: decode inflight frame: %w", err)
			}

			q.states[frame.Key] = StateInflight
		case frameComplete:
			var frame leaseFrame
			if err := json.Unmarshal(payload, &frame); err != nil {
				return fmt.Errorf("filequeue: decode complete frame: %w", err)
			}

			q.states[frame.Key] = StateCompleted
		case frameRequeue:
			var frame leaseFrame
			if err := json.Unmarshal(payload, &frame); err != nil {
				return fmt.Errorf("filequeue: decode requeue frame: %w", err)
			}

			q.applyRequeue(&frame)

			requeued++
		case frameAbandon:
			var frame leaseFrame
			if err := json.Unmarshal(payload, &frame); err != nil {
				return fmt.Errorf("filequeue: decode abandon frame: %w", err)
			}

			q.applyAbandon(&frame)

			abandoned++

			switch frame.Failure.Kind {
			case FailureUnspecified:
				// unclassified failure: counted only in the abandoned total.
			case FailureNetwork:
				netFailed++
			case FailureRejected:
				rejFailed++
			case FailureParse:
				parseFailed++
			}
		case framePace:
			var frame paceFrame
			if err := json.Unmarshal(payload, &frame); err != nil {
				return fmt.Errorf("filequeue: decode pace frame: %w", err)
			}

			q.pacing[frame.Host] = frame.AtUnixNano
		}

		return nil
	})
	if err != nil {
		return err
	}

	if !haveSnapshot {
		q.stats = Stats{
			Enqueued:       enqueued,
			DedupSkipped:   dedup,
			Completed:      int64(q.countState(StateCompleted)),
			Inflight:       int64(q.countState(StateInflight)),
			Requeued:       requeued,
			Abandoned:      abandoned,
			NetworkFailed:  netFailed,
			RejectedFailed: rejFailed,
			ParseFailed:    parseFailed,
		}
	} else {
		// Snapshot counters plus everything that happened after the snapshot.
		q.stats.Enqueued += enqueued
		q.stats.DedupSkipped += dedup
		q.stats.Requeued += requeued
		q.stats.Abandoned += abandoned
		q.stats.NetworkFailed += netFailed
		q.stats.RejectedFailed += rejFailed
		q.stats.ParseFailed += parseFailed
		q.stats.Completed = int64(q.countState(StateCompleted))
		q.stats.Inflight = int64(q.countState(StateInflight))
	}

	return nil
}

func (q *queueImpl) countState(want State) int {
	count := 0

	for _, state := range q.states {
		if state == want {
			count++
		}
	}

	return count
}

func (q *queueImpl) applySnapshot(payload []byte) error {
	var snap snapshotFrame
	if err := json.Unmarshal(payload, &snap); err != nil {
		return fmt.Errorf("filequeue: decode snapshot: %w", err)
	}

	q.records = snap.Records
	q.states = snap.States
	q.failures = append([]FailureRecord(nil), snap.Failures...)
	q.pacing = snap.Pacing
	q.nextSeq = snap.NextSeq
	q.stats = snap.Stats

	if q.records == nil {
		q.records = make(map[string]*jobRecord)
	}

	if q.states == nil {
		q.states = make(map[string]State)
	}

	if q.pacing == nil {
		q.pacing = make(map[string]int64)
	}

	return nil
}

// applyEnqueue restores an enqueued record during replay. The log is the
// source of truth, so an already-present key keeps its first occurrence.
func (q *queueImpl) applyEnqueue(rec *jobRecord) error {
	if _, exists := q.records[rec.Key]; exists {
		return nil
	}

	q.records[rec.Key] = rec
	q.states[rec.Key] = StatePending

	if rec.Seq >= q.nextSeq {
		q.nextSeq = rec.Seq + 1
	}

	return nil
}

func (q *queueImpl) applyComplete(key string) {
	if _, ok := q.records[key]; !ok {
		return
	}

	q.states[key] = StateCompleted
}

func (q *queueImpl) applyRequeue(frame *leaseFrame) {
	rec, ok := q.records[frame.Key]
	if !ok {
		return
	}

	rec.Attempt = frame.Attempt
	q.states[frame.Key] = StatePending
}

func (q *queueImpl) applyAbandon(frame *leaseFrame) {
	if _, ok := q.records[frame.Key]; !ok {
		return
	}

	q.states[frame.Key] = StateAbandoned
	q.failures = append(q.failures, frame.Failure)
}

// writeFrame marshals payload and appends it to the log.
func (q *queueImpl) writeFrame(typ frameType, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("filequeue: marshal frame: %w", err)
	}

	if err := q.log.appendFrame(typ, raw); err != nil {
		return err
	}

	// frameNo counts frames appended during this process only. Replay does
	// not increment it: after opening an existing log, the next compaction
	// threshold is measured from the frames written in the current run.
	q.frameNo = 0

	return q.maybeCompact()
}

// signal wakes one blocked Pop.
func (q *queueImpl) signal() {
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

// now is indirected so timing-dependent code can be reasoned about in tests.
func now() time.Time {
	return time.Now().UTC()
}
