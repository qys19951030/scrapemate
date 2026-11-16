package filequeue

import (
	"context"
	"errors"
	"time"

	"github.com/gosom/scrapemate"
)

// ErrQueueClosed is returned by Pop/Enqueue after the queue has been closed.
var ErrQueueClosed = errors.New("filequeue: queue is closed")

// persistentJob is the JSON-serializable mirror of scrapemate.Job. The
// CheckResponse callback cannot be persisted and is intentionally excluded;
// the reconstructed job falls back to the default status-code check.
type persistentJob struct {
	ID             string                 `json:"id"`
	ParentID       string                 `json:"parent_id"`
	Method         string                 `json:"method"`
	Body           []byte                 `json:"body"`
	URL            string                 `json:"url"`
	Headers        map[string]string      `json:"headers"`
	URLParams      map[string]string      `json:"url_params"`
	Timeout        time.Duration          `json:"timeout"`
	Priority       int                    `json:"priority"`
	MaxRetries     int                    `json:"max_retries"`
	RetryPolicy    scrapemate.RetryPolicy `json:"retry_policy"`
	MaxRetryDelay  time.Duration          `json:"max_retry_delay"`
	TakeScreenshot bool                   `json:"take_screenshot"`
}

// jobRecord is the serialized form of a job.
type jobRecord struct {
	Kind    Kind          `json:"kind"`
	Seq     int64         `json:"seq"`
	Key     string        `json:"key"`
	Attempt int           `json:"attempts"`
	Job     persistentJob `json:"job"`
}

// frameType enumerates the durable record types written to the WAL.
type frameType byte

const (
	frameEnqueue frameType = iota + 1
	frameInflight
	frameComplete
	frameRequeue
	frameAbandon
	framePace
	frameSnapshot
	frameDedup
)

// enqueueFrame is written when a new job enters the queue.
type enqueueFrame struct {
	Record jobRecord `json:"record"`
}

// leaseFrame records state transitions for an already known job.
type leaseFrame struct {
	Seq         int64         `json:"seq"`
	Key         string        `json:"key"`
	Attempt     int           `json:"attempts,omitempty"`
	Reason      string        `json:"reason,omitempty"`
	FailureKind FailureKind   `json:"failure_kind,omitempty"`
	Failure     FailureRecord `json:"failure,omitempty"`
}

// paceFrame records the last time a request was dispatched to a host, so that
// per-host pacing survives a restart.
type paceFrame struct {
	Host       string `json:"host"`
	AtUnixNano int64  `json:"at_unix_nano"`
}

// snapshotFrame is a compacted image of the whole queue.
type snapshotFrame struct {
	NextSeq  int64                 `json:"next_seq"`
	Records  map[string]*jobRecord `json:"records"`
	States   map[string]State      `json:"states"`
	Failures []FailureRecord       `json:"failures"`
	Pacing   map[string]int64      `json:"pacing"`
	Stats    Stats                 `json:"stats"`
}

// dedupFrame records a rejected enqueue so the dedup counter is recomputable
// from the log.
type dedupFrame struct {
	Key string `json:"key"`
}

// ctxDone reports whether ctx has already been cancelled.
func ctxDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}
