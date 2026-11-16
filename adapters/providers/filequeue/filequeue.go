// Package filequeue provides a durable, resumable and de-duplicating job
// queue for scrapemate.
//
// The queue persists every state transition to a crash-safe write-ahead log
// (length-prefixed, checksummed frames). A process restart replays the log:
// completed jobs are never executed twice, abandoned jobs remain queryable in
// the failure list and a torn write at the tail of the log is discarded so the
// queue always recovers to its last complete state.
//
// A job is de-duplicated by the normalized form of its URL (see the urlnorm
// package): at most one pending item exists for each normalized key.
package filequeue

import (
	"context"

	"github.com/gosom/scrapemate"
)

// State describes the lifecycle state of a queued job.
type State int

const (
	// StatePending means the job is waiting to be executed.
	StatePending State = iota
	// StateInflight means the job has been leased to a worker but not yet
	// reported as finished.
	StateInflight
	// StateCompleted means the job finished successfully.
	StateCompleted
	// StateAbandoned means the job failed and the configured policy gave up
	// on it. The job remains available in the failure list.
	StateAbandoned
)

// Kind is the durable kind of a queue record. Custom jobs are reconstructed
// through a registered Factory; jobs that do not implement KindKeeper are
// stored as plain scrapemate.Job values.
type Kind string

const (
	// KindBase is the kind used for plain scrapemate.Job values.
	KindBase Kind = "scrapemate.Job"
)

// Options configures a durable queue.
type Options struct {
	// Dir is the directory that holds the queue log and snapshots.
	// It must be writable; it is created when missing.
	Dir string
	// Fsync forces every log frame to disk before it is acknowledged.
	// Defaults to true; only disable it for tests that do not test crash
	// recovery.
	Fsync bool
	// CompactionFrames triggers a snapshot + log rotation once the log
	// contains more than this many frames. Zero disables automatic
	// compaction.
	CompactionFrames int
}

// Queue is a durable job queue.
//
// It implements scrapemate.JobProvider so it can be used as a drop-in
// replacement for the in-memory provider, and it exposes the additional
// lease/ack operations the durable coordinator needs.
type Queue struct {
	impl *queueImpl
}

// var assertions are filled in once the implementation lands.
var (
	_ scrapemate.JobProvider = (*Queue)(nil)
	_ DurableQueue           = (*Queue)(nil)
)

// Jobs implements scrapemate.JobProvider.
func (q *Queue) Jobs(ctx context.Context) (jobs <-chan scrapemate.IJob, errs <-chan error) {
	return q.impl.Jobs(ctx)
}

// Push implements scrapemate.JobProvider.
func (q *Queue) Push(ctx context.Context, job scrapemate.IJob) error {
	return q.impl.Push(ctx, job)
}

// Enqueue adds job to the queue.
func (q *Queue) Enqueue(ctx context.Context, job scrapemate.IJob) (bool, error) {
	return q.impl.Enqueue(ctx, job)
}

// Pop leases the next pending job.
func (q *Queue) Pop(ctx context.Context) (Lease, error) {
	return q.impl.Pop(ctx)
}

// Complete marks a leased job as completed.
func (q *Queue) Complete(ctx context.Context, lease Lease) error {
	return q.impl.Complete(ctx, lease)
}

// Requeue puts a leased job back into the pending set.
func (q *Queue) Requeue(ctx context.Context, lease Lease, reason string) error {
	return q.impl.Requeue(ctx, lease, reason)
}

// RequeueWithKind works like Requeue but also records the failure class.
func (q *Queue) RequeueWithKind(ctx context.Context, lease Lease, reason string, kind FailureKind) error {
	return q.impl.RequeueWithKind(ctx, lease, reason, kind)
}

// Abandon marks a leased job as abandoned.
func (q *Queue) Abandon(ctx context.Context, lease Lease, failure FailureRecord) error { //nolint:gocritic // failure is part of the durable public API
	return q.impl.Abandon(ctx, lease, failure)
}

// Failures returns the durable failure list.
func (q *Queue) Failures() []FailureRecord {
	return q.impl.Failures()
}

// Stats returns a snapshot of the queue counters.
func (q *Queue) Stats() Stats {
	return q.impl.Stats()
}

// Pending returns the number of pending plus inflight jobs.
func (q *Queue) Pending() int {
	return q.impl.Pending()
}

// Close flushes the log and releases all resources.
func (q *Queue) Close() error {
	return q.impl.Close()
}

// RecordPacing persists the last request dispatch time for a host so that
// per-host minimum interval constraints survive restarts.
func (q *Queue) RecordPacing(host string, atUnixNano int64) error {
	return q.impl.RecordPacing(host, atUnixNano)
}

// LastPacing returns the persisted last dispatch time for a host.
func (q *Queue) LastPacing(host string) (int64, bool) {
	return q.impl.LastPacing(host)
}

// DurableQueue is the extended queue contract used by scraperun.
type DurableQueue interface {
	scrapemate.JobProvider

	// Enqueue adds job to the queue. It returns true when the job was
	// enqueued and false when it was skipped because its normalized key was
	// already pending, inflight, completed or abandoned.
	Enqueue(ctx context.Context, job scrapemate.IJob) (enqueued bool, err error)
	// Pop leases the next pending job. It blocks until a job is available,
	// the context is cancelled or the queue is closed.
	Pop(ctx context.Context) (Lease, error)
	// Complete marks a leased job as completed.
	Complete(ctx context.Context, lease Lease) error
	// Requeue puts a leased job back into the pending set, recording one
	// requeue attempt.
	Requeue(ctx context.Context, lease Lease, reason string) error
	// Abandon marks a leased job as abandoned and stores it in the failure
	// list.
	Abandon(ctx context.Context, lease Lease, failure FailureRecord) error
	// Failures returns the failure list (abandoned and exhausted jobs).
	Failures() []FailureRecord
	// Stats returns a snapshot of the queue counters.
	Stats() Stats
	// Pending returns the number of pending plus inflight jobs.
	Pending() int
	// Close flushes the log and releases all resources.
	Close() error
}

// Lease is a job handed out by Pop.
type Lease struct {
	// Seq is the monotonic queue sequence number assigned at enqueue time.
	Seq int64
	// Key is the normalized de-duplication key.
	Key string
	// Job is the leased job.
	Job scrapemate.IJob
	// Attempts is the number of times the job has already been requeued.
	Attempts int
}

// FailureRecord is the durable description of an abandoned job.
type FailureRecord struct {
	Seq      int64
	Key      string
	URL      string
	Kind     FailureKind
	Message  string
	Attempts int
	AtUnix   int64
}

// FailureKind mirrors scraperun's failure classification. The queue keeps its
// own copy so it does not depend on the coordinator package.
type FailureKind int

const (
	// FailureUnspecified is the zero value and means "no classified failure".
	FailureUnspecified FailureKind = iota
	// FailureNetwork means no content could be fetched.
	FailureNetwork
	// FailureRejected means the target site explicitly refused the request.
	FailureRejected
	// FailureParse means content was fetched but the expected structure could
	// not be parsed.
	FailureParse
)

// Stats holds counters that can be recomputed by replaying the queue files.
type Stats struct {
	Enqueued       int64
	DedupSkipped   int64
	Completed      int64
	Abandoned      int64
	Requeued       int64
	Inflight       int64
	NetworkFailed  int64
	RejectedFailed int64
	ParseFailed    int64
}

// Factory reconstructs a custom job instance of the registered kind before its
// persisted fields are unmarshalled into it.
type Factory func() scrapemate.IJob

// KindKeeper is implemented by custom jobs that want to be persisted with
// their concrete type. Jobs without it are persisted as plain scrapemate.Job.
type KindKeeper interface {
	Kind() Kind
}

// New creates a durable queue in opts.Dir, replaying any existing log.
func New(opts Options) (*Queue, error) {
	return newQueue(opts)
}

// RegisterFactory registers a reconstruction factory for a custom job kind.
// It must be called before New for any kind present in an existing log.
func RegisterFactory(kind Kind, factory Factory) {
	registerFactory(kind, factory)
}
