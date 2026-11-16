// Package scraperun provides the durable coordinator that combines the
// persistent file queue, classified failure handling, fault-tolerant
// multi-sink output, host-aware scheduling and recomputable statistics.
//
// It is a parallel capability to the existing scrapemate engine: the entry
// API, job/result structures and fetcher/parser/cache/writer adapters are
// reused unchanged.
package scraperun

import (
	"context"
	"time"

	"github.com/gosom/scrapemate"
	"github.com/gosom/scrapemate/adapters/providers/filequeue"
	memprovider "github.com/gosom/scrapemate/adapters/providers/memory"
	"github.com/gosom/scrapemate/adapters/writers/multisink"
)

// FailureKind classifies why a job could not be completed.
type FailureKind int

const (
	// FailureUnspecified is the zero value meaning "no classified failure".
	FailureUnspecified FailureKind = iota
	// FailureNetwork means no content could be obtained (DNS, connection,
	// timeout, ...).
	FailureNetwork
	// FailureRejected means the target site explicitly refused the request
	// (HTTP 4xx/5xx rejection, authorization failure, ...).
	FailureRejected
	// FailureParse means content was fetched but the expected structure could
	// not be parsed or the job returned an error.
	FailureParse
)

// String returns a stable human-readable name for the kind.
func (k FailureKind) String() string {
	return failureKindName(k)
}

// FailureAction decides what happens to a job after a classified failure.
type FailureAction int

const (
	// ActionRequeue puts the job back into the pending set.
	ActionRequeue FailureAction = iota
	// ActionAbandon moves the job into the durable failure list.
	ActionAbandon
)

// FailurePolicy configures the action per failure kind.
type FailurePolicy struct {
	Network  FailureAction
	Rejected FailureAction
	Parse    FailureAction
}

// SchedulerConfig holds the concurrency and pacing constraints. All of them
// remain effective across restarts because per-host pacing is persisted by the
// queue.
type SchedulerConfig struct {
	// GlobalConcurrency is the maximum number of jobs running at once.
	GlobalConcurrency int
	// PerHostConcurrency is the maximum number of simultaneous jobs against
	// the same host. Zero falls back to GlobalConcurrency.
	PerHostConcurrency int
	// MinHostInterval is the minimum time between two requests dispatched to
	// the same host.
	MinHostInterval time.Duration
}

// Config configures a Runner.
type Config struct {
	// QueueDir is the directory of the durable queue. Must be writable.
	QueueDir string
	// StateDir is the directory for result logs, sink ledgers and statistics
	// reports. Must be writable.
	StateDir string
	// Scheduler holds the concurrency and pacing constraints.
	Scheduler SchedulerConfig
	// Failures configures the action per failure kind.
	Failures FailurePolicy
	// MaxAttempts is the maximum number of requeues before a job is
	// abandoned regardless of policy. Must be >= 1.
	MaxAttempts int
	// Fsync forces durable writes per frame. Defaults to true.
	Fsync bool
	// fsyncSet records whether Fsync was explicitly configured.
	fsyncSet bool
	// ExitOnInactivity stops the run after this duration without any job
	// completing or being (re)enqueued. Zero means run until the queue is
	// drained.
	ExitOnInactivity time.Duration
}

// WithFsync toggles per-frame fsync for the durable components.
func WithFsync(enabled bool) func(*Config) {
	return func(c *Config) {
		c.Fsync = enabled
		c.fsyncSet = true
	}
}

// Runner orchestrates a durable crawl.
type Runner struct {
	impl *runnerImpl
}

// Mate pairs a scrapemate engine with the recording fetcher used to classify
// failures. Build one with NewMate.
type Mate struct {
	mate     *scrapemate.ScrapeMate
	recorder *RecordingFetcher
}

// Engine returns the underlying scrapemate engine.
func (m *Mate) Engine() *scrapemate.ScrapeMate { return m.mate }

// NewMate builds a scrapemate engine whose fetcher records every response, so
// the runner can distinguish network, rejection and parse failures. The
// provider argument is ignored by the durable runner (the file queue is the
// provider) but is kept to mirror the engine's option surface.
func NewMate(fetcher scrapemate.HTTPFetcher, parser scrapemate.HTMLParser, cache scrapemate.Cacher, opts ...func(*scrapemate.ScrapeMate) error) (*Mate, error) {
	recorder := NewRecordingFetcher(fetcher)

	options := append([]func(*scrapemate.ScrapeMate) error{
		scrapemate.WithHTTPFetcher(recorder),
	}, opts...)

	if parser != nil {
		options = append(options, scrapemate.WithHTMLParser(parser))
	}

	if cache != nil {
		options = append(options, scrapemate.WithCache(cache))
	}

	// A throwaway in-memory provider satisfies the engine constructor; the
	// runner drives jobs directly and never reads from it.
	engine, err := scrapemate.New(append(options, scrapemate.WithJobProvider(memprovider.New()))...)
	if err != nil {
		return nil, err
	}

	return &Mate{mate: engine, recorder: recorder}, nil
}

// New validates the configuration and wires the durable components together.
// It fails before any work starts when a precondition is invalid.
func New(cfg Config, mate *Mate, sinks *multisink.MultiSink) (*Runner, error) { //nolint:gocritic // value keeps the public configuration API ergonomic
	impl, err := newRunner(cfg, mate, sinks)
	if err != nil {
		return nil, err
	}

	return &Runner{impl: impl}, nil
}

// Run pushes the seed jobs (skipped when already known to the queue) and runs
// until the queue is drained, the context is cancelled or the inactivity
// timeout fires. A single job failure never stops the run.
func (r *Runner) Run(ctx context.Context, seeds ...scrapemate.IJob) error {
	return r.impl.run(ctx, seeds)
}

// Failures returns the durable failure list.
func (r *Runner) Failures() []filequeue.FailureRecord {
	return r.impl.queue.Failures()
}

// Stats returns the recomputable run statistics.
func (r *Runner) Stats() Stats {
	return r.impl.statsSnapshot()
}

// Close flushes every durable component.
func (r *Runner) Close() error {
	return r.impl.close()
}
