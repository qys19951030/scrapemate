// Package multisink fans result records out to several independent output
// targets.
//
// A failing sink never stops the other sinks: its missed records are recorded
// in a durable ledger and replayed (back-filled) on the next run. Every output
// line carries the source URL and the queue sequence number, and back-fill
// never writes the same sequence twice to the same sink.
package multisink

import (
	"context"

	"github.com/gosom/scrapemate"
)

// Envelope is the durable wrapper around one result record.
type Envelope struct {
	// Seq is the queue sequence number of the producing job.
	Seq int64 `json:"seq"`
	// SourceURL is the URL the result was scraped from.
	SourceURL string `json:"source_url"`
	// JobID is the id of the producing job.
	JobID string `json:"job_id"`
	// ParentID is the parent id of the producing job.
	ParentID string `json:"parent_id"`
	// AtUnixNano is the production timestamp.
	AtUnixNano int64 `json:"at_unix_nano"`
	// Data is the job-produced payload, encoded by the sink.
	Data any `json:"data"`
}

// Sink is one output target.
type Sink interface {
	// Name uniquely identifies the sink inside a fan-out.
	Name() string
	// Write persists one envelope. A non-nil error marks the envelope as
	// missing for this sink; it will be back-filled later.
	Write(ctx context.Context, env Envelope) error
	// Has reports whether the envelope with the given sequence was already
	// written by this sink. It is used to de-duplicate back-fills.
	Has(seq int64) (bool, error)
	// Close releases sink resources.
	Close() error
}

// Options configures a fan-out writer.
type Options struct {
	// Dir holds the durable result log and per-sink ledgers.
	Dir string
	// Sinks are the output targets. Must not be empty.
	Sinks []Sink
	// Fsync forces the result log to disk per record. Defaults to true.
	Fsync bool
	// fsyncSet records whether Fsync was explicitly configured.
	fsyncSet bool
}

// WithFsync toggles per-record fsync. Without it the result log is synced
// after every record.
func WithFsync(enabled bool) func(*Options) {
	return func(o *Options) {
		o.Fsync = enabled
		o.fsyncSet = true
	}
}

// MultiSink coordinates the output targets and the back-fill process. It
// implements scrapemate.ResultWriter.
type MultiSink struct {
	impl *multiSinkImpl
}

var _ scrapemate.ResultWriter = (*MultiSink)(nil)

// Stats describes one sink's write progress.
type Stats struct {
	Name       string `json:"name"`
	Written    int64  `json:"written"`
	Backfilled int64  `json:"backfilled"`
	Missing    int64  `json:"missing"`
	Failed     bool   `json:"failed"`
	LastError  string `json:"last_error,omitempty"`
}

// New creates a MultiSink and replays any pending back-fills before serving
// new results. Functional options are applied before validation.
func New(opts Options, options ...func(*Options)) (*MultiSink, error) {
	for _, opt := range options {
		opt(&opts)
	}

	if !opts.fsyncSet {
		opts.Fsync = true
	}

	impl, err := newMultiSink(opts)
	if err != nil {
		return nil, err
	}

	return &MultiSink{impl: impl}, nil
}

// Run consumes scrapemate results until in is closed. One sink failing only
// marks its records missing; the remaining sinks keep writing.
func (m *MultiSink) Run(ctx context.Context, in <-chan scrapemate.Result) error {
	return m.impl.run(ctx, in)
}

// Stats returns the per-sink write statistics.
func (m *MultiSink) Stats() []Stats {
	return m.impl.stats()
}

// Close flushes ledgers and closes every sink.
func (m *MultiSink) Close() error {
	return m.impl.close()
}
