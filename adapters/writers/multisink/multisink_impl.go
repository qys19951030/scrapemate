package multisink

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/gosom/scrapemate"
)

const (
	resultLogName = "results.log"
	ledgerSuffix  = ".ledger.jsonl"
	dirPerm       = 0o755
	filePerm      = 0o600
)

// ErrNoSinks is returned when a fan-out is configured without output targets.
var ErrNoSinks = errors.New("multisink: at least one sink is required")

// ErrEmptyStateDir is returned when the state directory is not provided.
var ErrEmptyStateDir = errors.New("multisink: state directory is empty")

// ledgerEntry records that a sink could not write an envelope.
type ledgerEntry struct {
	Seq       int64  `json:"seq"`
	SourceURL string `json:"source_url"`
	Error     string `json:"error"`
}

// sinkState tracks one sink in memory.
type sinkState struct {
	sink Sink

	mu sync.Mutex

	written     int64
	backfilled  int64
	missing     int64
	failed      bool
	lastError   string
	writtenSeqs map[int64]struct{}
}

func newSinkState(sink Sink) *sinkState {
	return &sinkState{
		sink:        sink,
		writtenSeqs: make(map[int64]struct{}),
	}
}

// multiSinkImpl is the runtime state reconstructed from the result log and
// the per-sink ledgers.
type multiSinkImpl struct {
	opts Options

	mu     sync.RWMutex
	states []*sinkState

	logFile *os.File
	logMu   sync.Mutex

	// envelopes holds every durable result keyed by sequence for back-fill.
	envelopes map[int64]Envelope

	closed    bool
	closeOnce sync.Once
}

func newMultiSink(opts Options) (*multiSinkImpl, error) {
	if len(opts.Sinks) == 0 {
		return nil, ErrNoSinks
	}

	if opts.Dir == "" {
		return nil, ErrEmptyStateDir
	}

	if err := os.MkdirAll(opts.Dir, dirPerm); err != nil {
		return nil, fmt.Errorf("multisink: create state dir: %w", err)
	}

	if !dirWritable(opts.Dir) {
		return nil, fmt.Errorf("multisink: state directory not writable: %s", opts.Dir)
	}

	impl := &multiSinkImpl{
		opts:      opts,
		envelopes: make(map[int64]Envelope),
	}

	for i := range opts.Sinks {
		impl.states = append(impl.states, newSinkState(opts.Sinks[i]))
	}

	if err := impl.openResultLog(); err != nil {
		return nil, err
	}

	if err := impl.replayResultLog(); err != nil {
		return nil, err
	}

	if err := impl.backfill(); err != nil {
		return nil, err
	}

	return impl, nil
}

func dirWritable(dir string) bool {
	probe := filepath.Join(dir, ".write-probe")
	if err := os.WriteFile(probe, []byte("x"), filePerm); err != nil {
		return false
	}

	_ = os.Remove(probe)

	return true
}

func (m *multiSinkImpl) openResultLog() error {
	path := filepath.Join(m.opts.Dir, resultLogName)

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, filePerm)
	if err != nil {
		return fmt.Errorf("multisink: open result log: %w", err)
	}

	m.logFile = file

	return nil
}

// replayResultLog rebuilds the envelope map and each sink's written/missing
// view. The result log is the authoritative record of produced results.
func (m *multiSinkImpl) replayResultLog() error {
	path := filepath.Join(m.opts.Dir, resultLogName)

	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("multisink: replay result log: %w", err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)

	for {
		var env Envelope
		if err := decoder.Decode(&env); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			// A torn final JSON object from a crash: the result before it is
			// intact; stop replay at the last complete object.
			break
		}

		m.envelopes[env.Seq] = env
	}

	return m.replayLedgers()
}

// replayLedgers loads each sink's outstanding missing sequences.
func (m *multiSinkImpl) replayLedgers() error {
	for _, state := range m.states {
		path := m.ledgerPath(state.sink.Name())

		file, err := os.Open(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}

			return fmt.Errorf("multisink: open ledger %s: %w", state.sink.Name(), err)
		}

		decoder := json.NewDecoder(file)

		for {
			var entry ledgerEntry
			if err := decoder.Decode(&entry); err != nil {
				break
			}

			state.mu.Lock()
			if _, ok := state.writtenSeqs[entry.Seq]; !ok {
				state.writtenSeqs[entry.Seq] = struct{}{}
			}

			state.missing++
			state.mu.Unlock()
		}

		file.Close()
	}

	return nil
}

// backfill replays every durable envelope into every sink, skipping sequences
// the sink already holds. A sink that wrote seq N before crashing will never
// write it twice because Has()/writtenSeqs gates every write.
func (m *multiSinkImpl) backfill() error {
	seqs := make([]int64, 0, len(m.envelopes))
	for seq := range m.envelopes {
		seqs = append(seqs, seq)
	}

	sortInt64s(seqs)

	for _, state := range m.states {
		for _, seq := range seqs {
			env := m.envelopes[seq]

			has, err := state.sink.Has(seq)
			if err != nil {
				return fmt.Errorf("multisink: backfill probe %s: %w", state.sink.Name(), err)
			}

			if has {
				state.mu.Lock()
				state.written++
				state.writtenSeqs[seq] = struct{}{}
				state.mu.Unlock()

				continue
			}

			if err := state.sink.Write(context.Background(), env); err != nil {
				if recordErr := m.recordMissing(state, &env, err); recordErr != nil {
					return recordErr
				}

				continue
			}

			state.mu.Lock()
			state.backfilled++
			state.written++
			state.writtenSeqs[seq] = struct{}{}

			if state.missing > 0 {
				state.missing--
			}

			state.mu.Unlock()

			if err := m.clearMissing(state.sink.Name(), seq); err != nil {
				return err
			}
		}
	}

	return nil
}

// run consumes scrapemate results until in is closed. One sink failing only
// marks its records missing; the remaining sinks keep writing.
func (m *multiSinkImpl) run(ctx context.Context, in <-chan scrapemate.Result) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case result, ok := <-in:
			if !ok {
				return nil
			}

			env := m.toEnvelope(result)

			if err := m.appendResultLog(&env); err != nil {
				return err
			}

			m.fanOut(ctx, &env)
		}
	}
}

// toEnvelope builds a durable envelope from a scrapemate result, preserving
// the source URL and queue sequence for traceability.
func (m *multiSinkImpl) toEnvelope(result scrapemate.Result) Envelope {
	seq := extractSeq(result.Job)

	return Envelope{
		Seq:        seq,
		SourceURL:  result.Job.GetFullURL(),
		JobID:      result.Job.GetID(),
		ParentID:   result.Job.GetParentID(),
		AtUnixNano: nowUnixNano(),
		Data:       result.Data,
	}
}

// fanOut writes one envelope to every sink independently.
func (m *multiSinkImpl) fanOut(ctx context.Context, env *Envelope) {
	var wg sync.WaitGroup

	for i := range m.states {
		state := m.states[i]

		wg.Add(1)

		go func(state *sinkState) {
			defer wg.Done()

			has, err := state.sink.Has(env.Seq)
			if err == nil && has {
				return
			}

			if err := state.sink.Write(ctx, *env); err != nil {
				_ = m.recordMissing(state, env, err)

				return
			}

			state.mu.Lock()
			state.written++
			state.writtenSeqs[env.Seq] = struct{}{}
			state.mu.Unlock()
		}(state)
	}

	wg.Wait()
}

// recordMissing persists the failed write in the sink's ledger.
func (m *multiSinkImpl) recordMissing(state *sinkState, env *Envelope, cause error) error {
	state.mu.Lock()
	state.failed = true
	state.lastError = cause.Error()
	state.missing++
	state.mu.Unlock()

	entry := ledgerEntry{
		Seq:       env.Seq,
		SourceURL: env.SourceURL,
		Error:     cause.Error(),
	}

	return m.appendLedger(state.sink.Name(), entry)
}

func (m *multiSinkImpl) stats() []Stats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]Stats, 0, len(m.states))

	for _, state := range m.states {
		state.mu.Lock()
		out = append(out, Stats{
			Name:       state.sink.Name(),
			Written:    state.written,
			Backfilled: state.backfilled,
			Missing:    state.missing,
			Failed:     state.failed,
			LastError:  state.lastError,
		})
		state.mu.Unlock()
	}

	return out
}

func (m *multiSinkImpl) close() error {
	var err error

	m.closeOnce.Do(func() {
		if m.logFile != nil {
			err = m.logFile.Close()
		}

		for _, state := range m.states {
			if closeErr := state.sink.Close(); closeErr != nil && err == nil {
				err = closeErr
			}
		}

		m.mu.Lock()
		m.closed = true
		m.mu.Unlock()
	})

	return err
}
