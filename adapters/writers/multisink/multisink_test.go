package multisink_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gosom/scrapemate"
	"github.com/gosom/scrapemate/adapters/writers/multisink"
)

// seqJob is a test job that carries an explicit queue sequence.
type seqJob struct {
	scrapemate.Job
	seq int64
}

func (j *seqJob) QueueSeq() int64 { return j.seq }

func resultWith(seq int64, url string, data any) scrapemate.Result {
	return scrapemate.Result{
		Job: &seqJob{
			Job: scrapemate.Job{ID: fmt.Sprintf("job-%d", seq), URL: url},
			seq: seq,
		},
		Data: data,
	}
}

// memorySink is a controllable in-memory sink used to inject failures.
type memorySink struct {
	name     string
	mu       sync.Mutex
	written  map[int64]multisink.Envelope
	failSeq  map[int64]bool
	writtenN int64
}

func newMemorySink(name string) *memorySink {
	return &memorySink{name: name, written: make(map[int64]multisink.Envelope), failSeq: make(map[int64]bool)}
}

func (s *memorySink) Name() string { return s.name }

func (s *memorySink) Write(_ context.Context, env multisink.Envelope) error { //nolint:gocritic // matches the public Sink contract
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.failSeq[env.Seq] {
		return errors.New("simulated sink failure")
	}

	if _, ok := s.written[env.Seq]; ok {
		return nil
	}

	s.written[env.Seq] = env
	s.writtenN++

	return nil
}

func (s *memorySink) Has(seq int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.written[seq]

	return ok, nil
}

func (s *memorySink) Close() error { return nil }

func (s *memorySink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.written)
}

func TestOneSinkFailureDoesNotStopOthers(t *testing.T) {
	t.Parallel()

	stateDir := t.TempDir()

	good := newMemorySink("good")
	bad := newMemorySink("bad")
	bad.failSeq[2] = true

	ms, err := multisink.New(multisink.Options{
		Dir:   stateDir,
		Sinks: []multisink.Sink{good, bad},
	})
	require.NoError(t, err)

	in := make(chan scrapemate.Result, 3)

	in <- resultWith(1, "https://example.com/1", map[string]string{"k": "v1"})

	in <- resultWith(2, "https://example.com/2", map[string]string{"k": "v2"})

	in <- resultWith(3, "https://example.com/3", map[string]string{"k": "v3"})

	close(in)

	require.NoError(t, ms.Run(context.Background(), in))

	// the healthy sink received everything
	require.Equal(t, 3, good.count())
	// the failing sink got the two healthy records and recorded seq 2 missing
	require.Equal(t, 2, bad.count())

	stats := ms.Stats()
	statsByName := map[string]multisink.Stats{}

	for _, st := range stats {
		statsByName[st.Name] = st
	}

	require.False(t, statsByName["good"].Failed)
	require.True(t, statsByName["bad"].Failed)
	require.Equal(t, int64(1), statsByName["bad"].Missing)
	require.Contains(t, statsByName["bad"].LastError, "simulated sink failure")

	require.NoError(t, ms.Close())

	// the ledger records exactly which result was not written to the bad sink
	ledgerPath := filepath.Join(stateDir, "bad.ledger.jsonl")
	content, err := os.ReadFile(ledgerPath)
	require.NoError(t, err)
	require.Contains(t, string(content), `"seq":2`)
}

func TestBackfillNextRunDoesNotDuplicate(t *testing.T) {
	t.Parallel()

	stateDir := t.TempDir()

	bad := newMemorySink("bad")
	bad.failSeq[2] = true

	first, err := multisink.New(multisink.Options{
		Dir:   stateDir,
		Sinks: []multisink.Sink{bad},
	})
	require.NoError(t, err)

	in := make(chan scrapemate.Result, 3)

	in <- resultWith(1, "https://example.com/1", "a")

	in <- resultWith(2, "https://example.com/2", "b")

	in <- resultWith(3, "https://example.com/3", "c")

	close(in)

	require.NoError(t, first.Run(context.Background(), in))
	require.Equal(t, 2, bad.count())
	require.NoError(t, first.Close())

	// second run: the sink now accepts everything. Back-fill must write the
	// missing seq exactly once and never duplicate existing lines.
	recovered := newMemorySink("bad")
	// start from the same in-memory content to simulate a repaired sink that
	// retained its previous lines
	recovered.written = bad.written

	second, err := multisink.New(multisink.Options{
		Dir:   stateDir,
		Sinks: []multisink.Sink{recovered},
	})
	require.NoError(t, err)

	stats := second.Stats()
	require.Len(t, stats, 1)
	require.Equal(t, int64(1), stats[0].Backfilled)
	require.Equal(t, int64(3), stats[0].Written)
	require.Equal(t, int64(0), stats[0].Missing)
	require.Equal(t, 3, recovered.count())

	require.NoError(t, second.Close())

	// the ledger entry for seq 2 must have been cleared
	ledgerPath := filepath.Join(stateDir, "bad.ledger.jsonl")
	content, err := os.ReadFile(ledgerPath)
	require.NoError(t, err)
	require.NotContains(t, string(content), `"seq":2`)
}

func TestFileSinkIdempotentAndTraceable(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "out.jsonl")

	sink, err := multisink.NewFileSink("jsonl", path)
	require.NoError(t, err)

	env := multisink.Envelope{
		Seq:        42,
		SourceURL:  "https://example.com/source",
		JobID:      "job-42",
		ParentID:   "parent-7",
		AtUnixNano: 123456,
		Data:       map[string]string{"hello": "world"},
	}

	require.NoError(t, sink.Write(context.Background(), env))
	// duplicate write must not produce a second line
	require.NoError(t, sink.Write(context.Background(), env))
	require.NoError(t, sink.Close())

	lines := readLines(t, path)
	require.Len(t, lines, 1)
	require.Contains(t, lines[0], `"seq":42`)
	require.Contains(t, lines[0], `"source_url":"https://example.com/source"`)
	require.Contains(t, lines[0], `"job_id":"job-42"`)
	require.Contains(t, lines[0], `"parent_id":"parent-7"`)

	// re-opening discovers the existing sequence and reports Has()
	reopened, err := multisink.NewFileSink("jsonl", path)
	require.NoError(t, err)

	has, err := reopened.Has(42)
	require.NoError(t, err)
	require.True(t, has)

	has, err = reopened.Has(43)
	require.NoError(t, err)
	require.False(t, has)
	require.NoError(t, reopened.Close())
}

func TestNewRejectsEmptySinksAndUnwritableDir(t *testing.T) {
	t.Parallel()

	_, err := multisink.New(multisink.Options{Dir: t.TempDir(), Sinks: nil})
	require.Error(t, err)

	// a path that exists as a file cannot be a state directory
	filePath := filepath.Join(t.TempDir(), "afile")
	require.NoError(t, os.WriteFile(filePath, []byte("x"), 0o600))

	_, err = multisink.New(multisink.Options{
		Dir:   filePath,
		Sinks: []multisink.Sink{newMemorySink("x")},
	})
	require.Error(t, err)
}

func readLines(t *testing.T, path string) []string {
	t.Helper()

	content, err := os.ReadFile(path)
	require.NoError(t, err)

	var out []string

	start := 0

	for i, b := range content {
		if b == '\n' {
			out = append(out, string(content[start:i]))
			start = i + 1
		}
	}

	return out
}
