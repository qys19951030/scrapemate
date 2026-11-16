package scraperun_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gosom/scrapemate"
	"github.com/gosom/scrapemate/adapters/providers/filequeue"
	"github.com/gosom/scrapemate/adapters/writers/multisink"
	"github.com/gosom/scrapemate/scraperun"
)

// simpleJob is a plain GET job that yields one data row.
type simpleJob struct {
	scrapemate.Job
}

const simpleJobKind filequeue.Kind = "scraperun_test.simpleJob"

func init() {
	filequeue.RegisterFactory(simpleJobKind, func() scrapemate.IJob {
		return &simpleJob{}
	})
}

func (j *simpleJob) Kind() filequeue.Kind { return simpleJobKind }

func (j *simpleJob) Process(_ context.Context, _ *scrapemate.Response) (any, []scrapemate.IJob, error) {
	return map[string]string{"url": j.GetURL()}, nil, nil
}

func newSimpleJob(id, rawURL string) *simpleJob {
	return &simpleJob{
		Job: scrapemate.Job{
			ID:     id,
			Method: http.MethodGet,
			URL:    rawURL,
		},
	}
}

// discovererJob discovers the same child link twice from its own URL. Because
// the children are derived inside Process (from persisted fields, exactly like
// a real crawler), they survive queue persistence; emitting the same
// normalized link twice must produce one enqueue and one de-dup skip.
type discovererJob struct {
	scrapemate.Job
}

const discovererJobKind filequeue.Kind = "scraperun_test.discovererJob"

func init() {
	filequeue.RegisterFactory(discovererJobKind, func() scrapemate.IJob {
		return &discovererJob{}
	})
}

func (j *discovererJob) Kind() filequeue.Kind { return discovererJobKind }

func (j *discovererJob) Process(_ context.Context, _ *scrapemate.Response) (any, []scrapemate.IJob, error) {
	child := newSimpleJob("child", "https://example.com/child")

	return nil, []scrapemate.IJob{child, child}, nil
}

// parseFailJob always fails inside Process, simulating content that arrived
// but could not yield the expected structure.
type parseFailJob struct {
	scrapemate.Job
}

const parseFailJobKind filequeue.Kind = "scraperun_test.parseFailJob"

func init() {
	filequeue.RegisterFactory(parseFailJobKind, func() scrapemate.IJob {
		return &parseFailJob{}
	})
}

func (j *parseFailJob) Kind() filequeue.Kind { return parseFailJobKind }

func (j *parseFailJob) Process(_ context.Context, _ *scrapemate.Response) (any, []scrapemate.IJob, error) {
	return nil, nil, errors.New("could not parse expected structure")
}

func baseConfig(queueDir, stateDir string) scraperun.Config {
	return scraperun.Config{
		QueueDir: queueDir,
		StateDir: stateDir,
		Scheduler: scraperun.SchedulerConfig{
			GlobalConcurrency:  2,
			PerHostConcurrency: 2,
			MinHostInterval:    0,
		},
		Failures: scraperun.FailurePolicy{
			Network:  scraperun.ActionRequeue,
			Rejected: scraperun.ActionAbandon,
			Parse:    scraperun.ActionAbandon,
		},
		MaxAttempts:      2,
		ExitOnInactivity: 3 * time.Second,
	}
}

func TestPreflightFailures(t *testing.T) {
	fetcher := newScriptedFetcher()

	t.Run("queue dir not writable", func(t *testing.T) {
		stateDir := t.TempDir()
		blocked := filepath.Join(t.TempDir(), "afile")
		require.NoError(t, os.WriteFile(blocked, []byte("x"), 0o600))

		mate, err := scraperun.NewMate(fetcher, staticParser{}, nil)
		require.NoError(t, err)

		sinks, err := multisink.New(multisink.Options{
			Dir:   stateDir,
			Sinks: []multisink.Sink{newNamedMemorySink("s")},
		})
		require.NoError(t, err)

		cfg := baseConfig(blocked, stateDir)
		_, err = scraperun.New(cfg, mate, sinks)
		require.Error(t, err)
		require.ErrorIs(t, err, scraperun.ErrQueueDirNotWritable())
	})

	t.Run("state dir not writable", func(t *testing.T) {
		queueDir := t.TempDir()
		blocked := filepath.Join(t.TempDir(), "afile")
		require.NoError(t, os.WriteFile(blocked, []byte("x"), 0o600))

		mate, err := scraperun.NewMate(fetcher, staticParser{}, nil)
		require.NoError(t, err)

		sinks, err := multisink.New(multisink.Options{
			Dir:   t.TempDir(),
			Sinks: []multisink.Sink{newNamedMemorySink("s")},
		})
		require.NoError(t, err)

		cfg := baseConfig(queueDir, blocked)
		_, err = scraperun.New(cfg, mate, sinks)
		require.ErrorIs(t, err, scraperun.ErrStateDirNotWritable())
	})

	t.Run("empty output sink list", func(t *testing.T) {
		_, err := multisink.New(multisink.Options{
			Dir:   t.TempDir(),
			Sinks: nil,
		})
		require.Error(t, err)
		require.ErrorIs(t, err, multisink.ErrNoSinks)
	})

	t.Run("invalid per host concurrency", func(t *testing.T) {
		mate, _ := scraperun.NewMate(fetcher, staticParser{}, nil)
		sinks, _ := multisink.New(multisink.Options{
			Dir:   t.TempDir(),
			Sinks: []multisink.Sink{newNamedMemorySink("s")},
		})

		cfg := baseConfig(t.TempDir(), t.TempDir())
		cfg.Scheduler.PerHostConcurrency = -1

		_, err := scraperun.New(cfg, mate, sinks)
		require.ErrorIs(t, err, scraperun.ErrInvalidHostConcurrency())
	})

	t.Run("invalid max attempts", func(t *testing.T) {
		mate, _ := scraperun.NewMate(fetcher, staticParser{}, nil)
		sinks, _ := multisink.New(multisink.Options{
			Dir:   t.TempDir(),
			Sinks: []multisink.Sink{newNamedMemorySink("s")},
		})

		cfg := baseConfig(t.TempDir(), t.TempDir())
		cfg.MaxAttempts = 0

		_, err := scraperun.New(cfg, mate, sinks)
		require.ErrorIs(t, err, scraperun.ErrInvalidMaxAttempts())
	})

	t.Run("invalid concurrency", func(t *testing.T) {
		mate, _ := scraperun.NewMate(fetcher, staticParser{}, nil)
		sinks, _ := multisink.New(multisink.Options{
			Dir:   t.TempDir(),
			Sinks: []multisink.Sink{newNamedMemorySink("s")},
		})

		cfg := baseConfig(t.TempDir(), t.TempDir())
		cfg.Scheduler.GlobalConcurrency = 0

		_, err := scraperun.New(cfg, mate, sinks)
		require.ErrorIs(t, err, scraperun.ErrInvalidConcurrency())
	})

	t.Run("invalid interval", func(t *testing.T) {
		mate, _ := scraperun.NewMate(fetcher, staticParser{}, nil)
		sinks, _ := multisink.New(multisink.Options{
			Dir:   t.TempDir(),
			Sinks: []multisink.Sink{newNamedMemorySink("s")},
		})

		cfg := baseConfig(t.TempDir(), t.TempDir())
		cfg.Scheduler.MinHostInterval = -time.Second

		_, err := scraperun.New(cfg, mate, sinks)
		require.ErrorIs(t, err, scraperun.ErrInvalidInterval())
	})

	t.Run("invalid seed url", func(t *testing.T) {
		queueDir, stateDir := t.TempDir(), t.TempDir()
		mate, _ := scraperun.NewMate(fetcher, staticParser{}, nil)
		sinks, _ := multisink.New(multisink.Options{
			Dir:   stateDir,
			Sinks: []multisink.Sink{newNamedMemorySink("s")},
		})

		runner, err := scraperun.New(baseConfig(queueDir, stateDir), mate, sinks)
		require.NoError(t, err)

		bad := newSimpleJob("bad", "ftp://not-http/something")
		err = runner.Run(context.Background(), bad)
		require.ErrorIs(t, err, scraperun.ErrInvalidSeedURL())
	})
}

func TestThreeFailureClassesAndAbandonList(t *testing.T) {
	queueDir, stateDir := t.TempDir(), t.TempDir()

	fetcher := newScriptedFetcher()
	// network: transport error, no status
	fetcher.failWith("network", errors.New("dial tcp: connection refused"))
	// rejected: explicit 403
	fetcher.on("rejected", &scrapemate.Response{StatusCode: http.StatusForbidden, Body: []byte("forbidden")})

	mate, err := scraperun.NewMate(fetcher, staticParser{}, nil)
	require.NoError(t, err)

	sinks, err := multisink.New(multisink.Options{
		Dir:   stateDir,
		Sinks: []multisink.Sink{newNamedMemorySink("mem")},
	})
	require.NoError(t, err)

	cfg := baseConfig(queueDir, stateDir)
	// network requeues up to MaxAttempts then abandons; rejected/parse abandon immediately
	cfg.Failures.Network = scraperun.ActionRequeue
	cfg.MaxAttempts = 2

	runner, err := scraperun.New(cfg, mate, sinks)
	require.NoError(t, err)

	seeds := []scrapemate.IJob{
		newSimpleJob("net", "https://a.example/network"),
		newSimpleJob("rej", "https://b.example/rejected"),
		&parseFailJob{Job: scrapemate.Job{ID: "parse", Method: http.MethodGet, URL: "https://c.example/ok"}},
	}

	require.NoError(t, runner.Run(context.Background(), seeds...))

	stats := runner.Stats()
	require.Equal(t, int64(1), stats.NetworkFailed, "network attempts are counted as network failures")
	require.Equal(t, int64(1), stats.RejectedFailed)
	require.Equal(t, int64(1), stats.ParseFailed)
	require.Equal(t, int64(3), stats.Abandoned, "all three jobs end in the failure list")
	require.Equal(t, int64(1), stats.Requeued, "the network job was retried once")

	failures := runner.Failures()
	require.Len(t, failures, 3)

	kinds := map[scraperun.FailureKind]int{}

	for _, f := range failures {
		kinds[scraperun.FailureKind(f.Kind)]++
	}

	require.Equal(t, 1, kinds[scraperun.FailureNetwork])
	require.Equal(t, 1, kinds[scraperun.FailureRejected])
	require.Equal(t, 1, kinds[scraperun.FailureParse])
}

func TestResumeRunsNoCompletedJobTwice(t *testing.T) {
	queueDir, stateDir := t.TempDir(), t.TempDir()

	fetcher := newScriptedFetcher()

	cfg := baseConfig(queueDir, stateDir)
	cfg.ExitOnInactivity = time.Second

	buildRunner := func() (*scraperun.Runner, *scraperun.Mate) {
		mate, err := scraperun.NewMate(fetcher, staticParser{}, nil)
		require.NoError(t, err)

		sinks, err := multisink.New(multisink.Options{
			Dir:   stateDir,
			Sinks: []multisink.Sink{newNamedMemorySink("mem")},
		})
		require.NoError(t, err)

		runner, err := scraperun.New(cfg, mate, sinks)
		require.NoError(t, err)

		return runner, mate
	}

	first, _ := buildRunner()
	require.NoError(t, first.Run(context.Background(),
		newSimpleJob("a", "https://example.com/alpha"),
		newSimpleJob("b", "https://example.com/beta"),
	))

	afterFirst := fetcher.fetchCount("/alpha")
	require.GreaterOrEqual(t, afterFirst, int32(1))

	// restart with the same durable state: completed jobs must not be fetched again
	second, _ := buildRunner()
	require.NoError(t, second.Run(context.Background(),
		newSimpleJob("a", "https://example.com/alpha"),
	))

	require.Equal(t, afterFirst, fetcher.fetchCount("/alpha"),
		"completed job a is never re-executed after restart")
	require.Equal(t, int64(2), second.Stats().Completed)
}

func TestStatisticsMatchDiskRecompute(t *testing.T) {
	queueDir, stateDir := t.TempDir(), t.TempDir()

	fetcher := newScriptedFetcher()
	fetcher.failWith("flaky", errors.New("timeout"))

	mate, err := scraperun.NewMate(fetcher, staticParser{}, nil)
	require.NoError(t, err)

	sinks, err := multisink.New(multisink.Options{
		Dir:   stateDir,
		Sinks: []multisink.Sink{newNamedMemorySink("mem")},
	})
	require.NoError(t, err)

	cfg := baseConfig(queueDir, stateDir)
	cfg.MaxAttempts = 1

	runner, err := scraperun.New(cfg, mate, sinks)
	require.NoError(t, err)

	require.NoError(t, runner.Run(context.Background(),
		newSimpleJob("good", "https://example.com/good"),
		newSimpleJob("bad", "https://example.com/flaky"),
	))

	live := runner.Stats()

	recomputed, err := scraperun.RecomputeFromDisk(context.Background(), queueDir, stateDir)
	require.NoError(t, err)

	require.Equal(t, live.Enqueued, recomputed.Enqueued)
	require.Equal(t, live.Completed, recomputed.Completed)
	require.Equal(t, live.Abandoned, recomputed.Abandoned)
	require.Equal(t, live.NetworkFailed, recomputed.NetworkFailed)

	// the two report files exist and the JSON one parses back to the same counters
	jsonPath := filepath.Join(stateDir, "stats.json")
	textPath := filepath.Join(stateDir, "stats.txt")

	require.FileExists(t, jsonPath)
	require.FileExists(t, textPath)

	require.NoError(t, scraperun.VerifyReports(context.Background(), queueDir, stateDir, live))

	// the human and structured reports must expose the same counters
	var fromJSON scraperun.Stats

	rawJSON, err := os.ReadFile(jsonPath)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(rawJSON, &fromJSON))
	require.Equal(t, live.Enqueued, fromJSON.Enqueued)
	require.Equal(t, live.Completed, fromJSON.Completed)
	require.Equal(t, live.Abandoned, fromJSON.Abandoned)
	require.Equal(t, live.NetworkFailed, fromJSON.NetworkFailed)

	rawText, err := os.ReadFile(textPath)
	require.NoError(t, err)

	text := string(rawText)
	require.Contains(t, text, fmt.Sprintf("enqueued:            %d", live.Enqueued))
	require.Contains(t, text, fmt.Sprintf("completed:           %d", live.Completed))
	require.Contains(t, text, fmt.Sprintf("abandoned:           %d", live.Abandoned))
	require.Contains(t, text, fmt.Sprintf("network_failed:      %d", live.NetworkFailed))
}

func TestResultRecordTracesBackToSourceAndSeq(t *testing.T) {
	queueDir, stateDir := t.TempDir(), t.TempDir()

	fetcher := newScriptedFetcher()

	mate, err := scraperun.NewMate(fetcher, staticParser{}, nil)
	require.NoError(t, err)

	sink := newNamedMemorySink("trace")

	sinks, err := multisink.New(multisink.Options{
		Dir:   stateDir,
		Sinks: []multisink.Sink{sink},
	})
	require.NoError(t, err)

	runner, err := scraperun.New(baseConfig(queueDir, stateDir), mate, sinks)
	require.NoError(t, err)

	require.NoError(t, runner.Run(context.Background(),
		newSimpleJob("src", "https://example.com/source-page"),
	))

	envelopes := sink.envelopes()
	require.Len(t, envelopes, 1)

	env := envelopes[0]
	require.Equal(t, "https://example.com/source-page", env.SourceURL)
	require.Greater(t, env.Seq, int64(0))
	require.Equal(t, "src", env.JobID)
}

func TestDiscoveredLinksAreEnqueuedAndDeduplicated(t *testing.T) {
	queueDir, stateDir := t.TempDir(), t.TempDir()

	fetcher := newScriptedFetcher()

	mate, err := scraperun.NewMate(fetcher, staticParser{}, nil)
	require.NoError(t, err)

	sinks, err := multisink.New(multisink.Options{
		Dir:   stateDir,
		Sinks: []multisink.Sink{newNamedMemorySink("mem")},
	})
	require.NoError(t, err)

	runner, err := scraperun.New(baseConfig(queueDir, stateDir), mate, sinks)
	require.NoError(t, err)

	parent := &discovererJob{
		Job: scrapemate.Job{
			ID:     "parent",
			Method: http.MethodGet,
			URL:    "https://example.com/parent",
		},
	}

	require.NoError(t, runner.Run(context.Background(), parent))

	stats := runner.Stats()
	// parent + a single child: the duplicate child discovery is de-duplicated.
	require.Equal(t, int64(2), stats.Enqueued)
	require.Equal(t, int64(1), stats.DedupSkipped)
	require.Equal(t, int64(2), stats.Completed)
}

// newNamedMemorySink returns an in-memory sink used by the runner tests.
func newNamedMemorySink(name string) *namedMemorySink {
	return &namedMemorySink{name: name, items: make(map[int64]multisink.Envelope)}
}

type namedMemorySink struct {
	name  string
	items map[int64]multisink.Envelope
}

func (s *namedMemorySink) Name() string { return s.name }

func (s *namedMemorySink) Write(_ context.Context, env multisink.Envelope) error { //nolint:gocritic // matches the public Sink contract
	s.items[env.Seq] = env

	return nil
}

func (s *namedMemorySink) Has(seq int64) (bool, error) {
	_, ok := s.items[seq]

	return ok, nil
}

func (s *namedMemorySink) Close() error { return nil }

func (s *namedMemorySink) envelopes() []multisink.Envelope {
	out := make([]multisink.Envelope, 0, len(s.items))

	for _, env := range s.items {
		out = append(out, env)
	}

	return out
}

var _ = fmt.Sprintf
