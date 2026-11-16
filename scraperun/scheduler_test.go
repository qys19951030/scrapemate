package scraperun_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gosom/scrapemate/scraperun"
)

func newTestScheduler(global, perHost int, interval time.Duration) *scraperun.Scheduler {
	pacing := make(map[string]time.Time)

	var mu sync.Mutex

	load := func(host string) (time.Time, bool) {
		mu.Lock()
		defer mu.Unlock()

		at, ok := pacing[host]

		return at, ok
	}

	save := func(host string, at time.Time) {
		mu.Lock()
		defer mu.Unlock()

		pacing[host] = at
	}

	return scraperun.NewScheduler(scraperun.SchedulerConfig{
		GlobalConcurrency:  global,
		PerHostConcurrency: perHost,
		MinHostInterval:    interval,
	}, load, save)
}

func TestScheduler_GlobalConcurrencyLimit(t *testing.T) {
	t.Parallel()

	sched := newTestScheduler(2, 2, 0)

	var inFlight int32

	var maxObserved int32

	var wg sync.WaitGroup

	for range 6 {
		wg.Add(1)

		go func() {
			defer wg.Done()

			release, err := sched.Acquire(context.Background(), "a.example")
			require.NoError(t, err)

			cur := atomic.AddInt32(&inFlight, 1)

			for {
				observed := atomic.LoadInt32(&maxObserved)
				if cur <= observed || atomic.CompareAndSwapInt32(&maxObserved, observed, cur) {
					break
				}
			}

			time.Sleep(20 * time.Millisecond)
			atomic.AddInt32(&inFlight, -1)
			release()
		}()
	}

	wg.Wait()

	require.LessOrEqual(t, maxObserved, int32(2))
	require.Equal(t, int32(2), maxObserved)
}

func TestScheduler_PerHostConcurrencyLimit(t *testing.T) {
	t.Parallel()

	sched := newTestScheduler(4, 1, 0)

	var hostAInFlight int32

	var hostAMax int32

	var wg sync.WaitGroup

	for i := range 6 {
		wg.Add(1)

		host := "a.example"
		if i%2 == 0 {
			host = "b.example"
		}

		go func(host string) {
			defer wg.Done()

			release, err := sched.Acquire(context.Background(), host)
			require.NoError(t, err)

			if host == "a.example" {
				cur := atomic.AddInt32(&hostAInFlight, 1)

				for {
					observed := atomic.LoadInt32(&hostAMax)
					if cur <= observed || atomic.CompareAndSwapInt32(&hostAMax, observed, cur) {
						break
					}
				}

				time.Sleep(15 * time.Millisecond)
				atomic.AddInt32(&hostAInFlight, -1)
			}

			release()
		}(host)
	}

	wg.Wait()

	require.Equal(t, int32(1), hostAMax, "per-host concurrency for a.example must stay at 1")

	counters := sched.HostCounters()
	limitWaitSeen := false

	for _, c := range counters {
		if c.Host == "a.example" && c.LimitWaits > 0 {
			limitWaitSeen = true
		}
	}

	require.True(t, limitWaitSeen, "per-host limit waiting must be observable")
}

func TestScheduler_MinHostInterval(t *testing.T) {
	t.Parallel()

	sched := newTestScheduler(4, 4, 40*time.Millisecond)

	release1, err := sched.Acquire(context.Background(), "slow.example")
	require.NoError(t, err)

	start := time.Now()

	release2, err := sched.Acquire(context.Background(), "slow.example")
	require.NoError(t, err)

	elapsed := time.Since(start)
	require.GreaterOrEqual(t, elapsed, 35*time.Millisecond, "second dispatch must respect min interval")

	release1()
	release2()

	// a different host is not throttled by slow.example's rhythm
	otherStart := time.Now()

	release3, err := sched.Acquire(context.Background(), "other.example")
	require.NoError(t, err)
	require.Less(t, time.Since(otherStart), 15*time.Millisecond)
	release3()
}

func TestScheduler_PacingSurvivesSchedulerRestart(t *testing.T) {
	t.Parallel()

	pacing := make(map[string]time.Time)

	var mu sync.Mutex

	load := func(host string) (time.Time, bool) {
		mu.Lock()
		defer mu.Unlock()

		at, ok := pacing[host]

		return at, ok
	}

	save := func(host string, at time.Time) {
		mu.Lock()
		defer mu.Unlock()

		pacing[host] = at
	}

	cfg := scraperun.SchedulerConfig{
		GlobalConcurrency:  4,
		PerHostConcurrency: 4,
		MinHostInterval:    50 * time.Millisecond,
	}

	first := scraperun.NewScheduler(cfg, load, save)

	release, err := first.Acquire(context.Background(), "paced.example")
	require.NoError(t, err)
	release()

	// simulate a process restart: build a fresh scheduler over the same
	// durable pacing state
	second := scraperun.NewScheduler(cfg, load, save)

	start := time.Now()

	release2, err := second.Acquire(context.Background(), "paced.example")
	require.NoError(t, err)

	defer release2()

	require.GreaterOrEqual(t, time.Since(start), 40*time.Millisecond,
		"min interval must still apply after a restart because pacing is durable")
}

func TestScheduler_ContextCancellation(t *testing.T) {
	t.Parallel()

	sched := newTestScheduler(1, 1, 0)

	release, err := sched.Acquire(context.Background(), "busy.example")
	require.NoError(t, err)

	defer release()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	_, err = sched.Acquire(ctx, "busy.example")
	require.ErrorIs(t, err, context.DeadlineExceeded)
}
