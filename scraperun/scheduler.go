package scraperun

import (
	"context"
	"sync"
	"time"
)

// scheduler enforces the global concurrency, per-host concurrency and
// per-host minimum interval constraints. Per-host pacing is loaded from and
// saved to the durable queue through the injected callbacks, so the minimum
// interval remains effective after a restart.
type Scheduler struct {
	cfg SchedulerConfig

	global chan struct{}

	mu       sync.Mutex
	hosts    map[string]*hostState
	paceLoad func(host string) (time.Time, bool)
	paceSave func(host string, at time.Time)
}

// HostCounters exposes observable per-host scheduling state so that a
// per-host concurrency breach (waiting) is visible.
type HostCounters struct {
	Host           string `json:"host"`
	InFlight       int    `json:"in_flight"`
	LimitWaits     int64  `json:"limit_waits"`
	LastDispatchNs int64  `json:"last_dispatch_unix_nano"`
}

// hostState is the runtime gate for one host.
type hostState struct {
	// sem bounds the number of simultaneous jobs against this host.
	sem chan struct{}
	// lastRun is the last dispatch time observed in this process.
	lastRun time.Time
	// cond wakes waiters when a slot or the interval opens up.
	cond *sync.Cond
	// inFlight is the current number of leased jobs against the host.
	inFlight int
	// limitWaits counts how often a job had to wait for the per-host
	// concurrency limit, making breaches observable.
	limitWaits int64
}

func NewScheduler(cfg SchedulerConfig, paceLoad func(string) (time.Time, bool), paceSave func(string, time.Time)) *Scheduler {
	hostCap := cfg.PerHostConcurrency
	if hostCap <= 0 {
		hostCap = cfg.GlobalConcurrency
	}

	return &Scheduler{
		cfg: SchedulerConfig{
			GlobalConcurrency:  cfg.GlobalConcurrency,
			PerHostConcurrency: hostCap,
			MinHostInterval:    cfg.MinHostInterval,
		},
		global:   make(chan struct{}, cfg.GlobalConcurrency),
		hosts:    make(map[string]*hostState),
		paceLoad: paceLoad,
		paceSave: paceSave,
	}
}

// host returns the gate for a host, creating it and seeding its last-run time
// from the durable pacing log on first use.
func (s *Scheduler) host(hostName string) *hostState {
	s.mu.Lock()
	defer s.mu.Unlock()

	hs, ok := s.hosts[hostName]
	if ok {
		return hs
	}

	hs = &hostState{
		sem: make(chan struct{}, s.cfg.PerHostConcurrency),
	}
	hs.cond = sync.NewCond(&s.mu)

	if s.paceLoad != nil {
		if at, ok := s.paceLoad(hostName); ok {
			hs.lastRun = at
		}
	}

	s.hosts[hostName] = hs

	return hs
}

// acquire blocks until the job for host may run, honoring all three
// constraints, and returns a release function that must be invoked when the
// job finishes. The durable pacing timestamp is written at dispatch time so a
// crash still remembers the rhythm.
func (s *Scheduler) Acquire(ctx context.Context, hostName string) (func(), error) {
	hs := s.host(hostName)

	// 1. global concurrency
	select {
	case s.global <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// 2. per-host concurrency + 3. minimum interval
	waited := false

	s.mu.Lock()

	for {
		if ctx.Err() != nil {
			s.mu.Unlock()
			<-s.global

			return nil, ctx.Err()
		}

		if hs.inFlight >= s.cfg.PerHostConcurrency {
			if !waited {
				hs.limitWaits++
				waited = true
			}

			hs.cond.Wait()

			continue
		}

		if s.cfg.MinHostInterval > 0 && !hs.lastRun.IsZero() {
			earliest := hs.lastRun.Add(s.cfg.MinHostInterval)
			now := time.Now()

			if now.Before(earliest) {
				waitDur := earliest.Sub(now)
				s.mu.Unlock()

				timer := time.NewTimer(waitDur)
				select {
				case <-timer.C:
				case <-ctx.Done():
					timer.Stop()
					<-s.global

					return nil, ctx.Err()
				}

				s.mu.Lock()

				continue
			}
		}

		break
	}

	dispatchAt := time.Now()
	hs.lastRun = dispatchAt
	hs.inFlight++
	s.mu.Unlock()

	hs.sem <- struct{}{}

	if s.paceSave != nil {
		s.paceSave(hostName, dispatchAt)
	}

	release := func() {
		<-hs.sem

		s.mu.Lock()
		hs.inFlight--
		hs.cond.Broadcast()
		s.mu.Unlock()

		<-s.global
	}

	return release, nil
}

// hostCounters returns the observable per-host counters.
func (s *Scheduler) HostCounters() []HostCounters {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]HostCounters, 0, len(s.hosts))

	for hostName, hs := range s.hosts {
		out = append(out, HostCounters{
			Host:           hostName,
			InFlight:       hs.inFlight,
			LimitWaits:     hs.limitWaits,
			LastDispatchNs: hs.lastRun.UnixNano(),
		})
	}

	return out
}
