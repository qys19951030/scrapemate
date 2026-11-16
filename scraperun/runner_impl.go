package scraperun

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gosom/kit/logging"
	"github.com/gosom/scrapemate"
	"github.com/gosom/scrapemate/adapters/providers/filequeue"
	"github.com/gosom/scrapemate/adapters/writers/multisink"
)

// Pre-flight validation errors. They are wrapped with the offending
// configuration path so callers can locate the problem before any job runs.
var (
	errQueueDirNotWritable    = errors.New("scraperun: queue directory is not writable")
	errStateDirNotWritable    = errors.New("scraperun: state directory is not writable")
	errNoSinks                = errors.New("scraperun: output sink list is empty")
	errInvalidConcurrency     = errors.New("scraperun: scheduler.global_concurrency must be >= 1")
	errInvalidHostConcurrency = errors.New("scraperun: scheduler.per_host_concurrency must be >= 1 when set")
	errInvalidInterval        = errors.New("scraperun: scheduler.min_host_interval must be >= 0")
	errInvalidMaxAttempts     = errors.New("scraperun: max_attempts must be >= 1")
	errInvalidSeedURL         = errors.New("scraperun: seed job has an invalid URL")
	errNilMate                = errors.New("scraperun: mate must not be nil")
	errNilSinks               = errors.New("scraperun: sinks must not be nil")
)

// Exported sentinel accessors so callers can use errors.Is on pre-flight
// failures without depending on internal variables.
func ErrQueueDirNotWritable() error    { return errQueueDirNotWritable }
func ErrStateDirNotWritable() error    { return errStateDirNotWritable }
func ErrNoSinks() error                { return errNoSinks }
func ErrInvalidConcurrency() error     { return errInvalidConcurrency }
func ErrInvalidHostConcurrency() error { return errInvalidHostConcurrency }
func ErrInvalidInterval() error        { return errInvalidInterval }
func ErrInvalidMaxAttempts() error     { return errInvalidMaxAttempts }
func ErrInvalidSeedURL() error         { return errInvalidSeedURL }

// runnerImpl holds the runtime wiring.
type runnerImpl struct {
	cfg       Config
	mate      *scrapemate.ScrapeMate
	queue     *filequeue.Queue
	sinks     *multisink.MultiSink
	scheduler *Scheduler
	recorder  *RecordingFetcher
	log       logging.Logger

	inflight atomic.Int64

	errMu sync.Mutex
	errs  map[string]error

	lastActivityMu sync.RWMutex
	lastActivity   time.Time

	startedAt time.Time
}

// leasedJob wraps a queued job so its results carry the durable queue
// sequence while preserving every other IJob behavior. It explicitly
// delegates every IJob method to the inner job rather than embedding the
// interface, because an embedded concrete *scrapemate.Job would otherwise
// shadow a custom job's overridden methods through method promotion.
type leasedJob struct {
	inner scrapemate.IJob
	seq   int64
}

func (j *leasedJob) QueueSeq() int64 { return j.seq }

func (j *leasedJob) String() string                  { return j.inner.String() }
func (j *leasedJob) GetID() string                   { return j.inner.GetID() }
func (j *leasedJob) GetParentID() string             { return j.inner.GetParentID() }
func (j *leasedJob) GetMethod() string               { return j.inner.GetMethod() }
func (j *leasedJob) GetBody() []byte                 { return j.inner.GetBody() }
func (j *leasedJob) GetURL() string                  { return j.inner.GetURL() }
func (j *leasedJob) GetHeaders() map[string]string   { return j.inner.GetHeaders() }
func (j *leasedJob) GetURLParams() map[string]string { return j.inner.GetURLParams() }
func (j *leasedJob) GetFullURL() string              { return j.inner.GetFullURL() }
func (j *leasedJob) GetTimeout() time.Duration       { return j.inner.GetTimeout() }
func (j *leasedJob) GetPriority() int                { return j.inner.GetPriority() }
func (j *leasedJob) DoCheckResponse(resp *scrapemate.Response) bool {
	return j.inner.DoCheckResponse(resp)
}
func (j *leasedJob) GetRetryPolicy() scrapemate.RetryPolicy { return j.inner.GetRetryPolicy() }
func (j *leasedJob) GetMaxRetries() int                     { return j.inner.GetMaxRetries() }
func (j *leasedJob) Process(ctx context.Context, resp *scrapemate.Response) (any, []scrapemate.IJob, error) {
	return j.inner.Process(ctx, resp)
}
func (j *leasedJob) GetMaxRetryDelay() time.Duration { return j.inner.GetMaxRetryDelay() }
func (j *leasedJob) BrowserActions(ctx context.Context, page scrapemate.BrowserPage) scrapemate.Response {
	return j.inner.BrowserActions(ctx, page)
}
func (j *leasedJob) DoScreenshot() bool        { return j.inner.DoScreenshot() }
func (j *leasedJob) GetCacheKey() string       { return j.inner.GetCacheKey() }
func (j *leasedJob) UseInResults() bool        { return j.inner.UseInResults() }
func (j *leasedJob) ProcessOnFetchError() bool { return j.inner.ProcessOnFetchError() }

func newRunner(cfg Config, mate *Mate, sinks *multisink.MultiSink) (*runnerImpl, error) { //nolint:gocritic // mirrors the public New value configuration
	// Validate configuration before wiring so an invalid scheduler or path is
	// reported with its own location rather than a nil-component error.
	if err := validateConfig(&cfg); err != nil {
		return nil, err
	}

	if mate == nil {
		return nil, errNilMate
	}

	if sinks == nil {
		return nil, errNilSinks
	}

	return &runnerImpl{
		cfg:          cfg,
		mate:         mate.mate,
		sinks:        sinks,
		recorder:     mate.recorder,
		errs:         make(map[string]error),
		log:          logging.Get().With("component", "scraperun"),
		lastActivity: time.Now().UTC(),
	}, nil
}

// newRunner builds a runner from a validated configuration.

// validateConfig runs every precondition that must hold before work starts.
func validateConfig(cfg *Config) error {
	if err := ensureWritableDir("queue_dir", cfg.QueueDir, errQueueDirNotWritable); err != nil {
		return err
	}

	if err := ensureWritableDir("state_dir", cfg.StateDir, errStateDirNotWritable); err != nil {
		return err
	}

	if cfg.Scheduler.GlobalConcurrency < 1 {
		return fmt.Errorf("%w (got %d)", errInvalidConcurrency, cfg.Scheduler.GlobalConcurrency)
	}

	if cfg.Scheduler.PerHostConcurrency < 0 {
		return fmt.Errorf("%w (got %d)", errInvalidHostConcurrency, cfg.Scheduler.PerHostConcurrency)
	}

	if cfg.Scheduler.MinHostInterval < 0 {
		return fmt.Errorf("%w (got %s)", errInvalidInterval, cfg.Scheduler.MinHostInterval)
	}

	if cfg.MaxAttempts < 1 {
		return fmt.Errorf("%w (got %d)", errInvalidMaxAttempts, cfg.MaxAttempts)
	}

	return nil
}

// ensureWritableDir creates dir and proves it is writable up front.
func ensureWritableDir(label, dir string, base error) error {
	if dir == "" {
		return fmt.Errorf("%w: %s is empty", base, label)
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("%w: %s=%s: %v", base, label, dir, err)
	}

	probe := filepath.Join(dir, ".write-probe")
	if err := os.WriteFile(probe, []byte("x"), 0o600); err != nil {
		return fmt.Errorf("%w: %s=%s: %v", base, label, dir, err)
	}

	_ = os.Remove(probe)

	return nil
}

// validateSeeds fails fast on a malformed entry URL.
func validateSeeds(seeds []scrapemate.IJob) error {
	for i, job := range seeds {
		if job == nil {
			return fmt.Errorf("%w: seed[%d] is nil", errInvalidSeedURL, i)
		}

		u, err := url.Parse(job.GetFullURL())
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return fmt.Errorf("%w: seed[%d]=%q", errInvalidSeedURL, i, job.GetFullURL())
		}
	}

	return nil
}

func (r *runnerImpl) run(ctx context.Context, seeds []scrapemate.IJob) error {
	if err := validateSeeds(seeds); err != nil {
		return err
	}

	r.startedAt = time.Now().UTC()

	queueOpts := filequeue.Options{
		Dir:   r.cfg.QueueDir,
		Fsync: r.fsyncEnabled(),
	}

	queue, err := filequeue.New(queueOpts)
	if err != nil {
		return err
	}

	r.queue = queue
	defer queue.Close()

	r.scheduler = NewScheduler(r.cfg.Scheduler, r.loadPacing, r.savePacing)

	// seed jobs are enqueued first and de-duplicated like every discovered link
	for i := range seeds {
		if _, err := queue.Enqueue(ctx, seeds[i]); err != nil {
			return fmt.Errorf("scraperun: enqueue seed: %w", err)
		}

		r.touchActivity()
	}

	results := make(chan scrapemate.Result)

	sinkErr := make(chan error, 1)

	go func() {
		sinkErr <- r.sinks.Run(ctx, results)
	}()

	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()

	popCtx, popCancel := context.WithCancel(runCtx)
	defer popCancel()

	workers := r.cfg.Scheduler.GlobalConcurrency

	var wg sync.WaitGroup

	for range workers {
		wg.Add(1)

		go r.worker(runCtx, popCtx, &wg, results)
	}

	stopMonitor := r.startMonitor(runCtx, popCancel)

	wg.Wait()
	close(results)

	if err := <-sinkErr; err != nil && !errors.Is(err, context.Canceled) {
		stopMonitor()
		return err
	}

	stopMonitor()

	// A clean drain cancels the working contexts on purpose; report writing
	// and disk verification must run on an independent context.
	return r.finishReports(context.Background())
}

func (r *runnerImpl) fsyncEnabled() bool {
	if !r.cfg.fsyncSet {
		return true
	}

	return r.cfg.Fsync
}

// worker is the durable crawl loop. A single job failure is classified and
// handled per policy but never stops the other workers. popCtx only gates the
// blocking Pop; the leased job itself runs under runCtx so a drain cannot
// cancel an in-flight job or its durable terminal transition.
func (r *runnerImpl) worker(runCtx, popCtx context.Context, wg *sync.WaitGroup, results chan<- scrapemate.Result) {
	defer wg.Done()

	for {
		lease, err := r.queue.Pop(popCtx)
		if err != nil {
			return
		}

		r.processOne(runCtx, lease, results)

		if r.shouldDrain() {
			return
		}
	}
}

// processOne executes one leased job end to end.
func (r *runnerImpl) processOne(ctx context.Context, lease filequeue.Lease, results chan<- scrapemate.Result) {
	host, err := hostOfJob(lease.Job)
	if err != nil {
		host = "unknown"
	}

	release, err := r.scheduler.Acquire(ctx, host)
	if err != nil {
		// context cancelled while waiting for a slot: requeue so it reruns
		_ = r.queue.Requeue(context.Background(), lease, "scheduler acquire cancelled")

		return
	}

	r.inflight.Add(1)
	r.touchActivity()

	defer func() {
		release()
		r.inflight.Add(-1)
		r.touchActivity()
	}()

	wrapped := &leasedJob{inner: lease.Job, seq: lease.Seq}

	ans, next, jobErr := r.mate.DoJob(ctx, wrapped)
	if jobErr != nil {
		r.recordErr(wrapped, jobErr)
		r.handleFailure(ctx, lease, wrapped)

		return
	}

	if err := r.queue.Complete(ctx, lease); err != nil {
		r.recordErr(wrapped, err)
		r.handleFailure(ctx, lease, wrapped)

		return
	}

	// discovered links enter the queue before anything else and are de-duped
	for i := range next {
		if _, err := r.queue.Enqueue(ctx, next[i]); err != nil {
			continue
		}
	}

	if wrapped.UseInResults() {
		select {
		case results <- scrapemate.Result{Job: wrapped, Data: ans}:
		case <-ctx.Done():
		}
	}
}

// handleFailure classifies the error and applies the configured action.
// Failure transitions use a detached context: drain cancellation only stops
// new pops, it must never lose an in-flight job's terminal state.
func (r *runnerImpl) handleFailure(_ context.Context, lease filequeue.Lease, job *leasedJob) {
	resp, hasResp := r.recorder.ResponseFor(job)

	var respForClassify *scrapemate.Response
	if hasResp {
		respForClassify = &resp
	}

	kind := Classify(job, respForClassify, r.lastErr(job))

	action := r.actionFor(kind)

	exhausted := lease.Attempts+1 >= r.cfg.MaxAttempts

	durableCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if action == ActionAbandon || exhausted {
		failure := filequeue.FailureRecord{
			URL:     job.GetFullURL(),
			Kind:    filequeue.FailureKind(kind),
			Message: r.failureMessage(job),
		}

		if err := r.queue.Abandon(durableCtx, lease, failure); err != nil {
			r.logError("abandon failed job", "error", err, "url", job.GetFullURL())
		}

		r.recorder.drop(job)
		r.clearErr(job)

		return
	}

	if err := r.queue.RequeueWithKind(durableCtx, lease, failureKindName(kind), filequeue.FailureKind(kind)); err != nil {
		r.logError("requeue failed job", "error", err, "url", job.GetFullURL())
	}
}

// logError surfaces a durable state-transition failure without stopping the
// run; the job is retried on the next run if the transition did not persist.
func (r *runnerImpl) logError(msg string, args ...any) {
	r.log.Error(msg, args...)
}

func (r *runnerImpl) recordErr(job *leasedJob, err error) {
	r.errMu.Lock()
	defer r.errMu.Unlock()

	r.errs[job.GetID()] = err
}

func (r *runnerImpl) lastErr(job *leasedJob) error {
	r.errMu.Lock()
	defer r.errMu.Unlock()

	return r.errs[job.GetID()]
}

func (r *runnerImpl) clearErr(job *leasedJob) {
	r.errMu.Lock()
	defer r.errMu.Unlock()

	delete(r.errs, job.GetID())
}

func (r *runnerImpl) failureMessage(job *leasedJob) string {
	if err := r.lastErr(job); err != nil {
		return err.Error()
	}

	if resp, ok := r.recorder.ResponseFor(job); ok && resp.Error != nil {
		return resp.Error.Error()
	}

	return "unknown failure"
}

func (r *runnerImpl) actionFor(kind FailureKind) FailureAction {
	switch kind {
	case FailureUnspecified:
		return ActionRequeue
	case FailureNetwork:
		return r.cfg.Failures.Network
	case FailureRejected:
		return r.cfg.Failures.Rejected
	case FailureParse:
		return r.cfg.Failures.Parse
	default:
		return ActionRequeue
	}
}

// shouldDrain reports true when no job can ever make progress again.
func (r *runnerImpl) shouldDrain() bool {
	return r.inflight.Load() == 0 && r.queue.Pending() == 0
}

func (r *runnerImpl) touchActivity() {
	r.lastActivityMu.Lock()
	r.lastActivity = time.Now().UTC()
	r.lastActivityMu.Unlock()
}

func (r *runnerImpl) startMonitor(ctx context.Context, popCancel context.CancelFunc) func() {
	stop := make(chan struct{})

	var once sync.Once

	stopFn := func() {
		once.Do(func() { close(stop) })
	}

	go func() {
		interval := 100 * time.Millisecond
		if r.cfg.ExitOnInactivity > 0 && r.cfg.ExitOnInactivity < interval {
			interval = r.cfg.ExitOnInactivity / 2
		}

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-stop:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				if r.cfg.ExitOnInactivity > 0 {
					r.lastActivityMu.RLock()
					idle := time.Since(r.lastActivity)
					r.lastActivityMu.RUnlock()

					if idle > r.cfg.ExitOnInactivity {
						popCancel()

						return
					}
				}

				if r.shouldDrain() {
					popCancel()

					return
				}
			}
		}
	}()

	return stopFn
}

func (r *runnerImpl) loadPacing(host string) (time.Time, bool) {
	at, ok := r.queue.LastPacing(host)
	if !ok {
		return time.Time{}, false
	}

	return time.Unix(0, at).UTC(), true
}

func (r *runnerImpl) savePacing(host string, at time.Time) {
	_ = r.queue.RecordPacing(host, at.UnixNano())
}

func (r *runnerImpl) statsSnapshot() Stats {
	if r.queue == nil {
		return Stats{}
	}

	qs := r.queue.Stats()

	stats := Stats{
		Enqueued:       qs.Enqueued,
		DedupSkipped:   qs.DedupSkipped,
		Completed:      qs.Completed,
		NetworkFailed:  qs.NetworkFailed,
		RejectedFailed: qs.RejectedFailed,
		ParseFailed:    qs.ParseFailed,
		Requeued:       qs.Requeued,
		Abandoned:      qs.Abandoned,
		StartedAt:      r.startedAt,
	}

	if r.scheduler != nil {
		stats.Hosts = r.scheduler.HostCounters()
	}

	stats.Sinks = r.sinks.Stats()

	return stats
}

func (r *runnerImpl) finishReports(ctx context.Context) error {
	stats := r.statsSnapshot()
	stats.FinishedAt = time.Now().UTC()

	_, _, err := WriteReports(r.cfg.StateDir, stats)
	if err != nil {
		return err
	}

	return VerifyReports(ctx, r.cfg.QueueDir, r.cfg.StateDir, stats)
}

func (r *runnerImpl) close() error {
	if r.queue != nil {
		return r.queue.Close()
	}

	return nil
}

func hostOfJob(job scrapemate.IJob) (string, error) {
	u, err := url.Parse(job.GetFullURL())
	if err != nil {
		return "", err
	}

	return u.Hostname(), nil
}
