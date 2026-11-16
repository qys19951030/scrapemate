package scraperun

import (
	"context"
	"sync"

	"github.com/gosom/scrapemate"
)

// RecordingFetcher wraps an HTTPFetcher and keeps the most recent response
// produced for each job id. The runner uses the recorded response to classify
// a failure into network/rejected/parse accurately.
type RecordingFetcher struct {
	inner scrapemate.HTTPFetcher

	mu        sync.RWMutex
	responses map[string]scrapemate.Response
}

// NewRecordingFetcher wraps inner so responses can be inspected after DoJob.
func NewRecordingFetcher(inner scrapemate.HTTPFetcher) *RecordingFetcher {
	return &RecordingFetcher{
		inner:     inner,
		responses: make(map[string]scrapemate.Response),
	}
}

// Fetch delegates to the inner fetcher and records the response by job id.
func (f *RecordingFetcher) Fetch(ctx context.Context, job scrapemate.IJob) scrapemate.Response {
	resp := f.inner.Fetch(ctx, job)

	f.mu.Lock()
	f.responses[job.GetID()] = resp
	f.mu.Unlock()

	return resp
}

// Close closes the inner fetcher.
func (f *RecordingFetcher) Close() error {
	return f.inner.Close()
}

// ResponseFor returns the last response recorded for the job.
func (f *RecordingFetcher) ResponseFor(job scrapemate.IJob) (scrapemate.Response, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	resp, ok := f.responses[job.GetID()]

	return resp, ok
}

// drop removes the recorded response for a job, keeping the map bounded.
func (f *RecordingFetcher) drop(job scrapemate.IJob) {
	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.responses, job.GetID())
}
