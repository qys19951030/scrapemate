package scraperun_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/gosom/scrapemate"
)

// scriptedFetcher returns canned responses keyed by URL substring and lets a
// test inject transport errors. It records how often each URL was fetched.
type scriptedFetcher struct {
	mu        sync.Mutex
	responses map[string]*scrapemate.Response
	errors    map[string]error
	fetches   map[string]*atomic.Int32
}

func newScriptedFetcher() *scriptedFetcher {
	return &scriptedFetcher{
		responses: make(map[string]*scrapemate.Response),
		errors:    make(map[string]error),
		fetches:   make(map[string]*atomic.Int32),
	}
}

func (f *scriptedFetcher) on(urlSubstr string, resp *scrapemate.Response) {
	f.responses[urlSubstr] = resp
}

func (f *scriptedFetcher) failWith(urlSubstr string, err error) {
	f.errors[urlSubstr] = err
}

func (f *scriptedFetcher) Fetch(_ context.Context, job scrapemate.IJob) scrapemate.Response {
	full := job.GetFullURL()

	f.mu.Lock()

	counter, ok := f.fetches[full]
	if !ok {
		counter = &atomic.Int32{}
		f.fetches[full] = counter
	}

	f.mu.Unlock()
	counter.Add(1)

	for substr, err := range f.errors {
		if strings.Contains(full, substr) {
			return scrapemate.Response{Error: err}
		}
	}

	for substr, resp := range f.responses {
		if strings.Contains(full, substr) {
			return *resp
		}
	}

	return scrapemate.Response{StatusCode: http.StatusOK, Body: []byte("<html></html>")}
}

func (f *scriptedFetcher) Close() error { return nil }

func (f *scriptedFetcher) fetchCount(urlSubstr string) int32 {
	f.mu.Lock()
	defer f.mu.Unlock()

	for full, counter := range f.fetches {
		if strings.Contains(full, urlSubstr) {
			return counter.Load()
		}
	}

	return 0
}

// staticParser is an HTMLParser that always succeeds (the body is irrelevant
// for these coordination tests).
type staticParser struct{}

func (staticParser) Parse(_ context.Context, _ []byte) (any, error) {
	return struct{}{}, nil
}

// failingParser returns a parse error, exercising the parse-failure class.
type failingParser struct{}

func (failingParser) Parse(_ context.Context, _ []byte) (any, error) {
	return nil, fmt.Errorf("expected structure not found")
}

var _ io.Closer = (*scriptedFetcher)(nil)
