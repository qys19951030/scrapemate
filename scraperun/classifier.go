package scraperun

import (
	"errors"
	"net/http"

	"github.com/gosom/scrapemate"
)

// Sentinel errors used to classify failures. They are exported so callers can
// wrap them from custom fetchers/jobs to force a specific classification.
var (
	// ErrNetworkFailure wraps transport-level failures: no content could be
	// obtained.
	ErrNetworkFailure = errors.New("network failure")
	// ErrSiteRejected wraps explicit refusals by the target site.
	ErrSiteRejected = errors.New("site rejected the request")
	// ErrContentParse wraps content that was fetched but could not be parsed
	// into the expected structure.
	ErrContentParse = errors.New("content parse failure")
)

// Classify maps a failed job execution to one of the three failure kinds.
//
// The classification inspects, in order:
//  1. wrapped sentinel errors (ErrNetworkFailure/ErrSiteRejected/ErrContentParse);
//  2. the recorded response: a transport error with no status => network,
//     a 4xx/5xx status => rejected, otherwise => parse;
//  3. when no response is available, a non-nil error defaults to network.
func Classify(job scrapemate.IJob, resp *scrapemate.Response, err error) FailureKind {
	return classify(job, resp, err)
}

func classify(_ scrapemate.IJob, resp *scrapemate.Response, err error) FailureKind {
	switch {
	case errors.Is(err, ErrNetworkFailure):
		return FailureNetwork
	case errors.Is(err, ErrSiteRejected):
		return FailureRejected
	case errors.Is(err, ErrContentParse):
		return FailureParse
	}

	if resp != nil {
		switch {
		case resp.StatusCode >= http.StatusBadRequest:
			// any explicit 4xx/5xx (including 401/403/429/5xx) is a refusal
			return FailureRejected
		case resp.Error != nil && resp.StatusCode == 0:
			// transport never produced a response status
			return FailureNetwork
		default:
			// content arrived but parsing/processing could not use it
			return FailureParse
		}
	}

	// No response and no sentinel: the safest distinction is that the content
	// never made it back, which is a network-side failure.
	return FailureNetwork
}

func failureKindName(k FailureKind) string {
	switch k {
	case FailureUnspecified:
		return "unspecified"
	case FailureNetwork:
		return "network"
	case FailureRejected:
		return "rejected"
	case FailureParse:
		return "parse"
	default:
		return "unknown"
	}
}
