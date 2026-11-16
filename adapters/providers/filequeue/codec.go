package filequeue

import (
	"crypto/md5" //nolint:gosec // non-cryptographic content key
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"strings"

	"github.com/gosom/scrapemate"
	"github.com/gosom/scrapemate/urlnorm"
)

// registry maps custom job kinds to their reconstruction factories.
type registry map[Kind]Factory

func newRegistry() registry {
	return make(registry)
}

func (r registry) register(kind Kind, factory Factory) {
	r[kind] = factory
}

func (r registry) build(kind Kind) scrapemate.IJob {
	if factory, ok := r[kind]; ok {
		return factory()
	}

	return &scrapemate.Job{}
}

// durableKey computes the normalized de-duplication key for a job.
//
// The key is built from the HTTP method and the normalized URL. POST jobs
// additionally include their body hash because the same URL with different
// bodies is a different job.
func durableKey(job scrapemate.IJob) (string, error) {
	fullURL := job.GetFullURL()

	normalized, err := urlnorm.Normalize(fullURL)
	if err != nil {
		return "", fmt.Errorf("filequeue: normalize job url: %w", err)
	}

	raw := strings.ToUpper(job.GetMethod()) + " " + normalized

	if strings.EqualFold(job.GetMethod(), http.MethodPost) && len(job.GetBody()) > 0 {
		sum := md5.Sum(job.GetBody()) //nolint:gosec // non-cryptographic content key
		raw += " body:" + hex.EncodeToString(sum[:])
	}

	sum := md5.Sum([]byte(raw)) //nolint:gosec // non-cryptographic content key

	return hex.EncodeToString(sum[:]), nil
}

// keyForURL normalizes a raw URL without a job, used by pre-flight checks.
func keyForURL(rawURL string) (string, error) {
	normalized, err := urlnorm.Normalize(rawURL)
	if err != nil {
		return "", err
	}

	sum := md5.Sum([]byte(http.MethodGet + " " + normalized)) //nolint:gosec // non-cryptographic content key

	return hex.EncodeToString(sum[:]), nil
}

// kindOf returns the durable kind for a job.
func kindOf(job scrapemate.IJob) Kind {
	if keeper, ok := job.(KindKeeper); ok {
		if k := keeper.Kind(); k != "" {
			return k
		}
	}

	return KindBase
}

// encodeJobRecord turns a job into its persisted record. Only the exported,
// serializable fields of the embedded scrapemate.Job survive; the
// CheckResponse callback is not persistable.
func encodeJobRecord(seq int64, key string, attempt int, job scrapemate.IJob) jobRecord {
	base := extractBase(job)

	return jobRecord{
		Kind:    kindOf(job),
		Seq:     seq,
		Key:     key,
		Attempt: attempt,
		Job: persistentJob{
			ID:             base.ID,
			ParentID:       base.ParentID,
			Method:         base.Method,
			Body:           base.Body,
			URL:            base.URL,
			Headers:        base.Headers,
			URLParams:      base.URLParams,
			Timeout:        base.Timeout,
			Priority:       base.Priority,
			MaxRetries:     base.MaxRetries,
			RetryPolicy:    base.RetryPolicy,
			MaxRetryDelay:  base.MaxRetryDelay,
			TakeScreenshot: base.TakeScreenshot,
		},
	}
}

// extractBase copies the serializable fields of any IJob into a plain
// scrapemate.Job. Custom jobs embed scrapemate.Job, so a type assertion covers
// every supported implementation.
func extractBase(job scrapemate.IJob) scrapemate.Job {
	if base, ok := job.(*scrapemate.Job); ok {
		return *base
	}

	// Fall back to the accessor surface so external IJob implementations are
	// still persistable.
	return scrapemate.Job{
		ID:             job.GetID(),
		ParentID:       job.GetParentID(),
		Method:         job.GetMethod(),
		Body:           job.GetBody(),
		URL:            job.GetURL(),
		Headers:        job.GetHeaders(),
		URLParams:      job.GetURLParams(),
		Timeout:        job.GetTimeout(),
		Priority:       job.GetPriority(),
		MaxRetries:     job.GetMaxRetries(),
		RetryPolicy:    job.GetRetryPolicy(),
		MaxRetryDelay:  job.GetMaxRetryDelay(),
		TakeScreenshot: job.DoScreenshot(),
	}
}

// decodeJobRecord reconstructs a job from a persisted record. A registered
// factory provides the concrete type; the persisted base fields are copied
// onto its embedded scrapemate.Job via reflection so custom job kinds round
// trip correctly.
func decodeJobRecord(reg registry, rec *jobRecord) scrapemate.IJob {
	base := scrapemate.Job{
		ID:             rec.Job.ID,
		ParentID:       rec.Job.ParentID,
		Method:         rec.Job.Method,
		Body:           rec.Job.Body,
		URL:            rec.Job.URL,
		Headers:        rec.Job.Headers,
		URLParams:      rec.Job.URLParams,
		Timeout:        rec.Job.Timeout,
		Priority:       rec.Job.Priority,
		MaxRetries:     rec.Job.MaxRetries,
		RetryPolicy:    rec.Job.RetryPolicy,
		MaxRetryDelay:  rec.Job.MaxRetryDelay,
		TakeScreenshot: rec.Job.TakeScreenshot,
	}

	if rec.Kind == KindBase {
		return &base
	}

	job := reg.build(rec.Kind)
	if !assignEmbeddedJob(job, &base) {
		// Custom factory returned something without an embedded *Job: best
		// effort is to hand back the base job so the crawl can continue.
		return &base
	}

	return job
}

// assignEmbeddedJob copies base into the embedded scrapemate.Job field of a
// custom job instance. It supports both pointer and value embedding.
func assignEmbeddedJob(target scrapemate.IJob, base *scrapemate.Job) bool {
	v := reflect.ValueOf(target)
	if v.Kind() != reflect.Pointer || v.IsNil() {
		return false
	}

	elem := v.Elem()
	baseType := reflect.TypeOf(*base)

	for i := 0; i < elem.NumField(); i++ {
		field := elem.Field(i)

		if field.Type() == baseType && field.CanSet() {
			field.Set(reflect.ValueOf(*base))

			return true
		}
	}

	// Last resort: if the target itself is *scrapemate.Job.
	if elem.Type() == baseType && elem.CanSet() {
		elem.Set(reflect.ValueOf(*base))

		return true
	}

	return false
}

// fullURL rebuilds the full URL (including URL params) from a persisted job.
func fullURL(j *persistentJob) string {
	if len(j.URLParams) == 0 {
		return j.URL
	}

	values := url.Values{}
	for k, v := range j.URLParams {
		values.Set(k, v)
	}

	return j.URL + "?" + values.Encode()
}
