package scraperun

import (
	"time"

	"github.com/gosom/scrapemate/adapters/writers/multisink"
)

// Stats is the recomputable run statistics. Every counter can be re-derived by
// scanning the queue files, result log and sink ledgers.
type Stats struct {
	// Enqueued counts every enqueue attempt (including seeds).
	Enqueued int64 `json:"enqueued"`
	// DedupSkipped counts enqueues rejected by normalized-key de-duplication.
	DedupSkipped int64 `json:"dedup_skipped"`
	// Completed counts jobs that finished successfully.
	Completed int64 `json:"completed"`
	// NetworkFailed counts failures classified as network-side.
	NetworkFailed int64 `json:"network_failed"`
	// RejectedFailed counts failures classified as site refusals.
	RejectedFailed int64 `json:"rejected_failed"`
	// ParseFailed counts failures classified as parse/structure problems.
	ParseFailed int64 `json:"parse_failed"`
	// Requeued counts how many times jobs were put back into the queue.
	Requeued int64 `json:"requeued"`
	// Abandoned counts jobs moved to the durable failure list.
	Abandoned int64 `json:"abandoned"`
	// Sinks holds per-output-target write statistics.
	Sinks []multisink.Stats `json:"sinks"`
	// Hosts holds per-host scheduling counters.
	Hosts []HostCounters `json:"hosts"`
	// StartedAt / FinishedAt bracket the run.
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
}
