package scraperun

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gosom/scrapemate/adapters/providers/filequeue"
	"github.com/gosom/scrapemate/adapters/writers/multisink"
)

const (
	textReportName = "stats.txt"
	jsonReportName = "stats.json"
	resultLogName  = "results.log"
	ledgerSuffix   = ".ledger.jsonl"
	filePerm       = 0o600
)

// WriteReports emits the statistics in two formats: a human-readable text
// report and a structured JSON report. Both contain the same counters and are
// written atomically. It returns the two report file paths.
func WriteReports(dir string, stats Stats) (textPath, jsonPath string, err error) { //nolint:gocritic // value keeps the public reporting API ergonomic
	return writeReports(dir, &stats)
}

// writeReports emits the statistics in two formats: a human-readable text
// report and a structured JSON report. Both contain the same counters and are
// written atomically.
func writeReports(dir string, stats *Stats) (textPath, jsonPath string, err error) {
	if mkErr := os.MkdirAll(dir, 0o755); mkErr != nil {
		return "", "", fmt.Errorf("scraperun: create report dir: %w", mkErr)
	}

	textPath = filepath.Join(dir, textReportName)
	jsonPath = filepath.Join(dir, jsonReportName)

	if writeErr := atomicWrite(textPath, []byte(renderText(stats))); writeErr != nil {
		return "", "", writeErr
	}

	raw, err := json.MarshalIndent(stats, "", "  ")
	if err != nil {
		return "", "", fmt.Errorf("scraperun: encode json report: %w", err)
	}

	raw = append(raw, '\n')

	if err := atomicWrite(jsonPath, raw); err != nil {
		return "", "", err
	}

	return textPath, jsonPath, nil
}

// renderText produces the human-readable report. The field order matches the
// JSON report so the two can be compared line by line.
func renderText(s *Stats) string {
	var b strings.Builder

	fmt.Fprintf(&b, "scrapemate run statistics\n")
	fmt.Fprintf(&b, "=========================\n")
	fmt.Fprintf(&b, "enqueued:            %d\n", s.Enqueued)
	fmt.Fprintf(&b, "dedup_skipped:       %d\n", s.DedupSkipped)
	fmt.Fprintf(&b, "completed:           %d\n", s.Completed)
	fmt.Fprintf(&b, "network_failed:      %d\n", s.NetworkFailed)
	fmt.Fprintf(&b, "rejected_failed:     %d\n", s.RejectedFailed)
	fmt.Fprintf(&b, "parse_failed:        %d\n", s.ParseFailed)
	fmt.Fprintf(&b, "requeued:            %d\n", s.Requeued)
	fmt.Fprintf(&b, "abandoned:           %d\n", s.Abandoned)

	if !s.StartedAt.IsZero() {
		fmt.Fprintf(&b, "started_at:          %s\n", s.StartedAt.Format(time.RFC3339))
	}

	if !s.FinishedAt.IsZero() {
		fmt.Fprintf(&b, "finished_at:         %s\n", s.FinishedAt.Format(time.RFC3339))
	}

	b.WriteString("\noutput targets\n--------------\n")

	for _, sink := range s.Sinks {
		fmt.Fprintf(&b, "%s: written=%d backfilled=%d missing=%d failed=%t",
			sink.Name, sink.Written, sink.Backfilled, sink.Missing, sink.Failed)

		if sink.LastError != "" {
			fmt.Fprintf(&b, " last_error=%q", sink.LastError)
		}

		b.WriteByte('\n')
	}

	if len(s.Hosts) > 0 {
		b.WriteString("\nper-host scheduling\n-------------------\n")

		for _, host := range s.Hosts {
			fmt.Fprintf(&b, "%s: in_flight=%d limit_waits=%d\n", host.Host, host.InFlight, host.LimitWaits)
		}
	}

	return b.String()
}

// atomicWrite writes data to path via a temp file and rename.
func atomicWrite(path string, data []byte) error {
	tmp := path + ".tmp"

	if err := os.WriteFile(tmp, data, filePerm); err != nil {
		return fmt.Errorf("scraperun: write %s: %w", path, err)
	}

	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("scraperun: finalize %s: %w", path, err)
	}

	return nil
}

// RecomputeFromDisk rescans the queue files, the result log and the sink
// ledgers and returns independently derived statistics.
func RecomputeFromDisk(ctx context.Context, queueDir, stateDir string) (Stats, error) {
	if err := ctx.Err(); err != nil {
		return Stats{}, err
	}

	// The queue already knows how to replay its own WAL into counters.
	queue, err := filequeue.New(filequeue.Options{Dir: queueDir, Fsync: false})
	if err != nil {
		return Stats{}, fmt.Errorf("scraperun: reopen queue for recompute: %w", err)
	}
	defer queue.Close()

	qs := queue.Stats()

	stats := Stats{
		Enqueued:       qs.Enqueued,
		DedupSkipped:   qs.DedupSkipped,
		Completed:      qs.Completed,
		NetworkFailed:  qs.NetworkFailed,
		RejectedFailed: qs.RejectedFailed,
		ParseFailed:    qs.ParseFailed,
		Requeued:       qs.Requeued,
		Abandoned:      qs.Abandoned,
	}

	stats.Sinks = recomputeSinks(stateDir)

	return stats, nil
}

// recomputeSinks counts produced results and outstanding ledger entries from
// the on-disk result log and per-sink ledgers.
func recomputeSinks(stateDir string) []multisink.Stats {
	produced := countJSONLines(filepath.Join(stateDir, resultLogName))

	entries, _ := os.ReadDir(stateDir)

	var out []multisink.Stats

	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ledgerSuffix) {
			continue
		}

		sinkName := strings.TrimSuffix(name, ledgerSuffix)
		missing := countJSONLines(filepath.Join(stateDir, name))

		out = append(out, multisink.Stats{
			Name:    sinkName,
			Written: produced - missing,
			Missing: missing,
			Failed:  missing > 0,
		})
	}

	return out
}

// countJSONLines returns the number of non-empty lines in a JSONL file.
func countJSONLines(path string) int64 {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0
	}

	if len(raw) == 0 {
		return 0
	}

	return int64(strings.Count(strings.TrimRight(string(raw), "\n"), "\n") + 1)
}

// VerifyReports compares the live statistics with a fresh disk recomputation
// and returns an error describing the first mismatching counter.
func VerifyReports(ctx context.Context, queueDir, stateDir string, live Stats) error { //nolint:gocritic // value keeps the public verification API simple
	recomputed, err := RecomputeFromDisk(ctx, queueDir, stateDir)
	if err != nil {
		return err
	}

	checks := []struct {
		name string
		want int64
		got  int64
	}{
		{"enqueued", live.Enqueued, recomputed.Enqueued},
		{"dedup_skipped", live.DedupSkipped, recomputed.DedupSkipped},
		{"completed", live.Completed, recomputed.Completed},
		{"network_failed", live.NetworkFailed, recomputed.NetworkFailed},
		{"rejected_failed", live.RejectedFailed, recomputed.RejectedFailed},
		{"parse_failed", live.ParseFailed, recomputed.ParseFailed},
		{"requeued", live.Requeued, recomputed.Requeued},
		{"abandoned", live.Abandoned, recomputed.Abandoned},
	}

	for _, check := range checks {
		if check.want != check.got {
			return fmt.Errorf("scraperun: stats mismatch on %s: live=%d disk=%d",
				check.name, check.want, check.got)
		}
	}

	return verifySinks(live.Sinks, recomputed.Sinks)
}

// verifySinks reconciles the per-sink missing counts that can be recomputed.
func verifySinks(live, disk []multisink.Stats) error {
	diskByName := make(map[string]multisink.Stats, len(disk))

	for _, s := range disk {
		diskByName[s.Name] = s
	}

	for _, s := range live {
		recomputed, ok := diskByName[s.Name]
		if !ok {
			// a sink that never failed has no ledger and therefore nothing to
			// recompute; require its missing count to be zero.
			if s.Missing != 0 {
				return fmt.Errorf("scraperun: sink %s missing=%d but no ledger found", s.Name, s.Missing)
			}

			continue
		}

		if s.Missing != recomputed.Missing {
			return fmt.Errorf("scraperun: sink %s missing mismatch: live=%d disk=%d",
				s.Name, s.Missing, recomputed.Missing)
		}
	}

	return nil
}
