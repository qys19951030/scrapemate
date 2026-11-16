package multisink

import (
	"bufio"
	"crypto/md5" //nolint:gosec // stable non-cryptographic envelope key
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/gosom/scrapemate"
)

// SeqCarrier is implemented by jobs that know their durable queue sequence.
// The coordinator wraps leased jobs so every result envelope carries a
// traceable, de-duplicating sequence number.
type SeqCarrier interface {
	QueueSeq() int64
}

// extractSeq resolves the durable queue sequence for a job. Jobs that carry a
// positive sequence via SeqCarrier take precedence; otherwise a stable
// non-negative number is derived from the job id and URL so plain
// scrapemate.Job users still get a unique envelope key.
func extractSeq(job scrapemate.IJob) int64 {
	if carrier, ok := job.(SeqCarrier); ok {
		if seq := carrier.QueueSeq(); seq > 0 {
			return seq
		}
	}

	return stableSeqFromID(job.GetID(), job.GetFullURL())
}

// stableSeqFromID derives a positive int64 from the job identity.
func stableSeqFromID(parts ...string) int64 {
	joined := strings.Join(parts, "|")
	sum := md5.Sum([]byte(joined)) //nolint:gosec // stable non-cryptographic key

	// take the first 8 bytes and force the sign bit positive
	v := binary.BigEndian.Uint64(sum[:8])

	return int64(v >> 1)
}

func nowUnixNano() int64 {
	return time.Now().UTC().UnixNano()
}

func sortInt64s(in []int64) {
	sort.Slice(in, func(i, j int) bool { return in[i] < in[j] })
}

func (m *multiSinkImpl) ledgerPath(sinkName string) string {
	safe := strings.ReplaceAll(sinkName, string(filepath.Separator), "_")

	return filepath.Join(m.opts.Dir, safe+ledgerSuffix)
}

// appendResultLog durably appends one envelope to the shared result log.
func (m *multiSinkImpl) appendResultLog(env *Envelope) error {
	encoded, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("multisink: encode result: %w", err)
	}

	encoded = append(encoded, '\n')

	m.logMu.Lock()
	defer m.logMu.Unlock()

	if _, err := m.logFile.Write(encoded); err != nil {
		return fmt.Errorf("multisink: append result log: %w", err)
	}

	if m.opts.Fsync {
		if err := m.logFile.Sync(); err != nil {
			return fmt.Errorf("multisink: sync result log: %w", err)
		}
	}

	m.envelopes[env.Seq] = *env

	return nil
}

// appendLedger durably records a missing write for a sink.
func (m *multiSinkImpl) appendLedger(sinkName string, entry ledgerEntry) error {
	encoded, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("multisink: encode ledger entry: %w", err)
	}

	encoded = append(encoded, '\n')

	path := m.ledgerPath(sinkName)

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, filePerm)
	if err != nil {
		return fmt.Errorf("multisink: open ledger %s: %w", sinkName, err)
	}
	defer file.Close()

	if _, err := file.Write(encoded); err != nil {
		return fmt.Errorf("multisink: write ledger %s: %w", sinkName, err)
	}

	if err := file.Sync(); err != nil {
		return fmt.Errorf("multisink: sync ledger %s: %w", sinkName, err)
	}

	return nil
}

// clearMissing removes a seq from a sink's ledger after a successful
// back-fill by rewriting the ledger without that entry.
func (m *multiSinkImpl) clearMissing(sinkName string, seq int64) error {
	path := m.ledgerPath(sinkName)

	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}

		return fmt.Errorf("multisink: open ledger for cleanup %s: %w", sinkName, err)
	}

	var kept []ledgerEntry

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var entry ledgerEntry
		if unmarshalErr := json.Unmarshal(scanner.Bytes(), &entry); unmarshalErr != nil {
			continue
		}

		if entry.Seq != seq {
			kept = append(kept, entry)
		}
	}

	file.Close()

	tmpPath := path + ".tmp"

	tmp, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, filePerm)
	if err != nil {
		return fmt.Errorf("multisink: rewrite ledger %s: %w", sinkName, err)
	}

	writer := bufio.NewWriter(tmp)

	for i := range kept {
		encoded, err := json.Marshal(kept[i])
		if err != nil {
			tmp.Close()

			return err
		}

		if _, err := writer.Write(append(encoded, '\n')); err != nil {
			tmp.Close()

			return err
		}
	}

	if err := writer.Flush(); err != nil {
		tmp.Close()

		return err
	}

	if err := tmp.Sync(); err != nil {
		tmp.Close()

		return err
	}

	tmp.Close()

	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("multisink: replace ledger %s: %w", sinkName, err)
	}

	return nil
}

// ledger file helpers live above.
