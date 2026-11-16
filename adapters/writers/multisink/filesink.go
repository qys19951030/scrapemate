package multisink

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// FileSink writes envelopes as JSON Lines to a single file.
//
// On open it scans the file and records the sequence numbers already present,
// so replaying envelopes after a crash or a back-fill never produces
// duplicate lines.
type FileSink struct {
	name string
	path string
	file *os.File
	mu   sync.Mutex
	seqs map[int64]struct{}
}

// fileLine is the on-disk JSONL representation.
type fileLine struct {
	Seq        int64           `json:"seq"`
	SourceURL  string          `json:"source_url"`
	JobID      string          `json:"job_id"`
	ParentID   string          `json:"parent_id"`
	AtUnixNano int64           `json:"at_unix_nano"`
	Data       json.RawMessage `json:"data"`
}

// NewFileSink creates (or re-opens) a JSONL sink backed by path. Existing
// lines are scanned so back-fill can skip sequences already present.
func NewFileSink(name, path string) (*FileSink, error) {
	return newFileSink(name, path)
}

// newFileSink creates (or re-opens) a JSONL sink backed by path.
func newFileSink(name, path string) (*FileSink, error) {
	if name == "" {
		return nil, fmt.Errorf("multisink: sink name is empty")
	}

	if path == "" {
		return nil, fmt.Errorf("multisink: sink %s has empty path", name)
	}

	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("multisink: create sink dir: %w", err)
		}
	}

	f := &FileSink{
		name: name,
		path: path,
		seqs: make(map[int64]struct{}),
	}

	if err := f.scanExisting(); err != nil {
		return nil, err
	}

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("multisink: open sink file %s: %w", path, err)
	}

	f.file = file

	return f, nil
}

// scanExisting reads every complete JSON line and records its sequence.
func (f *FileSink) scanExisting() error {
	file, err := os.Open(f.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}

		return fmt.Errorf("multisink: scan sink %s: %w", f.name, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	// sink lines can be large; give the scanner a generous buffer.
	scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var decoded fileLine
		if err := json.Unmarshal(line, &decoded); err != nil {
			// A torn tail line from a crash: stop the scan here; later writes
			// append after it, and the missing seq will be back-filled.
			break
		}

		f.seqs[decoded.Seq] = struct{}{}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("multisink: scan sink %s: %w", f.name, err)
	}

	return nil
}

// Name returns the sink name.
func (f *FileSink) Name() string {
	return f.name
}

// Write appends one envelope as a JSON line. It is safe for concurrent use.
func (f *FileSink) Write(_ context.Context, env Envelope) error { //nolint:gocritic // Envelope is the public Sink contract value
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, ok := f.seqs[env.Seq]; ok {
		// idempotent: never write the same sequence twice
		return nil
	}

	rawData, err := json.Marshal(env.Data)
	if err != nil {
		return fmt.Errorf("multisink: encode data for %s: %w", f.name, err)
	}

	line := fileLine{
		Seq:        env.Seq,
		SourceURL:  env.SourceURL,
		JobID:      env.JobID,
		ParentID:   env.ParentID,
		AtUnixNano: env.AtUnixNano,
		Data:       rawData,
	}

	encoded, err := json.Marshal(line)
	if err != nil {
		return fmt.Errorf("multisink: encode envelope for %s: %w", f.name, err)
	}

	encoded = append(encoded, '\n')

	if _, err := f.file.Write(encoded); err != nil {
		return fmt.Errorf("multisink: write sink %s: %w", f.name, err)
	}

	if err := f.file.Sync(); err != nil {
		return fmt.Errorf("multisink: sync sink %s: %w", f.name, err)
	}

	f.seqs[env.Seq] = struct{}{}

	return nil
}

// Has reports whether seq is already present in the output file.
func (f *FileSink) Has(seq int64) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	_, ok := f.seqs[seq]

	return ok, nil
}

// WrittenCount returns how many distinct sequences the sink holds.
func (f *FileSink) WrittenCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.seqs)
}

// Close flushes and closes the file.
func (f *FileSink) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.file == nil {
		return nil
	}

	err := f.file.Close()
	f.file = nil

	return err
}
