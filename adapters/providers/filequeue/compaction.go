package filequeue

import (
	"encoding/json"
	"fmt"
)

// maybeCompact rewrites the log as a single snapshot frame once it grows past
// the configured frame threshold. The caller must hold q.mu.
func (q *queueImpl) maybeCompact() error {
	if q.opts.CompactionFrames <= 0 || q.frameNo < q.opts.CompactionFrames {
		return nil
	}

	snap := snapshotFrame{
		NextSeq:  q.nextSeq,
		Records:  q.records,
		States:   q.states,
		Failures: append([]FailureRecord(nil), q.failures...),
		Pacing:   q.pacing,
		Stats:    q.stats,
	}

	payload, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("filequeue: marshal snapshot: %w", err)
	}

	if err := q.log.rotateWAL(payload); err != nil {
		return err
	}

	// The rotated log contains exactly one frame (the snapshot).
	q.frameNo = 1

	return nil
}
