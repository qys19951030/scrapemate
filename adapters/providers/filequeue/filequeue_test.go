package filequeue_test

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gosom/scrapemate"
	"github.com/gosom/scrapemate/adapters/providers/filequeue"
)

func newTestJob(t *testing.T, id, rawURL string) *scrapemate.Job {
	t.Helper()

	return &scrapemate.Job{
		ID:     id,
		Method: http.MethodGet,
		URL:    rawURL,
	}
}

func TestEnqueuePopCompleteAndDedup(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	q, err := filequeue.New(filequeue.Options{Dir: dir, Fsync: true})
	require.NoError(t, err)

	ctx := context.Background()

	enqueued, err := q.Enqueue(ctx, newTestJob(t, "1", "https://example.com/a?utm_source=x"))
	require.NoError(t, err)
	require.True(t, enqueued)

	// same normalized link discovered from different pages: only one pending
	// item may exist.
	enqueued, err = q.Enqueue(ctx, newTestJob(t, "2", "https://EXAMPLE.com/a/#frag"))
	require.NoError(t, err)
	require.False(t, enqueued)

	enqueued, err = q.Enqueue(ctx, newTestJob(t, "3", "https://example.com/a?page=2"))
	require.NoError(t, err)
	require.True(t, enqueued)

	lease, err := q.Pop(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(0), lease.Seq)
	require.NoError(t, q.Complete(ctx, lease))

	stats := q.Stats()
	require.Equal(t, int64(2), stats.Enqueued)
	require.Equal(t, int64(1), stats.DedupSkipped)
	require.Equal(t, int64(1), stats.Completed)

	// the completed job must be rejected on re-enqueue
	enqueued, err = q.Enqueue(ctx, newTestJob(t, "again", "https://example.com/a"))
	require.NoError(t, err)
	require.False(t, enqueued)

	lease2, err := q.Pop(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), lease2.Seq)
	require.NoError(t, q.Complete(ctx, lease2))

	require.NoError(t, q.Close())
}

func TestResumeDoesNotRerunCompletedJobs(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	ctx := context.Background()

	q, err := filequeue.New(filequeue.Options{Dir: dir})
	require.NoError(t, err)

	_, err = q.Enqueue(ctx, newTestJob(t, "a", "https://example.com/a"))
	require.NoError(t, err)

	_, err = q.Enqueue(ctx, newTestJob(t, "b", "https://example.com/b"))
	require.NoError(t, err)

	lease, err := q.Pop(ctx)
	require.NoError(t, err)
	require.NoError(t, q.Complete(ctx, lease))

	require.NoError(t, q.Close())

	// restart: the completed job must never be leased again.
	q2, err := filequeue.New(filequeue.Options{Dir: dir})
	require.NoError(t, err)

	stats := q2.Stats()
	require.Equal(t, int64(1), stats.Completed)
	require.Equal(t, int64(2), stats.Enqueued)
	require.Equal(t, 1, q2.Pending())

	lease2, err := q2.Pop(ctx)
	require.NoError(t, err)
	require.Equal(t, "https://example.com/b", lease2.Job.GetURL())

	// no second job is ever available once the only pending one is leased.
	popCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()

	_, err = q2.Pop(popCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	require.NoError(t, q2.Close())
}

func TestRecoveredInflightJobIsRedispatched(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	ctx := context.Background()

	q, err := filequeue.New(filequeue.Options{Dir: dir})
	require.NoError(t, err)

	_, err = q.Enqueue(ctx, newTestJob(t, "a", "https://example.com/a"))
	require.NoError(t, err)

	lease, err := q.Pop(ctx)
	require.NoError(t, err)
	// the job is now actively leased by this process, so it is not pending
	require.Equal(t, 0, q.Pending())
	require.Equal(t, int64(1), q.Stats().Inflight)

	// simulate a hard crash before Complete
	require.NoError(t, q.Close())

	q2, err := filequeue.New(filequeue.Options{Dir: dir})
	require.NoError(t, err)

	// after restart the crashed lease is eligible for one re-dispatch
	require.Equal(t, 1, q2.Pending())

	lease2, err := q2.Pop(ctx)
	require.NoError(t, err)
	require.Equal(t, lease.Key, lease2.Key)
	require.Equal(t, lease.Seq, lease2.Seq)

	// once re-leased it is owned by this run and cannot be popped twice
	require.Equal(t, 0, q2.Pending())

	require.NoError(t, q2.Complete(ctx, lease2))
	require.NoError(t, q2.Close())
}

func TestTornTailIsDiscardedAndCompletedResultsSurvive(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	logPath := filepath.Join(dir, "queue.wal")
	ctx := context.Background()

	q, err := filequeue.New(filequeue.Options{Dir: dir})
	require.NoError(t, err)

	_, err = q.Enqueue(ctx, newTestJob(t, "a", "https://example.com/a"))
	require.NoError(t, err)

	lease, err := q.Pop(ctx)
	require.NoError(t, err)
	require.NoError(t, q.Complete(ctx, lease))

	require.NoError(t, q.Close())

	goodSize, err := os.Stat(logPath)
	require.NoError(t, err)

	// append a partial, garbage frame to simulate a write interrupted mid-frame
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0o600)
	require.NoError(t, err)

	_, err = f.Write([]byte{'S', 'M', 'Q', '1', 1, 0, 0, 0, 50, 1, 2, 3})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	q2, err := filequeue.New(filequeue.Options{Dir: dir})
	require.NoError(t, err)

	require.True(t, q2.RecoveredCorrupt(), "torn tail must be detected")

	// completed results survived and nothing extra was enqueued
	require.Equal(t, int64(1), q2.Stats().Completed)
	require.Equal(t, int64(1), q2.Stats().Enqueued)
	require.Equal(t, 0, q2.Pending())

	info, err := os.Stat(logPath)
	require.NoError(t, err)
	require.Equal(t, goodSize.Size(), info.Size(), "log must be truncated to last complete frame")

	require.NoError(t, q2.Close())
}

func TestRequeueAndAbandonFailureList(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	ctx := context.Background()

	q, err := filequeue.New(filequeue.Options{Dir: dir})
	require.NoError(t, err)

	_, err = q.Enqueue(ctx, newTestJob(t, "a", "https://example.com/a"))
	require.NoError(t, err)

	lease, err := q.Pop(ctx)
	require.NoError(t, err)

	require.NoError(t, q.Requeue(ctx, lease, "transient"))

	stats := q.Stats()
	require.Equal(t, int64(1), stats.Requeued)
	require.Equal(t, 1, q.Pending())

	lease2, err := q.Pop(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, lease2.Attempts)

	failure := filequeue.FailureRecord{
		URL:     "https://example.com/a",
		Kind:    filequeue.FailureNetwork,
		Message: "connection refused",
	}

	require.NoError(t, q.Abandon(ctx, lease2, failure))

	failures := q.Failures()
	require.Len(t, failures, 1)
	require.Equal(t, filequeue.FailureNetwork, failures[0].Kind)
	require.Equal(t, "connection refused", failures[0].Message)
	require.Equal(t, int64(1), q.Stats().Abandoned)
	require.Equal(t, int64(1), q.Stats().NetworkFailed)

	// abandoned links stay out of the queue and in the failure list after restart
	require.NoError(t, q.Close())

	q2, err := filequeue.New(filequeue.Options{Dir: dir})
	require.NoError(t, err)

	require.Len(t, q2.Failures(), 1)
	require.Equal(t, 0, q2.Pending())

	// re-enqueue of an abandoned key is still de-duplicated
	enqueued, err := q2.Enqueue(ctx, newTestJob(t, "dup", "https://example.com/a"))
	require.NoError(t, err)
	require.False(t, enqueued)

	require.NoError(t, q2.Close())
}

func TestPacingSurvivesRestart(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	q, err := filequeue.New(filequeue.Options{Dir: dir})
	require.NoError(t, err)

	when := time.Now().UTC().UnixNano()
	require.NoError(t, q.RecordPacing("example.com", when))
	require.NoError(t, q.Close())

	q2, err := filequeue.New(filequeue.Options{Dir: dir})
	require.NoError(t, err)

	got, ok := q2.LastPacing("example.com")
	require.True(t, ok)
	require.Equal(t, when, got)

	require.NoError(t, q2.Close())
}

func TestCompactionKeepsState(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	ctx := context.Background()

	q, err := filequeue.New(filequeue.Options{Dir: dir, CompactionFrames: 4})
	require.NoError(t, err)

	for i := range 6 {
		job := newTestJob(t, string(rune('a'+i)), "https://example.com/"+string(rune('a'+i)))

		_, err = q.Enqueue(ctx, job)
		require.NoError(t, err)
	}

	for range 6 {
		var lease filequeue.Lease

		lease, err = q.Pop(ctx)
		require.NoError(t, err)
		require.NoError(t, q.Complete(ctx, lease))
	}

	require.Equal(t, int64(6), q.Stats().Completed)
	require.NoError(t, q.Close())

	// the compacted log must rebuild the same state
	q2, err := filequeue.New(filequeue.Options{Dir: dir})
	require.NoError(t, err)

	require.Equal(t, int64(6), q2.Stats().Enqueued)
	require.Equal(t, int64(6), q2.Stats().Completed)
	require.Equal(t, 0, q2.Pending())
	require.NoError(t, q2.Close())
}

func TestCustomKindRoundTrip(t *testing.T) {
	t.Parallel()

	customKind := filequeue.Kind("custom-kind")

	filequeue.RegisterFactory(customKind, func() scrapemate.IJob {
		return &kindedJob{kind: customKind}
	})

	dir := t.TempDir()
	ctx := context.Background()

	q, err := filequeue.New(filequeue.Options{Dir: dir})
	require.NoError(t, err)

	job := &kindedJob{
		Job:  *newTestJob(t, "c", "https://example.com/custom"),
		kind: customKind,
	}

	_, err = q.Enqueue(ctx, job)
	require.NoError(t, err)

	lease, err := q.Pop(ctx)
	require.NoError(t, err)
	require.IsType(t, &kindedJob{}, lease.Job)
	require.Equal(t, "https://example.com/custom", lease.Job.GetURL())
	require.NoError(t, q.Close())
}

type kindedJob struct {
	scrapemate.Job
	kind filequeue.Kind
}

func (k *kindedJob) Kind() filequeue.Kind { return k.kind }
