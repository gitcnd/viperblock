package viperblock

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// slowBackend wraps a real types.Backend and adds artificial latency to every
// write, standing in for a backend (e.g. S3/predastore over the network)
// that can't keep up with a fast local guest write. Everything else proxies
// straight through via the embedded interface.
type slowBackend struct {
	types.Backend

	delay time.Duration
}

func (s *slowBackend) Write(fileType types.FileType, objectId uint64, headers *[]byte, data *[]byte) error {
	time.Sleep(s.delay)
	return s.Backend.Write(fileType, objectId, headers, data)
}

func (s *slowBackend) WriteCtx(ctx context.Context, fileType types.FileType, objectId uint64, headers *[]byte, data *[]byte) error {
	time.Sleep(s.delay)
	return s.Backend.WriteCtx(ctx, fileType, objectId, headers, data)
}

// newBackpressureTestVB builds a minimal file-backed VB (legacy single-file
// WAL, no LRU cache, WAL syncer disabled) and swaps in a slowBackend so
// drains take drainDelay per backend write. chunkUploadInterval <= 0
// disables the background uploader entirely (writers then drive drains
// inline via awaitBackpressure's fallback); a large positive value (e.g.
// time.Hour) runs the uploader goroutine but leaves it purely
// trigger-driven, which is the deterministic stand-in for the production
// serving shape.
func newBackpressureTestVB(t *testing.T, maxPendingBytes uint64, drainDelay time.Duration, chunkUploadInterval time.Duration) *VB {
	t.Helper()

	tmpDir := t.TempDir()
	testVol := fmt.Sprintf("test_backpressure_%d", time.Now().UnixNano())

	backendConfig := file.FileConfig{
		VolumeName: testVol,
		VolumeSize: 64 * 1024 * 1024,
		BaseDir:    tmpDir,
	}

	vbconfig := VB{
		VolumeName: testVol,
		VolumeSize: 64 * 1024 * 1024,
		BaseDir:    fmt.Sprintf("%s/%s", tmpDir, "viperblock"),
		// Deterministic: no background WAL fsync racing the test; chunk
		// uploads only when disabled (<= 0) or trigger-driven (large
		// interval), never on a mid-test ticker.
		WALSyncInterval:     -1,
		ChunkUploadInterval: chunkUploadInterval,
		MaxPendingBytes:     maxPendingBytes,
		// Serial chunk uploads so drain time scales with chunk count: the
		// admission-latency gate below must distinguish "released after ONE
		// chunk freed headroom" from "held for a full drain" -- with the
		// default 16-wide worker pool a small full drain finishes in ~one
		// batch and the two contracts are indistinguishable (verified by a
		// differential run 2026-07-28: the old drain-to-low code PASSED the
		// gate until uploads were serialized).
		UploadWorkers: 1,
		Cache: Cache{
			Config: CacheConfig{Size: 0},
		},
	}

	vb, err := New(&vbconfig, FileBackend, backendConfig)
	require.NoError(t, err)
	require.NotNil(t, vb)

	// Registered before the setup below can call FailNow, which would otherwise
	// skip cleanup and leave the VB tree behind. Stops are no-ops when the
	// corresponding goroutine was never started.
	t.Cleanup(func() {
		vb.StopChunkUploader()
		vb.StopWALSyncer()
		assert.NoError(t, vb.RemoveLocalFiles())
	})

	vb.UseShardedWAL = false
	vb.ShardedWAL = nil

	require.NoError(t, vb.Backend.Init())
	vb.Backend = &slowBackend{Backend: vb.Backend, delay: drainDelay}

	require.NoError(t, vb.OpenWAL(&vb.WAL, fmt.Sprintf("%s/%s", vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, vb.WAL.WallNum.Load(), vb.GetVolume()))))
	require.NoError(t, vb.OpenWAL(&vb.BlockToObjectWAL, fmt.Sprintf("%s/%s", vb.BlockToObjectWAL.BaseDir, types.GetFilePath(types.FileTypeWALBlock, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume()))))

	return vb
}

// TestWriteAtBackpressureBoundsPendingBytes drives many WriteAt calls, single
// threaded and back to back (i.e. faster than the artificially slowed backend
// drain), and asserts that outstanding buffered bytes (Writes.Blocks +
// PendingBackendWrites.Blocks) stay bounded near MaxPendingBytes instead of
// growing to the full amount written — the core guest-write-balloons-memory
// regression this backpressure gate fixes.
func TestWriteAtBackpressureBoundsPendingBytes(t *testing.T) {
	const maxPendingBytes = 256 * 1024 // 64 blocks @ 4KB
	const numBlocks = 2000             // 2000 * 4KB ~= 8MB total written

	vb := newBackpressureTestVB(t, maxPendingBytes, 15*time.Millisecond, -1)

	blockSize := uint64(vb.BlockSize)
	totalWritten := uint64(numBlocks) * blockSize

	var maxObserved atomic.Uint64
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if v := vb.PendingBytes(); v > maxObserved.Load() {
					maxObserved.Store(v)
				}
			case <-stop:
				return
			}
		}
	}()

	// Write each block with content identifying its block number, so we can
	// verify no block was lost or reordered by the backpressure gate.
	expected := make(map[uint64][]byte, numBlocks)
	for i := range uint64(numBlocks) {
		data := make([]byte, blockSize)
		msg := fmt.Sprintf("block-%d-payload", i)
		copy(data, msg)
		expected[i] = data

		err := vb.WriteAt(i*blockSize, data)
		assert.NoError(t, err)
	}

	close(stop)
	<-done

	// Drain whatever remains under the watermark so a final readback can
	// verify every block, including the tail that never crossed the gate.
	require.NoError(t, vb.DrainToBackendCtx(context.Background()))

	t.Logf("total written = %d bytes, max observed pending = %d bytes, MaxPendingBytes = %d",
		totalWritten, maxObserved.Load(), maxPendingBytes)

	// The gate must have actually engaged (buffer filled at least past the
	// low watermark) — otherwise this test would trivially pass on a no-op
	// implementation.
	assert.Greater(t, maxObserved.Load(), uint64(maxPendingBytes/2),
		"backpressure gate never appeared to engage; test is not exercising it")

	// The real assertion: pending bytes stayed near MaxPendingBytes, not the
	// full amount written. One in-flight WriteAt's worth of overshoot above
	// the watermark is expected (the counter is bumped before the gate is
	// checked), so allow a small margin.
	assert.LessOrEqual(t, maxObserved.Load(), uint64(maxPendingBytes)+blockSize,
		"outstanding buffered bytes exceeded MaxPendingBytes + one block; backpressure gate did not bound memory")
	assert.Less(t, maxObserved.Load(), totalWritten/4,
		"outstanding buffered bytes grew proportionally to total written; backpressure gate is not bounding memory")

	assert.Equal(t, uint64(0), vb.PendingBytes(), "pending bytes should be fully drained after final DrainToBackendCtx")

	// Correctness: every block must read back exactly what was written, in
	// spite of being throttled through the gate.
	for i := range uint64(numBlocks) {
		got, err := vb.ReadAt(i*blockSize, blockSize)
		if !assert.NoError(t, err, "block %d", i) {
			continue
		}
		assert.Equal(t, expected[i], got, "block %d data mismatch", i)
	}
}

// TestWriteAtBackpressureBlocksThenReleases directly measures that a single
// WriteAt call which pushes pendingBytes over MaxPendingBytes blocks for
// roughly the drain latency, and that pending has fallen at least back to
// the high watermark by the time the call returns.
//
// CONTRACT NOTE (restructured 2026-07-28, P1.6): this VB runs WITHOUT a
// background uploader, so the blocked writer itself drives a FULL inline
// DrainToBackendCtx -- after which pending is (far) below the old low
// watermark, so the original low-watermark assertion still holds on this
// fallback path and is kept. The production serving shape (uploader
// running) is covered by TestWriteAtAdmissionLatencyBoundedWithUploader,
// where writers wait for headroom instead of driving drains.
func TestWriteAtBackpressureBlocksThenReleases(t *testing.T) {
	const maxPendingBytes = 64 * 1024 // 16 blocks @ 4KB
	const drainDelay = 50 * time.Millisecond

	vb := newBackpressureTestVB(t, maxPendingBytes, drainDelay, -1)
	blockSize := uint64(vb.BlockSize)

	// Fill up to (but not past) the high watermark: these calls must not block.
	fillBlocks := maxPendingBytes / blockSize
	fillStart := time.Now()
	for i := range fillBlocks {
		data := make([]byte, blockSize)
		err := vb.WriteAt(i*blockSize, data)
		assert.NoError(t, err)
	}
	fillElapsed := time.Since(fillStart)
	assert.Less(t, fillElapsed, drainDelay, "filling up to the watermark should not have blocked on a drain")
	assert.Equal(t, uint64(maxPendingBytes), vb.PendingBytes(), "pending bytes should equal exactly what was written so far")

	// This next write pushes pendingBytes past MaxPendingBytes and must block
	// until a drain (paced by drainDelay) brings it back under the low
	// watermark.
	triggerStart := time.Now()
	err := vb.WriteAt(fillBlocks*blockSize, make([]byte, blockSize))
	triggerElapsed := time.Since(triggerStart)
	assert.NoError(t, err)

	assert.GreaterOrEqual(t, triggerElapsed, drainDelay,
		"write crossing the high watermark should have blocked for at least one drain cycle")
	assert.LessOrEqual(t, vb.PendingBytes(), uint64(maxPendingBytes/2),
		"pending bytes should be at or below the low watermark once the blocking write returns")

	require.NoError(t, vb.DrainToBackendCtx(context.Background()))
	assert.Equal(t, uint64(0), vb.PendingBytes())
}

// TestWriteAtAdmissionLatencyBoundedWithUploader is the unit leg of spinifex
// gate P1.6 (sustained-write stalls bounded). It reproduces the production
// serving shape -- background uploader RUNNING, writes outrunning a slow
// backend past the high watermark -- and asserts the restructured
// backpressure contract: a blocked write is admitted as soon as drained
// chunks free headroom (~one chunk's drain time), never held for the
// drain-to-low-watermark quantum that froze guest I/O for minutes
// (246 s single-write clat max measured in-guest 2026-07-28; the old
// contract's worst case here would be a full drain of maxPendingBytes,
// ~12 chunk uploads x 300 ms >= ~3.6 s serial).
//
// WHAT IS GATED: per-write admission latency bound (2.0 s -- justified by
// differential measurement 2026-07-28 on this geometry: old drain-to-low
// contract FAILS at 4.65 s, new headroom contract passes at 1.17 s under
// -race, so the bound sits ~2x above the passing measurement and ~2.3x
// below the failing one) + memory boundedness + data correctness.
// WHAT IS REPORTED: the observed max admission latency.
func TestWriteAtAdmissionLatencyBoundedWithUploader(t *testing.T) {
	const maxPendingBytes = 48 * 1024 * 1024 // 12 x 4 MiB chunks
	const drainDelay = 300 * time.Millisecond
	const throttledWrites = 256

	// time.Hour = uploader goroutine alive but purely trigger-driven: the
	// only drains are the ones blocked writers request via
	// signalDrainWanted (maxPendingBytes < default FlushSize, so
	// signalSizeTrigger stays inert by design).
	vb := newBackpressureTestVB(t, maxPendingBytes, drainDelay, time.Hour)
	blockSize := uint64(vb.BlockSize)

	var maxObservedPending atomic.Uint64
	stopSampler := make(chan struct{})
	samplerDone := make(chan struct{})
	go func() {
		defer close(samplerDone)
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if v := vb.PendingBytes(); v > maxObservedPending.Load() {
					maxObservedPending.Store(v)
				}
			case <-stopSampler:
				return
			}
		}
	}()

	// Fill phase: exactly up to the high watermark, unblocked memory writes.
	fillBlocks := uint64(maxPendingBytes) / blockSize
	for i := range fillBlocks {
		data := make([]byte, blockSize)
		copy(data, fmt.Sprintf("fill-%d", i))
		require.NoError(t, vb.WriteAt(i*blockSize, data))
	}

	// Throttled phase: every write now crosses the watermark and must wait
	// for drained-chunk headroom -- but never for a full drain.
	var maxAdmissionLatency time.Duration
	for i := range uint64(throttledWrites) {
		data := make([]byte, blockSize)
		copy(data, fmt.Sprintf("throttled-%d", i))
		start := time.Now()
		require.NoError(t, vb.WriteAt((fillBlocks+i)*blockSize, data))
		if elapsed := time.Since(start); elapsed > maxAdmissionLatency {
			maxAdmissionLatency = elapsed
		}
	}

	close(stopSampler)
	<-samplerDone

	t.Logf("max admission latency = %v (bound 2s); max observed pending = %d (high watermark %d)",
		maxAdmissionLatency, maxObservedPending.Load(), maxPendingBytes)

	// Gate engaged: at least one write must actually have blocked on the
	// slow backend, or this test proves nothing.
	assert.GreaterOrEqual(t, maxAdmissionLatency, 100*time.Millisecond,
		"no write ever blocked; the backpressure gate was not exercised")

	// THE P1.6 BOUND: admission latency is one-chunk-class, not
	// full-drain-class. The old contract measured 4.65 s here.
	assert.LessOrEqual(t, maxAdmissionLatency, 2*time.Second,
		"a write waited longer than the drained-chunk admission bound; writers are being held for full drains again")

	// Memory stays bounded at the watermark plus one in-flight write.
	assert.LessOrEqual(t, maxObservedPending.Load(), uint64(maxPendingBytes)+blockSize,
		"pending bytes exceeded MaxPendingBytes + one block")

	// Correctness: the throttled writes and a sample of the fill range read
	// back exactly (throttled blocks may still be buffered; fill blocks may
	// be in chunks -- both tiers must agree).
	require.NoError(t, vb.DrainToBackendCtx(context.Background()))
	assert.Equal(t, uint64(0), vb.PendingBytes())
	for i := range uint64(throttledWrites) {
		got, err := vb.ReadAt((fillBlocks+i)*blockSize, blockSize)
		if !assert.NoError(t, err, "throttled block %d", i) {
			continue
		}
		expected := make([]byte, blockSize)
		copy(expected, fmt.Sprintf("throttled-%d", i))
		assert.Equal(t, expected, got, "throttled block %d mismatch", i)
	}
	for _, i := range []uint64{0, fillBlocks / 3, fillBlocks / 2, fillBlocks - 1} {
		got, err := vb.ReadAt(i*blockSize, blockSize)
		if !assert.NoError(t, err, "fill block %d", i) {
			continue
		}
		expected := make([]byte, blockSize)
		copy(expected, fmt.Sprintf("fill-%d", i))
		assert.Equal(t, expected, got, "fill block %d mismatch", i)
	}
}
