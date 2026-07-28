package viperblock

// Gate test for spinifex P1.6 slice 4 (pipelined drain rounds).
//
// WHAT IS GATED: a writer blocked at the high watermark must be admitted
// after ~one drain ROUND (bounded flush slice + its chunk uploads free
// pendingBytes), NOT after the drain's entire flush phase. Before slice 4
// the drain flushed the whole entry buffer before the first chunk upload
// could free anything, so the blocked writer waited out the full flush
// (measured 16.3 s in-guest at a 256 MiB buffer). WHAT IS REPORTED: the
// observed max admission latency during the throttled phase.
//
// DIFFERENTIAL (2026-07-28, this geometry -- 96 MiB buffer, 8 MiB test
// rounds, 200 ms serial uploads, -race): the pre-slice-4 algorithm
// (emulated exactly by one giant round: flush-everything-then-chunk)
// FAILS the bound at 1.007-1.104 s measured; pipelined rounds pass at
// 66 ms. Bound 400 ms sits ~6x above the pass and ~2.75x below the fail.

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDrainPipelinesChunkUploadsWithFlush(t *testing.T) {
	tmpDir := t.TempDir()
	testVol := fmt.Sprintf("test_drainpipeline_%d", time.Now().UnixNano())
	const volumeSize = 256 * 1024 * 1024
	const maxPendingBytes = 96 * 1024 * 1024 // 24576 records
	const drainDelay = 200 * time.Millisecond
	const throttledWrites = 64

	vbconfig := VB{
		VolumeName:          testVol,
		VolumeSize:          volumeSize,
		BaseDir:             fmt.Sprintf("%s/%s", tmpDir, "viperblock"),
		WALSyncInterval:     -1,
		ChunkUploadInterval: time.Hour, // uploader alive, purely trigger-driven
		MaxPendingBytes:     maxPendingBytes,
		UploadWorkers:       1, // serial uploads: drain time scales with chunks
		Cache:               Cache{Config: CacheConfig{Size: 0}},
	}
	vb, err := New(&vbconfig, FileBackend, file.FileConfig{
		VolumeName: testVol, VolumeSize: volumeSize, BaseDir: tmpDir,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		vb.StopChunkUploader()
		vb.StopWALSyncer()
		assert.NoError(t, vb.RemoveLocalFiles())
	})
	require.NoError(t, vb.Backend.Init())
	vb.Backend = &slowBackend{Backend: vb.Backend, delay: drainDelay}
	require.NoError(t, vb.OpenWAL(&vb.WAL, fmt.Sprintf("%s/%s", vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, vb.WAL.WallNum.Load(), vb.GetVolume()))))
	require.NoError(t, vb.OpenWAL(&vb.BlockToObjectWAL, fmt.Sprintf("%s/%s", vb.BlockToObjectWAL.BaseDir, types.GetFilePath(types.FileTypeWALBlock, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume()))))

	// 8 MiB rounds against the 96 MiB buffer = 12 rounds: round-1 headroom
	// arrives an order of magnitude before the full flush completes.
	vb.drainRoundRecordsOverrideForTests = 2048

	blockSize := uint64(vb.BlockSize)
	fillBlocks := uint64(maxPendingBytes) / blockSize

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

	for i := range fillBlocks {
		data := make([]byte, blockSize)
		copy(data, fmt.Sprintf("fill-%d", i))
		require.NoError(t, vb.WriteAt(i*blockSize, data))
	}

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

	t.Logf("max admission latency = %v (bound 400ms); max observed pending = %d (high watermark %d)",
		maxAdmissionLatency, maxObservedPending.Load(), maxPendingBytes)

	// Engagement proof: pending must have crossed the high watermark (a
	// writer inside awaitBackpressure is the only way past it). A latency
	// floor is NOT used here: with pipelined rounds the first freed chunk
	// can admit the writer in under 100 ms, which is the success mode.
	assert.Greater(t, maxObservedPending.Load(), uint64(maxPendingBytes),
		"pending never crossed the high watermark; the gate was not exercised")

	// THE SLICE-4 GATE: admission is round-class, not flush-phase-class.
	assert.LessOrEqual(t, maxAdmissionLatency, 400*time.Millisecond,
		"a write waited flush-phase-class time; the drain is not pipelining chunk uploads with the flush")

	assert.LessOrEqual(t, maxObservedPending.Load(), uint64(maxPendingBytes)+blockSize,
		"pending bytes exceeded MaxPendingBytes + one block")

	// Correctness spot checks straight off the hot/pending tiers (no full
	// drain needed; teardown's StopChunkUploader lets any in-flight drain
	// finish).
	for _, i := range []uint64{0, fillBlocks / 2, fillBlocks - 1} {
		got, err := vb.ReadAt(i*blockSize, blockSize)
		if !assert.NoError(t, err, "fill block %d", i) {
			continue
		}
		expected := make([]byte, blockSize)
		copy(expected, fmt.Sprintf("fill-%d", i))
		assert.Equal(t, expected, got, "fill block %d mismatch", i)
	}
	for i := range uint64(throttledWrites) {
		got, err := vb.ReadAt((fillBlocks+i)*blockSize, blockSize)
		if !assert.NoError(t, err, "throttled block %d", i) {
			continue
		}
		expected := make([]byte, blockSize)
		copy(expected, fmt.Sprintf("throttled-%d", i))
		assert.Equal(t, expected, got, "throttled block %d mismatch", i)
	}
}
