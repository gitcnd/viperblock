package viperblock

// Gate test for spinifex P1.6 slice 2 (bounded flush lock-hold).
//
// WHAT IS GATED: (1) a large Flush must NOT stall concurrent WriteAt
// calls for its own duration -- the old whole-buffer-under-Writes.mu
// flush had a measured 1:1 signature (flush 724.7 ms == concurrent write
// max 721.5 ms at 65k records; ~35 s in-guest at production scale), and
// the batched flush bounds a concurrent writer's wait to lock-section
// work only; (2) a same-block rewrite racing the flush must survive it
// (the SeqNum-exact removal invariant); (3) flushed data reads back
// exactly. WHAT IS REPORTED: flush duration and the concurrent writer's
// max latency.
//
// DIFFERENTIAL (2026-07-28): against the pre-slice-2 flush this test
// FAILS its concurrent-latency bound (old: max concurrent write latency
// tracks flush duration, > 150 ms by the flush-floor assertion); with
// the batched flush it passes with ~ms-class concurrent latency.

import (
	"bytes"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFlushDoesNotStallConcurrentWrites(t *testing.T) {
	tmpDir := t.TempDir()
	testVol := fmt.Sprintf("test_flushlatency_%d", time.Now().UnixNano())
	const volumeSize = 1024 * 1024 * 1024

	vbconfig := VB{
		VolumeName: testVol,
		VolumeSize: volumeSize,
		BaseDir:    fmt.Sprintf("%s/%s", tmpDir, "viperblock"),
		// No background syncer/uploader and a huge watermark: this test
		// isolates the FLUSH lock-hold, so backpressure and drains must
		// never engage.
		WALSyncInterval:     -1,
		ChunkUploadInterval: -1,
		MaxPendingBytes:     4 << 30,
		Cache:               Cache{Config: CacheConfig{Size: 0}},
	}
	vb, err := New(&vbconfig, FileBackend, file.FileConfig{
		VolumeName: testVol, VolumeSize: volumeSize, BaseDir: tmpDir,
	})
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, vb.RemoveLocalFiles()) })
	require.NoError(t, vb.Backend.Init())
	require.NoError(t, vb.OpenWAL(&vb.WAL, fmt.Sprintf("%s/%s", vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, vb.WAL.WallNum.Load(), vb.GetVolume()))))
	require.NoError(t, vb.OpenWAL(&vb.BlockToObjectWAL, fmt.Sprintf("%s/%s", vb.BlockToObjectWAL.BaseDir, types.GetFilePath(types.FileTypeWALBlock, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume()))))

	blockSize := uint64(vb.BlockSize)
	fillBlocks := uint64(256*1024*1024) / blockSize // 256 MiB buffered = 65536 records

	// rewriteBlock gets "old" content in the fill, then a racing rewrite
	// mid-flush; the rewrite must survive the flush's removal step.
	const rewriteBlock = uint64(10)
	oldContent := make([]byte, blockSize)
	copy(oldContent, "rewrite-me-OLD")
	newContent := make([]byte, blockSize)
	copy(newContent, "rewrite-me-NEW")

	for i := range fillBlocks {
		data := make([]byte, blockSize)
		copy(data, fmt.Sprintf("fill-%d", i))
		if i == rewriteBlock {
			copy(data, oldContent)
		}
		require.NoError(t, vb.WriteAt(i*blockSize, data))
	}

	// Concurrent writer: 4 KiB writes into a distant region every 2 ms,
	// max latency recorded; plus one rewrite of rewriteBlock mid-flush.
	var maxWriteNanos atomic.Int64
	flushStarted := make(chan struct{})
	stop := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		<-flushStarted
		time.Sleep(30 * time.Millisecond) // land inside the flush window
		s := time.Now()
		if err := vb.WriteAt(rewriteBlock*blockSize, newContent); err != nil {
			t.Errorf("mid-flush rewrite: %v", err)
			return
		}
		if d := time.Since(s).Nanoseconds(); d > maxWriteNanos.Load() {
			maxWriteNanos.Store(d)
		}
		offset := uint64(768 * 1024 * 1024)
		for i := uint64(0); ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			s := time.Now()
			if err := vb.WriteAt(offset+i*blockSize, make([]byte, blockSize)); err != nil {
				t.Errorf("concurrent write: %v", err)
				return
			}
			if d := time.Since(s).Nanoseconds(); d > maxWriteNanos.Load() {
				maxWriteNanos.Store(d)
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()

	flushStart := time.Now()
	close(flushStarted)
	require.NoError(t, vb.Flush())
	flushElapsed := time.Since(flushStart)

	close(stop)
	<-writerDone
	maxConcurrent := time.Duration(maxWriteNanos.Load())

	t.Logf("flush of %d records took %v; concurrent WriteAt max latency %v (gate 150 ms)",
		fillBlocks, flushElapsed, maxConcurrent)

	// Sanity floor: the flush must be long enough that "concurrent writes
	// were not stalled by it" is a meaningful claim (old behavior stalls
	// them for the whole flush; 65k WAL appends cannot beat this floor).
	assert.GreaterOrEqual(t, flushElapsed, 150*time.Millisecond,
		"flush finished implausibly fast; the gate is not exercising a large flush")

	// THE GATE: concurrent writes must not be held for flush I/O. Old
	// contract: maxConcurrent ~== flushElapsed (measured 1:1 at 65k
	// records). Batched contract: lock-section work only, ms-class.
	assert.LessOrEqual(t, maxConcurrent, 150*time.Millisecond,
		"a concurrent WriteAt stalled for flush-duration-class time; the flush is holding Writes.mu across WAL I/O again")

	// The mid-flush rewrite must have survived the SeqNum-exact removal:
	// flush everything remaining, then read back the NEW content.
	require.NoError(t, vb.Flush())
	got, err := vb.ReadAt(rewriteBlock*blockSize, blockSize)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(got, newContent),
		"mid-flush rewrite was lost by the flush's removal step (SeqNum filter broken)")

	// Spot-check flushed fill data.
	for _, i := range []uint64{0, 1, fillBlocks / 2, fillBlocks - 1} {
		got, err := vb.ReadAt(i*blockSize, blockSize)
		if !assert.NoError(t, err, "fill block %d", i) {
			continue
		}
		expected := make([]byte, blockSize)
		copy(expected, fmt.Sprintf("fill-%d", i))
		assert.True(t, bytes.Equal(got, expected), "fill block %d mismatch", i)
	}
}
