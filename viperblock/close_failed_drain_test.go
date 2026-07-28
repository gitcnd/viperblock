package viperblock

// Regression test for security-audit finding VB-1 (CRITICAL, data loss):
// Close() used to call RemoveLocalFiles even when its WAL-to-chunk
// consolidation FAILED -- deleting the local WAL that held the only copy
// of flushed-but-unconsolidated writes. A flushed, acknowledged block was
// permanently lost (runtime-confirmed by the out-of-tree reproducer in
// spinifex project_management/security_repros/vb1_close_dataloss).
//
// WHAT IS GATED: after a Close whose chunk upload fails (fault-injected
// backend failing ONLY FileTypeChunk writes), the local WAL files must
// survive, and a normal reopen must recover the flushed block's content.

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// chunkWriteFailingBackend fails only chunk-object writes; config,
// checkpoint and state writes pass through, mirroring a backend that
// rejects large chunk PUTs while small writes still land.
type chunkWriteFailingBackend struct {
	types.Backend
	failChunkWrites bool
}

func (b *chunkWriteFailingBackend) Write(ft types.FileType, id uint64, h *[]byte, d *[]byte) error {
	if b.failChunkWrites && ft == types.FileTypeChunk {
		return fmt.Errorf("injected chunk-write failure (VB-1 regression test)")
	}
	return b.Backend.Write(ft, id, h, d)
}

func (b *chunkWriteFailingBackend) WriteCtx(ctx context.Context, ft types.FileType, id uint64, h *[]byte, d *[]byte) error {
	if b.failChunkWrites && ft == types.FileTypeChunk {
		return fmt.Errorf("injected chunk-write failure (VB-1 regression test)")
	}
	return b.Backend.WriteCtx(ctx, ft, id, h, d)
}

func openVB1TestVolume(t *testing.T, root string, failChunks bool) *VB {
	t.Helper()
	for _, d := range []string{filepath.Join(root, "backend"), filepath.Join(root, "vb")} {
		require.NoError(t, os.MkdirAll(d, 0o755))
	}
	vbconfig := VB{
		VolumeName:          "vb1vol",
		VolumeSize:          64 * 1024 * 1024,
		BaseDir:             filepath.Join(root, "vb"),
		WALSyncInterval:     -1,
		ChunkUploadInterval: -1,
		Cache:               Cache{Config: CacheConfig{Size: 0}},
	}
	vb, err := New(&vbconfig, FileBackend, file.FileConfig{
		VolumeName: "vb1vol", VolumeSize: 64 * 1024 * 1024, BaseDir: filepath.Join(root, "backend"),
	})
	require.NoError(t, err)
	vb.Backend = &chunkWriteFailingBackend{Backend: vb.Backend, failChunkWrites: failChunks}
	require.NoError(t, vb.Backend.Init())
	if err := vb.LoadState(); err != nil {
		require.NoError(t, vb.SaveState())
		require.NoError(t, vb.LoadState())
	}
	require.NoError(t, vb.EnsureVolumeUUID())
	require.NoError(t, vb.LoadLiveCheckpoint())
	require.NoError(t, vb.RecoverLocalWALs())
	vb.WAL.WallNum.Add(1)
	require.NoError(t, vb.OpenWAL(&vb.WAL, fmt.Sprintf("%s/%s", vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, vb.WAL.WallNum.Load(), vb.GetVolume()))))
	require.NoError(t, vb.OpenWAL(&vb.BlockToObjectWAL, fmt.Sprintf("%s/%s", vb.BlockToObjectWAL.BaseDir, types.GetFilePath(types.FileTypeWALBlock, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume()))))
	return vb
}

func TestCloseAfterFailedDrainKeepsWALAndRecovers(t *testing.T) {
	root := t.TempDir()
	blockSize := uint64(4096)

	// Phase 1: chunk writes fail; write + flush a known block, then Close.
	vb := openVB1TestVolume(t, root, true)
	blockSize = uint64(vb.BlockSize)
	payload := make([]byte, blockSize)
	copy(payload, "VB1-REGRESSION-FLUSHED-BLOCK")
	require.NoError(t, vb.WriteAt(42*blockSize, payload))
	require.NoError(t, vb.Flush())

	closeErr := vb.Close()
	require.Error(t, closeErr, "Close must surface the failed chunk upload")

	// The local WAL must SURVIVE the failed close: it holds the only copy.
	walChunksDir := filepath.Join(root, "vb", "vb1vol", "wal", "chunks")
	entries, err := os.ReadDir(walChunksDir)
	require.NoError(t, err, "WAL directory must still exist after a failed close")
	assert.NotEmpty(t, entries, "local WAL files were deleted after a failed drain (VB-1 regression)")

	// Phase 2: reopen with a healthy backend; recovery must restore the block.
	vb2 := openVB1TestVolume(t, root, false)
	defer func() { assert.NoError(t, vb2.Close()) }()
	got, err := vb2.ReadAt(42*blockSize, blockSize)
	require.NoError(t, err, "flushed block must be readable after reopen")
	assert.True(t, bytes.Equal(got, payload),
		"flushed block content lost across failed-close + reopen (VB-1)")
}
