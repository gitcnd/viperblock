package viperblock

import (
	"path/filepath"
	"testing"

	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/stretchr/testify/require"
)

// newBarrierTestVB builds a file-backed VB with the background WAL syncer
// DISABLED so the WAL dirty flag observes only Flush's own behaviour.
func newBarrierTestVB(t *testing.T, syncOnFlush bool) *VB {
	t.Helper()
	tmpDir := t.TempDir()
	backendConfig := file.FileConfig{
		VolumeName: "barrier-test",
		VolumeSize: 64 * 1024 * 1024,
		BaseDir:    tmpDir,
	}
	vbconfig := VB{
		VolumeName:      "barrier-test",
		VolumeSize:      64 * 1024 * 1024,
		BaseDir:         filepath.Join(tmpDir, "vb"),
		WALSyncInterval: -1, // no background syncer in this test
		SyncOnFlush:     syncOnFlush,
	}
	vb, err := New(&vbconfig, "file", backendConfig)
	require.NoError(t, err)
	vb.UseShardedWAL = false
	vb.ShardedWAL = nil
	require.NoError(t, vb.Backend.Init())
	require.NoError(t, vb.OpenWAL(&vb.WAL, filepath.Join(vb.WAL.BaseDir,
		types.GetFilePath(types.FileTypeWALChunk, vb.WAL.WallNum.Load(), vb.GetVolume()))))
	require.NoError(t, vb.OpenWAL(&vb.BlockToObjectWAL, filepath.Join(vb.WAL.BaseDir,
		types.GetFilePath(types.FileTypeWALBlock, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume()))))
	return vb
}

// TestFlushWithSyncOnFlushSyncsTheWAL: with the barrier enabled, Flush must
// leave the WAL clean (records written AND fsynced). Gate P1.4 behaviour:
// the dirty flag is cleared only by a successful sync.
func TestFlushWithSyncOnFlushSyncsTheWAL(t *testing.T) {
	vb := newBarrierTestVB(t, true)
	data := make([]byte, DefaultBlockSize)
	require.NoError(t, vb.WriteAt(0, data))
	require.NoError(t, vb.Flush())
	require.False(t, vb.WAL.dirty.Load(),
		"SyncOnFlush barrier must fsync: WAL still marked dirty after Flush")
}

// TestFlushWithoutSyncOnFlushLeavesWALDirty documents the legacy behaviour
// the barrier fixes: Flush writes records but does NOT sync, so the WAL
// stays dirty until the background syncer runs.
func TestFlushWithoutSyncOnFlushLeavesWALDirty(t *testing.T) {
	vb := newBarrierTestVB(t, false)
	data := make([]byte, DefaultBlockSize)
	require.NoError(t, vb.WriteAt(0, data))
	require.NoError(t, vb.Flush())
	require.True(t, vb.WAL.dirty.Load(),
		"without SyncOnFlush, Flush is expected to leave the WAL dirty (page-cache only)")
}
