package viperblock

// Promotion test for fork F2 (spinifex gate P1.1's promotion leg): after
// the primary node is LOST (local state destroyed, never closed), a fresh
// VB over the same backend, seeded only with the replica's WAL files via
// InstallRecoveryWALFiles, must recover every FLUSHED write.
//
// WHAT IS GATED: flushed data survives primary loss through the replica
// (gen-2 rewrites must win over gen-1 -- SeqNum dedup across replay).
// WHAT IS DOCUMENTED (not a failure): an acked-but-UNFLUSHED write is
// expected LOST -- that is the known 14.2% memory-ack window (P-1.3),
// the target of the NEXT F2 slice (replicate-on-WriteAt / WAL-on-ack).

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/mulgadc/viperblock/walrepl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// openPromotionTestVB opens a production-shaped VB over the SHARED file
// backend at backendDir, with local WAL/state under baseDir. No background
// syncer/uploader: the primary must behave like a node that crashes before
// any chunk consolidation, so flushed data exists ONLY in WAL + replica.
func openPromotionTestVB(t *testing.T, backendDir, baseDir, volumeName string) *VB {
	t.Helper()
	vbconfig := VB{
		VolumeName:          volumeName,
		VolumeSize:          64 * 1024 * 1024,
		BaseDir:             baseDir,
		WALSyncInterval:     -1,
		ChunkUploadInterval: -1,
		SyncOnFlush:         true,
		Cache:               Cache{Config: CacheConfig{Size: 0}},
	}
	vb, err := New(&vbconfig, FileBackend, file.FileConfig{
		VolumeName: volumeName, VolumeSize: 64 * 1024 * 1024, BaseDir: backendDir,
	})
	require.NoError(t, err)
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

func TestPromotionRecoversFlushedWritesFromReplicaWALs(t *testing.T) {
	backendDir := t.TempDir()
	replicaDir := t.TempDir()
	const volumeName = "promotevol"

	server := &walrepl.ReplicaServer{ReplicaWALDirectoryPath: replicaDir}
	address, err := server.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go server.Serve()
	defer server.Close()

	// --- Primary: produce flushed history, then die without closing. ---
	primaryBase := filepath.Join(t.TempDir(), "primary")
	primary := openPromotionTestVB(t, backendDir, primaryBase, volumeName)

	// The replica needs the primary's WAL header so its file is a valid
	// WAL file (the active WAL's current content is exactly the header).
	primaryWALDir := filepath.Join(primaryBase, volumeName, "wal", "chunks")
	entries, err := os.ReadDir(primaryWALDir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	headerBytes, err := os.ReadFile(filepath.Join(primaryWALDir, entries[0].Name()))
	require.NoError(t, err)

	client, err := walrepl.Connect("tcp", address, volumeName, headerBytes)
	require.NoError(t, err)
	primary.Replicator = client

	blockSize := uint64(primary.BlockSize)
	gen1 := func(i uint64) []byte {
		d := make([]byte, blockSize)
		copy(d, fmt.Sprintf("gen1-block-%d", i))
		return d
	}
	gen2 := func(i uint64) []byte {
		d := make([]byte, blockSize)
		copy(d, fmt.Sprintf("gen2-block-%d", i))
		return d
	}
	for i := range uint64(64) {
		require.NoError(t, primary.WriteAt(i*blockSize, gen1(i)))
	}
	// Rewrite the first 16 blocks: replay must pick gen-2 (higher SeqNum).
	for i := range uint64(16) {
		require.NoError(t, primary.WriteAt(i*blockSize, gen2(i)))
	}
	require.NoError(t, primary.Flush()) // barrier: durable on the replica

	// One acked-but-UNFLUSHED write: documents the known memory-ack
	// window (P-1.3: 14.2%) -- replication does not cover it yet.
	unflushed := make([]byte, blockSize)
	copy(unflushed, "acked-but-unflushed")
	require.NoError(t, primary.WriteAt(100*blockSize, unflushed))

	// --- Primary node loss: local state destroyed, never closed. ---
	require.NoError(t, client.Close())
	require.NoError(t, os.RemoveAll(primaryBase))

	// --- Promotion: fresh node dir seeded ONLY with replica WAL files. ---
	replicaFiles, err := walrepl.ListReplicaWALFiles(replicaDir, volumeName)
	require.NoError(t, err)
	require.NotEmpty(t, replicaFiles, "replica produced no WAL files")

	promotedBase := filepath.Join(t.TempDir(), "promoted")
	installed, err := InstallRecoveryWALFiles(replicaFiles, promotedBase, volumeName)
	require.NoError(t, err)
	require.Equal(t, len(replicaFiles), installed)

	promoted := openPromotionTestVB(t, backendDir, promotedBase, volumeName)
	defer func() { assert.NoError(t, promoted.Close()) }()

	// Every flushed write must be present with its NEWEST content.
	for i := range uint64(64) {
		got, err := promoted.ReadAt(i*blockSize, blockSize)
		require.NoError(t, err, "block %d", i)
		expected := gen1(i)
		if i < 16 {
			expected = gen2(i)
		}
		assert.True(t, bytes.Equal(got, expected), "block %d content mismatch after promotion", i)
	}

	// The unflushed write is EXPECTED lost (known window, next F2 slice).
	// Never-written/lost blocks read back zero-filled with the ErrZeroBlock
	// sentinel -- the engine's zero-read contract.
	got, err := promoted.ReadAt(100*blockSize, blockSize)
	if err != nil {
		require.ErrorIs(t, err, ErrZeroBlock, "unflushed block read failed with a non-zero-block error")
	}
	assert.True(t, bytes.Equal(got, make([]byte, blockSize)),
		"acked-but-unflushed block unexpectedly present with data; if the next F2 slice landed, update this expectation")

	// A never-written region still reads as zeroes (sentinel expected).
	zero, err := promoted.ReadAt(200*blockSize, blockSize)
	if err != nil {
		require.ErrorIs(t, err, ErrZeroBlock)
	}
	assert.True(t, bytes.Equal(zero, make([]byte, blockSize)))
}
