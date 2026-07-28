package viperblock

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mulgadc/viperblock/walrepl"
	"github.com/stretchr/testify/require"
)

// TestFlushBarrierReplicatesWALByteExact wires a real walrepl replica into
// a file-backed VB: after WriteAt + Flush, the replica's WAL file must be
// byte-identical to the primary's active WAL file (fork F2 contract: the
// flush barrier means durable on the peer, in order, exactly).
func TestFlushBarrierReplicatesWALByteExact(t *testing.T) {
	replicaDir := t.TempDir()
	server := &walrepl.ReplicaServer{ReplicaWALDirectoryPath: replicaDir}
	address, err := server.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go server.Serve()
	defer server.Close()

	vb := newBarrierTestVB(t, true)

	// Hand the replica a copy of the primary's current WAL header so the
	// replica file is a valid WAL file. The active WAL was just opened, so
	// its current content is exactly the header.
	primaryWALPath := filepath.Join(vb.WAL.BaseDir,
		"barrier-test", "wal", "chunks")
	entries, err := os.ReadDir(primaryWALPath)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	primaryWALFile := filepath.Join(primaryWALPath, entries[0].Name())
	headerBytes, err := os.ReadFile(primaryWALFile)
	require.NoError(t, err)

	client, err := walrepl.Connect("tcp", address, "barrier-test", headerBytes)
	require.NoError(t, err)
	defer client.Close()
	vb.Replicator = client

	// Write blocks and flush: the barrier must cover replication.
	for i := uint64(0); i < 32; i++ {
		data := bytes.Repeat([]byte{byte(i + 1)}, int(DefaultBlockSize))
		require.NoError(t, vb.WriteAt(i*uint64(DefaultBlockSize), data))
	}
	require.NoError(t, vb.Flush())

	primaryBytes, err := os.ReadFile(primaryWALFile)
	require.NoError(t, err)

	var replicaBytes []byte
	require.Eventually(t, func() bool {
		replicaEntries, err := os.ReadDir(replicaDir)
		if err != nil || len(replicaEntries) != 1 {
			return false
		}
		replicaBytes, err = os.ReadFile(filepath.Join(replicaDir, replicaEntries[0].Name()))
		return err == nil && len(replicaBytes) == len(primaryBytes)
	}, 5*time.Second, 20*time.Millisecond, "replica file never matched primary size")

	require.Equal(t, primaryBytes, replicaBytes,
		"replica WAL must be byte-identical to the primary's active WAL after a flush barrier")
}

// TestFlushBarrierFailsWhenReplicaUnreachable: with a Replicator whose peer
// is gone, Flush must return an error, never silently ack.
func TestFlushBarrierFailsWhenReplicaUnreachable(t *testing.T) {
	replicaDir := t.TempDir()
	server := &walrepl.ReplicaServer{ReplicaWALDirectoryPath: replicaDir}
	address, err := server.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go server.Serve()

	vb := newBarrierTestVB(t, true)
	client, err := walrepl.Connect("tcp", address, "barrier-test", []byte("H"))
	require.NoError(t, err)
	vb.Replicator = client

	// Drop the replica hard: close the client-side socket so appends and
	// the ack stream break deterministically.
	require.NoError(t, client.Close())
	server.Close()

	data := make([]byte, DefaultBlockSize)
	require.NoError(t, vb.WriteAt(0, data))
	err = vb.Flush()
	require.Error(t, err, "flush barrier must fail when the replica is unreachable")
}
