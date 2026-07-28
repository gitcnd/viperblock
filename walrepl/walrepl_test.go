package walrepl

import (
	"bytes"
	"crypto/rand"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func startTestReplica(t *testing.T) (*ReplicaServer, string, string) {
	t.Helper()
	replicaDir := t.TempDir()
	server := &ReplicaServer{ReplicaWALDirectoryPath: replicaDir}
	address, err := server.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go server.Serve()
	t.Cleanup(func() { server.Close() })
	return server, address, replicaDir
}

func replicaFileIn(t *testing.T, replicaDir string) string {
	t.Helper()
	for attempt := 0; attempt < 50; attempt++ {
		entries, err := os.ReadDir(replicaDir)
		if err == nil && len(entries) == 1 {
			return filepath.Join(replicaDir, entries[0].Name())
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("replica file did not appear")
	return ""
}

func TestReplicationStreamsRecordsByteExact(t *testing.T) {
	_, address, replicaDir := startTestReplica(t)

	header := []byte("VBWL-TESTHEADER")
	client, err := Connect("tcp", address, "vol-test", header)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	var expected bytes.Buffer
	expected.Write(header)
	for i := 0; i < 100; i++ {
		record := make([]byte, 100+i*37)
		rand.Read(record)
		expected.Write(record)
		if err := client.Append(record); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if err := client.Barrier(); err != nil {
		t.Fatalf("barrier: %v", err)
	}

	replicaBytes, err := os.ReadFile(replicaFileIn(t, replicaDir))
	if err != nil {
		t.Fatalf("read replica file: %v", err)
	}
	if !bytes.Equal(replicaBytes, expected.Bytes()) {
		t.Fatalf("replica file differs: %d bytes vs %d expected", len(replicaBytes), expected.Len())
	}
}

func TestBarrierWithNothingPendingReturnsImmediately(t *testing.T) {
	_, address, _ := startTestReplica(t)
	client, err := Connect("tcp", address, "vol-test", []byte("H"))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()
	done := make(chan error, 1)
	go func() { done <- client.Barrier() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("empty barrier: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("empty barrier blocked")
	}
}

func TestBarrierFailsWhenReplicaDies(t *testing.T) {
	// Fake replica: accepts, reads the handshake, then hangs up without
	// ever acking -- a deterministic dead-replica shape.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			return
		}
		buffer := make([]byte, 1024)
		_, _ = connection.Read(buffer)
		connection.Close()
	}()

	client, err := Connect("tcp", listener.Addr().String(), "vol-test", []byte("H"))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()
	_ = client.Append(make([]byte, 4096)) // send may or may not error yet
	done := make(chan error, 1)
	go func() { done <- client.Barrier() }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("barrier succeeded after replica death")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("barrier hung after replica death")
	}
}
