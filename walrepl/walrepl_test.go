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

func TestReconnectingClientResyncsAfterReplicaRestart(t *testing.T) {
	replicaDir := t.TempDir()
	server := &ReplicaServer{ReplicaWALDirectoryPath: replicaDir}
	address, err := server.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go server.Serve()

	header := []byte("VBWL-RECONNECT-TEST")
	rc, err := ConnectWithReconnect("tcp", address, "vol-reconnect", header, 0, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer rc.Close()

	// Send 10 records and barrier.
	for i := 0; i < 10; i++ {
		record := bytes.Repeat([]byte{byte(i + 1)}, 100)
		if err := rc.Append(record); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if err := rc.Barrier(); err != nil {
		t.Fatalf("initial barrier: %v", err)
	}

	// Force connection break by closing the underlying client connection.
	rc.mu.Lock()
	if rc.client != nil {
		rc.client.Close()
	}
	rc.mu.Unlock()

	// Kill the old server and restart at the same address.
	server.Close()
	time.Sleep(200 * time.Millisecond)

	server2 := &ReplicaServer{ReplicaWALDirectoryPath: replicaDir}
	_, err = server2.Listen("tcp", address)
	if err != nil {
		t.Fatalf("restart listen: %v", err)
	}
	go server2.Serve()
	defer server2.Close()

	// Send 10 more records (should trigger reconnect and resync).
	for i := 10; i < 20; i++ {
		record := bytes.Repeat([]byte{byte(i + 1)}, 100)
		if err := rc.Append(record); err != nil {
			t.Fatalf("append %d after reconnect: %v", i, err)
		}
	}

	// Barrier should succeed after reconnect+resync.
	if err := rc.Barrier(); err != nil {
		t.Fatalf("barrier after reconnect: %v", err)
	}

	// Verify we have 2 replica files (one per connection).
	time.Sleep(100 * time.Millisecond)
	entries, err := os.ReadDir(replicaDir)
	if err != nil {
		t.Fatalf("read replica dir: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 replica files, got %d", len(entries))
	}
}

func TestReconnectingClientEntersDegradedModeAfterExhaustion(t *testing.T) {
	// Start a server, connect, then kill it permanently.
	server := &ReplicaServer{ReplicaWALDirectoryPath: t.TempDir()}
	address, err := server.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go server.Serve()

	rc, err := ConnectWithReconnect("tcp", address, "vol-test", []byte("H"), 3, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer rc.Close()

	// Force connection break by closing the underlying client.
	rc.mu.Lock()
	if rc.client != nil {
		rc.client.Close()
	}
	rc.mu.Unlock()

	// Kill the server permanently so reconnect attempts will fail.
	server.Close()
	time.Sleep(100 * time.Millisecond)

	// Append should trigger reconnect, exhaust 3 attempts, and enter degraded mode.
	err = rc.Append(make([]byte, 100))
	if err == nil {
		t.Fatal("expected append to fail with degraded mode error, got nil")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("degraded mode")) {
		t.Fatalf("expected degraded mode error on first operation, got: %v", err)
	}

	// Subsequent operations should also fail with degraded mode.
	err = rc.Barrier()
	if err == nil || !bytes.Contains([]byte(err.Error()), []byte("degraded mode")) {
		t.Fatalf("expected degraded mode error on barrier, got: %v", err)
	}
}
