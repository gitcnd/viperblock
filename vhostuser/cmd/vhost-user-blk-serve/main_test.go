// Tests for the viperblock-engine wiring of vhost-user-blk-serve.
//
// WHAT IS GATED: the zero-block translation semantics of the adapter (a
// virtio-blk read of a never-written region must succeed with zeroes, not
// error) and the write/flush/read round trip through the REAL viperblock
// engine opened by the production open sequence, file backend.
//
// Run with: GOTOOLCHAIN=auto GOFIPS140=v1.0.0 go test ./vhostuser/cmd/...
// (fipsboot guard; see the command's build note).
package main

import (
	"bytes"
	"path/filepath"
	"testing"
)

func TestZeroBlockTranslatingViperblockEngineReadWriteSemantics(t *testing.T) {
	tempDir := t.TempDir()
	vb, err := openViperblockVolumeProductionSequence(viperblockServeConfig{
		backendType:             "file",
		volumeName:              "vhost-adapter-test",
		volumeSizeBytes:         8 * 1024 * 1024,
		localWALAndStateBaseDir: filepath.Join(tempDir, "wal"),
		fileBackendObjectDir:    filepath.Join(tempDir, "objects"),
		cacheSizePercent:        20,
	})
	if err != nil {
		t.Fatalf("open volume: %v", err)
	}
	defer func() {
		if err := vb.Close(); err != nil {
			t.Errorf("close volume: %v", err)
		}
	}()
	engine := &zeroBlockTranslatingViperblockEngine{vb: vb}

	// A never-written region must read as full-length zeroes with a nil
	// error: the engine reports it with the ErrZeroBlock sentinel, and the
	// adapter must translate that to a normal successful read.
	zeroRegion, err := engine.ReadAt(1*1024*1024, 8192)
	if err != nil {
		t.Fatalf("read of never-written region returned error: %v", err)
	}
	if len(zeroRegion) != 8192 {
		t.Fatalf("read of never-written region returned %d bytes, want 8192", len(zeroRegion))
	}
	if !bytes.Equal(zeroRegion, make([]byte, 8192)) {
		t.Fatalf("read of never-written region returned non-zero bytes")
	}

	// Write / flush / read-back round trip through the real engine.
	const writeOffset = 12288 // block-aligned (3 x 4 KiB)
	payload := make([]byte, 3*4096)
	for i := range payload {
		payload[i] = byte(i % 251) // deterministic non-zero pattern
	}
	if err := engine.WriteAt(writeOffset, payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := engine.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	readBack, err := engine.ReadAt(writeOffset, uint64(len(payload)))
	if err != nil {
		t.Fatalf("read-back: %v", err)
	}
	if !bytes.Equal(readBack, payload) {
		t.Fatalf("read-back differs from written payload")
	}

	// A read straddling the last written block and the following
	// never-written block must return the written bytes then zeroes.
	straddle, err := engine.ReadAt(writeOffset+8192, 8192)
	if err != nil {
		t.Fatalf("straddling read returned error: %v", err)
	}
	if !bytes.Equal(straddle[:4096], payload[8192:]) {
		t.Fatalf("straddling read: written half differs")
	}
	if !bytes.Equal(straddle[4096:], make([]byte, 4096)) {
		t.Fatalf("straddling read: never-written half is non-zero")
	}
}
