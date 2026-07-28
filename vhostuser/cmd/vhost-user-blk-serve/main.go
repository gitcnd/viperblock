// Command vhost-user-blk-serve exposes a block engine as a vhost-user-blk
// device (spinifex fork F7). Engine selection:
//
//	--raw-file PATH      serve a plain file (QEMU protocol smoke tests)
//	--vb-backend TYPE    serve a REAL viperblock WAL engine; TYPE selects
//	                     the durable backing store: "file" or "s3"
//
// The viperblock mode (added 2026-07-28, the F7 production-wiring slice)
// mirrors the nbdkit plugin's open sequence exactly (nbd/viperblock.go
// Open: New -> Backend.Init -> LoadState -> EnsureVolumeUUID ->
// LoadLiveCheckpoint -> RecoverLocalWALs -> OpenWAL chunk+block), so
// vhost-user serving inherits identical recovery semantics. Unlike the
// nbdkit plugin, the volume is opened ONCE and stays open across frontend
// reconnects -- there is no per-connection close/open lifecycle, which
// also sidesteps the close/open race family fixed on
// fix/nbd-close-open-race. SIGINT/SIGTERM drain to the backend and close
// the volume cleanly before exit.
//
// Build note: MUST be built with GOFIPS140=v1.0.0 (viperblock's fipsboot
// guard panics at runtime otherwise -- see spinifex ENVIRONMENT.md traps).
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/vhostuser"
	"github.com/mulgadc/viperblock/viperblock"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/mulgadc/viperblock/viperblock/backends/s3"

	_ "github.com/mulgadc/viperblock/internal/fipsboot"
)

// rawFileBlockEngine adapts a plain file to vhostuser.BlockEngine.
type rawFileBlockEngine struct{ file *os.File }

func (e *rawFileBlockEngine) ReadAt(offset uint64, length uint64) ([]byte, error) {
	buffer := make([]byte, length)
	_, err := e.file.ReadAt(buffer, int64(offset))
	return buffer, err
}

func (e *rawFileBlockEngine) WriteAt(offset uint64, data []byte) error {
	_, err := e.file.WriteAt(data, int64(offset))
	return err
}

func (e *rawFileBlockEngine) Flush() error { return e.file.Sync() }

// zeroBlockTranslatingViperblockEngine adapts *viperblock.VB to
// vhostuser.BlockEngine. The signatures already match; the one semantic
// difference is that the engine's ReadAt reports never-written blocks with
// the ErrZeroBlock sentinel WHILE STILL returning a valid, full-length,
// zero-filled buffer. A virtio-blk read of an unwritten sector is a normal
// successful read of zeroes, so the sentinel is translated to success here
// -- exactly as the nbdkit plugin's PRead does. Matched with errors.Is
// (never a string compare): the sentinel can arrive wrapped.
type zeroBlockTranslatingViperblockEngine struct {
	vb *viperblock.VB
}

func (e *zeroBlockTranslatingViperblockEngine) ReadAt(offset uint64, length uint64) ([]byte, error) {
	data, err := e.vb.ReadAt(offset, length)
	if err != nil && !errors.Is(err, viperblock.ErrZeroBlock) {
		return nil, err
	}
	return data, nil
}

func (e *zeroBlockTranslatingViperblockEngine) WriteAt(offset uint64, data []byte) error {
	// Safe to hand the guest-memory span straight through: WriteAtCtx
	// copies into per-block buffers before buffering, so the engine never
	// retains the caller's slice after return.
	return e.vb.WriteAt(offset, data)
}

func (e *zeroBlockTranslatingViperblockEngine) Flush() error { return e.vb.Flush() }

// viperblockServeConfig carries the flag values for the viperblock engine
// modes from flag parsing to the open helper.
type viperblockServeConfig struct {
	backendType             string // "file" or "s3"
	volumeName              string
	volumeSizeBytes         uint64
	localWALAndStateBaseDir string
	fileBackendObjectDir    string // vb-backend=file only
	s3Bucket                string // vb-backend=s3 only
	s3Region                string
	s3Host                  string
	cacheSizePercent        int
}

// openViperblockVolumeProductionSequence opens (creating on first use) a
// viperblock volume through the same call sequence as the production nbdkit
// plugin, and returns the live *viperblock.VB ready to serve I/O.
func openViperblockVolumeProductionSequence(cfg viperblockServeConfig) (*viperblock.VB, error) {
	var backendConfig any
	switch cfg.backendType {
	case "file":
		// The file backend requires its object directory to pre-exist
		// (Backend.Init only creates the per-volume subdirectories).
		if err := os.MkdirAll(cfg.fileBackendObjectDir, 0o755); err != nil {
			return nil, fmt.Errorf("create file-backend dir: %w", err)
		}
		backendConfig = file.FileConfig{
			VolumeName: cfg.volumeName,
			VolumeSize: cfg.volumeSizeBytes,
			BaseDir:    cfg.fileBackendObjectDir,
		}
	case "s3":
		// Credentials come from the environment, never argv (a process
		// listing must not leak them -- security audit finding SP-5).
		accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
		secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
		if accessKey == "" || secretKey == "" {
			return nil, fmt.Errorf("vb-backend=s3 needs AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in the environment")
		}
		backendConfig = s3.S3Config{
			VolumeName: cfg.volumeName,
			VolumeSize: cfg.volumeSizeBytes,
			Bucket:     cfg.s3Bucket,
			Region:     cfg.s3Region,
			AccessKey:  accessKey,
			SecretKey:  secretKey,
			Host:       cfg.s3Host,
		}
	default:
		return nil, fmt.Errorf("unknown vb-backend %q (want file or s3)", cfg.backendType)
	}

	if err := os.MkdirAll(cfg.localWALAndStateBaseDir, 0o755); err != nil {
		return nil, fmt.Errorf("create WAL/state base dir: %w", err)
	}

	// Optional encryption exactly as the plugin: ENCRYPTION_KEY_FILE env
	// selects the key; unset means the volume runs unencrypted.
	loadedMasterKey, err := viperblock.LoadMasterKeyFromFlagOrEnv("")
	if err != nil {
		return nil, fmt.Errorf("load encryption key: %w", err)
	}

	vbConfig := viperblock.VB{
		VolumeName: cfg.volumeName,
		VolumeSize: cfg.volumeSizeBytes,
		BaseDir:    cfg.localWALAndStateBaseDir,
		Cache: viperblock.Cache{
			Config: viperblock.CacheConfig{Size: cfg.cacheSizePercent},
		},
		UploadWorkers:     16, // nbdkit plugin default
		MasterKey:         loadedMasterKey,
		EncryptionEnabled: loadedMasterKey != nil,
	}

	vb, err := viperblock.New(&vbConfig, cfg.backendType, backendConfig)
	if err != nil {
		return nil, fmt.Errorf("viperblock.New: %w", err)
	}
	if err := vb.Backend.Init(); err != nil {
		return nil, fmt.Errorf("backend init: %w", err)
	}
	if err := vb.LoadState(); err != nil {
		// No persisted state: brand-new volume. Mirror spinifex
		// CreateVolume (and the crash harness), which persists initial
		// state with SaveState before the first real open.
		if saveErr := vb.SaveState(); saveErr != nil {
			return nil, fmt.Errorf("initial SaveState: %w (LoadState was: %v)", saveErr, err)
		}
		if err := vb.LoadState(); err != nil {
			return nil, fmt.Errorf("load state after initial SaveState: %w", err)
		}
	}
	if err := vb.EnsureVolumeUUID(); err != nil {
		return nil, fmt.Errorf("mint volume UUID: %w", err)
	}
	if err := vb.LoadLiveCheckpoint(); err != nil {
		return nil, fmt.Errorf("load live checkpoint: %w", err)
	}
	if err := vb.RecoverLocalWALs(); err != nil {
		return nil, fmt.Errorf("recover local WALs: %w", err)
	}
	vb.WAL.WallNum.Add(1)
	// Legacy (non-sharded) WAL only: this matches the serving default the
	// nbdkit plugin uses, and is the shape the F2 replication work targets.
	if err := vb.OpenWAL(&vb.WAL, fmt.Sprintf("%s/%s", vb.WAL.BaseDir,
		types.GetFilePath(types.FileTypeWALChunk, vb.WAL.WallNum.Load(), vb.GetVolume()))); err != nil {
		return nil, fmt.Errorf("open chunk WAL: %w", err)
	}
	if err := vb.OpenWAL(&vb.BlockToObjectWAL, fmt.Sprintf("%s/%s", vb.WAL.BaseDir,
		types.GetFilePath(types.FileTypeWALBlock, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume()))); err != nil {
		return nil, fmt.Errorf("open block WAL: %w", err)
	}
	return vb, nil
}

func main() {
	socketPath := flag.String("socket", "", "unix socket to serve vhost-user on (required)")
	rawFilePath := flag.String("raw-file", "", "raw file to serve (smoke-test engine)")
	vbBackendType := flag.String("vb-backend", "", "serve a real viperblock volume backed by \"file\" or \"s3\"")
	vbVolumeName := flag.String("vb-volume", "", "viperblock volume name (vb modes)")
	vbVolumeSizeBytes := flag.Uint64("vb-size-bytes", 0, "viperblock volume size in bytes (vb modes)")
	vbLocalBaseDir := flag.String("vb-base-dir", "", "viperblock local WAL/state directory (vb modes)")
	vbFileBackendDir := flag.String("vb-file-dir", "", "file-backend object directory (vb-backend=file)")
	vbS3Bucket := flag.String("vb-s3-bucket", "", "S3 bucket (vb-backend=s3)")
	vbS3Region := flag.String("vb-s3-region", "", "S3 region (vb-backend=s3)")
	vbS3Host := flag.String("vb-s3-host", "", "S3 endpoint host (vb-backend=s3)")
	vbCacheSizePercent := flag.Int("vb-cache-size", 20, "viperblock read-cache size percent (nbdkit plugin default 20)")
	flag.Parse()

	if *socketPath == "" {
		fmt.Fprintln(os.Stderr, "--socket is required")
		os.Exit(4)
	}

	switch {
	case *rawFilePath != "" && *vbBackendType != "":
		fmt.Fprintln(os.Stderr, "--raw-file and --vb-backend are mutually exclusive")
		os.Exit(4)

	case *rawFilePath != "":
		serveRawFileUntilKilled(*socketPath, *rawFilePath)

	case *vbBackendType != "":
		if *vbVolumeName == "" || *vbVolumeSizeBytes == 0 || *vbLocalBaseDir == "" {
			fmt.Fprintln(os.Stderr, "--vb-volume, --vb-size-bytes and --vb-base-dir are required with --vb-backend")
			os.Exit(4)
		}
		if *vbBackendType == "file" && *vbFileBackendDir == "" {
			fmt.Fprintln(os.Stderr, "--vb-file-dir is required with --vb-backend=file")
			os.Exit(4)
		}
		if *vbBackendType == "s3" && (*vbS3Bucket == "" || *vbS3Region == "" || *vbS3Host == "") {
			fmt.Fprintln(os.Stderr, "--vb-s3-bucket, --vb-s3-region and --vb-s3-host are required with --vb-backend=s3")
			os.Exit(4)
		}
		serveViperblockUntilSignalled(*socketPath, viperblockServeConfig{
			backendType:             *vbBackendType,
			volumeName:              *vbVolumeName,
			volumeSizeBytes:         *vbVolumeSizeBytes,
			localWALAndStateBaseDir: *vbLocalBaseDir,
			fileBackendObjectDir:    *vbFileBackendDir,
			s3Bucket:                *vbS3Bucket,
			s3Region:                *vbS3Region,
			s3Host:                  *vbS3Host,
			cacheSizePercent:        *vbCacheSizePercent,
		})

	default:
		fmt.Fprintln(os.Stderr, "one of --raw-file or --vb-backend is required")
		os.Exit(4)
	}
}

// serveRawFileUntilKilled preserves the original smoke-test behavior: loop
// over single-frontend connections forever; signals kill the process (a raw
// file needs no orderly teardown beyond the kernel's page cache).
func serveRawFileUntilKilled(socketPath string, rawFilePath string) {
	fileHandle, err := os.OpenFile(rawFilePath, os.O_RDWR, 0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open raw file: %v\n", err)
		os.Exit(4)
	}
	info, err := fileHandle.Stat()
	if err != nil {
		fmt.Fprintf(os.Stderr, "stat raw file: %v\n", err)
		os.Exit(4)
	}
	backend := &vhostuser.Backend{
		Engine:        &rawFileBlockEngine{file: fileHandle},
		CapacityBytes: uint64(info.Size()),
	}
	fmt.Printf("vhost-user-blk-serve: raw file %s (%d bytes) on %s\n", rawFilePath, info.Size(), socketPath)
	for {
		if err := backend.ServeOneConnection(socketPath); err != nil {
			fmt.Fprintf(os.Stderr, "connection ended: %v\n", err)
		}
	}
}

// serveViperblockUntilSignalled opens the volume once, serves frontend
// connections (sequentially; reconnects reuse the same open volume), and on
// SIGINT/SIGTERM drains buffered writes to the backend and closes the
// volume before exiting -- the same teardown the nbdkit plugin performs.
func serveViperblockUntilSignalled(socketPath string, cfg viperblockServeConfig) {
	vb, err := openViperblockVolumeProductionSequence(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open viperblock volume: %v\n", err)
		os.Exit(4)
	}
	backend := &vhostuser.Backend{
		Engine:        &zeroBlockTranslatingViperblockEngine{vb: vb},
		CapacityBytes: vb.GetVolumeSize(),
	}
	fmt.Printf("vhost-user-blk-serve: viperblock volume %s (%d bytes, backend %s) on %s\n",
		cfg.volumeName, vb.GetVolumeSize(), cfg.backendType, socketPath)

	terminationSignals := make(chan os.Signal, 1)
	signal.Notify(terminationSignals, syscall.SIGINT, syscall.SIGTERM)
	connectionEnded := make(chan error, 1)
	go func() {
		for {
			connectionEnded <- backend.ServeOneConnection(socketPath)
		}
	}()
	for {
		select {
		case err := <-connectionEnded:
			fmt.Fprintf(os.Stderr, "connection ended: %v\n", err)
		case receivedSignal := <-terminationSignals:
			fmt.Printf("vhost-user-blk-serve: %v -- draining and closing volume\n", receivedSignal)
			if err := vb.DrainToBackend(); err != nil {
				fmt.Fprintf(os.Stderr, "drain to backend: %v\n", err)
			}
			if err := vb.Close(); err != nil {
				fmt.Fprintf(os.Stderr, "close volume: %v\n", err)
				os.Exit(1)
			}
			os.Exit(0)
		}
	}
}
