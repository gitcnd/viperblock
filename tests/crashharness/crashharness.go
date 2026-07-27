// Package crashharness implements a crash-consistency measurement harness
// for viperblock (gate P-1.3 of the spinifex Outposts-parity project's
// PHASE_GATES.md).
//
// It measures, through viperblock's PUBLIC API only (WriteAt / Flush / the
// production open-and-recover sequence), two numbers:
//
//   - how many acknowledged-but-not-yet-flushed writes a SIGKILL loses
//     (informational: viperblock acknowledges writes from the in-memory
//     buffer by design, so a nonzero number here is expected), and
//   - how many acknowledged-and-FLUSHed writes a SIGKILL loses
//     (gated: must be zero -- vb.Flush() is the barrier the NBD plugin maps
//     a guest FLUSH/fsync onto).
//
// SCOPE HONESTY: SIGKILL of the writer process validates the user-space
// pipeline (memory write buffer -> WAL file write via the OS page cache).
// It does NOT validate host-power-loss durability, which additionally
// requires fsync: as of v1.13.0, vb.Flush() writes WAL records to the file
// descriptor but does not fsync it (only the periodic WAL syncer, default
// every 200 ms, does). A power-loss test needs a VM/blkdebug harness and is
// tracked separately in the project plan.
package crashharness

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"

	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
)

// HarnessBlockSizeBytes matches viperblock.DefaultBlockSize; every harness
// write is exactly one block, block-aligned.
const HarnessBlockSizeBytes = 4096

// verifiableBlockPatternMagic marks a block as written by this harness.
// 8 bytes so the header stays aligned.
var verifiableBlockPatternMagic = []byte("VBCRHRN1")

// EncodeVerifiableBlockPattern builds a self-describing 4096-byte block:
//
//	[magic(8) | blockNumber(8) | generation(8) | deterministic payload | crc32(4)]
//
// The payload is derived from (blockNumber, generation) so the verifier can
// detect torn or foreign content without any external state.
func EncodeVerifiableBlockPattern(blockNumber uint64, generation uint64) []byte {
	blockData := make([]byte, HarnessBlockSizeBytes)
	copy(blockData[0:8], verifiableBlockPatternMagic)
	binary.BigEndian.PutUint64(blockData[8:16], blockNumber)
	binary.BigEndian.PutUint64(blockData[16:24], generation)
	// Deterministic payload: repeating 16-byte stripes of (blockNumber ^ i,
	// generation ^ i) so every byte position is content-checked.
	for offset := 24; offset+16 <= HarnessBlockSizeBytes-4; offset += 16 {
		binary.BigEndian.PutUint64(blockData[offset:offset+8], blockNumber^uint64(offset))
		binary.BigEndian.PutUint64(blockData[offset+8:offset+16], generation^uint64(offset))
	}
	checksum := crc32.ChecksumIEEE(blockData[:HarnessBlockSizeBytes-4])
	binary.BigEndian.PutUint32(blockData[HarnessBlockSizeBytes-4:], checksum)
	return blockData
}

// DecodeVerifiableBlockPattern inspects a 4096-byte block read back from the
// volume. Returns (generation, isAllZero, error). A block of all zeroes is
// reported with isAllZero=true and no error (a never-persisted block reads
// back as zeroes). Any other content that fails the magic/CRC/self-describing
// checks returns an error (= corruption).
func DecodeVerifiableBlockPattern(expectedBlockNumber uint64, blockData []byte) (generation uint64, isAllZero bool, err error) {
	if len(blockData) != HarnessBlockSizeBytes {
		return 0, false, fmt.Errorf("read returned %d bytes, want %d", len(blockData), HarnessBlockSizeBytes)
	}
	allZero := true
	for _, b := range blockData {
		if b != 0 {
			allZero = false
			break
		}
	}
	if allZero {
		return 0, true, nil
	}
	if !bytes.Equal(blockData[0:8], verifiableBlockPatternMagic) {
		return 0, false, errors.New("pattern magic mismatch")
	}
	storedChecksum := binary.BigEndian.Uint32(blockData[HarnessBlockSizeBytes-4:])
	computedChecksum := crc32.ChecksumIEEE(blockData[:HarnessBlockSizeBytes-4])
	if storedChecksum != computedChecksum {
		return 0, false, fmt.Errorf("pattern crc mismatch: stored %08x computed %08x", storedChecksum, computedChecksum)
	}
	storedBlockNumber := binary.BigEndian.Uint64(blockData[8:16])
	if storedBlockNumber != expectedBlockNumber {
		return 0, false, fmt.Errorf("pattern block number mismatch: stored %d expected %d", storedBlockNumber, expectedBlockNumber)
	}
	return binary.BigEndian.Uint64(blockData[16:24]), false, nil
}

// AcknowledgedWriteLog is the harness's own append-only evidence file. A "W"
// line is appended only AFTER vb.WriteAt returned success; a "FLUSH" line is
// appended only AFTER vb.Flush returned success. Plain (unsynced) file
// appends are sufficient for SIGKILL tests: the OS page cache survives
// process death. The safe failure direction is understatement: a crash
// between an ack and its log line hides a write we COULD have checked, and
// never invents one we could not.
type AcknowledgedWriteLog struct {
	logFile *os.File
	mu      sync.Mutex
}

func OpenAcknowledgedWriteLogForAppend(path string) (*AcknowledgedWriteLog, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &AcknowledgedWriteLog{logFile: f}, nil
}

func (l *AcknowledgedWriteLog) RecordAcknowledgedWrite(blockNumber uint64, generation uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, err := fmt.Fprintf(l.logFile, "W %d %d\n", blockNumber, generation)
	return err
}

// RecordSessionStart marks a writer-process boundary. Must be appended once
// at open, before any write of the new process is acknowledged.
func (l *AcknowledgedWriteLog) RecordSessionStart() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, err := fmt.Fprintf(l.logFile, "SESSION\n")
	return err
}

func (l *AcknowledgedWriteLog) RecordFlushBarrier() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, err := fmt.Fprintf(l.logFile, "FLUSH\n")
	return err
}

func (l *AcknowledgedWriteLog) Close() error { return l.logFile.Close() }

// ExpectedBlockState is what the verifier demands of one block after a crash.
type ExpectedBlockState struct {
	// DurableFloorGeneration is the highest generation that was BOTH
	// acknowledged AND covered by a FLUSH barrier within the same writer
	// session (process lifetime). Content below this generation is a lost
	// flushed write (gate violation). Zero means no write to this block was
	// ever covered by a barrier.
	//
	// SESSION SCOPING (lesson learned 2026-07-27): a FLUSH barrier can only
	// cover writes acknowledged by the SAME process. Writes that were
	// acknowledged but unflushed when their process was killed died in that
	// process's memory; a later session's FLUSH cannot resurrect them. The
	// first version of this parser ignored session boundaries and falsely
	// accused viperblock of losing 100s of "flushed" writes per cycle.
	DurableFloorGeneration uint64
	// HighestAcknowledgedGeneration is the highest generation ever
	// acknowledged across all sessions. Content between the floor and this
	// bound is an acceptable (acked-but-unflushed) loss, counted
	// informationally.
	HighestAcknowledgedGeneration uint64
}

// ParseAcknowledgedWriteLog folds the log into per-block expectations.
// Log grammar: "SESSION" (writer process started), "W <block> <gen>"
// (write acknowledged), "FLUSH" (vb.Flush returned in the same process).
func ParseAcknowledgedWriteLog(path string) (perBlock map[uint64]*ExpectedBlockState, flushBarrierCount int, acknowledgedWriteCount int, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, 0, err
	}
	defer f.Close()

	perBlock = make(map[uint64]*ExpectedBlockState)
	// currentSessionPendingGenerations: highest gen acked per block within
	// the CURRENT session that a future FLUSH in this session would cover.
	// Discarded wholesale at each SESSION line (the previous process died
	// with them, unless a FLUSH already promoted them to the floor).
	currentSessionPendingGenerations := make(map[uint64]uint64)
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case line == "SESSION":
			currentSessionPendingGenerations = make(map[uint64]uint64)
			continue
		case line == "FLUSH":
			flushBarrierCount++
			for blockNumber, pendingGeneration := range currentSessionPendingGenerations {
				state := perBlock[blockNumber]
				if pendingGeneration > state.DurableFloorGeneration {
					state.DurableFloorGeneration = pendingGeneration
				}
			}
			continue
		}
		var blockNumber, generation uint64
		if _, scanErr := fmt.Sscanf(line, "W %d %d", &blockNumber, &generation); scanErr != nil {
			// A torn final line (killed mid-append) is expected; ignore it.
			continue
		}
		acknowledgedWriteCount++
		state, ok := perBlock[blockNumber]
		if !ok {
			state = &ExpectedBlockState{}
			perBlock[blockNumber] = state
		}
		if generation > state.HighestAcknowledgedGeneration {
			state.HighestAcknowledgedGeneration = generation
		}
		if generation > currentSessionPendingGenerations[blockNumber] {
			currentSessionPendingGenerations[blockNumber] = generation
		}
	}
	return perBlock, flushBarrierCount, acknowledgedWriteCount, scanner.Err()
}

// VolumeHarnessConfig locates one harness volume on the local filesystem.
type VolumeHarnessConfig struct {
	// HarnessRootDirectoryPath holds everything: backend objects, WALs, and
	// the acknowledged-write log.
	HarnessRootDirectoryPath string
	VolumeName               string
	VolumeSizeBytes          uint64
}

func (c VolumeHarnessConfig) AcknowledgedWriteLogPath() string {
	return filepath.Join(c.HarnessRootDirectoryPath, "acknowledged_write_log.txt")
}

// OpenVolumeLikeProductionNBDPluginOpen creates (first run) or reopens+
// recovers (subsequent runs) the harness volume, mirroring the sequence in
// nbd/viperblock.go Open(): New -> Backend.Init -> LoadState ->
// EnsureVolumeUUID -> LoadLiveCheckpoint -> RecoverLocalWALs -> next WAL num
// -> OpenWAL(chunk) -> OpenWAL(block-to-object). First-run creation mirrors
// spinifex CreateVolume (New + Backend.Init + SaveState).
func OpenVolumeLikeProductionNBDPluginOpen(cfg VolumeHarnessConfig) (*viperblock.VB, error) {
	return openVolumeInternal(cfg, false)
}

// OpenVolumeSkippingWALRecovery is a DIAGNOSTIC open that loads state and
// the live checkpoint but does NOT replay orphaned WALs. Used to determine
// whether a missing block was already absent from the checkpointed map or
// was destroyed by the recovery replay itself.
func OpenVolumeSkippingWALRecovery(cfg VolumeHarnessConfig) (*viperblock.VB, error) {
	return openVolumeInternal(cfg, true)
}

func openVolumeInternal(cfg VolumeHarnessConfig, skipWALRecovery bool) (*viperblock.VB, error) {
	// The file backend's Init requires its BaseDir to already exist.
	for _, requiredDirectory := range []string{
		filepath.Join(cfg.HarnessRootDirectoryPath, "backend"),
		filepath.Join(cfg.HarnessRootDirectoryPath, "vb"),
	} {
		if err := os.MkdirAll(requiredDirectory, 0o755); err != nil {
			return nil, fmt.Errorf("mkdir %s: %w", requiredDirectory, err)
		}
	}

	backendConfig := file.FileConfig{
		VolumeName: cfg.VolumeName,
		VolumeSize: cfg.VolumeSizeBytes,
		BaseDir:    filepath.Join(cfg.HarnessRootDirectoryPath, "backend"),
	}
	vbconfig := viperblock.VB{
		VolumeName: cfg.VolumeName,
		VolumeSize: cfg.VolumeSizeBytes,
		BaseDir:    filepath.Join(cfg.HarnessRootDirectoryPath, "vb"),
		Cache: viperblock.Cache{
			Config: viperblock.CacheConfig{Size: 64 * 1024 * 1024},
		},
	}

	vb, err := viperblock.New(&vbconfig, "file", backendConfig)
	if err != nil {
		return nil, fmt.Errorf("viperblock.New: %w", err)
	}
	// Legacy (non-sharded) WAL: the NBD plugin's default mode.
	vb.UseShardedWAL = false
	vb.ShardedWAL = nil

	if err = vb.Backend.Init(); err != nil {
		return nil, fmt.Errorf("backend init: %w", err)
	}

	if err = vb.LoadState(); err != nil {
		// No persisted state yet: first run. Mirror spinifex CreateVolume,
		// which persists initial state with SaveState before any open.
		if saveErr := vb.SaveState(); saveErr != nil {
			return nil, fmt.Errorf("initial SaveState: %w (LoadState was: %v)", saveErr, err)
		}
		if err = vb.LoadState(); err != nil {
			return nil, fmt.Errorf("LoadState after initial SaveState: %w", err)
		}
	}

	if err = vb.EnsureVolumeUUID(); err != nil {
		return nil, fmt.Errorf("EnsureVolumeUUID: %w", err)
	}
	if err = vb.LoadLiveCheckpoint(); err != nil {
		return nil, fmt.Errorf("LoadLiveCheckpoint: %w", err)
	}
	if !skipWALRecovery {
		if err = vb.RecoverLocalWALs(); err != nil {
			return nil, fmt.Errorf("RecoverLocalWALs: %w", err)
		}
	}

	vb.WAL.WallNum.Add(1)
	if err = vb.OpenWAL(&vb.WAL, fmt.Sprintf("%s/%s", vb.WAL.BaseDir,
		types.GetFilePath(types.FileTypeWALChunk, vb.WAL.WallNum.Load(), vb.GetVolume()))); err != nil {
		return nil, fmt.Errorf("OpenWAL chunk: %w", err)
	}
	if err = vb.OpenWAL(&vb.BlockToObjectWAL, fmt.Sprintf("%s/%s", vb.WAL.BaseDir,
		types.GetFilePath(types.FileTypeWALBlock, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume()))); err != nil {
		return nil, fmt.Errorf("OpenWAL block-to-object: %w", err)
	}
	return vb, nil
}

// VerificationResult is the per-cycle outcome the controller aggregates.
type VerificationResult struct {
	BlocksChecked                  int
	AcknowledgedWriteCount         int
	FlushBarrierCount              int
	LostFlushedWriteCount          int // GATED: must be zero
	LostAcknowledgedUnflushedCount int // informational
	CorruptBlockCount              int
	PhantomFutureGenerationCount   int
}

// VerifyVolumeAgainstAcknowledgedWriteLog reopens the volume through the
// production recovery path and checks every block the log mentions.
func VerifyVolumeAgainstAcknowledgedWriteLog(cfg VolumeHarnessConfig) (VerificationResult, error) {
	var result VerificationResult

	perBlock, flushBarrierCount, acknowledgedWriteCount, err := ParseAcknowledgedWriteLog(cfg.AcknowledgedWriteLogPath())
	if err != nil {
		return result, fmt.Errorf("parse acknowledged-write log: %w", err)
	}
	result.FlushBarrierCount = flushBarrierCount
	result.AcknowledgedWriteCount = acknowledgedWriteCount

	vb, err := OpenVolumeLikeProductionNBDPluginOpen(cfg)
	if err != nil {
		return result, fmt.Errorf("open volume for verification: %w", err)
	}
	defer vb.Close()

	for blockNumber, expected := range perBlock {
		result.BlocksChecked++
		var generation uint64
		blockData, readErr := vb.ReadAt(blockNumber*HarnessBlockSizeBytes, HarnessBlockSizeBytes)
		switch {
		case errors.Is(readErr, viperblock.ErrZeroBlock):
			// Never-persisted block: semantically all-zero content,
			// generation 0. Classified below (lost-flushed if a flush
			// barrier covered it, otherwise an unflushed loss).
			generation = 0
		case readErr != nil:
			result.CorruptBlockCount++
			fmt.Fprintf(os.Stderr, "CORRUPT block %d: read error: %v\n", blockNumber, readErr)
			continue
		default:
			var isAllZero bool
			var decodeErr error
			generation, isAllZero, decodeErr = DecodeVerifiableBlockPattern(blockNumber, blockData)
			if decodeErr != nil {
				result.CorruptBlockCount++
				fmt.Fprintf(os.Stderr, "CORRUPT block %d: %v\n", blockNumber, decodeErr)
				continue
			}
			if isAllZero {
				generation = 0
			}
		}
		switch {
		case generation > expected.HighestAcknowledgedGeneration:
			result.PhantomFutureGenerationCount++
			fmt.Fprintf(os.Stderr, "PHANTOM block %d: content generation %d exceeds highest acknowledged %d\n",
				blockNumber, generation, expected.HighestAcknowledgedGeneration)
		case generation < expected.DurableFloorGeneration:
			result.LostFlushedWriteCount++
			fmt.Fprintf(os.Stderr, "LOST-FLUSHED block %d: content generation %d < flushed generation %d\n",
				blockNumber, generation, expected.DurableFloorGeneration)
		case generation < expected.HighestAcknowledgedGeneration:
			result.LostAcknowledgedUnflushedCount++
		}
	}
	return result, nil
}
