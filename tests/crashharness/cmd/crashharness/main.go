// Command crashharness measures viperblock crash consistency (gate P-1.3 of
// the spinifex Outposts-parity project). See package crashharness for scope.
//
// Subcommands:
//
//	writer  -- open/recover the volume, write verifiable blocks forever,
//	           log every acknowledged write, emit FLUSH barriers. Meant to
//	           be SIGKILLed.
//	verify  -- reopen/recover the volume and check its content against the
//	           acknowledged-write log.
//	run     -- controller: N cycles of (spawn writer, random sleep, SIGKILL,
//	           verify), then an aggregate PASS/FAIL report.
//
// Typical use:
//
//	crashharness run --dir /tmp/crashharness --cycles 20
//
// Runtime: roughly cycles * (max-run/2 + 2s verify overhead); 20 cycles of
// 2-6 s runs is ~2-4 minutes on an idle NVMe-backed host.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mulgadc/viperblock/tests/crashharness"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: crashharness <writer|verify|run> [flags]")
		os.Exit(4)
	}
	switch os.Args[1] {
	case "writer":
		runWriterSubcommandUntilKilled(os.Args[2:])
	case "verify":
		os.Exit(runVerifySubcommand(os.Args[2:]))
	case "run":
		os.Exit(runControllerSubcommand(os.Args[2:]))
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n", os.Args[1])
		os.Exit(4)
	}
}

type sharedVolumeFlags struct {
	harnessRootDirectoryPath string
	volumeName               string
	volumeSizeBytes          uint64
}

func registerSharedVolumeFlags(fs *flag.FlagSet) *sharedVolumeFlags {
	f := &sharedVolumeFlags{}
	fs.StringVar(&f.harnessRootDirectoryPath, "dir", "", "harness root directory (required)")
	fs.StringVar(&f.volumeName, "volume-name", "crashharness-vol0", "volume name")
	fs.Uint64Var(&f.volumeSizeBytes, "volume-size", 64*1024*1024, "volume size in bytes")
	return f
}

func (f *sharedVolumeFlags) toConfigOrDie() crashharness.VolumeHarnessConfig {
	if f.harnessRootDirectoryPath == "" {
		fmt.Fprintln(os.Stderr, "--dir is required")
		os.Exit(4)
	}
	if err := os.MkdirAll(f.harnessRootDirectoryPath, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir %s: %v\n", f.harnessRootDirectoryPath, err)
		os.Exit(4)
	}
	return crashharness.VolumeHarnessConfig{
		HarnessRootDirectoryPath: f.harnessRootDirectoryPath,
		VolumeName:               f.volumeName,
		VolumeSizeBytes:          f.volumeSizeBytes,
	}
}

// ---------------------------------------------------------------- writer --

func runWriterSubcommandUntilKilled(args []string) {
	fs := flag.NewFlagSet("writer", flag.ExitOnError)
	shared := registerSharedVolumeFlags(fs)
	workerCount := fs.Int("workers", 4, "concurrent writer goroutines")
	flushInterval := fs.Duration("flush-interval", 500*time.Millisecond, "interval between vb.Flush barriers (guest-fsync cadence)")
	perWriteDelay := fs.Duration("write-delay", time.Millisecond, "sleep after each write per worker (throttle)")
	pauseWritesAfterOpen := fs.Bool("pause-writes", false, "open (and recover) the volume but never write: isolates recovery side effects")
	_ = fs.Parse(args)
	cfg := shared.toConfigOrDie()

	vb, err := crashharness.OpenVolumeLikeProductionNBDPluginOpen(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "writer: open volume: %v\n", err)
		os.Exit(4)
	}

	ackLog, err := crashharness.OpenAcknowledgedWriteLogForAppend(cfg.AcknowledgedWriteLogPath())
	if err != nil {
		fmt.Fprintf(os.Stderr, "writer: open acknowledged-write log: %v\n", err)
		os.Exit(4)
	}
	if err := ackLog.RecordSessionStart(); err != nil {
		fmt.Fprintf(os.Stderr, "writer: record session start: %v\n", err)
		os.Exit(4)
	}

	// Resume per-block generation counters above anything previously
	// acknowledged, so generations stay monotonic across crash cycles.
	blockCount := cfg.VolumeSizeBytes / crashharness.HarnessBlockSizeBytes
	perBlockGenerationCounters := make([]atomic.Uint64, blockCount)
	if previous, _, _, parseErr := crashharness.ParseAcknowledgedWriteLog(cfg.AcknowledgedWriteLogPath()); parseErr == nil {
		for blockNumber, state := range previous {
			if blockNumber < blockCount {
				perBlockGenerationCounters[blockNumber].Store(state.HighestAcknowledgedGeneration)
			}
		}
	}

	// flushBarrierMutex orders writes against FLUSH barriers so the log's
	// happens-before matches reality: workers hold RLock across
	// {WriteAt + log append}; the flusher holds the write lock across
	// {Flush + log FLUSH}. Any W line before a FLUSH line therefore had its
	// WriteAt complete before that Flush began.
	var flushBarrierMutex sync.RWMutex
	var totalAcknowledgedWrites atomic.Uint64

	if *pauseWritesAfterOpen {
		fmt.Println("WRITER READY")
		for {
			time.Sleep(2 * time.Second)
			fmt.Fprintln(os.Stderr, "writer heartbeat: paused (no writes)")
		}
	}

	for workerIndex := 0; workerIndex < *workerCount; workerIndex++ {
		go func(workerIndex int) {
			// Static block ownership (block % workers == workerIndex) so no
			// two goroutines ever race the same block: content generations
			// per block are strictly ordered.
			rng := rand.New(rand.NewSource(int64(workerIndex) + 1))
			for {
				blockNumber := uint64(rng.Int63n(int64(blockCount)))
				blockNumber = blockNumber - (blockNumber % uint64(*workerCount)) + uint64(workerIndex)
				if blockNumber >= blockCount {
					continue
				}
				generation := perBlockGenerationCounters[blockNumber].Add(1)
				blockData := crashharness.EncodeVerifiableBlockPattern(blockNumber, generation)

				flushBarrierMutex.RLock()
				writeErr := vb.WriteAt(blockNumber*crashharness.HarnessBlockSizeBytes, blockData)
				if writeErr == nil {
					if logErr := ackLog.RecordAcknowledgedWrite(blockNumber, generation); logErr != nil {
						fmt.Fprintf(os.Stderr, "writer: acknowledged-write log append failed: %v\n", logErr)
						os.Exit(4)
					}
					totalAcknowledgedWrites.Add(1)
				}
				flushBarrierMutex.RUnlock()
				if writeErr != nil {
					fmt.Fprintf(os.Stderr, "writer: WriteAt block %d: %v\n", blockNumber, writeErr)
					os.Exit(4)
				}
				if *perWriteDelay > 0 {
					time.Sleep(*perWriteDelay)
				}
			}
		}(workerIndex)
	}

	// Flush-barrier goroutine: the guest-fsync cadence.
	go func() {
		ticker := time.NewTicker(*flushInterval)
		defer ticker.Stop()
		for range ticker.C {
			flushBarrierMutex.Lock()
			flushErr := vb.Flush()
			if flushErr == nil {
				if logErr := ackLog.RecordFlushBarrier(); logErr != nil {
					fmt.Fprintf(os.Stderr, "writer: flush-barrier log append failed: %v\n", logErr)
					os.Exit(4)
				}
			}
			flushBarrierMutex.Unlock()
			if flushErr != nil {
				fmt.Fprintf(os.Stderr, "writer: Flush: %v\n", flushErr)
				os.Exit(4)
			}
		}
	}()

	// Signal the controller that writes are flowing, then heartbeat until
	// SIGKILLed (guardrail: a silent process is indistinguishable from a
	// hung one).
	fmt.Println("WRITER READY")
	for {
		time.Sleep(2 * time.Second)
		fmt.Fprintf(os.Stderr, "writer heartbeat: %d acknowledged writes\n", totalAcknowledgedWrites.Load())
	}
}

// ---------------------------------------------------------------- verify --

func runVerifySubcommand(args []string) int {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	shared := registerSharedVolumeFlags(fs)
	probeBlockList := fs.String("probe-blocks", "", "DIAGNOSTIC: comma-separated block numbers to read WITHOUT WAL replay; prints each block's state and exits")
	_ = fs.Parse(args)
	cfg := shared.toConfigOrDie()

	if *probeBlockList != "" {
		return runDiagnosticBlockProbe(cfg, *probeBlockList)
	}

	result, err := crashharness.VerifyVolumeAgainstAcknowledgedWriteLog(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "verify: %v\n", err)
		return 4
	}
	fmt.Printf("RESULT blocks=%d acked=%d flush_barriers=%d lost_flushed=%d lost_unflushed=%d corrupt=%d phantom=%d\n",
		result.BlocksChecked, result.AcknowledgedWriteCount, result.FlushBarrierCount,
		result.LostFlushedWriteCount, result.LostAcknowledgedUnflushedCount,
		result.CorruptBlockCount, result.PhantomFutureGenerationCount)
	if result.CorruptBlockCount > 0 || result.PhantomFutureGenerationCount > 0 {
		return 3
	}
	if result.LostFlushedWriteCount > 0 {
		return 2
	}
	return 0
}

// runDiagnosticBlockProbe opens the volume WITHOUT WAL replay and reports the
// content generation of each requested block, so a missing block can be
// attributed either to the checkpointed map (absent before replay) or to the
// replay itself (present before, gone after).
func runDiagnosticBlockProbe(cfg crashharness.VolumeHarnessConfig, commaSeparatedBlockNumbers string) int {
	vb, err := crashharness.OpenVolumeSkippingWALRecovery(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "probe: open (no replay): %v\n", err)
		return 4
	}
	defer vb.Close()
	for _, token := range strings.Split(commaSeparatedBlockNumbers, ",") {
		blockNumber, parseErr := strconv.ParseUint(strings.TrimSpace(token), 10, 64)
		if parseErr != nil {
			fmt.Fprintf(os.Stderr, "probe: bad block number %q: %v\n", token, parseErr)
			return 4
		}
		objectID, objectOffset, _, lookupErr := vb.LookupBlockToObject(blockNumber)
		if lookupErr != nil {
			fmt.Printf("PROBE block %d: block-map lookup error: %v\n", blockNumber, lookupErr)
		} else {
			fmt.Printf("PROBE block %d: block-map -> object %d offset %d\n", blockNumber, objectID, objectOffset)
		}
		blockData, readErr := vb.ReadAt(blockNumber*crashharness.HarnessBlockSizeBytes, crashharness.HarnessBlockSizeBytes)
		if readErr != nil {
			fmt.Printf("PROBE block %d: read error: %v\n", blockNumber, readErr)
			continue
		}
		generation, isAllZero, decodeErr := crashharness.DecodeVerifiableBlockPattern(blockNumber, blockData)
		switch {
		case decodeErr != nil:
			fmt.Printf("PROBE block %d: undecodable: %v\n", blockNumber, decodeErr)
		case isAllZero:
			fmt.Printf("PROBE block %d: zero content\n", blockNumber)
		default:
			fmt.Printf("PROBE block %d: generation %d\n", blockNumber, generation)
		}
	}
	return 0
}

// ------------------------------------------------------------ controller --

func runControllerSubcommand(args []string) int {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	shared := registerSharedVolumeFlags(fs)
	cycleCount := fs.Int("cycles", 20, "number of crash cycles")
	minimumWriterRunDuration := fs.Duration("min-run", 2*time.Second, "minimum writer lifetime before SIGKILL")
	maximumWriterRunDuration := fs.Duration("max-run", 6*time.Second, "maximum writer lifetime before SIGKILL")
	workerCount := fs.Int("workers", 4, "writer goroutines (passed through)")
	flushInterval := fs.Duration("flush-interval", 500*time.Millisecond, "flush cadence (passed through)")
	_ = fs.Parse(args)
	cfg := shared.toConfigOrDie()

	selfExecutablePath, err := os.Executable()
	if err != nil {
		fmt.Fprintf(os.Stderr, "run: cannot locate own executable: %v\n", err)
		return 4
	}
	rng := rand.New(rand.NewSource(20260727))

	var totalLostFlushed, totalLostUnflushed, totalCorrupt, totalPhantom, totalAcked int
	for cycleIndex := 1; cycleIndex <= *cycleCount; cycleIndex++ {
		writerLogPath := filepath.Join(cfg.HarnessRootDirectoryPath, fmt.Sprintf("writer_cycle_%03d.log", cycleIndex))
		writerLogFile, err := os.Create(writerLogPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "run: create writer log: %v\n", err)
			return 4
		}

		writerCommand := exec.Command(selfExecutablePath, "writer",
			"--dir", cfg.HarnessRootDirectoryPath,
			"--volume-name", cfg.VolumeName,
			"--volume-size", fmt.Sprint(cfg.VolumeSizeBytes),
			"--workers", fmt.Sprint(*workerCount),
			"--flush-interval", flushInterval.String())
		writerCommand.Stderr = writerLogFile
		writerStdout, err := writerCommand.StdoutPipe()
		if err != nil {
			fmt.Fprintf(os.Stderr, "run: writer stdout pipe: %v\n", err)
			return 4
		}
		if err := writerCommand.Start(); err != nil {
			fmt.Fprintf(os.Stderr, "run: start writer: %v\n", err)
			return 4
		}

		// Wait for the writer's READY line (volume opened, writes flowing),
		// with a watchdog so a hung open cannot stall the whole study.
		readyChannel := make(chan bool, 1)
		go func() {
			scanner := bufio.NewScanner(writerStdout)
			for scanner.Scan() {
				if scanner.Text() == "WRITER READY" {
					readyChannel <- true
					// Keep draining so the child never blocks on stdout.
					for scanner.Scan() {
					}
					return
				}
			}
			readyChannel <- false
		}()
		select {
		case ready := <-readyChannel:
			if !ready {
				_ = writerCommand.Process.Kill()
				_ = writerCommand.Wait()
				writerLogFile.Close()
				fmt.Fprintf(os.Stderr, "run: cycle %d: writer exited before READY; see %s\n", cycleIndex, writerLogPath)
				return 4
			}
		case <-time.After(60 * time.Second):
			_ = writerCommand.Process.Kill()
			_ = writerCommand.Wait()
			writerLogFile.Close()
			fmt.Fprintf(os.Stderr, "run: cycle %d: writer not READY within 60s (hung open?); see %s\n", cycleIndex, writerLogPath)
			return 4
		}

		runDuration := *minimumWriterRunDuration +
			time.Duration(rng.Int63n(int64(*maximumWriterRunDuration-*minimumWriterRunDuration)+1))
		time.Sleep(runDuration)

		if err := writerCommand.Process.Kill(); err != nil { // SIGKILL
			fmt.Fprintf(os.Stderr, "run: SIGKILL writer: %v\n", err)
			return 4
		}
		_ = writerCommand.Wait()
		writerLogFile.Close()

		// Verify a COPY of the harness state, not the live directory: the
		// verifier's own open/recover/Close mutates and prunes local state,
		// and an early run of this harness showed that mutation destroying
		// flushed data for the NEXT cycle (see project journal 2026-07-27).
		// Copy-verification keeps each cycle's evidence independent.
		verifyScratchDirectoryPath := cfg.HarnessRootDirectoryPath + "_verify_scratch"
		if err := os.RemoveAll(verifyScratchDirectoryPath); err != nil {
			fmt.Fprintf(os.Stderr, "run: clean verify scratch: %v\n", err)
			return 4
		}
		if copyOutput, copyErr := exec.Command("cp", "-a", cfg.HarnessRootDirectoryPath, verifyScratchDirectoryPath).CombinedOutput(); copyErr != nil {
			fmt.Fprintf(os.Stderr, "run: copy for verification: %v: %s\n", copyErr, copyOutput)
			return 4
		}

		verifyCommand := exec.Command(selfExecutablePath, "verify",
			"--dir", verifyScratchDirectoryPath,
			"--volume-name", cfg.VolumeName,
			"--volume-size", fmt.Sprint(cfg.VolumeSizeBytes))
		verifyCommand.Stderr = os.Stderr
		verifyOutput, verifyErr := verifyCommand.Output()
		var exitError *exec.ExitError
		if verifyErr != nil && !errorsAs(verifyErr, &exitError) {
			fmt.Fprintf(os.Stderr, "run: verify failed to run: %v\n", verifyErr)
			return 4
		}

		_ = os.RemoveAll(verifyScratchDirectoryPath)

		var blocks, acked, flushBarriers, lostFlushed, lostUnflushed, corrupt, phantom int
		if _, scanErr := fmt.Sscanf(string(verifyOutput),
			"RESULT blocks=%d acked=%d flush_barriers=%d lost_flushed=%d lost_unflushed=%d corrupt=%d phantom=%d",
			&blocks, &acked, &flushBarriers, &lostFlushed, &lostUnflushed, &corrupt, &phantom); scanErr != nil {
			fmt.Fprintf(os.Stderr, "run: cannot parse verify output %q: %v\n", string(verifyOutput), scanErr)
			return 4
		}
		fmt.Printf("cycle %3d/%d: ran %6.2fs, acked=%d flush_barriers=%d lost_flushed=%d lost_unflushed=%d corrupt=%d phantom=%d\n",
			cycleIndex, *cycleCount, runDuration.Seconds(), acked, flushBarriers, lostFlushed, lostUnflushed, corrupt, phantom)

		totalAcked = acked // cumulative in the log, so the last cycle's number is the total
		totalLostFlushed += lostFlushed
		totalLostUnflushed += lostUnflushed
		totalCorrupt += corrupt
		totalPhantom += phantom
	}

	fmt.Printf("CRASHHARNESS SUMMARY: cycles=%d acked_total=%d lost_flushed=%d lost_unflushed_total=%d corrupt=%d phantom=%d\n",
		*cycleCount, totalAcked, totalLostFlushed, totalLostUnflushed, totalCorrupt, totalPhantom)
	// GATED: acknowledged-and-flushed writes must never be lost.
	// REPORTED (not gated): lost_unflushed -- viperblock acks from memory by
	// design; this number quantifies that window.
	if totalCorrupt > 0 || totalPhantom > 0 {
		fmt.Println("FAIL: corruption or phantom content detected")
		return 3
	}
	if totalLostFlushed > 0 {
		fmt.Println("FAIL: acknowledged-and-flushed writes were lost")
		return 2
	}
	fmt.Println("PASS: zero acknowledged-and-flushed writes lost")
	fmt.Println("=== ALL CRASHHARNESS CHECKS PASS ===")
	return 0
}

// errorsAs is a tiny local wrapper to keep the errors import obvious.
func errorsAs(err error, target *(*exec.ExitError)) bool {
	if e, ok := err.(*exec.ExitError); ok {
		*target = e
		return true
	}
	return false
}
