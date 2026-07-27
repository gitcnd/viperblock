// Command perfbench measures viperblock ENGINE-LEVEL performance (gate
// P-1.5 part (i) of the spinifex Outposts-parity project; feeds data-path
// fork F7 by providing the number the NBD path must be compared against).
//
// It drives the engine through the same public API the NBD plugin uses
// (WriteAt / ReadAt / Flush via the production open sequence), so the delta
// between these numbers and fio-through-NBD numbers is exactly the
// transport tax.
//
// Phases (each --duration long):
//   - randwrite-4k at 1 and 16 workers   (acked-write latency: memory path)
//   - flush-latency                      (vb.Flush barrier cost while dirty)
//   - randread-4k at 1 and 16 workers    (after prefill + drain: mixed
//     cache/chunk reads -- reported, with the cache share unknown; honest
//     cold-read numbers need a cache-sized note, see output header)
//   - seqwrite-128k at 1 worker
//
// Output: human lines plus a CSV (--csv) with one row per phase:
// phase,workers,ops,iops,mbps,p50_us,p99_us,max_us
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mulgadc/viperblock/tests/crashharness"
)

const fourKiB = 4096
const oneTwentyEightKiB = 128 * 1024

type phaseResultRow struct {
	phaseName                                         string
	workerCount                                       int
	operationCount                                    int64
	opsPerSecond                                      float64
	megabytesPerSecond                                float64
	p50Microseconds, p99Microseconds, maxMicroseconds int64
}

func main() {
	harnessRootDirectoryPath := flag.String("dir", "", "bench working directory (required, will hold backend+WAL)")
	volumeSizeBytes := flag.Uint64("volume-size", 1024*1024*1024, "volume size in bytes")
	phaseDuration := flag.Duration("duration", 10*time.Second, "duration of each timed phase")
	csvOutputPath := flag.String("csv", "", "optional CSV output path")
	flag.Parse()
	if *harnessRootDirectoryPath == "" {
		fmt.Fprintln(os.Stderr, "--dir is required")
		os.Exit(4)
	}
	if err := os.MkdirAll(*harnessRootDirectoryPath, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir: %v\n", err)
		os.Exit(4)
	}

	cfg := crashharness.VolumeHarnessConfig{
		HarnessRootDirectoryPath: *harnessRootDirectoryPath,
		VolumeName:               "perfbench-vol0",
		VolumeSizeBytes:          *volumeSizeBytes,
	}
	vb, err := crashharness.OpenVolumeLikeProductionNBDPluginOpen(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open volume: %v\n", err)
		os.Exit(4)
	}

	blockCount := *volumeSizeBytes / fourKiB
	fmt.Printf("# perfbench: engine-level, volume %d MiB, %s per phase, cache 64 MiB (see harness config)\n",
		*volumeSizeBytes/(1024*1024), phaseDuration)

	var allRows []phaseResultRow

	// --- randwrite-4k (the acked-write path: memory buffer append) ---
	for _, workerCount := range []int{1, 16} {
		row := runTimedPhase("randwrite-4k", workerCount, *phaseDuration, fourKiB, func(rng *rand.Rand, payload []byte) error {
			blockNumber := uint64(rng.Int63n(int64(blockCount)))
			return vb.WriteAt(blockNumber*fourKiB, payload)
		})
		allRows = append(allRows, row)
		// Drain between phases so the write buffer does not grow unboundedly
		// and later phases start from a settled engine.
		if err := vb.DrainToBackend(); err != nil {
			fmt.Fprintf(os.Stderr, "drain after %s: %v\n", row.phaseName, err)
			os.Exit(4)
		}
	}

	// --- flush-latency: cost of the guest-fsync barrier under load ---
	{
		payload := make([]byte, fourKiB)
		rng := rand.New(rand.NewSource(7))
		var flushDurations []int64
		deadline := time.Now().Add(*phaseDuration)
		var flushCount int64
		for time.Now().Before(deadline) {
			for i := 0; i < 1000; i++ { // ~4 MiB dirty between barriers
				blockNumber := uint64(rng.Int63n(int64(blockCount)))
				if err := vb.WriteAt(blockNumber*fourKiB, payload); err != nil {
					fmt.Fprintf(os.Stderr, "flush phase write: %v\n", err)
					os.Exit(4)
				}
			}
			startedAt := time.Now()
			if err := vb.Flush(); err != nil {
				fmt.Fprintf(os.Stderr, "flush: %v\n", err)
				os.Exit(4)
			}
			flushDurations = append(flushDurations, time.Since(startedAt).Microseconds())
			flushCount++
		}
		p50, p99, maxV := percentiles(flushDurations)
		row := phaseResultRow{
			phaseName: "flush-after-4MiB-dirty", workerCount: 1, operationCount: flushCount,
			opsPerSecond:    float64(flushCount) / phaseDuration.Seconds(),
			p50Microseconds: p50, p99Microseconds: p99, maxMicroseconds: maxV,
		}
		printRow(row)
		allRows = append(allRows, row)
		if err := vb.DrainToBackend(); err != nil {
			fmt.Fprintf(os.Stderr, "drain after flush phase: %v\n", err)
			os.Exit(4)
		}
	}

	// --- prefill whole volume + drain, so reads have real content ---
	{
		payload := make([]byte, oneTwentyEightKiB)
		for offset := uint64(0); offset+oneTwentyEightKiB <= *volumeSizeBytes; offset += oneTwentyEightKiB {
			if err := vb.WriteAt(offset, payload); err != nil {
				fmt.Fprintf(os.Stderr, "prefill: %v\n", err)
				os.Exit(4)
			}
			// Keep the dirty set bounded during prefill.
			if offset%(256*1024*1024) == 0 && offset > 0 {
				if err := vb.DrainToBackend(); err != nil {
					fmt.Fprintf(os.Stderr, "prefill drain: %v\n", err)
					os.Exit(4)
				}
			}
		}
		if err := vb.DrainToBackend(); err != nil {
			fmt.Fprintf(os.Stderr, "prefill final drain: %v\n", err)
			os.Exit(4)
		}
		fmt.Println("# prefill complete (volume fully written and drained)")
	}

	// --- randread-4k ---
	for _, workerCount := range []int{1, 16} {
		row := runTimedPhase("randread-4k", workerCount, *phaseDuration, fourKiB, func(rng *rand.Rand, _ []byte) error {
			blockNumber := uint64(rng.Int63n(int64(blockCount)))
			_, err := vb.ReadAt(blockNumber*fourKiB, fourKiB)
			return err
		})
		allRows = append(allRows, row)
	}

	// --- seqwrite-128k ---
	{
		var nextOffset atomic.Uint64
		row := runTimedPhase("seqwrite-128k", 1, *phaseDuration, oneTwentyEightKiB, func(_ *rand.Rand, payload []byte) error {
			offset := nextOffset.Add(oneTwentyEightKiB) - oneTwentyEightKiB
			offset = offset % (*volumeSizeBytes - oneTwentyEightKiB)
			return vb.WriteAt(offset, payload)
		})
		allRows = append(allRows, row)
		if err := vb.DrainToBackend(); err != nil {
			fmt.Fprintf(os.Stderr, "drain after seqwrite: %v\n", err)
			os.Exit(4)
		}
	}

	if err := vb.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "close: %v\n", err)
	}

	if *csvOutputPath != "" {
		f, err := os.Create(*csvOutputPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "csv: %v\n", err)
			os.Exit(4)
		}
		fmt.Fprintln(f, "phase,workers,ops,iops,mbps,p50_us,p99_us,max_us")
		for _, row := range allRows {
			fmt.Fprintf(f, "%s,%d,%d,%.0f,%.1f,%d,%d,%d\n", row.phaseName, row.workerCount,
				row.operationCount, row.opsPerSecond, row.megabytesPerSecond,
				row.p50Microseconds, row.p99Microseconds, row.maxMicroseconds)
		}
		f.Close()
		fmt.Printf("# csv written: %s\n", *csvOutputPath)
	}
	fmt.Println("=== PERFBENCH COMPLETE ===")
}

// runTimedPhase drives operationFunc from workerCount goroutines for the
// given duration, sampling every operation's latency.
func runTimedPhase(phaseName string, workerCount int, phaseDuration time.Duration, operationSizeBytes int,
	operationFunc func(rng *rand.Rand, payload []byte) error) phaseResultRow {

	var stopFlag atomic.Bool
	perWorkerDurations := make([][]int64, workerCount)
	var waitGroup sync.WaitGroup
	for workerIndex := 0; workerIndex < workerCount; workerIndex++ {
		waitGroup.Add(1)
		go func(workerIndex int) {
			defer waitGroup.Done()
			rng := rand.New(rand.NewSource(int64(workerIndex) + 42))
			payload := make([]byte, operationSizeBytes)
			durations := make([]int64, 0, 1<<20)
			for !stopFlag.Load() {
				startedAt := time.Now()
				if err := operationFunc(rng, payload); err != nil {
					fmt.Fprintf(os.Stderr, "%s: op error: %v\n", phaseName, err)
					os.Exit(4)
				}
				durations = append(durations, time.Since(startedAt).Microseconds())
			}
			perWorkerDurations[workerIndex] = durations
		}(workerIndex)
	}
	time.Sleep(phaseDuration)
	stopFlag.Store(true)
	waitGroup.Wait()

	var allDurations []int64
	for _, durations := range perWorkerDurations {
		allDurations = append(allDurations, durations...)
	}
	p50, p99, maxV := percentiles(allDurations)
	operationCount := int64(len(allDurations))
	row := phaseResultRow{
		phaseName: phaseName, workerCount: workerCount, operationCount: operationCount,
		opsPerSecond:       float64(operationCount) / phaseDuration.Seconds(),
		megabytesPerSecond: float64(operationCount) * float64(operationSizeBytes) / phaseDuration.Seconds() / (1024 * 1024),
		p50Microseconds:    p50, p99Microseconds: p99, maxMicroseconds: maxV,
	}
	printRow(row)
	return row
}

func printRow(row phaseResultRow) {
	fmt.Printf("%-24s workers=%-2d ops=%-9d iops=%-9.0f MB/s=%-8.1f p50=%dus p99=%dus max=%dus\n",
		row.phaseName, row.workerCount, row.operationCount, row.opsPerSecond,
		row.megabytesPerSecond, row.p50Microseconds, row.p99Microseconds, row.maxMicroseconds)
}

func percentiles(durationsMicroseconds []int64) (p50, p99, maxValue int64) {
	if len(durationsMicroseconds) == 0 {
		return 0, 0, 0
	}
	sorted := make([]int64, len(durationsMicroseconds))
	copy(sorted, durationsMicroseconds)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	p50 = sorted[len(sorted)/2]
	p99 = sorted[(len(sorted)*99)/100]
	maxValue = sorted[len(sorted)-1]
	return
}
