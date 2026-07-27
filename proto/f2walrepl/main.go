// Command f2walrepl is the Phase -1 prototype for spinifex project fork F2
// (durability architecture for acknowledged writes). It measures the
// acked-write latency cost of three candidate durability mechanisms, so the
// fork can be decided on numbers:
//
//	local   -- append 4 KiB WAL records to a local file with fsync group
//	           commit (batch sizes 1/8/64): the single-node floor.
//	peer    -- stream records to a peer process (TCP) that appends + fsyncs
//	           with group commit and acks; measures replicated-ack latency
//	           at pipeline windows 1 and 64. NOTE: localhost TCP
//	           understates real LAN RTT (~0.05-0.5 ms) -- add that mentally;
//	           it does not change the ordering vs option (c).
//	s3put   -- one 4 KiB object PUT per record to a running predastore
//	           cluster (concurrency 1 and 16): option (c), per-write
//	           quorum-object writes.
//
// Every mode reports ops/s and p50/p99/max acked latency in microseconds.
package main

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const recordSizeBytes = 4096

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: f2walrepl <local|peer-server|peer-client|s3put> [flags]")
		os.Exit(4)
	}
	switch os.Args[1] {
	case "local":
		runLocalFsyncGroupCommitMode(os.Args[2:])
	case "peer-server":
		runPeerServerMode(os.Args[2:])
	case "peer-client":
		runPeerClientMode(os.Args[2:])
	case "s3put":
		runS3PutMode(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "unknown mode %q\n", os.Args[1])
		os.Exit(4)
	}
}

func reportLatencies(modeLabel string, durationsMicroseconds []int64, elapsed time.Duration) {
	sorted := make([]int64, len(durationsMicroseconds))
	copy(sorted, durationsMicroseconds)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	operationCount := len(sorted)
	if operationCount == 0 {
		fmt.Printf("%s: no operations\n", modeLabel)
		return
	}
	fmt.Printf("%-32s ops=%-8d ops/s=%-9.0f p50=%dus p99=%dus max=%dus\n",
		modeLabel, operationCount, float64(operationCount)/elapsed.Seconds(),
		sorted[operationCount/2], sorted[(operationCount*99)/100], sorted[operationCount-1])
}

// ------------------------------------------------------------- local mode --

func runLocalFsyncGroupCommitMode(args []string) {
	fs := flag.NewFlagSet("local", flag.ExitOnError)
	walFilePath := fs.String("file", "", "WAL file path (required)")
	operationCount := fs.Int("ops", 2000, "records per batch-size variant")
	_ = fs.Parse(args)
	if *walFilePath == "" {
		fmt.Fprintln(os.Stderr, "--file required")
		os.Exit(4)
	}
	payload := bytes.Repeat([]byte{0xAB}, recordSizeBytes)

	for _, batchSize := range []int{1, 8, 64} {
		walFile, err := os.OpenFile(*walFilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "open: %v\n", err)
			os.Exit(4)
		}
		var ackDurations []int64
		startedAt := time.Now()
		for completed := 0; completed < *operationCount; completed += batchSize {
			batchStartedAt := time.Now()
			for i := 0; i < batchSize; i++ {
				if _, err := walFile.Write(payload); err != nil {
					fmt.Fprintf(os.Stderr, "write: %v\n", err)
					os.Exit(4)
				}
			}
			if err := walFile.Sync(); err != nil {
				fmt.Fprintf(os.Stderr, "fsync: %v\n", err)
				os.Exit(4)
			}
			// Every record in the batch is acked when the group fsync
			// lands; each is charged the full batch latency (worst case).
			batchMicroseconds := time.Since(batchStartedAt).Microseconds()
			for i := 0; i < batchSize; i++ {
				ackDurations = append(ackDurations, batchMicroseconds)
			}
		}
		walFile.Close()
		reportLatencies(fmt.Sprintf("local-fsync batch=%d", batchSize), ackDurations, time.Since(startedAt))
	}
}

// -------------------------------------------------------------- peer mode --

// Wire format client->server: 8-byte big-endian sequence + 4 KiB payload.
// Server->client: 8-byte big-endian highest-durable sequence (acks are
// cumulative; an ack covers every lower sequence).

func runPeerServerMode(args []string) {
	fs := flag.NewFlagSet("peer-server", flag.ExitOnError)
	listenAddress := fs.String("listen", "127.0.0.1:39999", "listen address")
	walFilePath := fs.String("file", "", "replica WAL file path (required)")
	_ = fs.Parse(args)
	if *walFilePath == "" {
		fmt.Fprintln(os.Stderr, "--file required")
		os.Exit(4)
	}
	listener, err := net.Listen("tcp", *listenAddress)
	if err != nil {
		fmt.Fprintf(os.Stderr, "listen: %v\n", err)
		os.Exit(4)
	}
	fmt.Println("PEER-SERVER READY")
	for {
		connection, err := listener.Accept()
		if err != nil {
			fmt.Fprintf(os.Stderr, "accept: %v\n", err)
			os.Exit(4)
		}
		serveOneReplicationConnection(connection, *walFilePath)
	}
}

func serveOneReplicationConnection(connection net.Conn, walFilePath string) {
	defer connection.Close()
	walFile, err := os.OpenFile(walFilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open wal: %v\n", err)
		os.Exit(4)
	}
	defer walFile.Close()

	// Group commit: the reader drains whatever is pending into the file;
	// when the incoming stream momentarily idles (or a batch cap is hit),
	// fsync once and ack the highest sequence covered.
	reader := bufio.NewReaderSize(connection, 1<<20)
	header := make([]byte, 8)
	payload := make([]byte, recordSizeBytes)
	var highestWrittenSequence uint64
	ackBuffer := make([]byte, 8)
	const groupCommitBatchCap = 256
	for {
		pendingInBatch := 0
		for {
			if _, err := io.ReadFull(reader, header); err != nil {
				if err == io.EOF || err == io.ErrUnexpectedEOF {
					return // client done
				}
				fmt.Fprintf(os.Stderr, "read header: %v\n", err)
				os.Exit(4)
			}
			sequence := binary.BigEndian.Uint64(header)
			if _, err := io.ReadFull(reader, payload); err != nil {
				fmt.Fprintf(os.Stderr, "read payload: %v\n", err)
				os.Exit(4)
			}
			if _, err := walFile.Write(payload); err != nil {
				fmt.Fprintf(os.Stderr, "wal write: %v\n", err)
				os.Exit(4)
			}
			highestWrittenSequence = sequence
			pendingInBatch++
			// Stop draining when the buffer is empty (group boundary) or
			// the batch cap is hit (bounds worst-case ack latency).
			if reader.Buffered() < len(header)+recordSizeBytes || pendingInBatch >= groupCommitBatchCap {
				break
			}
		}
		if err := walFile.Sync(); err != nil {
			fmt.Fprintf(os.Stderr, "fsync: %v\n", err)
			os.Exit(4)
		}
		binary.BigEndian.PutUint64(ackBuffer, highestWrittenSequence)
		if _, err := connection.Write(ackBuffer); err != nil {
			fmt.Fprintf(os.Stderr, "ack write: %v\n", err)
			os.Exit(4)
		}
	}
}

func runPeerClientMode(args []string) {
	fs := flag.NewFlagSet("peer-client", flag.ExitOnError)
	serverAddress := fs.String("server", "127.0.0.1:39999", "peer server address")
	operationCount := fs.Int("ops", 2000, "records per window variant")
	_ = fs.Parse(args)

	for _, pipelineWindow := range []int{1, 64} {
		connection, err := net.Dial("tcp", *serverAddress)
		if err != nil {
			fmt.Fprintf(os.Stderr, "dial: %v\n", err)
			os.Exit(4)
		}
		sendTimesBySequence := make(map[uint64]time.Time, *operationCount)
		var sendTimesMutex sync.Mutex
		ackDurations := make([]int64, 0, *operationCount)
		ackDone := make(chan struct{})

		go func() {
			defer close(ackDone)
			ackBuffer := make([]byte, 8)
			acknowledgedCount := 0
			for acknowledgedCount < *operationCount {
				if _, err := io.ReadFull(connection, ackBuffer); err != nil {
					fmt.Fprintf(os.Stderr, "ack read: %v\n", err)
					os.Exit(4)
				}
				ackedSequence := binary.BigEndian.Uint64(ackBuffer)
				receivedAt := time.Now()
				sendTimesMutex.Lock()
				for sequence, sentAt := range sendTimesBySequence {
					if sequence <= ackedSequence {
						ackDurations = append(ackDurations, receivedAt.Sub(sentAt).Microseconds())
						delete(sendTimesBySequence, sequence)
						acknowledgedCount++
					}
				}
				sendTimesMutex.Unlock()
			}
		}()

		payload := bytes.Repeat([]byte{0xCD}, recordSizeBytes)
		frame := make([]byte, 8+recordSizeBytes)
		copy(frame[8:], payload)
		startedAt := time.Now()
		for sequence := uint64(1); sequence <= uint64(*operationCount); sequence++ {
			// Bound the in-flight window.
			for {
				sendTimesMutex.Lock()
				inFlight := len(sendTimesBySequence)
				if inFlight < pipelineWindow {
					sendTimesBySequence[sequence] = time.Now()
					sendTimesMutex.Unlock()
					break
				}
				sendTimesMutex.Unlock()
				time.Sleep(20 * time.Microsecond)
			}
			binary.BigEndian.PutUint64(frame[:8], sequence)
			if _, err := connection.Write(frame); err != nil {
				fmt.Fprintf(os.Stderr, "send: %v\n", err)
				os.Exit(4)
			}
		}
		<-ackDone
		connection.Close()
		reportLatencies(fmt.Sprintf("peer-replicated window=%d", pipelineWindow), ackDurations, time.Since(startedAt))
	}
}

// ------------------------------------------------------------- s3put mode --

func runS3PutMode(args []string) {
	fs := flag.NewFlagSet("s3put", flag.ExitOnError)
	endpoint := fs.String("endpoint", "https://127.0.0.1:8443", "predastore S3 endpoint")
	bucketName := fs.String("bucket", "f2walrepl-proto", "bucket")
	operationCount := fs.Int("ops", 300, "PUTs per concurrency variant")
	_ = fs.Parse(args)

	s3Client := s3.New(s3.Options{
		Region:       "ap-southeast-2",
		BaseEndpoint: aws.String(*endpoint),
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", ""),
		HTTPClient: &http.Client{
			Timeout:   30 * time.Second,
			Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}},
		},
	})
	ctx := neverCancelledContext{}
	// The cluster may still be electing a leader right after start; retry
	// bucket creation, and fail loudly if it never succeeds.
	var createErr error
	for attempt := 1; attempt <= 5; attempt++ {
		_, createErr = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(*bucketName)})
		if createErr == nil || strings.Contains(createErr.Error(), "BucketAlready") {
			createErr = nil
			break
		}
		time.Sleep(3 * time.Second)
	}
	if createErr != nil {
		fmt.Fprintf(os.Stderr, "create bucket: %v\n", createErr)
		os.Exit(4)
	}
	payload := bytes.Repeat([]byte{0xEF}, recordSizeBytes)

	for _, concurrency := range []int{1, 16} {
		ackDurations := make([]int64, 0, *operationCount)
		var durationsMutex sync.Mutex
		var waitGroup sync.WaitGroup
		operationsPerWorker := *operationCount / concurrency
		startedAt := time.Now()
		for workerIndex := 0; workerIndex < concurrency; workerIndex++ {
			waitGroup.Add(1)
			go func(workerIndex int) {
				defer waitGroup.Done()
				for i := 0; i < operationsPerWorker; i++ {
					key := fmt.Sprintf("f2/c%d/w%d/rec-%06d", concurrency, workerIndex, i)
					putStartedAt := time.Now()
					_, err := s3Client.PutObject(ctx, &s3.PutObjectInput{
						Bucket: aws.String(*bucketName),
						Key:    aws.String(key),
						Body:   bytes.NewReader(payload),
					})
					if err != nil {
						fmt.Fprintf(os.Stderr, "PUT %s: %v\n", key, err)
						os.Exit(4)
					}
					elapsedMicroseconds := time.Since(putStartedAt).Microseconds()
					durationsMutex.Lock()
					ackDurations = append(ackDurations, elapsedMicroseconds)
					durationsMutex.Unlock()
				}
			}(workerIndex)
		}
		waitGroup.Wait()
		reportLatencies(fmt.Sprintf("s3put-4k concurrency=%d", concurrency), ackDurations, time.Since(startedAt))
	}
}

// neverCancelledContext avoids importing context for a prototype that has no
// cancellation story.
type neverCancelledContext struct{}

func (neverCancelledContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (neverCancelledContext) Done() <-chan struct{}       { return nil }
func (neverCancelledContext) Err() error                  { return nil }
func (neverCancelledContext) Value(any) any               { return nil }
