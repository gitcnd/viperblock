// Package walrepl implements synchronous WAL replication to a peer node --
// the durability architecture decided by spinifex project fork F2 (human
// ack 2026-07-28) on prototype evidence: replicated acks cost ~3 us over
// the local fsync at window 1 (proto/f2walrepl results).
//
// Model (thin slice): the primary streams every WAL record, in WAL order,
// to one replica. The replica appends to a replica WAL file, group-commit
// fsyncs, and acks cumulatively by sequence. A BARRIER on the primary
// resolves only when every record sent so far is durable on the replica.
// The handshake carries the volume name and the primary's WAL file header,
// so replica files are valid WAL files a future promotion can recover from.
//
// Out of scope in initial slice (reconnect/resync landed 2026-07-29):
// sharded-WAL replication, multi-replica quorum.
package walrepl

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// Wire format, all little-endian:
//
//	handshake:  magic "VBRH" | volNameLen u16 | volName | headerLen u32 | header
//	record:     seq u64 | recordLen u32 | recordBytes
//	ack:        highestDurableSeq u64  (cumulative)
var handshakeMagic = []byte("VBRH")

const maxRecordBytes = 64 * 1024 * 1024
const maxVolumeNameBytes = 1024

// ---------------------------------------------------------------- server --

// ReplicaServer accepts one primary connection at a time and persists its
// record stream.
type ReplicaServer struct {
	// ReplicaWALDirectoryPath is where replica WAL files land, one per
	// incoming connection: <volume>.replica.<unixnano>.wal
	ReplicaWALDirectoryPath string

	// GroupCommitBatchCap bounds records per fsync (bounds worst-case ack
	// latency under a firehose). 0 means DefaultGroupCommitBatchCap.
	GroupCommitBatchCap int

	listener net.Listener
}

const DefaultGroupCommitBatchCap = 256

func (s *ReplicaServer) Listen(network, address string) (string, error) {
	if err := os.MkdirAll(s.ReplicaWALDirectoryPath, 0o750); err != nil {
		return "", err
	}
	listener, err := net.Listen(network, address)
	if err != nil {
		return "", err
	}
	s.listener = listener
	return listener.Addr().String(), nil
}

// Serve handles connections until the listener is closed.
func (s *ReplicaServer) Serve() error {
	for {
		connection, err := s.listener.Accept()
		if err != nil {
			return err // listener closed
		}
		if err := s.serveOnePrimary(connection); err != nil && err != io.EOF {
			fmt.Fprintf(os.Stderr, "walrepl replica: connection ended: %v\n", err)
		}
	}
}

func (s *ReplicaServer) Close() error {
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

func (s *ReplicaServer) serveOnePrimary(connection net.Conn) error {
	defer connection.Close()

	// Handshake.
	magic := make([]byte, 4)
	if _, err := io.ReadFull(connection, magic); err != nil {
		return err
	}
	if string(magic) != string(handshakeMagic) {
		return fmt.Errorf("bad handshake magic %q", magic)
	}
	lengthBuffer := make([]byte, 2)
	if _, err := io.ReadFull(connection, lengthBuffer); err != nil {
		return err
	}
	volumeNameLength := binary.LittleEndian.Uint16(lengthBuffer)
	if volumeNameLength == 0 || volumeNameLength > maxVolumeNameBytes {
		return fmt.Errorf("unreasonable volume name length %d", volumeNameLength)
	}
	volumeNameBytes := make([]byte, volumeNameLength)
	if _, err := io.ReadFull(connection, volumeNameBytes); err != nil {
		return err
	}
	headerLengthBuffer := make([]byte, 4)
	if _, err := io.ReadFull(connection, headerLengthBuffer); err != nil {
		return err
	}
	headerLength := binary.LittleEndian.Uint32(headerLengthBuffer)
	if headerLength > maxRecordBytes {
		return fmt.Errorf("unreasonable header length %d", headerLength)
	}
	headerBytes := make([]byte, headerLength)
	if _, err := io.ReadFull(connection, headerBytes); err != nil {
		return err
	}

	replicaFilePath := filepath.Join(s.ReplicaWALDirectoryPath,
		fmt.Sprintf("%s.replica.%d.wal", sanitizeFileNameComponent(string(volumeNameBytes)), time.Now().UnixNano()))
	replicaFile, err := os.OpenFile(replicaFilePath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o640)
	if err != nil {
		return err
	}
	defer replicaFile.Close()
	if _, err := replicaFile.Write(headerBytes); err != nil {
		return err
	}
	if err := replicaFile.Sync(); err != nil {
		return err
	}

	// Record loop with group commit: drain what is pending, fsync once,
	// ack the highest sequence covered.
	batchCap := s.GroupCommitBatchCap
	if batchCap <= 0 {
		batchCap = DefaultGroupCommitBatchCap
	}
	recordHeader := make([]byte, 12)
	ackBuffer := make([]byte, 8)
	reader := newBufferedReader(connection, 1<<20)
	var highestSequence uint64
	for {
		recordsInBatch := 0
		for {
			if _, err := reader.ReadFull(recordHeader); err != nil {
				return err
			}
			sequence := binary.LittleEndian.Uint64(recordHeader[0:8])
			recordLength := binary.LittleEndian.Uint32(recordHeader[8:12])
			if recordLength > maxRecordBytes {
				return fmt.Errorf("unreasonable record length %d", recordLength)
			}
			if err := reader.CopyN(replicaFile, int64(recordLength)); err != nil {
				return err
			}
			highestSequence = sequence
			recordsInBatch++
			if reader.Buffered() < len(recordHeader) || recordsInBatch >= batchCap {
				break
			}
		}
		if err := replicaFile.Sync(); err != nil {
			return err
		}
		binary.LittleEndian.PutUint64(ackBuffer, highestSequence)
		if _, err := connection.Write(ackBuffer); err != nil {
			return err
		}
	}
}

// ListReplicaWALFiles returns the replica WAL files this package wrote for
// volumeName under directoryPath, sorted oldest-first by the unixnano
// timestamp embedded in the file name. This is the promotion entry point:
// each returned path is a VALID WAL file (handshake header + records) that
// viperblock's recovery can replay -- install them into a fresh volume
// base directory with viperblock.InstallRecoveryWALFiles and run the
// normal production open sequence (fork F2 promotion slice, 2026-07-28).
func ListReplicaWALFiles(directoryPath, volumeName string) ([]string, error) {
	pattern := filepath.Join(directoryPath,
		sanitizeFileNameComponent(volumeName)+".replica.*.wal")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}
	// Timestamps are fixed-width (19-digit unixnano until year 2262), so
	// the lexical sort Glob already applies IS chronological; keep an
	// explicit sort for clarity and future-proofing.
	sort.Strings(matches)
	return matches, nil
}

func sanitizeFileNameComponent(name string) string {
	out := make([]rune, 0, len(name))
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == '.':
			out = append(out, r)
		default:
			out = append(out, '_')
		}
	}
	return string(out)
}

// ---------------------------------------------------------------- client --

// Client streams WAL records to a replica and provides a durability
// barrier. Append preserves call order; Barrier blocks until every record
// appended before it is fsynced on the replica.
type Client struct {
	connection net.Conn

	mu               sync.Mutex
	nextSequence     uint64
	highestAcked     uint64
	ackCondition     *sync.Cond
	connectionBroken error
}

// ReconnectingClient wraps Client with automatic reconnect and resync
// capabilities (F2 reconnect slice 2026-07-29). When the replica connection
// breaks, it attempts reconnect with exponential backoff. On successful
// reconnect, it resyncs all pending (unacked) records before resuming normal
// operation. Degraded mode: if ReconnectAttempts is exhausted, the client
// enters degraded mode where Append/Barrier return a permanent error.
type ReconnectingClient struct {
	network    string
	address    string
	volumeName string
	walHeader  []byte

	// ReconnectAttempts bounds reconnect attempts per disconnection
	// (0 means retry forever). Exhaustion enters degraded mode.
	ReconnectAttempts int
	// InitialReconnectBackoff is the first sleep between reconnect attempts,
	// doubling on each subsequent retry (max 30s per attempt).
	InitialReconnectBackoff time.Duration

	mu                  sync.Mutex
	client              *Client
	pendingRecords      []pendingRecord
	nextSequence        uint64
	degradedMode        bool
	degradedModeError   error
	ackCondition        *sync.Cond
	reconnectInProgress bool
}

type pendingRecord struct {
	sequence uint64
	data     []byte
}

// Connect dials the replica and performs the handshake.
func Connect(network, address, volumeName string, walHeader []byte) (*Client, error) {
	connection, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}
	handshake := make([]byte, 0, 4+2+len(volumeName)+4+len(walHeader))
	handshake = append(handshake, handshakeMagic...)
	handshake = binary.LittleEndian.AppendUint16(handshake, uint16(len(volumeName)))
	handshake = append(handshake, volumeName...)
	handshake = binary.LittleEndian.AppendUint32(handshake, uint32(len(walHeader)))
	handshake = append(handshake, walHeader...)
	if _, err := connection.Write(handshake); err != nil {
		connection.Close()
		return nil, err
	}
	client := &Client{connection: connection}
	client.ackCondition = sync.NewCond(&client.mu)
	go client.receiveAcks()
	return client, nil
}

func (c *Client) receiveAcks() {
	ackBuffer := make([]byte, 8)
	for {
		if _, err := io.ReadFull(c.connection, ackBuffer); err != nil {
			c.mu.Lock()
			c.connectionBroken = fmt.Errorf("replica ack stream: %w", err)
			c.ackCondition.Broadcast()
			c.mu.Unlock()
			return
		}
		acked := binary.LittleEndian.Uint64(ackBuffer)
		c.mu.Lock()
		if acked > c.highestAcked {
			c.highestAcked = acked
			c.ackCondition.Broadcast()
		}
		c.mu.Unlock()
	}
}

// Append streams one WAL record. Callers must present records in WAL
// order (viperblock calls this under the WAL write lock).
func (c *Client) Append(record []byte) error {
	c.mu.Lock()
	if c.connectionBroken != nil {
		defer c.mu.Unlock()
		return c.connectionBroken
	}
	c.nextSequence++
	sequence := c.nextSequence
	c.mu.Unlock()

	frame := make([]byte, 12, 12+len(record))
	binary.LittleEndian.PutUint64(frame[0:8], sequence)
	binary.LittleEndian.PutUint32(frame[8:12], uint32(len(record)))
	frame = append(frame, record...)
	if _, err := c.connection.Write(frame); err != nil {
		c.mu.Lock()
		c.connectionBroken = fmt.Errorf("replica send: %w", err)
		c.mu.Unlock()
		return c.connectionBroken
	}
	return nil
}

// Barrier blocks until everything appended so far is durable on the
// replica, or the connection is broken. This is the replication half of
// viperblock's flush barrier (P1.4): a barrier that cannot replicate
// must fail.
func (c *Client) Barrier() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	target := c.nextSequence
	for c.highestAcked < target && c.connectionBroken == nil {
		c.ackCondition.Wait()
	}
	if c.connectionBroken != nil && c.highestAcked < target {
		return c.connectionBroken
	}
	return nil
}

func (c *Client) Close() error { return c.connection.Close() }

// -------------------------------------------------------- reconnecting client --

// ConnectWithReconnect creates a ReconnectingClient that automatically
// reconnects and resyncs on connection loss. Set reconnectAttempts to 0 for
// infinite retries, or a positive number to enter degraded mode after that
// many failures. initialBackoff defaults to 100ms if zero.
func ConnectWithReconnect(network, address, volumeName string, walHeader []byte, reconnectAttempts int, initialBackoff time.Duration) (*ReconnectingClient, error) {
	if initialBackoff == 0 {
		initialBackoff = 100 * time.Millisecond
	}
	rc := &ReconnectingClient{
		network:                 network,
		address:                 address,
		volumeName:              volumeName,
		walHeader:               walHeader,
		ReconnectAttempts:       reconnectAttempts,
		InitialReconnectBackoff: initialBackoff,
	}
	rc.ackCondition = sync.NewCond(&rc.mu)

	client, err := Connect(network, address, volumeName, walHeader)
	if err != nil {
		return nil, fmt.Errorf("initial connect: %w", err)
	}
	rc.client = client
	return rc, nil
}

// Append streams one WAL record with automatic reconnect/resync on failure.
func (rc *ReconnectingClient) Append(record []byte) error {
	rc.mu.Lock()

	if rc.degradedMode {
		rc.mu.Unlock()
		return rc.degradedModeError
	}

	rc.nextSequence++
	sequence := rc.nextSequence

	// Store the record in pending buffer for potential resync.
	rc.pendingRecords = append(rc.pendingRecords, pendingRecord{
		sequence: sequence,
		data:     append([]byte(nil), record...),
	})

	// Try to send to the current client (unlocked to avoid deadlock with receiveAcks).
	client := rc.client
	rc.mu.Unlock()

	if client != nil {
		err := client.Append(record)
		if err == nil {
			return nil // Success; record will be pruned on next successful barrier.
		}
		// Connection broken; trigger reconnect.
		fmt.Fprintf(os.Stderr, "walrepl: append seq=%d failed, triggering reconnect: %v\n", sequence, err)
		rc.mu.Lock()
		rc.triggerReconnectLocked()
		rc.mu.Unlock()
	}

	// Client is nil or broken; wait for reconnect to complete.
	// The record is already in pendingRecords, so resync will send it.
	rc.mu.Lock()
	defer rc.mu.Unlock()

	for rc.reconnectInProgress && !rc.degradedMode {
		rc.ackCondition.Wait()
	}

	if rc.degradedMode {
		return rc.degradedModeError
	}

	// Reconnect completed; the record was sent during resync.
	return nil
}

// Barrier blocks until everything appended is durable on the replica, with
// automatic reconnect/resync if the connection breaks during the wait.
func (rc *ReconnectingClient) Barrier() error {
	rc.mu.Lock()

	if rc.degradedMode {
		rc.mu.Unlock()
		return rc.degradedModeError
	}

	target := rc.nextSequence
	if target == 0 {
		rc.mu.Unlock()
		return nil // Nothing to barrier.
	}

	for {
		client := rc.client
		rc.mu.Unlock()

		if client != nil {
			err := client.Barrier()
			if err == nil {
				// Barrier succeeded; prune acked records from pending buffer.
				rc.mu.Lock()
				client.mu.Lock()
				highestAcked := client.highestAcked
				client.mu.Unlock()
				rc.prunePendingRecordsLocked(highestAcked)
				rc.mu.Unlock()
				return nil
			}
			// Barrier failed; trigger reconnect.
			fmt.Fprintf(os.Stderr, "walrepl: barrier failed, triggering reconnect: %v\n", err)
			rc.mu.Lock()
			rc.triggerReconnectLocked()
		} else {
			rc.mu.Lock()
		}

		if rc.degradedMode {
			rc.mu.Unlock()
			return rc.degradedModeError
		}

		// Wait for reconnect to complete and retry barrier.
		for rc.reconnectInProgress && !rc.degradedMode {
			rc.ackCondition.Wait()
		}

		if rc.degradedMode {
			rc.mu.Unlock()
			return rc.degradedModeError
		}
	}
}

// triggerReconnectLocked starts a reconnect goroutine if not already running.
// Caller must hold rc.mu.
func (rc *ReconnectingClient) triggerReconnectLocked() {
	if rc.client != nil {
		rc.client.Close()
		rc.client = nil
	}

	if rc.reconnectInProgress {
		return // Already reconnecting.
	}

	rc.reconnectInProgress = true
	go rc.reconnectLoop()
}

// reconnectLoop attempts reconnection with exponential backoff, then resyncs
// all pending records. On success, clears reconnectInProgress and broadcasts.
// On exhaustion, enters degraded mode.
func (rc *ReconnectingClient) reconnectLoop() {
	backoff := rc.InitialReconnectBackoff
	maxBackoff := 30 * time.Second
	attempts := 0

	for {
		if rc.ReconnectAttempts > 0 && attempts >= rc.ReconnectAttempts {
			rc.enterDegradedMode(fmt.Errorf("reconnect exhausted after %d attempts", attempts))
			return
		}

		attempts++
		fmt.Fprintf(os.Stderr, "walrepl: reconnect attempt %d to %s\n", attempts, rc.address)

		client, err := Connect(rc.network, rc.address, rc.volumeName, rc.walHeader)
		if err != nil {
			fmt.Fprintf(os.Stderr, "walrepl: reconnect attempt %d failed: %v\n", attempts, err)
			time.Sleep(backoff)
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		// Reconnect succeeded; resync pending records.
		fmt.Fprintf(os.Stderr, "walrepl: reconnect succeeded, resyncing %d pending records\n", len(rc.pendingRecords))
		if err := rc.resyncPendingRecords(client); err != nil {
			fmt.Fprintf(os.Stderr, "walrepl: resync failed: %v\n", err)
			client.Close()
			time.Sleep(backoff)
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		// Resync succeeded; install the new client and signal waiters.
		rc.mu.Lock()
		rc.client = client
		rc.reconnectInProgress = false
		rc.ackCondition.Broadcast()
		rc.mu.Unlock()
		fmt.Fprintf(os.Stderr, "walrepl: reconnect and resync complete\n")
		return
	}
}

// resyncPendingRecords sends all pending (unacked) records to the new client
// in sequence order. Caller must NOT hold rc.mu (to avoid deadlock with Append).
func (rc *ReconnectingClient) resyncPendingRecords(client *Client) error {
	rc.mu.Lock()
	pending := make([]pendingRecord, len(rc.pendingRecords))
	copy(pending, rc.pendingRecords)
	rc.mu.Unlock()

	for _, pr := range pending {
		if err := client.Append(pr.data); err != nil {
			return fmt.Errorf("resync record seq=%d: %w", pr.sequence, err)
		}
	}

	// Barrier to ensure all resynced records are durable.
	if err := client.Barrier(); err != nil {
		return fmt.Errorf("resync barrier: %w", err)
	}

	return nil
}

// prunePendingRecordsLocked removes all records with sequence <= highestAcked
// from the pending buffer. Caller must hold rc.mu.
func (rc *ReconnectingClient) prunePendingRecordsLocked(highestAcked uint64) {
	keep := rc.pendingRecords[:0]
	for _, pr := range rc.pendingRecords {
		if pr.sequence > highestAcked {
			keep = append(keep, pr)
		}
	}
	rc.pendingRecords = keep
}

// enterDegradedMode permanently marks the client as degraded (no replica).
func (rc *ReconnectingClient) enterDegradedMode(err error) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.degradedMode = true
	rc.degradedModeError = fmt.Errorf("walrepl degraded mode: %w", err)
	rc.reconnectInProgress = false
	rc.ackCondition.Broadcast()
	fmt.Fprintf(os.Stderr, "walrepl: entered degraded mode: %v\n", err)
}

func (rc *ReconnectingClient) Close() error {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	if rc.client != nil {
		return rc.client.Close()
	}
	return nil
}

// ------------------------------------------------------- buffered reader --

// bufferedReader wraps a net.Conn with a growable buffer exposing
// Buffered(), used by the replica's group-commit boundary detection.
type bufferedReader struct {
	source io.Reader
	buffer []byte
	start  int
	end    int
}

func newBufferedReader(source io.Reader, size int) *bufferedReader {
	return &bufferedReader{source: source, buffer: make([]byte, size)}
}

func (r *bufferedReader) Buffered() int { return r.end - r.start }

func (r *bufferedReader) fill() error {
	if r.start == r.end {
		r.start, r.end = 0, 0
	}
	if r.end == len(r.buffer) && r.start > 0 {
		copy(r.buffer, r.buffer[r.start:r.end])
		r.end -= r.start
		r.start = 0
	}
	n, err := r.source.Read(r.buffer[r.end:])
	r.end += n
	if n > 0 {
		return nil
	}
	return err
}

func (r *bufferedReader) ReadFull(destination []byte) (int, error) {
	total := 0
	for total < len(destination) {
		if r.Buffered() == 0 {
			if err := r.fill(); err != nil {
				return total, err
			}
		}
		n := copy(destination[total:], r.buffer[r.start:r.end])
		r.start += n
		total += n
	}
	return total, nil
}

func (r *bufferedReader) CopyN(destination io.Writer, length int64) error {
	remaining := length
	for remaining > 0 {
		if r.Buffered() == 0 {
			if err := r.fill(); err != nil {
				return err
			}
		}
		chunk := int64(r.Buffered())
		if chunk > remaining {
			chunk = remaining
		}
		if _, err := destination.Write(r.buffer[r.start : r.start+int(chunk)]); err != nil {
			return err
		}
		r.start += int(chunk)
		remaining -= chunk
	}
	return nil
}
