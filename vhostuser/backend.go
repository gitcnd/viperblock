// The vhost-user-blk backend daemon: accepts a frontend (QEMU) connection,
// negotiates features, maps guest memory, and services one virtio-blk
// request queue against a BlockEngine. Thin single-queue slice per the F7
// design note; reconnect and multiqueue are explicitly out of scope here.
package vhostuser

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"

	"golang.org/x/sys/unix"
)

// BlockEngine is the minimal surface the backend needs; satisfied by
// *viperblock.VB and by in-memory fakes in tests.
type BlockEngine interface {
	ReadAt(offsetBytes uint64, lengthBytes uint64) ([]byte, error)
	WriteAt(offsetBytes uint64, data []byte) error
	Flush() error
}

// Feature bits offered to the frontend.
const (
	featureVirtioVersion1Bit = 32 // VIRTIO_F_VERSION_1
	featureBlockFlushBit     = 9  // VIRTIO_BLK_F_FLUSH
)

const virtioBlockSectorSizeBytes = 512

// blockRequestHeaderSize: type u32 | reserved u32 | sector u64.
const blockRequestHeaderSize = 16

// Backend serves one vhost-user-blk device.
type Backend struct {
	Engine          BlockEngine
	CapacityBytes   uint64
	Logger          *slog.Logger
	ZeroBlockToZero bool // translate engine "zero block" errors to zero reads

	memoryRegions []MemoryRegion
	guestMemory   *GuestMemory
	queueState    vringState

	// queueAccessMutex serialises the service goroutine's ring access
	// against memory-table changes: QEMU legitimately re-sends
	// SET_MEM_TABLE while the queue is live (observed during machine
	// init on QEMU 7.2), and the live virtqueue's slices must be rebuilt
	// against the new mapping -- never touched mid-swap. This was the
	// 96%-CPU-spin wedge in the first real-QEMU smoke test (2026-07-28).
	queueAccessMutex sync.Mutex

	negotiatedFeatures         uint64
	negotiatedProtocolFeatures uint64
}

// vringState accumulates the per-queue SET_VRING_* pieces until the queue
// can be mapped and served.
type vringState struct {
	sizeDescriptors    uint16
	descriptorUserAddr uint64
	availUserAddr      uint64
	usedUserAddr       uint64
	lastAvailBase      uint16
	kickEventFile      *os.File
	callEventFile      *os.File
	queue              *VirtQueue
	stopChannel        chan struct{}
	running            bool
}

// ServeOneConnection accepts exactly one frontend connection on the given
// listening socket path and runs its message loop until disconnect.
func (b *Backend) ServeOneConnection(socketPath string) error {
	if b.Logger == nil {
		b.Logger = slog.Default()
	}
	_ = os.Remove(socketPath)
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socketPath, Net: "unix"})
	if err != nil {
		return fmt.Errorf("listen %s: %w", socketPath, err)
	}
	defer listener.Close()
	connection, err := listener.AcceptUnix()
	if err != nil {
		return fmt.Errorf("accept: %w", err)
	}
	defer connection.Close()
	return b.runMessageLoop(connection)
}

func (b *Backend) runMessageLoop(connection *net.UnixConn) error {
	for {
		message, err := ReadMessageWithFileDescriptors(connection)
		if err != nil {
			b.stopQueue()
			return err // includes clean EOF on frontend disconnect
		}
		if err := b.handleMessage(connection, message); err != nil {
			b.stopQueue()
			return fmt.Errorf("request %d: %w", message.Request, err)
		}
	}
}

func (b *Backend) handleMessage(connection *net.UnixConn, message *Message) error {
	switch message.Request {

	case RequestGetFeatures:
		features := uint64(1)<<featureVirtioVersion1Bit |
			uint64(1)<<FeatureProtocolFeaturesBit |
			uint64(1)<<featureBlockFlushBit
		return WriteReply(connection, message.Request, U64Payload(features))

	case RequestSetFeatures:
		value, err := DecodeU64Payload(message.Payload)
		if err != nil {
			return err
		}
		b.negotiatedFeatures = value
		return b.ackIfNeeded(connection, message)

	case RequestGetProtocolFeatures:
		protocolFeatures := uint64(1) << ProtocolFeatureConfig
		return WriteReply(connection, message.Request, U64Payload(protocolFeatures))

	case RequestSetProtocolFeatures:
		value, err := DecodeU64Payload(message.Payload)
		if err != nil {
			return err
		}
		b.negotiatedProtocolFeatures = value
		return b.ackIfNeeded(connection, message)

	case RequestGetQueueNum:
		return WriteReply(connection, message.Request, U64Payload(1))

	case RequestSetOwner, RequestResetOwner:
		return b.ackIfNeeded(connection, message)

	case RequestSetMemTable:
		regions, err := DecodeMemoryTablePayload(message.Payload)
		if err != nil {
			return err
		}
		if len(message.FileDescriptors) < len(regions) {
			return fmt.Errorf("mem table: %d regions but %d fds", len(regions), len(message.FileDescriptors))
		}
		for i := range regions {
			mapped, err := unix.Mmap(message.FileDescriptors[i],
				int64(regions[i].MmapOffsetBytes), int(regions[i].SizeBytes),
				unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
			if err != nil {
				return fmt.Errorf("mmap region %d: %w", i, err)
			}
			regions[i].MappedLocalBytes = mapped
			unix.Close(message.FileDescriptors[i])
		}

		// Swap the mapping under the queue lock: a live queue's ring
		// slices point into the OLD regions and must be rebuilt against
		// the new table before any further ring access.
		b.queueAccessMutex.Lock()
		oldRegions := b.memoryRegions
		b.memoryRegions = regions
		b.guestMemory = &GuestMemory{Regions: regions}
		var remapErr error
		if b.queueState.queue != nil {
			remapErr = b.remapLiveQueueLocked()
		}
		b.queueAccessMutex.Unlock()
		for i := range oldRegions {
			if oldRegions[i].MappedLocalBytes != nil {
				_ = unix.Munmap(oldRegions[i].MappedLocalBytes)
			}
		}
		if remapErr != nil {
			return fmt.Errorf("remap live queue after mem-table change: %w", remapErr)
		}
		return b.ackIfNeeded(connection, message)

	case RequestSetVringNum:
		value, err := DecodeU64Payload(message.Payload)
		if err != nil {
			return err
		}
		b.queueState.sizeDescriptors = uint16(value & 0xFFFF)
		return b.ackIfNeeded(connection, message)

	case RequestSetVringBase:
		value, err := DecodeU64Payload(message.Payload)
		if err != nil {
			return err
		}
		b.queueState.lastAvailBase = uint16(value & 0xFFFF)
		return b.ackIfNeeded(connection, message)

	case RequestSetVringAddr:
		// Payload: index u32, flags u32, descUser u64, usedUser u64,
		// availUser u64, logGuest u64.
		if len(message.Payload) < 40 {
			return fmt.Errorf("vring addr payload %d bytes", len(message.Payload))
		}
		b.queueState.descriptorUserAddr = binary.LittleEndian.Uint64(message.Payload[8:16])
		b.queueState.usedUserAddr = binary.LittleEndian.Uint64(message.Payload[16:24])
		b.queueState.availUserAddr = binary.LittleEndian.Uint64(message.Payload[24:32])
		return b.ackIfNeeded(connection, message)

	case RequestSetVringKick:
		file, err := b.takeEventFileDescriptor(message)
		if err != nil {
			return err
		}
		b.queueState.kickEventFile = file
		// Kick fd delivery is the point where QEMU considers the ring
		// startable; map and launch the service goroutine.
		return b.startQueueIfReady(connection, message)

	case RequestSetVringCall:
		file, err := b.takeEventFileDescriptor(message)
		if err != nil {
			return err
		}
		b.queueState.callEventFile = file
		return b.ackIfNeeded(connection, message)

	case RequestSetVringErr:
		if len(message.FileDescriptors) > 0 {
			unix.Close(message.FileDescriptors[0])
		}
		return b.ackIfNeeded(connection, message)

	case RequestSetVringEnable:
		return b.ackIfNeeded(connection, message)

	case RequestGetVringBase:
		b.stopQueue()
		lastAvail := uint64(0)
		if b.queueState.queue != nil {
			lastAvail = uint64(b.queueState.queue.lastSeenAvailIndex)
		}
		return WriteReply(connection, message.Request, U64Payload(lastAvail))

	case RequestGetConfig:
		// Payload in AND out: offset u32 | size u32 | flags u32 | bytes.
		if len(message.Payload) < 12 {
			return fmt.Errorf("get-config payload %d bytes", len(message.Payload))
		}
		requestedOffset := binary.LittleEndian.Uint32(message.Payload[0:4])
		requestedSize := binary.LittleEndian.Uint32(message.Payload[4:8])
		configSpace := make([]byte, 64)
		binary.LittleEndian.PutUint64(configSpace[0:8], b.CapacityBytes/virtioBlockSectorSizeBytes)
		end := int(requestedOffset) + int(requestedSize)
		if end > len(configSpace) {
			end = len(configSpace)
		}
		reply := make([]byte, 12+end-int(requestedOffset))
		copy(reply[0:12], message.Payload[0:12])
		copy(reply[12:], configSpace[requestedOffset:end])
		return WriteReply(connection, message.Request, reply)

	default:
		b.Logger.Warn("vhost-user: unhandled request", "request", message.Request)
		return b.ackIfNeeded(connection, message)
	}
}

// ackIfNeeded replies with a zero u64 only when the frontend set
// NEED_REPLY (VHOST_USER_PROTOCOL_F_REPLY_ACK semantics).
func (b *Backend) ackIfNeeded(connection *net.UnixConn, message *Message) error {
	if message.Flags&FlagNeedReply == 0 {
		return nil
	}
	return WriteReply(connection, message.Request, U64Payload(0))
}

func (b *Backend) takeEventFileDescriptor(message *Message) (*os.File, error) {
	// Payload u64: low 8 bits queue index; bit 8 set = "invalid fd/polling".
	value, err := DecodeU64Payload(message.Payload)
	if err != nil {
		return nil, err
	}
	if value&0x100 != 0 {
		return nil, fmt.Errorf("frontend requested polling mode (no eventfd); unsupported")
	}
	if len(message.FileDescriptors) < 1 {
		return nil, fmt.Errorf("no eventfd delivered")
	}
	return os.NewFile(uintptr(message.FileDescriptors[0]), "vhost-eventfd"), nil
}

func (b *Backend) startQueueIfReady(connection *net.UnixConn, message *Message) error {
	if b.guestMemory == nil || b.queueState.sizeDescriptors == 0 || b.queueState.kickEventFile == nil {
		return b.ackIfNeeded(connection, message)
	}
	descriptorGPA, err := TranslateUserToGuestPhysical(b.memoryRegions, b.queueState.descriptorUserAddr)
	if err != nil {
		return fmt.Errorf("descriptor addr: %w", err)
	}
	availGPA, err := TranslateUserToGuestPhysical(b.memoryRegions, b.queueState.availUserAddr)
	if err != nil {
		return fmt.Errorf("avail addr: %w", err)
	}
	usedGPA, err := TranslateUserToGuestPhysical(b.memoryRegions, b.queueState.usedUserAddr)
	if err != nil {
		return fmt.Errorf("used addr: %w", err)
	}
	queue, err := MapVirtQueue(b.guestMemory, b.queueState.sizeDescriptors, descriptorGPA, availGPA, usedGPA)
	if err != nil {
		return err
	}
	queue.lastSeenAvailIndex = b.queueState.lastAvailBase
	b.queueState.queue = queue
	b.queueState.stopChannel = make(chan struct{})
	b.queueState.running = true
	go b.serveQueueUntilStopped()
	b.Logger.Info("vhost-user: queue started", "size", b.queueState.sizeDescriptors)
	return b.ackIfNeeded(connection, message)
}

// remapLiveQueueLocked rebuilds the running queue's ring slices against the
// current guest memory, preserving the avail cursor. Caller holds
// queueAccessMutex. The stored per-queue user addresses (from
// SET_VRING_ADDR) are re-translated through the new region table, so a
// remap that moved the rings is handled.
func (b *Backend) remapLiveQueueLocked() error {
	descriptorGPA, err := TranslateUserToGuestPhysical(b.memoryRegions, b.queueState.descriptorUserAddr)
	if err != nil {
		return fmt.Errorf("descriptor addr: %w", err)
	}
	availGPA, err := TranslateUserToGuestPhysical(b.memoryRegions, b.queueState.availUserAddr)
	if err != nil {
		return fmt.Errorf("avail addr: %w", err)
	}
	usedGPA, err := TranslateUserToGuestPhysical(b.memoryRegions, b.queueState.usedUserAddr)
	if err != nil {
		return fmt.Errorf("used addr: %w", err)
	}
	rebuilt, err := MapVirtQueue(b.guestMemory, b.queueState.sizeDescriptors, descriptorGPA, availGPA, usedGPA)
	if err != nil {
		return err
	}
	// Preserve the consumer cursor: the driver's avail index is authoritative
	// in the ring, but lastSeenAvailIndex is ours and must carry over.
	rebuilt.lastSeenAvailIndex = b.queueState.queue.lastSeenAvailIndex
	b.queueState.queue = rebuilt
	return nil
}

func (b *Backend) stopQueue() {
	if b.queueState.running {
		close(b.queueState.stopChannel)
		b.queueState.running = false
	}
}

// serveQueueUntilStopped blocks on the kick eventfd and drains the avail
// ring on every kick.
func (b *Backend) serveQueueUntilStopped() {
	kickBuffer := make([]byte, 8)
	for {
		select {
		case <-b.queueState.stopChannel:
			return
		default:
		}
		if _, err := b.queueState.kickEventFile.Read(kickBuffer); err != nil {
			return // eventfd closed = frontend gone
		}
		// Hold the queue lock across ring access so a concurrent
		// SET_MEM_TABLE remap cannot swap the mapping mid-walk.
		b.queueAccessMutex.Lock()
		queue := b.queueState.queue
		chains, err := queue.PopAvailableChains()
		if err != nil {
			b.queueAccessMutex.Unlock()
			b.Logger.Error("vhost-user: ring walk failed", "err", err)
			return
		}
		for _, chain := range chains {
			bytesWritten := b.processBlockRequest(chain)
			queue.PushUsed(chain, bytesWritten)
		}
		callFile := b.queueState.callEventFile
		b.queueAccessMutex.Unlock()
		if len(chains) > 0 && callFile != nil {
			one := make([]byte, 8)
			binary.LittleEndian.PutUint64(one, 1)
			_, _ = callFile.Write(one)
		}
	}
}

// processBlockRequest executes one virtio-blk request chain and returns the
// number of bytes the device wrote into writable spans (including status).
func (b *Backend) processBlockRequest(chain *DescriptorChain) uint32 {
	statusSpanIndex := len(chain.WritableSpans) - 1
	if len(chain.ReadableSpans) == 0 || statusSpanIndex < 0 ||
		len(chain.WritableSpans[statusSpanIndex]) < 1 ||
		len(chain.ReadableSpans[0]) < blockRequestHeaderSize {
		if statusSpanIndex >= 0 && len(chain.WritableSpans[statusSpanIndex]) >= 1 {
			chain.WritableSpans[statusSpanIndex][0] = BlockStatusIOError
			return 1
		}
		return 0
	}
	header := chain.ReadableSpans[0]
	requestType := binary.LittleEndian.Uint32(header[0:4])
	sector := binary.LittleEndian.Uint64(header[8:16])
	offsetBytes := sector * virtioBlockSectorSizeBytes
	statusSpan := chain.WritableSpans[statusSpanIndex]
	dataWritableSpans := chain.WritableSpans[:statusSpanIndex]

	switch requestType {
	case BlockRequestTypeIn:
		var totalWritten uint32
		for _, span := range dataWritableSpans {
			data, err := b.Engine.ReadAt(offsetBytes, uint64(len(span)))
			if err != nil && b.ZeroBlockToZero && isZeroBlockError(err) {
				data, err = make([]byte, len(span)), nil
			}
			if err != nil {
				statusSpan[0] = BlockStatusIOError
				return totalWritten + 1
			}
			copy(span, data)
			offsetBytes += uint64(len(span))
			totalWritten += uint32(len(span))
		}
		statusSpan[0] = BlockStatusOK
		return totalWritten + 1

	case BlockRequestTypeOut:
		for _, span := range chain.ReadableSpans[1:] {
			if err := b.Engine.WriteAt(offsetBytes, span); err != nil {
				statusSpan[0] = BlockStatusIOError
				return 1
			}
			offsetBytes += uint64(len(span))
		}
		statusSpan[0] = BlockStatusOK
		return 1

	case BlockRequestTypeFlush:
		if err := b.Engine.Flush(); err != nil {
			statusSpan[0] = BlockStatusIOError
			return 1
		}
		statusSpan[0] = BlockStatusOK
		return 1

	case BlockRequestTypeGetID:
		serial := []byte("viperblock-vhost0")
		if len(dataWritableSpans) > 0 {
			copy(dataWritableSpans[0], serial)
		}
		statusSpan[0] = BlockStatusOK
		return uint32(len(serial)) + 1

	default:
		statusSpan[0] = BlockStatusUnsupported
		return 1
	}
}

func isZeroBlockError(err error) bool {
	return err != nil && err.Error() == "zero block"
}
