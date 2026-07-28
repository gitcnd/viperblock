package vhostuser

import (
	"bytes"
	"encoding/binary"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// memoryBlockEngine is an in-memory BlockEngine fake.
type memoryBlockEngine struct {
	mu         sync.Mutex
	content    []byte
	flushCount int
}

func (m *memoryBlockEngine) ReadAt(offset uint64, length uint64) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if offset+length > uint64(len(m.content)) {
		return nil, errors.New("out of range")
	}
	out := make([]byte, length)
	copy(out, m.content[offset:offset+length])
	return out, nil
}

func (m *memoryBlockEngine) WriteAt(offset uint64, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if offset+uint64(len(data)) > uint64(len(m.content)) {
		return errors.New("out of range")
	}
	copy(m.content[offset:], data)
	return nil
}

func (m *memoryBlockEngine) Flush() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.flushCount++
	return nil
}

// syntheticFrontend plays QEMU's role: owns the guest memory (a memfd),
// lays out the rings, sends the vhost-user message sequence, kicks, and
// waits for calls.
type syntheticFrontend struct {
	t             *testing.T
	connection    *net.UnixConn
	memoryFile    *os.File
	memoryBytes   []byte
	kickWriteEnd  *os.File
	callReadEnd   *os.File
	queueSize     uint16
	availIndexOwn uint16

	// Fixed layout inside the one memory region (offsets from base).
	descriptorTableOffset uint64
	availRingOffset       uint64
	usedRingOffset        uint64
	dataAreaOffset        uint64
}

const frontendGuestPhysicalBase = uint64(0x40000000)
const frontendFakeUserBase = uint64(0x7f4200000000)

func newSyntheticFrontend(t *testing.T, connection *net.UnixConn) *syntheticFrontend {
	t.Helper()
	const memorySize = 4 << 20
	memoryFileDescriptor, err := unix.MemfdCreate("guestmem", unix.MFD_CLOEXEC)
	if err != nil {
		t.Fatalf("memfd: %v", err)
	}
	if err := unix.Ftruncate(memoryFileDescriptor, memorySize); err != nil {
		t.Fatalf("ftruncate: %v", err)
	}
	mapped, err := unix.Mmap(memoryFileDescriptor, 0, memorySize,
		unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		t.Fatalf("mmap: %v", err)
	}
	return &syntheticFrontend{
		t:                     t,
		connection:            connection,
		memoryFile:            os.NewFile(uintptr(memoryFileDescriptor), "guestmem"),
		memoryBytes:           mapped,
		queueSize:             8,
		descriptorTableOffset: 0x0,
		availRingOffset:       0x1000,
		usedRingOffset:        0x2000,
		dataAreaOffset:        0x10000,
	}
}

func (f *syntheticFrontend) sendMessage(request uint32, flags uint32, payload []byte, fileDescriptors []int) {
	f.t.Helper()
	header := make([]byte, 12)
	binary.LittleEndian.PutUint32(header[0:4], request)
	binary.LittleEndian.PutUint32(header[4:8], flags|0x1)
	binary.LittleEndian.PutUint32(header[8:12], uint32(len(payload)))
	var ancillary []byte
	if len(fileDescriptors) > 0 {
		ancillary = unix.UnixRights(fileDescriptors...)
	}
	if _, _, err := f.connection.WriteMsgUnix(append(header, payload...), ancillary, nil); err != nil {
		f.t.Fatalf("send request %d: %v", request, err)
	}
}

func (f *syntheticFrontend) receiveReplyPayload(request uint32) []byte {
	f.t.Helper()
	f.connection.SetReadDeadline(time.Now().Add(5 * time.Second))
	header := make([]byte, 12)
	if _, err := readFull(f.connection, header); err != nil {
		f.t.Fatalf("reply header for %d: %v", request, err)
	}
	if got := binary.LittleEndian.Uint32(header[0:4]); got != request {
		f.t.Fatalf("reply for request %d, want %d", got, request)
	}
	payload := make([]byte, binary.LittleEndian.Uint32(header[8:12]))
	if _, err := readFull(f.connection, payload); err != nil {
		f.t.Fatalf("reply payload for %d: %v", request, err)
	}
	return payload
}

// negotiate performs the full startup sequence a frontend would.
func (f *syntheticFrontend) negotiate() {
	f.t.Helper()
	features, err := DecodeU64Payload(f.receiveAfter(RequestGetFeatures, nil))
	if err != nil || features&(1<<featureVirtioVersion1Bit) == 0 {
		f.t.Fatalf("features: 0x%x err %v", features, err)
	}
	f.sendMessage(RequestSetFeatures, 0, U64Payload(features), nil)
	f.receiveAfter(RequestGetProtocolFeatures, nil)
	f.sendMessage(RequestSetProtocolFeatures, 0, U64Payload(1<<ProtocolFeatureConfig), nil)
	f.sendMessage(RequestSetOwner, 0, nil, nil)

	// Memory table: one region covering the whole memfd.
	memPayload := make([]byte, 8+32)
	binary.LittleEndian.PutUint32(memPayload[0:4], 1)
	binary.LittleEndian.PutUint64(memPayload[8:16], frontendGuestPhysicalBase)
	binary.LittleEndian.PutUint64(memPayload[16:24], uint64(len(f.memoryBytes)))
	binary.LittleEndian.PutUint64(memPayload[24:32], frontendFakeUserBase)
	binary.LittleEndian.PutUint64(memPayload[32:40], 0)
	f.sendMessage(RequestSetMemTable, 0, memPayload, []int{int(f.memoryFile.Fd())})

	f.sendMessage(RequestSetVringNum, 0, U64Payload(uint64(f.queueSize)), nil)
	f.sendMessage(RequestSetVringBase, 0, U64Payload(0), nil)

	addrPayload := make([]byte, 40)
	binary.LittleEndian.PutUint64(addrPayload[8:16], frontendFakeUserBase+f.descriptorTableOffset)
	binary.LittleEndian.PutUint64(addrPayload[16:24], frontendFakeUserBase+f.usedRingOffset)
	binary.LittleEndian.PutUint64(addrPayload[24:32], frontendFakeUserBase+f.availRingOffset)
	f.sendMessage(RequestSetVringAddr, 0, addrPayload, nil)

	kickFD, err := unix.Eventfd(0, unix.EFD_CLOEXEC)
	if err != nil {
		f.t.Fatalf("kick eventfd: %v", err)
	}
	callFD, err := unix.Eventfd(0, unix.EFD_CLOEXEC)
	if err != nil {
		f.t.Fatalf("call eventfd: %v", err)
	}
	f.kickWriteEnd = os.NewFile(uintptr(kickFD), "kick")
	f.callReadEnd = os.NewFile(uintptr(callFD), "call")
	f.sendMessage(RequestSetVringCall, 0, U64Payload(0), []int{callFD})
	f.sendMessage(RequestSetVringKick, 0, U64Payload(0), []int{kickFD})
}

func (f *syntheticFrontend) receiveAfter(request uint32, payload []byte) []byte {
	f.t.Helper()
	f.sendMessage(request, 0, payload, nil)
	return f.receiveReplyPayload(request)
}

// submitBlockRequest lays out a virtio-blk request chain in guest memory,
// publishes it, kicks, and waits for the call eventfd.
func (f *syntheticFrontend) submitBlockRequest(requestType uint32, sector uint64, dataLength uint32, outData []byte) (status byte, inData []byte) {
	f.t.Helper()
	headerOffset := f.dataAreaOffset
	dataOffset := f.dataAreaOffset + 0x100
	statusOffset := f.dataAreaOffset + 0x8000

	header := f.memoryBytes[headerOffset : headerOffset+blockRequestHeaderSize]
	binary.LittleEndian.PutUint32(header[0:4], requestType)
	binary.LittleEndian.PutUint64(header[8:16], sector)
	f.memoryBytes[statusOffset] = 0xEE

	writeDescriptor := func(index uint16, guestOffset uint64, length uint32, flags uint16, next uint16) {
		d := f.memoryBytes[f.descriptorTableOffset+uint64(index)*16:]
		binary.LittleEndian.PutUint64(d[0:8], frontendGuestPhysicalBase+guestOffset)
		binary.LittleEndian.PutUint32(d[8:12], length)
		binary.LittleEndian.PutUint16(d[12:14], flags)
		binary.LittleEndian.PutUint16(d[14:16], next)
	}

	hasData := dataLength > 0
	if requestType == BlockRequestTypeOut && hasData {
		copy(f.memoryBytes[dataOffset:], outData)
	}
	switch {
	case !hasData: // e.g. FLUSH: header -> status
		writeDescriptor(0, headerOffset, blockRequestHeaderSize, descriptorFlagHasNext, 1)
		writeDescriptor(1, statusOffset, 1, descriptorFlagWriteOnly, 0)
	case requestType == BlockRequestTypeOut:
		writeDescriptor(0, headerOffset, blockRequestHeaderSize, descriptorFlagHasNext, 1)
		writeDescriptor(1, dataOffset, dataLength, descriptorFlagHasNext, 2)
		writeDescriptor(2, statusOffset, 1, descriptorFlagWriteOnly, 0)
	default: // IN: writable data span
		writeDescriptor(0, headerOffset, blockRequestHeaderSize, descriptorFlagHasNext, 1)
		writeDescriptor(1, dataOffset, dataLength, descriptorFlagHasNext|descriptorFlagWriteOnly, 2)
		writeDescriptor(2, statusOffset, 1, descriptorFlagWriteOnly, 0)
	}

	avail := f.memoryBytes[f.availRingOffset:]
	slot := f.availIndexOwn % f.queueSize
	binary.LittleEndian.PutUint16(avail[4+slot*2:], 0)
	f.availIndexOwn++
	binary.LittleEndian.PutUint16(avail[2:4], f.availIndexOwn)

	one := make([]byte, 8)
	binary.LittleEndian.PutUint64(one, 1)
	if _, err := f.kickWriteEnd.Write(one); err != nil {
		f.t.Fatalf("kick: %v", err)
	}

	callBuffer := make([]byte, 8)
	f.callReadEnd.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := f.callReadEnd.Read(callBuffer); err != nil {
		f.t.Fatalf("waiting for call eventfd: %v", err)
	}
	status = f.memoryBytes[statusOffset]
	if requestType == BlockRequestTypeIn {
		inData = make([]byte, dataLength)
		copy(inData, f.memoryBytes[dataOffset:dataOffset+uint64(dataLength)])
	}
	return status, inData
}

func TestBackendEndToEndWriteReadFlush(t *testing.T) {
	socketPath := filepath.Join(t.TempDir(), "vhost.sock")
	engine := &memoryBlockEngine{content: make([]byte, 1<<20)}
	backend := &Backend{Engine: engine, CapacityBytes: 1 << 20}

	serveDone := make(chan error, 1)
	go func() { serveDone <- backend.ServeOneConnection(socketPath) }()

	var rawConnection net.Conn
	var err error
	for attempt := 0; attempt < 50; attempt++ {
		rawConnection, err = net.Dial("unix", socketPath)
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("dial backend: %v", err)
	}
	frontend := newSyntheticFrontend(t, rawConnection.(*net.UnixConn))
	frontend.negotiate()

	// GET_CONFIG: capacity in sectors.
	configRequest := make([]byte, 12)
	binary.LittleEndian.PutUint32(configRequest[4:8], 8)
	configReply := frontend.receiveAfter(RequestGetConfig, configRequest)
	if capacitySectors := binary.LittleEndian.Uint64(configReply[12:20]); capacitySectors != (1<<20)/512 {
		t.Fatalf("capacity = %d sectors, want %d", capacitySectors, (1<<20)/512)
	}

	// WRITE 4 KiB of pattern at sector 8, then READ it back.
	pattern := bytes.Repeat([]byte{0x5A, 0xA5}, 2048)
	if status, _ := frontend.submitBlockRequest(BlockRequestTypeOut, 8, 4096, pattern); status != BlockStatusOK {
		t.Fatalf("write status = %d", status)
	}
	if !bytes.Equal(engine.content[8*512:8*512+4096], pattern) {
		t.Fatal("engine content does not match written pattern")
	}
	status, readBack := frontend.submitBlockRequest(BlockRequestTypeIn, 8, 4096, nil)
	if status != BlockStatusOK || !bytes.Equal(readBack, pattern) {
		t.Fatalf("read status=%d match=%v", status, bytes.Equal(readBack, pattern))
	}

	// FLUSH must reach the engine.
	if status, _ := frontend.submitBlockRequest(BlockRequestTypeFlush, 0, 0, nil); status != BlockStatusOK {
		t.Fatalf("flush status = %d", status)
	}
	if engine.flushCount != 1 {
		t.Fatalf("flush count = %d, want 1", engine.flushCount)
	}

	rawConnection.Close()
	select {
	case <-serveDone:
	case <-time.After(5 * time.Second):
		t.Fatal("backend did not exit after disconnect")
	}
}
