package vhostuser

import (
	"encoding/binary"
	"testing"
)

// buildSyntheticQueueMemory lays out a descriptor table, avail ring, used
// ring, and a data area inside one fake guest memory region, exactly as a
// driver would, so ring logic is testable with no QEMU.
type syntheticQueueLayout struct {
	memory        *GuestMemory
	queue         *VirtQueue
	descriptorGPA uint64
	availGPA      uint64
	usedGPA       uint64
	dataGPA       uint64
	backing       []byte
}

func buildSyntheticQueueMemory(t *testing.T, queueSize uint16) *syntheticQueueLayout {
	t.Helper()
	const regionBase = uint64(0x100000)
	backing := make([]byte, 1<<20)
	memory := &GuestMemory{Regions: []MemoryRegion{{
		GuestPhysicalAddress: regionBase,
		SizeBytes:            uint64(len(backing)),
		UserspaceAddress:     0x7f0000000000,
		MappedLocalBytes:     backing,
	}}}

	layout := &syntheticQueueLayout{
		memory:        memory,
		descriptorGPA: regionBase,
		availGPA:      regionBase + 0x1000,
		usedGPA:       regionBase + 0x2000,
		dataGPA:       regionBase + 0x10000,
		backing:       backing,
	}
	queue, err := MapVirtQueue(memory, queueSize, layout.descriptorGPA, layout.availGPA, layout.usedGPA)
	if err != nil {
		t.Fatalf("MapVirtQueue: %v", err)
	}
	layout.queue = queue
	return layout
}

// writeDescriptor fills descriptor slot i.
func (l *syntheticQueueLayout) writeDescriptor(index uint16, address uint64, length uint32, flags uint16, next uint16) {
	base := uint64(index) * descriptorEncodedSize // descriptor table at region start
	d := l.backing[base : base+descriptorEncodedSize]
	binary.LittleEndian.PutUint64(d[0:8], address)
	binary.LittleEndian.PutUint32(d[8:12], length)
	binary.LittleEndian.PutUint16(d[12:14], flags)
	binary.LittleEndian.PutUint16(d[14:16], next)
}

// publishAvail appends a head index to the avail ring and bumps its index.
func (l *syntheticQueueLayout) publishAvail(headIndex uint16) {
	availBase := l.availGPA - l.memory.Regions[0].GuestPhysicalAddress
	avail := l.backing[availBase:]
	index := binary.LittleEndian.Uint16(avail[2:4])
	slot := index % l.queue.QueueSize
	binary.LittleEndian.PutUint16(avail[4+slot*2:4+slot*2+2], headIndex)
	binary.LittleEndian.PutUint16(avail[2:4], index+1)
}

func TestPopAvailableChainsWalksReadableAndWritableSpans(t *testing.T) {
	l := buildSyntheticQueueMemory(t, 8)

	// Three-descriptor virtio-blk-style chain: 16-byte readable header,
	// 4096-byte writable data, 1-byte writable status.
	headerGPA := l.dataGPA
	dataGPA := l.dataGPA + 0x100
	statusGPA := l.dataGPA + 0x2000
	l.writeDescriptor(0, headerGPA, 16, descriptorFlagHasNext, 1)
	l.writeDescriptor(1, dataGPA, 4096, descriptorFlagHasNext|descriptorFlagWriteOnly, 2)
	l.writeDescriptor(2, statusGPA, 1, descriptorFlagWriteOnly, 0)
	l.publishAvail(0)

	chains, err := l.queue.PopAvailableChains()
	if err != nil {
		t.Fatalf("PopAvailableChains: %v", err)
	}
	if len(chains) != 1 {
		t.Fatalf("got %d chains, want 1", len(chains))
	}
	chain := chains[0]
	if chain.HeadIndex != 0 {
		t.Errorf("head index = %d, want 0", chain.HeadIndex)
	}
	if len(chain.ReadableSpans) != 1 || len(chain.ReadableSpans[0]) != 16 {
		t.Errorf("readable spans wrong: %d spans", len(chain.ReadableSpans))
	}
	if len(chain.WritableSpans) != 2 || len(chain.WritableSpans[0]) != 4096 || len(chain.WritableSpans[1]) != 1 {
		t.Errorf("writable spans wrong: %d spans", len(chain.WritableSpans))
	}

	// Nothing new: second pop must return empty.
	chains, err = l.queue.PopAvailableChains()
	if err != nil || len(chains) != 0 {
		t.Errorf("second pop: %d chains, err %v; want 0, nil", len(chains), err)
	}
}

func TestPushUsedPublishesEntryAndBumpsIndex(t *testing.T) {
	l := buildSyntheticQueueMemory(t, 8)
	l.writeDescriptor(3, l.dataGPA, 512, descriptorFlagWriteOnly, 0)
	l.publishAvail(3)
	chains, err := l.queue.PopAvailableChains()
	if err != nil || len(chains) != 1 {
		t.Fatalf("setup pop failed: %v", err)
	}

	l.queue.PushUsed(chains[0], 512)

	usedBase := l.usedGPA - l.memory.Regions[0].GuestPhysicalAddress
	used := l.backing[usedBase:]
	if got := binary.LittleEndian.Uint16(used[2:4]); got != 1 {
		t.Errorf("used index = %d, want 1", got)
	}
	if got := binary.LittleEndian.Uint32(used[4:8]); got != 3 {
		t.Errorf("used entry id = %d, want 3", got)
	}
	if got := binary.LittleEndian.Uint32(used[8:12]); got != 512 {
		t.Errorf("used entry len = %d, want 512", got)
	}
}

func TestWalkDescriptorChainRejectsLoops(t *testing.T) {
	l := buildSyntheticQueueMemory(t, 8)
	l.writeDescriptor(0, l.dataGPA, 16, descriptorFlagHasNext, 1)
	l.writeDescriptor(1, l.dataGPA, 16, descriptorFlagHasNext, 0) // loop 0->1->0
	l.publishAvail(0)
	if _, err := l.queue.PopAvailableChains(); err == nil {
		t.Fatal("expected loop detection error, got nil")
	}
}

func TestMemoryTableCodecRoundTrip(t *testing.T) {
	payload := make([]byte, 8+2*32)
	binary.LittleEndian.PutUint32(payload[0:4], 2)
	for i := 0; i < 2; i++ {
		base := 8 + i*32
		binary.LittleEndian.PutUint64(payload[base:base+8], uint64(0x1000*(i+1)))
		binary.LittleEndian.PutUint64(payload[base+8:base+16], 0x2000)
		binary.LittleEndian.PutUint64(payload[base+16:base+24], uint64(0x7f0000000000+i*0x2000))
		binary.LittleEndian.PutUint64(payload[base+24:base+32], 0)
	}
	regions, err := DecodeMemoryTablePayload(payload)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(regions) != 2 || regions[1].GuestPhysicalAddress != 0x2000 {
		t.Fatalf("decoded regions wrong: %+v", regions)
	}
	gpa, err := TranslateUserToGuestPhysical(regions, 0x7f0000002000+0x10)
	if err != nil || gpa != 0x2000+0x10 {
		t.Fatalf("translate: gpa=0x%x err=%v", gpa, err)
	}
}

func TestU64PayloadRoundTrip(t *testing.T) {
	value := uint64(1)<<FeatureProtocolFeaturesBit | 42
	decoded, err := DecodeU64Payload(U64Payload(value))
	if err != nil || decoded != value {
		t.Fatalf("roundtrip: got %d err %v, want %d", decoded, err, value)
	}
}
