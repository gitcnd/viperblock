// Split-virtqueue handling (virtio 1.2 spec sections 2.6-2.7) over guest
// memory shared via SET_MEM_TABLE. amd64-first: relies on x86-TSO plus Go
// atomics for the avail/used index ordering the spec requires.
package vhostuser

import (
	"encoding/binary"
	"fmt"
)

// MEMORY-ORDERING NOTE (amd64-first, per the F7 design note): the virtio
// spec requires acquire semantics reading the avail index and release
// semantics publishing the used index. The ring areas are only guaranteed
// 2-byte aligned, which rules out Go's 4-byte atomics on the packed
// flags/idx pairs. On amd64, aligned 2-byte loads/stores are
// single-copy-atomic and x86-TSO forbids the store reordering that would
// break the publish protocol, so plain accesses in PROGRAM ORDER are
// sufficient. Revisit with explicit fences before any non-x86 port.

// Descriptor flags (virtio 1.2, 2.7.5).
const (
	descriptorFlagHasNext       = 1
	descriptorFlagWriteOnly     = 2
	descriptorFlagIndirectTable = 4
)

const descriptorEncodedSize = 16

// virtio-blk request types (virtio 1.2, 5.2.6).
const (
	BlockRequestTypeIn    = 0 // read
	BlockRequestTypeOut   = 1 // write
	BlockRequestTypeFlush = 4
	BlockRequestTypeGetID = 8
)

// virtio-blk status byte values.
const (
	BlockStatusOK          = 0
	BlockStatusIOError     = 1
	BlockStatusUnsupported = 2
)

// VirtQueue is one split virtqueue mapped into our address space.
type VirtQueue struct {
	QueueSize uint16
	Memory    *GuestMemory

	descriptorTableBytes []byte // QueueSize * 16
	availRingBytes       []byte // 4 + QueueSize*2 (+2 used_event)
	usedRingBytes        []byte // 4 + QueueSize*8 (+2 avail_event)

	lastSeenAvailIndex uint16
}

// MapVirtQueue resolves the three ring areas from guest physical addresses
// (SET_VRING_ADDR delivers frontend USER addresses; the caller translates
// them to guest-physical via the memory table's UserspaceAddress fields
// before calling this -- see TranslateUserToGuestPhysical).
func MapVirtQueue(memory *GuestMemory, queueSize uint16, descriptorGPA, availGPA, usedGPA uint64) (*VirtQueue, error) {
	if queueSize == 0 || (queueSize&(queueSize-1)) != 0 {
		return nil, fmt.Errorf("queue size %d not a power of two", queueSize)
	}
	descriptorTable, err := memory.Slice(descriptorGPA, uint64(queueSize)*descriptorEncodedSize)
	if err != nil {
		return nil, fmt.Errorf("descriptor table: %w", err)
	}
	availRing, err := memory.Slice(availGPA, 4+uint64(queueSize)*2+2)
	if err != nil {
		return nil, fmt.Errorf("avail ring: %w", err)
	}
	usedRing, err := memory.Slice(usedGPA, 4+uint64(queueSize)*8+2)
	if err != nil {
		return nil, fmt.Errorf("used ring: %w", err)
	}
	return &VirtQueue{
		QueueSize:            queueSize,
		Memory:               memory,
		descriptorTableBytes: descriptorTable,
		availRingBytes:       availRing,
		usedRingBytes:        usedRing,
	}, nil
}

// TranslateUserToGuestPhysical maps a frontend userspace address (as
// delivered in SET_VRING_ADDR) to a guest-physical address using the
// memory table.
func TranslateUserToGuestPhysical(regions []MemoryRegion, userAddress uint64) (uint64, error) {
	for i := range regions {
		region := &regions[i]
		if userAddress >= region.UserspaceAddress &&
			userAddress < region.UserspaceAddress+region.SizeBytes {
			return region.GuestPhysicalAddress + (userAddress - region.UserspaceAddress), nil
		}
	}
	return 0, fmt.Errorf("user address 0x%x not in any region", userAddress)
}

// DescriptorChain is one guest request: a chain of descriptors, split into
// device-readable and device-writable spans, plus the head index for the
// used-ring completion.
type DescriptorChain struct {
	HeadIndex      uint16
	ReadableSpans  [][]byte
	WritableSpans  [][]byte
	totalWrittenLE uint32
}

// availIndex reads the driver's avail index (offset 2, after the u16
// flags field). See the memory-ordering note above.
func (vq *VirtQueue) availIndex() uint16 {
	return binary.LittleEndian.Uint16(vq.availRingBytes[2:4])
}

// PopAvailableChains collects all descriptor chains the driver has
// published since the last call.
func (vq *VirtQueue) PopAvailableChains() ([]*DescriptorChain, error) {
	var chains []*DescriptorChain
	currentAvailIndex := vq.availIndex()
	for vq.lastSeenAvailIndex != currentAvailIndex {
		slot := vq.lastSeenAvailIndex % vq.QueueSize
		ringEntryOffset := 4 + uint64(slot)*2
		headIndex := binary.LittleEndian.Uint16(vq.availRingBytes[ringEntryOffset : ringEntryOffset+2])
		chain, err := vq.walkDescriptorChain(headIndex)
		if err != nil {
			return chains, err
		}
		chains = append(chains, chain)
		vq.lastSeenAvailIndex++
	}
	return chains, nil
}

func (vq *VirtQueue) walkDescriptorChain(headIndex uint16) (*DescriptorChain, error) {
	chain := &DescriptorChain{HeadIndex: headIndex}
	descriptorIndex := headIndex
	for hop := 0; ; hop++ {
		if hop > int(vq.QueueSize) {
			return nil, fmt.Errorf("descriptor chain loop at head %d", headIndex)
		}
		base := uint64(descriptorIndex) * descriptorEncodedSize
		descriptorBytes := vq.descriptorTableBytes[base : base+descriptorEncodedSize]
		address := binary.LittleEndian.Uint64(descriptorBytes[0:8])
		length := binary.LittleEndian.Uint32(descriptorBytes[8:12])
		flags := binary.LittleEndian.Uint16(descriptorBytes[12:14])
		next := binary.LittleEndian.Uint16(descriptorBytes[14:16])

		if flags&descriptorFlagIndirectTable != 0 {
			return nil, fmt.Errorf("indirect descriptors not supported (feature not offered)")
		}
		span, err := vq.Memory.Slice(address, uint64(length))
		if err != nil {
			return nil, fmt.Errorf("descriptor %d: %w", descriptorIndex, err)
		}
		if flags&descriptorFlagWriteOnly != 0 {
			chain.WritableSpans = append(chain.WritableSpans, span)
		} else {
			chain.ReadableSpans = append(chain.ReadableSpans, span)
		}
		if flags&descriptorFlagHasNext == 0 {
			return chain, nil
		}
		descriptorIndex = next
	}
}

// PushUsed publishes a completed chain to the used ring with the number of
// bytes the device wrote, then bumps the used index. Entry is written
// before the index in program order; x86-TSO preserves that store order
// (see the memory-ordering note above).
func (vq *VirtQueue) PushUsed(chain *DescriptorChain, bytesWritten uint32) {
	usedIndex := binary.LittleEndian.Uint16(vq.usedRingBytes[2:4])
	slot := usedIndex % vq.QueueSize
	entryOffset := 4 + uint64(slot)*8
	binary.LittleEndian.PutUint32(vq.usedRingBytes[entryOffset:entryOffset+4], uint32(chain.HeadIndex))
	binary.LittleEndian.PutUint32(vq.usedRingBytes[entryOffset+4:entryOffset+8], bytesWritten)
	binary.LittleEndian.PutUint16(vq.usedRingBytes[2:4], usedIndex+1)
}
