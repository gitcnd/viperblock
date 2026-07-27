// Package vhostuser implements the pieces of the vhost-user protocol and
// virtio split-virtqueue handling needed to serve viperblock volumes as
// vhost-user-blk devices (spinifex project fork F7, design note
// F7_VHOST_USER_BLK_DESIGN_NOTE.md: pure Go, amd64-first, single queue).
//
// This file: the control-plane message codec. vhost-user messages travel
// over a unix stream socket as a 12-byte little-endian header
// (request u32 | flags u32 | payloadSize u32) followed by the payload;
// file descriptors (memory regions, kick/call eventfds) arrive as
// SCM_RIGHTS ancillary data on the same message.
//
// References: QEMU docs/interop/vhost-user.rst; virtio 1.2 spec 2.6/2.7.
package vhostuser

import (
	"encoding/binary"
	"fmt"
	"net"

	"golang.org/x/sys/unix"
)

// Request codes (docs/interop/vhost-user.rst, "Message types").
const (
	RequestGetFeatures         = 1
	RequestSetFeatures         = 2
	RequestSetOwner            = 3
	RequestResetOwner          = 4
	RequestSetMemTable         = 5
	RequestSetLogBase          = 6
	RequestSetLogFD            = 7
	RequestSetVringNum         = 8
	RequestSetVringAddr        = 9
	RequestSetVringBase        = 10
	RequestGetVringBase        = 11
	RequestSetVringKick        = 12
	RequestSetVringCall        = 13
	RequestSetVringErr         = 14
	RequestGetProtocolFeatures = 15
	RequestSetProtocolFeatures = 16
	RequestGetQueueNum         = 17
	RequestSetVringEnable      = 18
	RequestGetConfig           = 24
	RequestSetConfig           = 25
)

// Header flags.
const (
	flagVersion1  = 0x1
	FlagReply     = 0x4
	FlagNeedReply = 0x8
)

// Feature bits this backend cares about.
const (
	FeatureProtocolFeaturesBit = 30 // VHOST_USER_F_PROTOCOL_FEATURES
	ProtocolFeatureMQ          = 0  // VHOST_USER_PROTOCOL_F_MQ
	ProtocolFeatureConfig      = 9  // VHOST_USER_PROTOCOL_F_CONFIG
)

const messageHeaderSizeBytes = 12

// maxInlinePayloadBytes bounds a sane control message; SET_MEM_TABLE with
// 8 regions is well under this.
const maxInlinePayloadBytes = 4096

// maxAncillaryFileDescriptors matches VHOST_MEMORY_BASELINE_NREGIONS.
const maxAncillaryFileDescriptors = 8

// Message is one decoded vhost-user control message.
type Message struct {
	Request         uint32
	Flags           uint32
	Payload         []byte
	FileDescriptors []int
}

// ReadMessageWithFileDescriptors reads one control message plus any
// SCM_RIGHTS file descriptors from a unix socket.
func ReadMessageWithFileDescriptors(connection *net.UnixConn) (*Message, error) {
	headerBuffer := make([]byte, messageHeaderSizeBytes)
	ancillaryBuffer := make([]byte, unix.CmsgSpace(4*maxAncillaryFileDescriptors))

	headerBytesRead, ancillaryBytesRead, _, _, err := connection.ReadMsgUnix(headerBuffer, ancillaryBuffer)
	if err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	if headerBytesRead != messageHeaderSizeBytes {
		return nil, fmt.Errorf("short header: %d bytes", headerBytesRead)
	}

	message := &Message{
		Request: binary.LittleEndian.Uint32(headerBuffer[0:4]),
		Flags:   binary.LittleEndian.Uint32(headerBuffer[4:8]),
	}
	payloadSize := binary.LittleEndian.Uint32(headerBuffer[8:12])
	if payloadSize > maxInlinePayloadBytes {
		return nil, fmt.Errorf("payload size %d exceeds limit", payloadSize)
	}
	if payloadSize > 0 {
		message.Payload = make([]byte, payloadSize)
		if _, err := readFull(connection, message.Payload); err != nil {
			return nil, fmt.Errorf("read payload: %w", err)
		}
	}

	if ancillaryBytesRead > 0 {
		controlMessages, err := unix.ParseSocketControlMessage(ancillaryBuffer[:ancillaryBytesRead])
		if err != nil {
			return nil, fmt.Errorf("parse ancillary: %w", err)
		}
		for _, controlMessage := range controlMessages {
			fileDescriptors, err := unix.ParseUnixRights(&controlMessage)
			if err != nil {
				continue // not SCM_RIGHTS
			}
			message.FileDescriptors = append(message.FileDescriptors, fileDescriptors...)
		}
	}
	return message, nil
}

// WriteReply sends a reply message (no file descriptors) for the given
// request code.
func WriteReply(connection *net.UnixConn, request uint32, payload []byte) error {
	header := make([]byte, messageHeaderSizeBytes, messageHeaderSizeBytes+len(payload))
	binary.LittleEndian.PutUint32(header[0:4], request)
	binary.LittleEndian.PutUint32(header[4:8], flagVersion1|FlagReply)
	binary.LittleEndian.PutUint32(header[8:12], uint32(len(payload)))
	_, err := connection.Write(append(header, payload...))
	return err
}

// U64Payload encodes a single little-endian u64, the payload shape of the
// feature and vring-base messages.
func U64Payload(value uint64) []byte {
	payload := make([]byte, 8)
	binary.LittleEndian.PutUint64(payload, value)
	return payload
}

// DecodeU64Payload is the inverse of U64Payload.
func DecodeU64Payload(payload []byte) (uint64, error) {
	if len(payload) < 8 {
		return 0, fmt.Errorf("u64 payload too short: %d bytes", len(payload))
	}
	return binary.LittleEndian.Uint64(payload), nil
}

// MemoryRegion is one entry of a SET_MEM_TABLE message: a span of guest
// physical addresses backed by an mmap-able file descriptor.
type MemoryRegion struct {
	GuestPhysicalAddress uint64
	SizeBytes            uint64
	UserspaceAddress     uint64 // frontend's address; used only for vring translation quirks
	MmapOffsetBytes      uint64

	// MappedLocalBytes is filled by the backend after mmap: the region's
	// content addressed locally.
	MappedLocalBytes []byte
}

// DecodeMemoryTablePayload parses a SET_MEM_TABLE payload:
// numRegions u32 | padding u32 | numRegions * (gpa u64, size u64,
// userAddr u64, mmapOffset u64).
func DecodeMemoryTablePayload(payload []byte) ([]MemoryRegion, error) {
	if len(payload) < 8 {
		return nil, fmt.Errorf("mem-table payload too short: %d bytes", len(payload))
	}
	regionCount := binary.LittleEndian.Uint32(payload[0:4])
	if regionCount == 0 || regionCount > maxAncillaryFileDescriptors {
		return nil, fmt.Errorf("unreasonable region count %d", regionCount)
	}
	const regionEncodedSize = 32
	needed := 8 + int(regionCount)*regionEncodedSize
	if len(payload) < needed {
		return nil, fmt.Errorf("mem-table payload %d bytes, need %d for %d regions",
			len(payload), needed, regionCount)
	}
	regions := make([]MemoryRegion, regionCount)
	for i := range regions {
		base := 8 + i*regionEncodedSize
		regions[i] = MemoryRegion{
			GuestPhysicalAddress: binary.LittleEndian.Uint64(payload[base : base+8]),
			SizeBytes:            binary.LittleEndian.Uint64(payload[base+8 : base+16]),
			UserspaceAddress:     binary.LittleEndian.Uint64(payload[base+16 : base+24]),
			MmapOffsetBytes:      binary.LittleEndian.Uint64(payload[base+24 : base+32]),
		}
	}
	return regions, nil
}

// GuestMemory translates guest-physical spans to locally mapped bytes.
type GuestMemory struct {
	Regions []MemoryRegion
}

// Slice returns the local bytes for [guestPhysicalAddress,
// guestPhysicalAddress+length). Spans crossing region boundaries are
// rejected (QEMU does not produce them for virtqueue structures).
func (gm *GuestMemory) Slice(guestPhysicalAddress uint64, length uint64) ([]byte, error) {
	for i := range gm.Regions {
		region := &gm.Regions[i]
		if guestPhysicalAddress >= region.GuestPhysicalAddress &&
			guestPhysicalAddress+length <= region.GuestPhysicalAddress+region.SizeBytes {
			offset := guestPhysicalAddress - region.GuestPhysicalAddress
			if region.MappedLocalBytes == nil {
				return nil, fmt.Errorf("region %d not mapped", i)
			}
			return region.MappedLocalBytes[offset : offset+length], nil
		}
	}
	return nil, fmt.Errorf("guest address 0x%x+%d not in any region", guestPhysicalAddress, length)
}

func readFull(connection *net.UnixConn, buffer []byte) (int, error) {
	total := 0
	for total < len(buffer) {
		n, err := connection.Read(buffer[total:])
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}
