// Command viperblock-nbd-server is the fork-F7 candidate (a) prototype:
// serve a viperblock volume over the NBD protocol DIRECTLY from Go -- no
// nbdkit, no C shim, no extra process. Gate P-1.9 measures it with the
// same qemu-img bench matrix as the nbdkit path (P-1.5(ii)).
//
// Protocol: fixed-newstyle negotiation (NBD_OPT_GO and the legacy
// NBD_OPT_EXPORT_NAME), simple replies, commands READ/WRITE/FLUSH/DISC.
// Structured replies are refused (ERR_UNSUP); qemu falls back cleanly.
// Requests are read sequentially and dispatched to a worker pool so queue
// depth > 1 actually overlaps engine calls; replies are serialised.
//
// PROTOTYPE: single export, no TLS, no resize, TRIM unsupported. Not
// production code -- it exists to produce the F7 decision numbers.
package main

import (
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"sync"

	"github.com/mulgadc/viperblock/tests/crashharness"
	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock"
	"github.com/mulgadc/viperblock/viperblock/backends/s3"
)

// NBD protocol constants (https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md).
const (
	nbdMagicOldstyle       = 0x4e42444d41474943 // "NBDMAGIC"
	nbdMagicOptionRequest  = 0x49484156454F5054 // "IHAVEOPT"
	nbdMagicOptionReply    = 0x3e889045565a9
	nbdMagicRequest        = 0x25609513
	nbdMagicSimpleReply    = 0x67446698
	nbdHandshakeFlagFixedNewstyle = 1
	nbdHandshakeFlagNoZeroes      = 2

	nbdOptionExportName      = 1
	nbdOptionAbort           = 2
	nbdOptionList            = 3
	nbdOptionStructuredReply = 8
	nbdOptionGo              = 7
	nbdOptionInfo            = 6

	nbdReplyAck                = 1
	nbdReplyServer             = 2
	nbdReplyInfo               = 3
	nbdReplyErrorUnsupported   = 0x80000001
	nbdInfoExport              = 0

	nbdCommandRead  = 0
	nbdCommandWrite = 1
	nbdCommandDisc  = 2
	nbdCommandFlush = 3

	nbdTransmissionFlagHasFlags  = 1
	nbdTransmissionFlagSendFlush = 4

	nbdErrorEINVAL = 22
	nbdErrorEIO    = 5
)

func main() {
	socketPath := flag.String("socket", "", "unix socket path to listen on (required)")
	backendKind := flag.String("backend", "file", "viperblock backend: file | s3")
	harnessRootDirectoryPath := flag.String("dir", "", "for file backend: harness root dir (required with --backend file)")
	volumeName := flag.String("volume", "vol-goserver", "volume name")
	volumeSizeBytes := flag.Uint64("size", 1024*1024*1024, "volume size in bytes")
	bucketName := flag.String("bucket", "", "s3 backend: bucket")
	endpointHost := flag.String("host", "https://127.0.0.1:8443", "s3 backend: endpoint")
	walBaseDirectory := flag.String("base-dir", "", "s3 backend: local WAL base dir")
	flag.Parse()
	if *socketPath == "" {
		fmt.Fprintln(os.Stderr, "--socket is required")
		os.Exit(4)
	}

	var vb *viperblock.VB
	var err error
	switch *backendKind {
	case "file":
		if *harnessRootDirectoryPath == "" {
			fmt.Fprintln(os.Stderr, "--dir required for file backend")
			os.Exit(4)
		}
		vb, err = crashharness.OpenVolumeLikeProductionNBDPluginOpen(crashharness.VolumeHarnessConfig{
			HarnessRootDirectoryPath: *harnessRootDirectoryPath,
			VolumeName:               *volumeName,
			VolumeSizeBytes:          *volumeSizeBytes,
		})
	case "s3":
		if *bucketName == "" || *walBaseDirectory == "" {
			fmt.Fprintln(os.Stderr, "--bucket and --base-dir required for s3 backend")
			os.Exit(4)
		}
		vb, err = openS3VolumeLikeProductionNBDPluginOpen(*volumeName, *volumeSizeBytes, *bucketName, *endpointHost, *walBaseDirectory)
	default:
		fmt.Fprintf(os.Stderr, "unknown backend %q\n", *backendKind)
		os.Exit(4)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "open volume: %v\n", err)
		os.Exit(4)
	}

	_ = os.Remove(*socketPath)
	listener, err := net.Listen("unix", *socketPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "listen: %v\n", err)
		os.Exit(4)
	}
	fmt.Printf("viperblock-nbd-server: serving %s (%d bytes) on %s\n", *volumeName, *volumeSizeBytes, *socketPath)
	for {
		connection, err := listener.Accept()
		if err != nil {
			fmt.Fprintf(os.Stderr, "accept: %v\n", err)
			os.Exit(4)
		}
		go serveNBDConnection(connection, vb, *volumeSizeBytes)
	}
}

// openS3VolumeLikeProductionNBDPluginOpen mirrors nbd/viperblock.go Open()
// for the s3 backend (same sequence the crashharness helper uses for file).
func openS3VolumeLikeProductionNBDPluginOpen(volumeName string, volumeSizeBytes uint64, bucketName, endpointHost, walBaseDirectory string) (*viperblock.VB, error) {
	if err := os.MkdirAll(walBaseDirectory, 0o755); err != nil {
		return nil, err
	}
	backendConfig := s3.S3Config{
		VolumeName: volumeName, VolumeSize: volumeSizeBytes,
		Bucket: bucketName, Region: "ap-southeast-2",
		AccessKey: "AKIAIOSFODNN7EXAMPLE", SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		Host: endpointHost,
	}
	vbconfig := viperblock.VB{
		VolumeName: volumeName, VolumeSize: volumeSizeBytes, BaseDir: walBaseDirectory,
		Cache: viperblock.Cache{Config: viperblock.CacheConfig{Size: 64 * 1024 * 1024}},
	}
	vb, err := viperblock.New(&vbconfig, "s3", backendConfig)
	if err != nil {
		return nil, err
	}
	vb.UseShardedWAL = false
	vb.ShardedWAL = nil
	if err := vb.Backend.Init(); err != nil {
		return nil, err
	}
	if err := vb.LoadState(); err != nil {
		if saveErr := vb.SaveState(); saveErr != nil {
			return nil, fmt.Errorf("initial SaveState: %w (LoadState: %v)", saveErr, err)
		}
		if err := vb.LoadState(); err != nil {
			return nil, err
		}
	}
	if err := vb.EnsureVolumeUUID(); err != nil {
		return nil, err
	}
	if err := vb.LoadLiveCheckpoint(); err != nil {
		return nil, err
	}
	if err := vb.RecoverLocalWALs(); err != nil {
		return nil, err
	}
	vb.WAL.WallNum.Add(1)
	if err := vb.OpenWAL(&vb.WAL, fmt.Sprintf("%s/%s", vb.WAL.BaseDir,
		types.GetFilePath(types.FileTypeWALChunk, vb.WAL.WallNum.Load(), vb.GetVolume()))); err != nil {
		return nil, err
	}
	if err := vb.OpenWAL(&vb.BlockToObjectWAL, fmt.Sprintf("%s/%s", vb.WAL.BaseDir,
		types.GetFilePath(types.FileTypeWALBlock, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume()))); err != nil {
		return nil, err
	}
	return vb, nil
}

// ---------------------------------------------------------- negotiation --

func serveNBDConnection(connection net.Conn, vb *viperblock.VB, exportSizeBytes uint64) {
	defer connection.Close()

	// Fixed-newstyle greeting.
	greeting := make([]byte, 18)
	binary.BigEndian.PutUint64(greeting[0:8], nbdMagicOldstyle)
	binary.BigEndian.PutUint64(greeting[8:16], nbdMagicOptionRequest)
	binary.BigEndian.PutUint16(greeting[16:18], nbdHandshakeFlagFixedNewstyle|nbdHandshakeFlagNoZeroes)
	if _, err := connection.Write(greeting); err != nil {
		return
	}
	clientFlagsBuffer := make([]byte, 4)
	if _, err := io.ReadFull(connection, clientFlagsBuffer); err != nil {
		return
	}
	clientHonoursNoZeroes := binary.BigEndian.Uint32(clientFlagsBuffer)&nbdHandshakeFlagNoZeroes != 0

	transmissionFlags := uint16(nbdTransmissionFlagHasFlags | nbdTransmissionFlagSendFlush)

	// Option haggling.
	for {
		optionHeader := make([]byte, 16)
		if _, err := io.ReadFull(connection, optionHeader); err != nil {
			return
		}
		if binary.BigEndian.Uint64(optionHeader[0:8]) != nbdMagicOptionRequest {
			return
		}
		option := binary.BigEndian.Uint32(optionHeader[8:12])
		optionDataLength := binary.BigEndian.Uint32(optionHeader[12:16])
		optionData := make([]byte, optionDataLength)
		if _, err := io.ReadFull(connection, optionData); err != nil {
			return
		}

		switch option {
		case nbdOptionGo, nbdOptionInfo:
			// Reply NBD_REP_INFO (export info) then NBD_REP_ACK.
			infoPayload := make([]byte, 2+8+2)
			binary.BigEndian.PutUint16(infoPayload[0:2], nbdInfoExport)
			binary.BigEndian.PutUint64(infoPayload[2:10], exportSizeBytes)
			binary.BigEndian.PutUint16(infoPayload[10:12], transmissionFlags)
			if err := writeOptionReply(connection, option, nbdReplyInfo, infoPayload); err != nil {
				return
			}
			if err := writeOptionReply(connection, option, nbdReplyAck, nil); err != nil {
				return
			}
			if option == nbdOptionGo {
				serveNBDTransmission(connection, vb)
				return
			}
		case nbdOptionExportName:
			// Legacy path: size + flags (+ 124 zero bytes unless NO_ZEROES).
			response := make([]byte, 10, 134)
			binary.BigEndian.PutUint64(response[0:8], exportSizeBytes)
			binary.BigEndian.PutUint16(response[8:10], transmissionFlags)
			if !clientHonoursNoZeroes {
				response = append(response, make([]byte, 124)...)
			}
			if _, err := connection.Write(response); err != nil {
				return
			}
			serveNBDTransmission(connection, vb)
			return
		case nbdOptionAbort:
			_ = writeOptionReply(connection, option, nbdReplyAck, nil)
			return
		default:
			// Includes NBD_OPT_STRUCTURED_REPLY and NBD_OPT_LIST: refuse;
			// clients fall back to simple replies / GO.
			if err := writeOptionReply(connection, option, nbdReplyErrorUnsupported, nil); err != nil {
				return
			}
		}
	}
}

func writeOptionReply(connection net.Conn, option uint32, replyType uint32, payload []byte) error {
	header := make([]byte, 20)
	binary.BigEndian.PutUint64(header[0:8], nbdMagicOptionReply)
	binary.BigEndian.PutUint32(header[8:12], option)
	binary.BigEndian.PutUint32(header[12:16], replyType)
	binary.BigEndian.PutUint32(header[16:20], uint32(len(payload)))
	if _, err := connection.Write(header); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := connection.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

// ---------------------------------------------------------- transmission --

type nbdInFlightRequest struct {
	commandType uint16
	handle      uint64
	offset      uint64
	length      uint32
	writeData   []byte
}

func serveNBDTransmission(connection net.Conn, vb *viperblock.VB) {
	// Reader dispatches to a bounded worker pool; replies serialised by a
	// mutex so queue depth > 1 overlaps ENGINE work, not socket writes.
	var replyMutex sync.Mutex
	requestChannel := make(chan nbdInFlightRequest, 64)
	var workerWaitGroup sync.WaitGroup
	const workerCount = 16

	writeSimpleReply := func(handle uint64, errorCode uint32, payload []byte) {
		replyMutex.Lock()
		defer replyMutex.Unlock()
		header := make([]byte, 16)
		binary.BigEndian.PutUint32(header[0:4], nbdMagicSimpleReply)
		binary.BigEndian.PutUint32(header[4:8], errorCode)
		binary.BigEndian.PutUint64(header[8:16], handle)
		if _, err := connection.Write(header); err != nil {
			return
		}
		if payload != nil {
			_, _ = connection.Write(payload)
		}
	}

	for workerIndex := 0; workerIndex < workerCount; workerIndex++ {
		workerWaitGroup.Add(1)
		go func() {
			defer workerWaitGroup.Done()
			for request := range requestChannel {
				switch request.commandType {
				case nbdCommandRead:
					data, err := vb.ReadAt(request.offset, uint64(request.length))
					if errors.Is(err, viperblock.ErrZeroBlock) {
						// Never-written region: NBD semantics = zeroes.
						data, err = make([]byte, request.length), nil
					}
					if err != nil {
						writeSimpleReply(request.handle, nbdErrorEIO, nil)
						continue
					}
					writeSimpleReply(request.handle, 0, data)
				case nbdCommandWrite:
					if err := vb.WriteAt(request.offset, request.writeData); err != nil {
						writeSimpleReply(request.handle, nbdErrorEIO, nil)
						continue
					}
					writeSimpleReply(request.handle, 0, nil)
				case nbdCommandFlush:
					if err := vb.Flush(); err != nil {
						writeSimpleReply(request.handle, nbdErrorEIO, nil)
						continue
					}
					writeSimpleReply(request.handle, 0, nil)
				default:
					writeSimpleReply(request.handle, nbdErrorEINVAL, nil)
				}
			}
		}()
	}

	requestHeader := make([]byte, 28)
	for {
		if _, err := io.ReadFull(connection, requestHeader); err != nil {
			break
		}
		if binary.BigEndian.Uint32(requestHeader[0:4]) != nbdMagicRequest {
			break
		}
		request := nbdInFlightRequest{
			commandType: binary.BigEndian.Uint16(requestHeader[6:8]),
			handle:      binary.BigEndian.Uint64(requestHeader[8:16]),
			offset:      binary.BigEndian.Uint64(requestHeader[16:24]),
			length:      binary.BigEndian.Uint32(requestHeader[24:28]),
		}
		if request.commandType == nbdCommandWrite {
			request.writeData = make([]byte, request.length)
			if _, err := io.ReadFull(connection, request.writeData); err != nil {
				break
			}
		}
		if request.commandType == nbdCommandDisc {
			break
		}
		requestChannel <- request
	}
	close(requestChannel)
	workerWaitGroup.Wait()
}
