// Command vhost-user-blk-serve exposes a block engine as a vhost-user-blk
// device (spinifex fork F7). Engine selection:
//
//	--raw-file PATH   serve a plain file (QEMU protocol smoke tests)
//
// The viperblock-engine variant lands with the F7 integration slice; this
// command's first job is proving QEMU compatibility of the backend.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/mulgadc/viperblock/vhostuser"
)

// rawFileBlockEngine adapts a plain file to vhostuser.BlockEngine.
type rawFileBlockEngine struct{ file *os.File }

func (e *rawFileBlockEngine) ReadAt(offset uint64, length uint64) ([]byte, error) {
	buffer := make([]byte, length)
	_, err := e.file.ReadAt(buffer, int64(offset))
	return buffer, err
}

func (e *rawFileBlockEngine) WriteAt(offset uint64, data []byte) error {
	_, err := e.file.WriteAt(data, int64(offset))
	return err
}

func (e *rawFileBlockEngine) Flush() error { return e.file.Sync() }

func main() {
	socketPath := flag.String("socket", "", "unix socket to serve vhost-user on (required)")
	rawFilePath := flag.String("raw-file", "", "raw file to serve (required in this slice)")
	flag.Parse()
	if *socketPath == "" || *rawFilePath == "" {
		fmt.Fprintln(os.Stderr, "--socket and --raw-file are required")
		os.Exit(4)
	}
	file, err := os.OpenFile(*rawFilePath, os.O_RDWR, 0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open raw file: %v\n", err)
		os.Exit(4)
	}
	info, err := file.Stat()
	if err != nil {
		fmt.Fprintf(os.Stderr, "stat raw file: %v\n", err)
		os.Exit(4)
	}

	backend := &vhostuser.Backend{
		Engine:        &rawFileBlockEngine{file: file},
		CapacityBytes: uint64(info.Size()),
	}
	fmt.Printf("vhost-user-blk-serve: %s (%d bytes) on %s\n", *rawFilePath, info.Size(), *socketPath)
	for {
		if err := backend.ServeOneConnection(*socketPath); err != nil {
			fmt.Fprintf(os.Stderr, "connection ended: %v\n", err)
		}
	}
}
