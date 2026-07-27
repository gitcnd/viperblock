// Command createvol-s3 initialises a viperblock volume's state on an S3
// (predastore) backend, mirroring what spinifex CreateVolume does
// (New + Backend.Init + SaveState), so the nbdkit plugin -- whose Open
// requires LoadState to succeed -- can serve it. Part of the F7 data-path
// measurement rig (gates P-1.5(ii)/P-1.9).
//
// The self-signed dev-cluster cert is trusted via SSL_CERT_FILE (Go treats
// it as the system root pool); this program adds no TLS knobs of its own.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/mulgadc/viperblock/viperblock"
	"github.com/mulgadc/viperblock/viperblock/backends/s3"
)

func main() {
	volumeName := flag.String("volume", "", "volume name (required)")
	volumeSizeBytes := flag.Uint64("size", 1024*1024*1024, "volume size in bytes")
	bucketName := flag.String("bucket", "", "predastore bucket (required, must exist)")
	endpointHost := flag.String("host", "https://127.0.0.1:8443", "predastore endpoint")
	region := flag.String("region", "ap-southeast-2", "region")
	accessKeyID := flag.String("access-key", "AKIAIOSFODNN7EXAMPLE", "access key")
	secretAccessKey := flag.String("secret-key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "secret key")
	walBaseDirectory := flag.String("base-dir", "", "local WAL base directory (required)")
	flag.Parse()
	if *volumeName == "" || *bucketName == "" || *walBaseDirectory == "" {
		fmt.Fprintln(os.Stderr, "--volume, --bucket and --base-dir are required")
		os.Exit(4)
	}
	if err := os.MkdirAll(*walBaseDirectory, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir base dir: %v\n", err)
		os.Exit(4)
	}

	backendConfig := s3.S3Config{
		VolumeName: *volumeName,
		VolumeSize: *volumeSizeBytes,
		Bucket:     *bucketName,
		Region:     *region,
		AccessKey:  *accessKeyID,
		SecretKey:  *secretAccessKey,
		Host:       *endpointHost,
	}
	vbconfig := viperblock.VB{
		VolumeName: *volumeName,
		VolumeSize: *volumeSizeBytes,
		BaseDir:    *walBaseDirectory,
	}
	vb, err := viperblock.New(&vbconfig, "s3", backendConfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "viperblock.New: %v\n", err)
		os.Exit(4)
	}
	if err := vb.Backend.Init(); err != nil {
		fmt.Fprintf(os.Stderr, "backend init: %v\n", err)
		os.Exit(4)
	}
	if err := vb.SaveState(); err != nil {
		fmt.Fprintf(os.Stderr, "SaveState: %v\n", err)
		os.Exit(4)
	}
	fmt.Printf("volume %s (%d bytes) initialised on %s/%s\n", *volumeName, *volumeSizeBytes, *endpointHost, *bucketName)
}
