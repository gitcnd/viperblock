package viperblock

// Promotion support for fork F2 (synchronous peer WAL replication): after
// a primary node is lost, the replica's WAL files hold the only durable
// copy of flushed-but-unconsolidated writes. InstallRecoveryWALFiles
// places them where the normal production open sequence's
// RecoverLocalWALs will replay them, turning a fresh VB over the same
// backend into the promoted serving copy.

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// InstallRecoveryWALFiles copies the given WAL files (typically from
// walrepl.ListReplicaWALFiles, oldest first) into the recovery location
// for volumeName under baseDirectoryPath, and returns how many files were
// installed. The target WAL directory must be EMPTY: promotion targets a
// fresh node directory, and refusing a non-empty target prevents mixing
// replica files into a live volume's WAL by accident. Replay order across
// files does not affect correctness (recovery deduplicates by highest
// SeqNum per block), but names preserve the given order for auditability.
func InstallRecoveryWALFiles(sourceWALFilePaths []string, baseDirectoryPath, volumeName string) (int, error) {
	targetWALDirectoryPath := filepath.Join(baseDirectoryPath, volumeName, "wal", "chunks")
	if err := os.MkdirAll(targetWALDirectoryPath, 0o750); err != nil {
		return 0, fmt.Errorf("create recovery WAL directory: %w", err)
	}
	existingEntries, err := os.ReadDir(targetWALDirectoryPath)
	if err != nil {
		return 0, err
	}
	if len(existingEntries) > 0 {
		return 0, fmt.Errorf("recovery WAL directory %s is not empty (%d entries): promotion requires a fresh volume directory", targetWALDirectoryPath, len(existingEntries))
	}

	installed := 0
	for index, sourcePath := range sourceWALFilePaths {
		targetPath := filepath.Join(targetWALDirectoryPath,
			fmt.Sprintf("wal.recovered.%08d.bin", index))
		if err := copyFileDurably(sourcePath, targetPath); err != nil {
			return installed, fmt.Errorf("install %s: %w", sourcePath, err)
		}
		installed++
	}
	return installed, nil
}

// copyFileDurably copies source to an exclusive new target and fsyncs it:
// a promotion that survives a crash mid-install must never leave a
// half-copied WAL file that recovery would then read as torn.
func copyFileDurably(sourcePath, targetPath string) error {
	source, err := os.Open(sourcePath)
	if err != nil {
		return err
	}
	defer source.Close()
	target, err := os.OpenFile(targetPath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o640)
	if err != nil {
		return err
	}
	if _, err := io.Copy(target, source); err != nil {
		target.Close()
		return err
	}
	if err := target.Sync(); err != nil {
		target.Close()
		return err
	}
	return target.Close()
}
