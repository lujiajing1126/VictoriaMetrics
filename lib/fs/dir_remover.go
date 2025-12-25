package fs

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// directories with this filename are scheduled to be removed by MustRemoveDir().
const deleteDirFilename = ".delete-this-dir"

// MustRemoveDir removes the dirPath with all its contents.
//
// The dirPath contents may be partially deleted if unclean shutdown happens during the removal.
// The caller must verify whether the given directory is partially removed via IsPartiallyRemovedDir() call
// on the startup before using it. If the directory is partially removed, it must be removed again
// via MustRemoveDir() call.
func MustRemoveDir(dirPath string) {
	if !IsPathExist(dirPath) {
		// Nothing do delete.
		return
	}

	// The code below is written in the way that partially deleted directories could be deleted
	// on the next start after unclean shutdown, by verifying them with IsPartiallyRemovedDir() call.
	//
	// The code below doesn't depend on atomic renaming of directories, since it isn't supported
	// by NFS and object storage.

	// Create a deleteDirFilename file, which indicates that the dirPath must be removed.
	deleteFilePath := filepath.Join(dirPath, deleteDirFilename)
	f, err := os.Create(deleteFilePath)
	if err != nil {
		logger.Panicf("FATAL: cannot create %q while deleting %q: %s", deleteFilePath, dirPath, err)
	}
	if err := f.Close(); err != nil {
		logger.Panicf("FATAL: cannot close %q: %s", deleteFilePath, err)
	}

	// Make sure the deleteDirFilename file is visible in the dirPath.
	MustSyncPath(dirPath)

	// Remove the contents of the dirPath except of the deleteDirFilename file.
	//
	// Make this in parallel in order to reduce the time needed for the removal of big number of items
	// on high-latency storage systems such as NFS.
	// Directories for VitoriaLogs parts may contain big number of items when wide events are stored there.
	// Also the number of parts in a partition may be quite big.
	des := MustReadDir(dirPath)
	var wg sync.WaitGroup
	concurrencyCh := make(chan struct{}, min(32, len(des)-1))
	for _, de := range des {
		name := de.Name()
		if name == deleteDirFilename {
			continue
		}
		dirEntryPath := filepath.Join(dirPath, name)

		concurrencyCh <- struct{}{}
		wg.Add(1)
		go func(dirEntryPath string) {
			defer func() {
				wg.Done()
				<-concurrencyCh
			}()
			if err := os.RemoveAll(dirEntryPath); err != nil {
				logger.Panicf("FATAL: cannot remove %q: %s", dirEntryPath, err)
			}
		}(dirEntryPath)
	}
	wg.Wait()

	// Make sure the deleted names are properly synced to the dirPath,
	// so they are no longer visible after unclean shutdown.
	MustSyncPath(dirPath)

	// Remove the deleteDirFilename file, since there are no other entries left in the directory.
	MustRemovePath(deleteFilePath)

	// Sync the directory after the removing deletDirFilename file in order to make sure
	// all the metadata files are removed at some exotic filesystems such as OSSFS2.
	// See https://github.com/VictoriaMetrics/VictoriaLogs/issues/649
	// and https://github.com/VictoriaMetrics/VictoriaMetrics/pull/9709
	MustSyncPath(dirPath)

	// Remove the dirPath itself with retry logic for NFS/NAS compatibility
	// On NFS, temporary .nfs* files may prevent immediate directory removal
	// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/9842
	mustRemovePathWithRetry(dirPath)

	// Do not sync the parent directory for the dirPath - the caller can do this if needed.
	// It is OK if the dirPath will remain undeleted after unclean shutdown - it will be deleted
	// on the next startup.
}

// IsPartiallyRemovedDir returns true if dirPath is partially removed because of unclean shutdown during the MustRemoveDir() call.
//
// The caller must call MustRemoveDir(dirPath) on partially removed dirPath.
func IsPartiallyRemovedDir(dirPath string) bool {
	des := MustReadDir(dirPath)
	if len(des) == 0 {
		// Delete empty dirs too, since they may appear when the unclean shutdown happens after the deleteDirFilename is deleted,
		// but before the directory is deleted itself.
		return true
	}

	deleteFilePath := filepath.Join(dirPath, deleteDirFilename)
	for _, de := range des {
		if de.IsDir() {
			continue
		}
		name := de.Name()
		if name == deleteFilePath {
			// The directory contains the deleteDirFilename. This means it is partially deleted.
			return true
		}
	}
	return false
}

// mustRemovePathWithRetry removes the given path with retry logic for NFS/NAS filesystems.
//
// On NFS, when files are deleted while still having open handles, the filesystem creates
// temporary .nfs* files which prevent immediate directory removal. This function retries
// the removal with exponential backoff to handle such cases gracefully.
//
// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/9842 for details.
func mustRemovePathWithRetry(path string) {
	const maxRetries = 10
	const initialDelay = 10 * time.Millisecond
	const maxDelay = 5 * time.Second

	delay := initialDelay
	var lastErr error
	startTime := time.Now()

	for attempt := 0; attempt < maxRetries; attempt++ {
		err := os.Remove(path)
		if err == nil {
			// Successfully removed
			if attempt > 0 {
				elapsed := time.Since(startTime)
				logger.Infof("successfully removed %q after %d retries in %v (initial attempt + %d retries)", path, attempt, elapsed, attempt)
			}
			return
		}

		lastErr = err

		// Check if this is a "directory not empty" error which is common on NFS
		if !isDirectoryNotEmptyError(err) {
			// Not a retryable error, panic immediately
			logger.Panicf("FATAL: cannot remove %q: %s", path, err)
		}

		// Check if the directory contains NFS temporary files
		nfsFiles := checkForNFSTempFiles(path)
		if len(nfsFiles) > 0 {
			logger.Warnf("cannot remove %q on attempt %d/%d: directory contains NFS temporary files: %v; will retry in %v",
				path, attempt+1, maxRetries, nfsFiles, delay)
		} else {
			// Directory not empty but no .nfs* files found - check what's there
			entries := listDirEntries(path)
			logger.Warnf("cannot remove %q on attempt %d/%d: directory not empty, contains: %v; will retry in %v",
				path, attempt+1, maxRetries, entries, delay)
		}

		// Wait before retrying
		time.Sleep(delay)

		// Exponential backoff with cap
		delay *= 2
		if delay > maxDelay {
			delay = maxDelay
		}
	}

	// All retries exhausted
	elapsed := time.Since(startTime)
	entries := listDirEntries(path)
	logger.Panicf("FATAL: cannot remove %q after %d attempts over %v: %s; directory still contains: %v",
		path, maxRetries, elapsed, lastErr, entries)
}

// isDirectoryNotEmptyError checks if the error indicates that a directory is not empty.
func isDirectoryNotEmptyError(err error) bool {
	if err == nil {
		return false
	}

	// Check for syscall.ENOTEMPTY or syscall.EEXIST
	var pathErr *os.PathError
	if errors.As(err, &pathErr) {
		if errors.Is(pathErr.Err, syscall.ENOTEMPTY) {
			return true
		}
		// Some NFS implementations return EEXIST instead of ENOTEMPTY
		// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/6398
		if errors.Is(pathErr.Err, syscall.EEXIST) {
			return true
		}
	}

	// Also check the error string as a fallback
	errStr := err.Error()
	return strings.Contains(errStr, "directory not empty") || strings.Contains(errStr, "not empty")
}

// checkForNFSTempFiles checks if the directory contains NFS temporary files (.nfs*).
func checkForNFSTempFiles(dirPath string) []string {
	des, err := os.ReadDir(dirPath)
	if err != nil {
		// If we can't read the directory, return empty slice
		return nil
	}

	var nfsFiles []string
	for _, de := range des {
		name := de.Name()
		if strings.HasPrefix(name, ".nfs") {
			nfsFiles = append(nfsFiles, name)
		}
	}
	return nfsFiles
}

// listDirEntries returns a list of directory entries for diagnostic purposes.
func listDirEntries(dirPath string) []string {
	des, err := os.ReadDir(dirPath)
	if err != nil {
		return []string{fmt.Sprintf("<error reading directory: %v>", err)}
	}

	if len(des) == 0 {
		return []string{"<empty>"}
	}

	var entries []string
	for i, de := range des {
		if i >= 10 {
			entries = append(entries, fmt.Sprintf("... and %d more", len(des)-i))
			break
		}
		entry := de.Name()
		if de.IsDir() {
			entry += "/"
		}
		entries = append(entries, entry)
	}
	return entries
}

// MustRemovePath removes the given path. It must be either a file or an empty directory.
//
// Use MustRemoveDir for removing non-empty directories.
func MustRemovePath(path string) {
	if err := os.Remove(path); err != nil {
		logger.Panicf("FATAL: cannot remove %q: %s", path, err)
	}
}

// MustRemoveDirContents removes all the contents of the given dir if it exists.
//
// It doesn't remove the dir itself, so the dir may be mounted to a separate partition.
func MustRemoveDirContents(dir string) {
	if !IsPathExist(dir) {
		// The path doesn't exist, so nothing to remove.
		return
	}

	des := MustReadDir(dir)
	for _, de := range des {
		name := de.Name()
		fullPath := filepath.Join(dir, name)
		if err := os.RemoveAll(fullPath); err != nil {
			logger.Panicf("FATAL: cannot remove %s: %s", fullPath, err)
		}
	}
	MustSyncPath(dir)
}
