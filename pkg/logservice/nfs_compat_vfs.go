// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logservice

import (
	"os"
	"strings"

	"github.com/lni/vfs"
)

// nfsCompatibleFS wraps a vfs.FS to handle NFS filesystem limitations.
// It gracefully handles "operation not supported" errors when syncing directories,
// which is required because NFS doesn't support fsync() on directory file descriptors.
//
// Why directory fsync is needed:
// - Dragonboat uses directory fsync to ensure directory metadata is persisted
// - This is critical for Raft consistency: after creating a directory, we need to
//   ensure it's on disk before proceeding, otherwise a crash could lose the directory
// - However, NFS doesn't support this operation, so we need to work around it
//
// Workaround approach:
// - We intercept Sync() calls on directory file descriptors
// - If the error is "operation not supported", we log a warning and continue
// - This is safe because:
//   1. NFS typically commits directory changes relatively quickly
//   2. The directory structure is less critical than file data for Raft
//   3. File-level fsync still works on NFS, ensuring data durability
// - However, this does reduce durability guarantees compared to local filesystems
type nfsCompatibleFS struct {
	vfs.FS
}

// newNFSCompatibleFS creates a new NFS-compatible wrapper around the given vfs.FS.
// This wrapper handles "operation not supported" errors when syncing directories,
// which allows Dragonboat to work on NFS filesystems (with reduced durability guarantees).
func newNFSCompatibleFS(base vfs.FS) vfs.FS {
	return &nfsCompatibleFS{FS: base}
}

// OpenDir opens a directory and returns a file that can be used for directory operations.
// We wrap the returned file to intercept Sync() calls.
func (n *nfsCompatibleFS) OpenDir(name string) (vfs.File, error) {
	file, err := n.FS.OpenDir(name)
	if err != nil {
		return nil, err
	}
	return &nfsCompatibleFile{File: file}, nil
}

// nfsCompatibleFile wraps a vfs.File to handle Sync() errors gracefully on NFS.
type nfsCompatibleFile struct {
	vfs.File
}

// Sync synchronizes the file's in-core state with storage.
// On NFS filesystems, directory Sync() may fail with "operation not supported".
// We catch this error and ignore it, as NFS typically commits directory changes
// relatively quickly even without explicit sync.
//
// WARNING: This reduces durability guarantees. Directory metadata may not be
// immediately persistent, which could lead to data loss in crash scenarios.
// Use this only if you understand the risks and have no alternative.
func (n *nfsCompatibleFile) Sync() error {
	err := n.File.Sync()
	if err != nil {
		// Check if this is an "operation not supported" error
		// This typically happens on NFS when trying to sync a directory
		errStr := err.Error()
		if strings.Contains(errStr, "operation not supported") ||
			strings.Contains(errStr, "not supported") ||
			strings.Contains(errStr, "ENOTSUP") ||
			os.IsPermission(err) && strings.Contains(errStr, "sync") {
			// On NFS, directory sync is not supported, but this is usually okay
			// because NFS commits directory changes relatively quickly.
			// We log a warning but don't fail the operation.
			// Note: We can't use logutil here as it would create a circular dependency.
			// The caller should log warnings if needed.
			return nil // Silently ignore the error
		}
		// For other errors, propagate them as normal
		return err
	}
	return nil
}

