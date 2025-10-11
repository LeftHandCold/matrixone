// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backup

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

// ExampleBackupWithGCProtection demonstrates how to use the backup GC protection mechanism
func ExampleBackupWithGCProtection(ctx context.Context, sid string, backupTS types.TS) error {
	// Step 1: Create GC protection instance
	gcProtection := NewBackupGCProtection(ctx, sid)

	// Step 2: Start GC protection for the backup timestamp
	if err := gcProtection.StartProtection(ctx, backupTS); err != nil {
		logutil.Errorf("[Example] Failed to start GC protection: %v", err)
		return err
	}

	// Step 3: Ensure protection is stopped when backup completes
	defer func() {
		if gcProtection.IsActive() {
			if stopErr := gcProtection.StopProtection(context.Background()); stopErr != nil {
				logutil.Errorf("[Example] Failed to stop GC protection: %v", stopErr)
			}
		}
	}()

	// Step 4: Perform backup operations
	logutil.Infof("[Example] Starting backup with GC protection for timestamp %s", backupTS.ToString())

	// Simulate backup work
	time.Sleep(2 * time.Second)

	// Step 5: Check if protection is still active
	if gcProtection.IsActive() {
		logutil.Infof("[Example] GC protection is active, last heartbeat: %v",
			gcProtection.GetLastHeartbeat())
	}

	logutil.Infof("[Example] Backup completed successfully")
	return nil
}

//// ExampleGCProtectionUsage shows how the GC side uses the protection
//func ExampleGCProtectionUsage() {
//	// This would typically be called from the GC process
//
//	// Get the global protection manager
//	protectionManager := GetGlobalBackupProtectionManager()
//
//	// Example: Check if a timestamp is protected
//	testTS := types.BuildTS(time.Now().UnixNano(), 0)
//	if protectedTS, isProtected := protectionManager.GetProtectedTimestamp(); isProtected {
//		if testTS.LE(&protectedTS) {
//			logutil.Infof("[Example] Timestamp %s is protected by backup (protected: %s)",
//				testTS.ToString(), protectedTS.ToString())
//		}
//	}
//
//	// Example: Clean up expired protection
//	protectionManager.CleanupExpiredProtection()
//}
