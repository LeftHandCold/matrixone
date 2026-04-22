// Copyright 2025 Matrix Origin
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

package native

import (
	"context"
	"sort"
	"strings"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

const (
	CurrentSegmentVersion uint32 = 4
)

const (
	SidecarFlagLocatorWritten uint16 = 1 << iota
	SidecarFlagReplayed
)

type PublishedSidecar struct {
	IndexTable     string
	SidecarPath    string
	LocatorPath    string
	SegmentVersion uint32
	DocCount       int64
	Flags          uint16
}

type ObjectSidecarSet struct {
	TableID    uint64
	ObjectName string
	Entries    map[string]PublishedSidecar
}

type runtimeSidecarRegistry struct {
	sync.RWMutex
	objects map[string]ObjectSidecarSet
}

var globalSidecarRegistry = &runtimeSidecarRegistry{
	objects: make(map[string]ObjectSidecarSet),
}

type missingSidecarCache struct {
	sync.RWMutex
	locators map[string]struct{}
	sidecars map[string]struct{}
}

var globalMissingSidecarCache = &missingSidecarCache{
	locators: make(map[string]struct{}),
	sidecars: make(map[string]struct{}),
}

func PublishRuntimeSidecars(tableID uint64, objectName string, entries []PublishedSidecar) {
	normalized := normalizePublishedSidecars(entries)
	if objectName == "" || len(normalized) == 0 {
		return
	}
	globalSidecarRegistry.Lock()
	defer globalSidecarRegistry.Unlock()
	entryMap := make(map[string]PublishedSidecar, len(normalized))
	for _, entry := range normalized {
		entryMap[entry.IndexTable] = entry
	}
	globalSidecarRegistry.objects[objectName] = ObjectSidecarSet{
		TableID:    tableID,
		ObjectName: objectName,
		Entries:    entryMap,
	}
	clearMissingLocator(objectName)
	for _, entry := range normalized {
		clearMissingSidecar(entry.SidecarPath)
	}
}

func LookupRuntimeSidecars(objectName string) (ObjectSidecarSet, bool) {
	globalSidecarRegistry.RLock()
	defer globalSidecarRegistry.RUnlock()
	set, ok := globalSidecarRegistry.objects[objectName]
	if !ok {
		return ObjectSidecarSet{}, false
	}
	return cloneObjectSidecarSet(set), true
}

func RemoveRuntimeSidecars(objectNames ...string) {
	if len(objectNames) == 0 {
		return
	}
	globalSidecarRegistry.Lock()
	defer globalSidecarRegistry.Unlock()
	for _, objectName := range objectNames {
		delete(globalSidecarRegistry.objects, objectName)
		clearMissingLocator(objectName)
	}
}

func ResetRuntimeSidecarRegistry() {
	globalSidecarRegistry.Lock()
	defer globalSidecarRegistry.Unlock()
	globalSidecarRegistry.objects = make(map[string]ObjectSidecarSet)
	resetMissingSidecarCache()
}

func ExpandDeletePathsWithSidecars(
	ctx context.Context,
	fs fileservice.FileService,
	objectPaths []string,
) []string {
	out := make([]string, 0, len(objectPaths))
	seen := make(map[string]struct{}, len(objectPaths))
	appendUnique := func(path string) {
		if path == "" {
			return
		}
		if _, ok := seen[path]; ok {
			return
		}
		seen[path] = struct{}{}
		out = append(out, path)
	}

	appendIfPresent := func(kind, objectPath, filePath string) {
		if filePath == "" {
			return
		}
		appendUnique(filePath)
		if kind == "locator" {
			return
		}
		if hasMissingSidecar(filePath) {
			logutil.Debug(
				"[NATIVE-FTS-REGISTRY-SKIP-MISSING]",
				zap.String("kind", kind),
				zap.String("object", objectPath),
				zap.String("path", filePath),
			)
		}
	}

	for _, objectPath := range objectPaths {
		appendUnique(objectPath)
		set, ok := LookupRuntimeSidecars(objectPath)
		if !ok {
			for _, expanded := range ExpandDeletePathsWithLocators(ctx, fs, []string{objectPath}) {
				appendUnique(expanded)
			}
			continue
		}
		entries := sortedPublishedSidecars(set)
		for _, entry := range entries {
			appendIfPresent("locator", objectPath, entry.LocatorPath)
			appendIfPresent("sidecar", objectPath, entry.SidecarPath)
		}
	}
	return out
}

func cloneObjectSidecarSet(in ObjectSidecarSet) ObjectSidecarSet {
	out := ObjectSidecarSet{
		TableID:    in.TableID,
		ObjectName: in.ObjectName,
	}
	if len(in.Entries) == 0 {
		return out
	}
	out.Entries = make(map[string]PublishedSidecar, len(in.Entries))
	for key, entry := range in.Entries {
		out.Entries[key] = entry
	}
	return out
}

func normalizePublishedSidecars(entries []PublishedSidecar) []PublishedSidecar {
	if len(entries) == 0 {
		return nil
	}
	uniq := make(map[string]PublishedSidecar, len(entries))
	for _, entry := range entries {
		if entry.SidecarPath == "" || entry.IndexTable == "" {
			continue
		}
		if entry.SegmentVersion == 0 {
			entry.SegmentVersion = CurrentSegmentVersion
		}
		if entry.LocatorPath == "" {
			entry.LocatorPath = SidecarLocatorPath(objectPathFromSidecar(entry.SidecarPath))
		}
		uniq[entry.IndexTable] = entry
	}
	if len(uniq) == 0 {
		return nil
	}
	keys := make([]string, 0, len(uniq))
	for key := range uniq {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]PublishedSidecar, 0, len(keys))
	for _, key := range keys {
		out = append(out, uniq[key])
	}
	return out
}

func sortedPublishedSidecars(set ObjectSidecarSet) []PublishedSidecar {
	if len(set.Entries) == 0 {
		return nil
	}
	keys := make([]string, 0, len(set.Entries))
	for key := range set.Entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]PublishedSidecar, 0, len(keys))
	for _, key := range keys {
		out = append(out, set.Entries[key])
	}
	return out
}

func objectPathFromSidecar(sidecarPath string) string {
	if idx := strings.LastIndex(sidecarPath, ".fts."); idx > 0 {
		return sidecarPath[:idx]
	}
	return sidecarPath
}

func hasMissingLocator(objectPath string) bool {
	globalMissingSidecarCache.RLock()
	defer globalMissingSidecarCache.RUnlock()
	_, ok := globalMissingSidecarCache.locators[objectPath]
	return ok
}

func markMissingLocator(objectPath string) {
	if objectPath == "" {
		return
	}
	globalMissingSidecarCache.Lock()
	defer globalMissingSidecarCache.Unlock()
	globalMissingSidecarCache.locators[objectPath] = struct{}{}
}

func clearMissingLocator(objectPath string) {
	if objectPath == "" {
		return
	}
	globalMissingSidecarCache.Lock()
	defer globalMissingSidecarCache.Unlock()
	delete(globalMissingSidecarCache.locators, objectPath)
}

func hasMissingSidecar(sidecarPath string) bool {
	globalMissingSidecarCache.RLock()
	defer globalMissingSidecarCache.RUnlock()
	_, ok := globalMissingSidecarCache.sidecars[sidecarPath]
	return ok
}

func markMissingSidecar(sidecarPath string) {
	if sidecarPath == "" {
		return
	}
	globalMissingSidecarCache.Lock()
	defer globalMissingSidecarCache.Unlock()
	globalMissingSidecarCache.sidecars[sidecarPath] = struct{}{}
}

func clearMissingSidecar(sidecarPath string) {
	if sidecarPath == "" {
		return
	}
	globalMissingSidecarCache.Lock()
	defer globalMissingSidecarCache.Unlock()
	delete(globalMissingSidecarCache.sidecars, sidecarPath)
}

func resetMissingSidecarCache() {
	globalMissingSidecarCache.Lock()
	defer globalMissingSidecarCache.Unlock()
	globalMissingSidecarCache.locators = make(map[string]struct{})
	globalMissingSidecarCache.sidecars = make(map[string]struct{})
}
