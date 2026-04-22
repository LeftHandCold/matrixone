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

package rpc

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	ftnative "github.com/matrixorigin/matrixone/pkg/fulltext/native"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type ftsRepairMode string

const (
	ftsRepairModeAll      ftsRepairMode = "all"
	ftsRepairModeLocator  ftsRepairMode = "locator"
	ftsRepairModeRegistry ftsRepairMode = "registry"
)

type ftsArg struct{}

func (c *ftsArg) PrepareCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "fts",
		Short: "inspect native fulltext sidecar metadata",
	}
	show := &ftsShowArg{}
	repair := &ftsRepairArg{}
	reconcile := &ftsReconcileArg{}
	cmd.AddCommand(show.PrepareCommand())
	cmd.AddCommand(repair.PrepareCommand())
	cmd.AddCommand(reconcile.PrepareCommand())
	return cmd
}

type ftsBaseArg struct {
	ctx *inspectContext
	tbl *catalog.TableEntry
}

func (c *ftsBaseArg) fromCommand(cmd *cobra.Command) error {
	c.ctx = cmd.Flag("ictx").Value.(*inspectContext)
	target, _ := cmd.Flags().GetString("target")
	if strings.TrimSpace(target) == "" {
		return moerr.NewInvalidInputNoCtx("need table target")
	}
	tbl, err := parseTableTarget(target, c.ctx.acinfo, c.ctx.db)
	if err != nil {
		return err
	}
	if tbl == nil {
		return moerr.NewInvalidInputNoCtx("need table target")
	}
	c.tbl = tbl
	return nil
}

func (c *ftsBaseArg) fullTableName() string {
	if c.tbl == nil {
		return ""
	}
	return fmt.Sprintf("%s.%s", c.tbl.GetDB().GetName(), c.tbl.GetLastestSchema(false).Name)
}

type ftsShowArg struct {
	ftsBaseArg
}

func (c *ftsShowArg) PrepareCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "show",
		Short: "show native fulltext sidecar metadata anomalies",
		Run:   RunFactory(c),
	}
	cmd.Flags().StringP("target", "t", "", "format: db.table")
	_ = cmd.MarkFlagRequired("target")
	return cmd
}

func (c *ftsShowArg) FromCommand(cmd *cobra.Command) error { return c.fromCommand(cmd) }
func (c *ftsShowArg) String() string                       { return fmt.Sprintf("fts show: %s", c.fullTableName()) }

func (c *ftsShowArg) Run() error {
	report, err := scanFTSMetadata(context.Background(), c.ctx.db.Runtime.Fs, c.tbl)
	if err != nil {
		return err
	}
	c.ctx.resp.Payload = []byte(report.render())
	return nil
}

type ftsRepairArg struct {
	ftsBaseArg
	mode ftsRepairMode
}

func (c *ftsRepairArg) PrepareCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "repair",
		Short: "repair native fulltext locator or runtime registry metadata",
		Run:   RunFactory(c),
	}
	cmd.Flags().StringP("target", "t", "", "format: db.table")
	cmd.Flags().String("mode", string(ftsRepairModeAll), "repair mode: all|locator|registry")
	_ = cmd.MarkFlagRequired("target")
	return cmd
}

func (c *ftsRepairArg) FromCommand(cmd *cobra.Command) error {
	if err := c.fromCommand(cmd); err != nil {
		return err
	}
	mode, _ := cmd.Flags().GetString("mode")
	switch ftsRepairMode(strings.ToLower(mode)) {
	case ftsRepairModeAll, ftsRepairModeLocator, ftsRepairModeRegistry:
		c.mode = ftsRepairMode(strings.ToLower(mode))
	default:
		return moerr.NewInvalidInputNoCtx("mode must be one of all|locator|registry")
	}
	return nil
}

func (c *ftsRepairArg) String() string {
	return fmt.Sprintf("fts repair: %s mode=%s", c.fullTableName(), c.mode)
}

func (c *ftsRepairArg) Run() error {
	result, err := repairFTSMetadata(context.Background(), c.ctx.db.Runtime.Fs, c.tbl, c.mode)
	if err != nil {
		return err
	}
	c.ctx.resp.Payload = []byte(result.render())
	return nil
}

type ftsReconcileArg struct {
	ftsBaseArg
}

func (c *ftsReconcileArg) PrepareCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "reconcile",
		Short: "reconcile native fulltext locator and runtime registry metadata",
		Run:   RunFactory(c),
	}
	cmd.Flags().StringP("target", "t", "", "format: db.table")
	_ = cmd.MarkFlagRequired("target")
	return cmd
}

func (c *ftsReconcileArg) FromCommand(cmd *cobra.Command) error { return c.fromCommand(cmd) }
func (c *ftsReconcileArg) String() string {
	return fmt.Sprintf("fts reconcile: %s", c.fullTableName())
}

func (c *ftsReconcileArg) Run() error {
	result, err := repairFTSMetadata(context.Background(), c.ctx.db.Runtime.Fs, c.tbl, ftsRepairModeAll)
	if err != nil {
		return err
	}
	c.ctx.resp.Payload = []byte(result.render())
	return nil
}

type ftsScanReport struct {
	TableID         uint64
	TableName       string
	ExpectedIndexes []string
	Objects         []*ftsObjectState
}

type ftsObjectState struct {
	ObjectName string

	RegistryEntries map[string]ftnative.PublishedSidecar
	LocatorEntries  map[string]ftnative.SidecarLocatorEntry

	HasRegistry    bool
	HasLocatorFile bool
	LocatorCorrupt string

	MissingRegistry       []string
	MissingLocator        []string
	LocatorMismatch       []string
	RegistrySidecarMiss   []string
	LocatorSidecarMiss    []string
	UnexpectedRegistry    []string
	UnexpectedLocator     []string
	RepairableRegistry    bool
	RepairableLocator     bool
	RepairableRegistryIdx []string
}

type ftsRepairResult struct {
	TableName string
	Mode      ftsRepairMode

	ScannedObjects          int
	RegistryBackfilledObjs  int
	RegistryBackfilledIdxs  int
	LocatorRewrittenObjs    int
	LocatorRewrittenEntries int
	Skipped                 int
	Notes                   []string
	PostRepair              *ftsScanReport
}

func scanFTSMetadata(
	ctx context.Context,
	fs fileservice.FileService,
	tbl *catalog.TableEntry,
) (*ftsScanReport, error) {
	indexes, err := inspectableFTSIndexes(tbl)
	if err != nil {
		return nil, err
	}
	expectedSet := make(map[string]struct{}, len(indexes))
	for _, indexTable := range indexes {
		expectedSet[indexTable] = struct{}{}
	}

	report := &ftsScanReport{
		TableID:         tbl.ID,
		TableName:       fmt.Sprintf("%s.%s", tbl.GetDB().GetName(), tbl.GetLastestSchema(false).Name),
		ExpectedIndexes: indexes,
	}

	it := tbl.MakeDataVisibleObjectIt(txnbase.MockTxnReaderWithNow())
	defer it.Release()

	for it.Next() {
		obj := it.Item()
		if obj.ObjectState != catalog.ObjectState_Create_ApplyCommit {
			continue
		}
		if !obj.IsActive() || obj.IsAppendable() {
			continue
		}

		objectName := obj.ObjectStats.ObjectName().String()
		state := &ftsObjectState{
			ObjectName:      objectName,
			RegistryEntries: make(map[string]ftnative.PublishedSidecar),
			LocatorEntries:  make(map[string]ftnative.SidecarLocatorEntry),
		}

		if set, ok := ftnative.LookupRuntimeSidecars(objectName); ok {
			state.HasRegistry = true
			for indexTable, entry := range set.Entries {
				state.RegistryEntries[indexTable] = entry
				if _, ok := expectedSet[indexTable]; !ok {
					state.UnexpectedRegistry = append(state.UnexpectedRegistry, indexTable)
				}
				exists, err := statFileExists(ctx, fs, entry.SidecarPath)
				if err != nil {
					return nil, err
				}
				if !exists {
					state.RegistrySidecarMiss = append(state.RegistrySidecarMiss, indexTable)
				}
			}
		}

		locatorPath := ftnative.SidecarLocatorPath(objectName)
		locatorExists, err := statFileExists(ctx, fs, locatorPath)
		if err != nil {
			return nil, err
		}
		state.HasLocatorFile = locatorExists
		if locatorExists {
			locator, _, err := ftnative.ReadSidecarLocator(ctx, fs, objectName)
			if err != nil {
				state.LocatorCorrupt = err.Error()
			} else {
				for _, entry := range locator.Entries {
					if entry.IndexTable == "" {
						state.UnexpectedLocator = append(state.UnexpectedLocator, entry.FilePath)
						continue
					}
					state.LocatorEntries[entry.IndexTable] = entry
					if _, ok := expectedSet[entry.IndexTable]; !ok {
						state.UnexpectedLocator = append(state.UnexpectedLocator, entry.IndexTable)
					}
					exists, err := statFileExists(ctx, fs, entry.FilePath)
					if err != nil {
						return nil, err
					}
					if !exists {
						state.LocatorSidecarMiss = append(state.LocatorSidecarMiss, entry.IndexTable)
					}
				}
			}
		}

		for _, indexTable := range indexes {
			regEntry, regOK := state.RegistryEntries[indexTable]
			locEntry, locOK := state.LocatorEntries[indexTable]
			if !regOK {
				state.MissingRegistry = append(state.MissingRegistry, indexTable)
			}
			if !locOK {
				state.MissingLocator = append(state.MissingLocator, indexTable)
			}
			if regOK && locOK && regEntry.SidecarPath != locEntry.FilePath {
				state.LocatorMismatch = append(state.LocatorMismatch, indexTable)
			}
		}

		if state.HasRegistry && len(state.RegistryEntries) > 0 && len(state.RegistrySidecarMiss) == 0 {
			if !locatorEqualsRegistry(state.RegistryEntries, state.LocatorEntries) || state.LocatorCorrupt != "" || !state.HasLocatorFile {
				state.RepairableLocator = true
			}
		}

		if state.LocatorCorrupt == "" {
			for indexTable, entry := range state.LocatorEntries {
				if _, ok := state.RegistryEntries[indexTable]; ok {
					continue
				}
				if entry.IndexTable == "" {
					continue
				}
				exists, err := statFileExists(ctx, fs, entry.FilePath)
				if err != nil {
					return nil, err
				}
				if exists {
					state.RepairableRegistry = true
					state.RepairableRegistryIdx = append(state.RepairableRegistryIdx, indexTable)
				}
			}
		}

		sort.Strings(state.MissingRegistry)
		sort.Strings(state.MissingLocator)
		sort.Strings(state.LocatorMismatch)
		sort.Strings(state.RegistrySidecarMiss)
		sort.Strings(state.LocatorSidecarMiss)
		sort.Strings(state.UnexpectedRegistry)
		sort.Strings(state.UnexpectedLocator)
		sort.Strings(state.RepairableRegistryIdx)
		report.Objects = append(report.Objects, state)
	}

	sort.Slice(report.Objects, func(i, j int) bool {
		return report.Objects[i].ObjectName < report.Objects[j].ObjectName
	})
	return report, nil
}

func repairFTSMetadata(
	ctx context.Context,
	fs fileservice.FileService,
	tbl *catalog.TableEntry,
	mode ftsRepairMode,
) (*ftsRepairResult, error) {
	report, err := scanFTSMetadata(ctx, fs, tbl)
	if err != nil {
		return nil, err
	}
	result := &ftsRepairResult{
		TableName:      report.TableName,
		Mode:           mode,
		ScannedObjects: len(report.Objects),
	}

	if mode == ftsRepairModeAll || mode == ftsRepairModeRegistry {
		for _, state := range report.Objects {
			if !state.RepairableRegistry || state.LocatorCorrupt != "" {
				continue
			}
			merged := clonePublishedMap(state.RegistryEntries)
			backfilled := 0
			for _, indexTable := range state.RepairableRegistryIdx {
				entry, ok := state.LocatorEntries[indexTable]
				if !ok {
					continue
				}
				info, exists, err := ftnative.InspectSidecarFile(ctx, fs, entry.FilePath)
				if err != nil {
					result.Skipped++
					result.appendNote(fmt.Sprintf("registry backfill skipped for %s index=%s: %v", state.ObjectName, indexTable, err))
					continue
				}
				if !exists {
					result.Skipped++
					result.appendNote(fmt.Sprintf("registry backfill skipped for %s index=%s: sidecar missing", state.ObjectName, indexTable))
					continue
				}
				merged[indexTable] = ftnative.PublishedSidecar{
					IndexTable:     indexTable,
					SidecarPath:    entry.FilePath,
					LocatorPath:    ftnative.SidecarLocatorPath(state.ObjectName),
					SegmentVersion: info.SegmentVersion,
					DocCount:       info.DocCount,
					Flags:          ftnative.SidecarFlagLocatorWritten,
				}
				backfilled++
			}
			if backfilled == 0 {
				continue
			}
			ftnative.PublishRuntimeSidecars(report.TableID, state.ObjectName, publishedSidecarsFromMap(merged))
			result.RegistryBackfilledObjs++
			result.RegistryBackfilledIdxs += backfilled
		}
	}

	if mode == ftsRepairModeAll || mode == ftsRepairModeLocator {
		for _, state := range report.Objects {
			if !state.RepairableLocator {
				continue
			}
			set, ok := ftnative.LookupRuntimeSidecars(state.ObjectName)
			if !ok || len(set.Entries) == 0 {
				continue
			}
			entries := publishedSidecarsFromMap(set.Entries)
			if len(entries) == 0 {
				continue
			}
			if !allSidecarsExist(ctx, fs, entries) {
				result.Skipped++
				result.appendNote(fmt.Sprintf("locator rewrite skipped for %s: registry sidecar file missing", state.ObjectName))
				continue
			}
			locatorEntries := make([]ftnative.SidecarLocatorEntry, 0, len(entries))
			for _, entry := range entries {
				locatorEntries = append(locatorEntries, ftnative.SidecarLocatorEntry{
					IndexTable: entry.IndexTable,
					FilePath:   entry.SidecarPath,
				})
				entry.Flags |= ftnative.SidecarFlagLocatorWritten
				set.Entries[entry.IndexTable] = entry
			}
			locatorPath := ftnative.SidecarLocatorPath(state.ObjectName)
			if exists, err := statFileExists(ctx, fs, locatorPath); err != nil {
				return nil, err
			} else if exists {
				if err := fs.Delete(ctx, locatorPath); err != nil && !moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
					return nil, err
				}
			}
			if err := ftnative.WriteSidecarLocator(ctx, fs, state.ObjectName, locatorEntries); err != nil {
				return nil, err
			}
			ftnative.PublishRuntimeSidecars(set.TableID, state.ObjectName, publishedSidecarsFromMap(set.Entries))
			result.LocatorRewrittenObjs++
			result.LocatorRewrittenEntries += len(locatorEntries)
		}
	}

	postRepair, err := scanFTSMetadata(ctx, fs, tbl)
	if err != nil {
		return nil, err
	}
	result.PostRepair = postRepair
	return result, nil
}

func inspectableFTSIndexes(tbl *catalog.TableEntry) ([]string, error) {
	defs, err := ftnative.ExtractIndexDefinitions(tbl.GetLastestSchema(false))
	if err != nil {
		return nil, err
	}
	uniq := make(map[string]struct{}, len(defs))
	for _, def := range defs {
		if def.SkipReason != "" || def.TableName == "" {
			continue
		}
		uniq[def.TableName] = struct{}{}
	}
	indexes := make([]string, 0, len(uniq))
	for indexTable := range uniq {
		indexes = append(indexes, indexTable)
	}
	sort.Strings(indexes)
	return indexes, nil
}

func locatorEqualsRegistry(
	registry map[string]ftnative.PublishedSidecar,
	locator map[string]ftnative.SidecarLocatorEntry,
) bool {
	if len(registry) != len(locator) {
		return false
	}
	for indexTable, entry := range registry {
		locatorEntry, ok := locator[indexTable]
		if !ok || locatorEntry.FilePath != entry.SidecarPath {
			return false
		}
	}
	return true
}

func allSidecarsExist(
	ctx context.Context,
	fs fileservice.FileService,
	entries []ftnative.PublishedSidecar,
) bool {
	for _, entry := range entries {
		exists, err := statFileExists(ctx, fs, entry.SidecarPath)
		if err != nil || !exists {
			return false
		}
	}
	return true
}

func statFileExists(ctx context.Context, fs fileservice.FileService, filePath string) (bool, error) {
	if _, err := fs.StatFile(ctx, filePath); err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func clonePublishedMap(in map[string]ftnative.PublishedSidecar) map[string]ftnative.PublishedSidecar {
	if len(in) == 0 {
		return make(map[string]ftnative.PublishedSidecar)
	}
	out := make(map[string]ftnative.PublishedSidecar, len(in))
	for key, entry := range in {
		out[key] = entry
	}
	return out
}

func publishedSidecarsFromMap(in map[string]ftnative.PublishedSidecar) []ftnative.PublishedSidecar {
	if len(in) == 0 {
		return nil
	}
	keys := make([]string, 0, len(in))
	for key := range in {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]ftnative.PublishedSidecar, 0, len(keys))
	for _, key := range keys {
		out = append(out, in[key])
	}
	return out
}

func (r *ftsScanReport) render() string {
	var (
		registryObjects     int
		locatorObjects      int
		missingRegistry     int
		missingLocator      int
		locatorCorrupt      int
		locatorMismatch     int
		registrySidecarMiss int
		locatorSidecarMiss  int
		unexpectedRegistry  int
		unexpectedLocator   int
		repairableRegistry  int
		repairableLocator   int
		details             []string
	)

	for _, state := range r.Objects {
		if state.HasRegistry {
			registryObjects++
		}
		if state.HasLocatorFile {
			locatorObjects++
		}
		if len(state.MissingRegistry) > 0 {
			missingRegistry++
		}
		if len(state.MissingLocator) > 0 {
			missingLocator++
		}
		if state.LocatorCorrupt != "" {
			locatorCorrupt++
		}
		if len(state.LocatorMismatch) > 0 {
			locatorMismatch++
		}
		if len(state.RegistrySidecarMiss) > 0 {
			registrySidecarMiss++
		}
		if len(state.LocatorSidecarMiss) > 0 {
			locatorSidecarMiss++
		}
		if len(state.UnexpectedRegistry) > 0 {
			unexpectedRegistry++
		}
		if len(state.UnexpectedLocator) > 0 {
			unexpectedLocator++
		}
		if state.RepairableRegistry {
			repairableRegistry++
		}
		if state.RepairableLocator {
			repairableLocator++
		}
		if detail := state.renderDetail(); detail != "" {
			details = append(details, detail)
		}
	}

	var b strings.Builder
	fmt.Fprintf(&b, "table=%s table_id=%d fulltext_indexes=%d visible_objects=%d registry_objects=%d locator_objects=%d\n",
		r.TableName, r.TableID, len(r.ExpectedIndexes), len(r.Objects), registryObjects, locatorObjects)
	fmt.Fprintf(&b, "counts: missing_registry=%d missing_locator=%d locator_corrupt=%d locator_mismatch=%d registry_sidecar_missing=%d locator_sidecar_missing=%d unexpected_registry=%d unexpected_locator=%d repairable_registry_backfill=%d repairable_locator_rewrite=%d\n",
		missingRegistry, missingLocator, locatorCorrupt, locatorMismatch, registrySidecarMiss, locatorSidecarMiss, unexpectedRegistry, unexpectedLocator, repairableRegistry, repairableLocator)
	if len(r.ExpectedIndexes) > 0 {
		fmt.Fprintf(&b, "indexes=%s\n", strings.Join(r.ExpectedIndexes, ","))
	}
	if len(details) > 0 {
		b.WriteString("details:\n")
		for _, detail := range details {
			b.WriteString("  - ")
			b.WriteString(detail)
			b.WriteByte('\n')
		}
	}
	return strings.TrimRight(b.String(), "\n")
}

func (r *ftsRepairResult) render() string {
	var b strings.Builder
	fmt.Fprintf(&b, "table=%s mode=%s scanned_objects=%d registry_backfilled_objects=%d registry_backfilled_indexes=%d locator_rewritten_objects=%d locator_rewritten_entries=%d skipped=%d\n",
		r.TableName, r.Mode, r.ScannedObjects, r.RegistryBackfilledObjs, r.RegistryBackfilledIdxs, r.LocatorRewrittenObjs, r.LocatorRewrittenEntries, r.Skipped)
	if len(r.Notes) > 0 {
		b.WriteString("notes:\n")
		for _, note := range r.Notes {
			b.WriteString("  - ")
			b.WriteString(note)
			b.WriteByte('\n')
		}
	}
	if r.PostRepair != nil {
		post := r.PostRepair.render()
		b.WriteString("post_repair:\n")
		for _, line := range strings.Split(post, "\n") {
			b.WriteString("  ")
			b.WriteString(line)
			b.WriteByte('\n')
		}
	}
	return strings.TrimRight(b.String(), "\n")
}

func (s *ftsObjectState) renderDetail() string {
	parts := make([]string, 0, 8)
	if len(s.MissingRegistry) > 0 {
		parts = append(parts, "missing_registry="+strings.Join(s.MissingRegistry, ","))
	}
	if len(s.MissingLocator) > 0 {
		parts = append(parts, "missing_locator="+strings.Join(s.MissingLocator, ","))
	}
	if s.LocatorCorrupt != "" {
		parts = append(parts, "locator_corrupt")
	}
	if len(s.LocatorMismatch) > 0 {
		parts = append(parts, "locator_mismatch="+strings.Join(s.LocatorMismatch, ","))
	}
	if len(s.RegistrySidecarMiss) > 0 {
		parts = append(parts, "registry_sidecar_missing="+strings.Join(s.RegistrySidecarMiss, ","))
	}
	if len(s.LocatorSidecarMiss) > 0 {
		parts = append(parts, "locator_sidecar_missing="+strings.Join(s.LocatorSidecarMiss, ","))
	}
	if len(s.UnexpectedRegistry) > 0 {
		parts = append(parts, "unexpected_registry="+strings.Join(s.UnexpectedRegistry, ","))
	}
	if len(s.UnexpectedLocator) > 0 {
		parts = append(parts, "unexpected_locator="+strings.Join(s.UnexpectedLocator, ","))
	}
	if s.RepairableRegistry {
		parts = append(parts, "repairable_registry_backfill")
	}
	if s.RepairableLocator {
		parts = append(parts, "repairable_locator_rewrite")
	}
	if len(parts) == 0 {
		return ""
	}
	return fmt.Sprintf("object=%s %s", s.ObjectName, strings.Join(parts, " "))
}

func (r *ftsRepairResult) appendNote(note string) {
	if note == "" {
		return
	}
	r.Notes = append(r.Notes, note)
	sort.Strings(r.Notes)
}
