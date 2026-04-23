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
	"encoding/json"
	"math"
	"runtime"
	"sort"
	"strings"
	"sync"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fulltext"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"go.uber.org/zap"
)

type IndexDefinition struct {
	Name       string
	TableName  string
	Parts      []string
	Param      fulltext.FullTextParserParam
	SkipReason string
}

type ObjectIndexer struct {
	schema   *catalog.Schema
	pkName   string
	pkType   types.T
	indexes  []IndexDefinition
	builders map[string]*Builder
	nextBlk  uint16
}

type resolvedIndex struct {
	def       IndexDefinition
	builder   *Builder
	partIdxes []int
	partTypes []types.T
}

type PersistedSidecarRelation interface {
	Ranges(context.Context, engine.RangesParam) (engine.RelData, error)
	BuildReaders(
		ctx context.Context,
		proc any,
		expr *plan.Expr,
		relData engine.RelData,
		num int,
		txnOffset int,
		orderBy bool,
		policy engine.TombstoneApplyPolicy,
		filterHint engine.FilterHint,
	) ([]engine.Reader, error)
}

type visibleObjectStatsRelation interface {
	GetVisibleObjectStats(context.Context) ([]objectio.ObjectStats, error)
}

type nonAppendableObjectStatsRelation interface {
	GetNonAppendableObjectStats(context.Context) ([]objectio.ObjectStats, error)
}

type rowIDIndexBatchAttrs struct {
	rowIDIdx  int
	pkIdx     int
	partIdxes []int
	partTypes []types.T
}

type objectSegmentBuilder struct {
	name    objectio.ObjectName
	builder *Builder
}

const maxPersistedSidecarBuildReaders = 8

func ExtractIndexDefinitions(schema *catalog.Schema) ([]IndexDefinition, error) {
	if len(schema.Constraint) == 0 {
		return nil, nil
	}

	cstrDef := new(engine.ConstraintDef)
	if err := cstrDef.UnmarshalBinary(schema.Constraint); err != nil {
		return nil, err
	}

	defs := make([]IndexDefinition, 0, 4)
	for _, ct := range cstrDef.Cts {
		idxDef, ok := ct.(*engine.IndexDef)
		if !ok {
			continue
		}
		for _, idx := range idxDef.Indexes {
			if idx == nil || !pkgcatalog.IsFullTextIndexAlgo(idx.IndexAlgo) {
				continue
			}
			param, err := parseIndexParam(idx)
			if err != nil {
				return nil, err
			}
			def := IndexDefinition{
				Name:      idx.IndexName,
				TableName: idx.IndexTableName,
				Parts:     append([]string(nil), idx.Parts...),
				Param:     param,
			}
			if containsDatalink(schema, idx.Parts) {
				def.SkipReason = "datalink columns require query-time tokenization fallback"
			}
			defs = append(defs, def)
		}
	}
	return defs, nil
}

func ExtractPlanIndexDefinitions(tableDef *plan.TableDef) ([]IndexDefinition, error) {
	if tableDef == nil || len(tableDef.Indexes) == 0 {
		return nil, nil
	}

	defs := make([]IndexDefinition, 0, len(tableDef.Indexes))
	for _, idx := range tableDef.Indexes {
		if idx == nil || !pkgcatalog.IsFullTextIndexAlgo(idx.IndexAlgo) {
			continue
		}
		param, err := parseIndexParam(idx)
		if err != nil {
			return nil, err
		}
		def := IndexDefinition{
			Name:      idx.IndexName,
			TableName: idx.IndexTableName,
			Parts:     append([]string(nil), idx.Parts...),
			Param:     param,
		}
		if hasDatalinkPartInPlan(tableDef, idx.Parts) {
			def.SkipReason = "datalink columns require query-time tokenization fallback"
		}
		defs = append(defs, def)
	}
	return defs, nil
}

func NewPlanObjectIndexer(tableDef *plan.TableDef) (*ObjectIndexer, error) {
	if tableDef == nil || tableDef.Pkey == nil {
		return nil, moerr.NewInternalErrorNoCtx("native fulltext sidecar requires primary key in table definition")
	}
	pkIdx, ok := tableDef.Name2ColIndex[tableDef.Pkey.PkeyColName]
	if !ok || int(pkIdx) >= len(tableDef.Cols) {
		return nil, moerr.NewInternalErrorNoCtx("native fulltext sidecar primary key column missing from table definition")
	}
	indexes, err := ExtractPlanIndexDefinitions(tableDef)
	if err != nil {
		return nil, err
	}
	return newObjectIndexer(tableDef.Pkey.PkeyColName, types.T(tableDef.Cols[pkIdx].Typ.Id), indexes), nil
}

func newObjectIndexer(pkName string, pkType types.T, indexes []IndexDefinition) *ObjectIndexer {
	ret := &ObjectIndexer{
		pkName:   pkName,
		pkType:   pkType,
		indexes:  indexes,
		builders: make(map[string]*Builder, len(indexes)),
	}
	for _, idx := range indexes {
		if idx.SkipReason != "" {
			continue
		}
		ret.builders[idx.TableName] = NewBuilder(idx.Param, nil)
	}
	return ret
}

func NewObjectIndexer(schema *catalog.Schema) (*ObjectIndexer, error) {
	indexes, err := ExtractIndexDefinitions(schema)
	if err != nil {
		return nil, err
	}
	pk := schema.GetPrimaryKey()
	ret := newObjectIndexer(pk.Name, pk.Type.Oid, indexes)
	ret.schema = schema
	return ret, nil
}

func (o *ObjectIndexer) Empty() bool {
	return len(o.builders) == 0
}

func (o *ObjectIndexer) IndexCount() int {
	return len(o.indexes)
}

func (o *ObjectIndexer) ActiveIndexCount() int {
	return len(o.builders)
}

func (o *ObjectIndexer) AddBatch(bat *batch.Batch, blockRows []uint32) error {
	if o.Empty() || bat == nil || bat.RowCount() == 0 {
		return nil
	}
	if len(blockRows) == 0 {
		return moerr.NewInternalErrorNoCtx("native fulltext sidecar requires block row layout")
	}

	resolved, pkIdx, err := o.resolveBatch(bat)
	if err != nil {
		return err
	}
	if len(resolved) == 0 {
		return nil
	}

	rowStart := 0
	for _, rows := range blockRows {
		rowCount := int(rows)
		rowEnd := rowStart + rowCount
		if rowEnd > bat.RowCount() {
			return moerr.NewInternalErrorNoCtx("native fulltext sidecar block layout exceeds batch rows")
		}
		for row := rowStart; row < rowEnd; row++ {
			pkBytes := types.EncodeValue(vector.GetAny(bat.Vecs[pkIdx], row, true), o.pkType)
			rowInBlock := uint32(row - rowStart)
			for _, idx := range resolved {
				values, ok, err := collectIndexValues(bat, row, idx.partIdxes, idx.partTypes)
				if err != nil {
					return err
				}
				if !ok {
					continue
				}
				if err := idx.builder.Add(Document{
					Block:  o.nextBlk,
					Row:    rowInBlock,
					PK:     pkBytes,
					Values: values,
				}); err != nil {
					return err
				}
			}
		}
		rowStart = rowEnd
		o.nextBlk++
	}
	if rowStart != bat.RowCount() {
		return moerr.NewInternalErrorNoCtx("native fulltext sidecar block layout does not cover all rows")
	}
	return nil
}

func (o *ObjectIndexer) Write(
	ctx context.Context,
	fs fileservice.FileService,
	objName objectio.ObjectName,
	expectedRows ...uint32,
) ([]PublishedSidecar, error) {
	if o.Empty() {
		return nil, nil
	}
	rowHint := uint32(0)
	if len(expectedRows) > 0 {
		rowHint = expectedRows[0]
	}
	entries := make([]SidecarLocatorEntry, 0, len(o.builders))
	published := make([]PublishedSidecar, 0, len(o.builders))
	for _, idx := range o.indexes {
		builder, ok := o.builders[idx.TableName]
		if !ok {
			continue
		}
		seg := builder.Build()
		if rowHint > 0 && seg.DocCount == 0 {
			logutil.Warn(
				"native fulltext sidecar skipped for empty segment",
				zap.String("object", objName.String()),
				zap.String("index_table", idx.TableName),
				zap.Uint32("rows", rowHint),
			)
			continue
		}
		buf, err := seg.MarshalBinary()
		if err != nil {
			return published, err
		}
		sidecarPath := SidecarPath(objName.String(), idx.TableName)
		if err := fs.Write(ctx, fileservice.IOVector{
			FilePath: sidecarPath,
			Entries: []fileservice.IOEntry{{
				Offset: 0,
				Size:   int64(len(buf)),
				Data:   buf,
			}},
		}); err != nil {
			return published, err
		}
		entries = append(entries, SidecarLocatorEntry{
			IndexTable: idx.TableName,
			FilePath:   sidecarPath,
		})
		published = append(published, PublishedSidecar{
			IndexTable:     idx.TableName,
			SidecarPath:    sidecarPath,
			LocatorPath:    SidecarLocatorPath(objName.String()),
			SegmentVersion: CurrentSegmentVersion,
			DocCount:       seg.DocCount,
		})
	}
	if len(entries) == 0 {
		return nil, nil
	}
	if err := WriteSidecarLocator(ctx, fs, objName.String(), entries); err != nil {
		return published, err
	}
	for i := range published {
		published[i].Flags |= SidecarFlagLocatorWritten
	}
	return published, nil
}

func BackfillCommittedPersistedSidecars(
	ctx context.Context,
	proc any,
	mp *mpool.MPool,
	rel PersistedSidecarRelation,
	fs fileservice.FileService,
	tableID uint64,
	tableDef *plan.TableDef,
	indexDef *plan.IndexDef,
) error {
	if rel == nil || mp == nil || fs == nil || tableDef == nil || indexDef == nil {
		return nil
	}
	if indexDef.IndexTableName == "" || len(indexDef.Parts) == 0 {
		return nil
	}
	param, err := parseIndexParam(indexDef)
	if err != nil {
		return err
	}
	if hasDatalinkPartInPlan(tableDef, indexDef.Parts) {
		return nil
	}

	readAttrs, colTypes, pkType, err := buildRowIDIndexReadAttrs(tableDef, indexDef.Parts)
	if err != nil {
		return err
	}
	relData, err := rel.Ranges(ctx, engine.RangesParam{
		PreAllocBlocks:     256,
		TxnOffset:          0,
		Policy:             engine.Policy_CollectCommittedPersistedData,
		DontSupportRelData: true,
	})
	if err != nil {
		return err
	}
	builders, err := buildPersistedSidecarBuilders(
		ctx,
		proc,
		mp,
		rel,
		relData,
		readAttrs,
		colTypes,
		tableDef,
		indexDef,
		pkType,
		param,
	)
	if err != nil {
		return err
	}
	if len(builders) == 0 {
		fallbackRelData, ok, err := buildVisibleObjectRelData(ctx, rel, relData)
		if err != nil {
			return err
		}
		if ok && fallbackRelData != nil && fallbackRelData.DataCnt() > 0 {
			builders, err = buildPersistedSidecarBuilders(
				ctx,
				proc,
				mp,
				rel,
				fallbackRelData,
				readAttrs,
				colTypes,
				tableDef,
				indexDef,
				pkType,
				param,
			)
			if err != nil {
				return err
			}
		}
	}
	return publishBuiltSidecars(ctx, fs, tableID, indexDef.IndexTableName, builders)
}

func buildPersistedSidecarBuilders(
	ctx context.Context,
	proc any,
	mp *mpool.MPool,
	rel PersistedSidecarRelation,
	relData engine.RelData,
	readAttrs []string,
	colTypes []types.Type,
	tableDef *plan.TableDef,
	indexDef *plan.IndexDef,
	pkType types.T,
	param fulltext.FullTextParserParam,
) (map[string]*objectSegmentBuilder, error) {
	parallelism := persistedSidecarBuildParallelism(relData)
	readers, err := rel.BuildReaders(
		ctx,
		proc,
		nil,
		relData,
		parallelism,
		0,
		false,
		engine.Policy_CheckAll,
		engine.FilterHint{},
	)
	if err != nil {
		return nil, err
	}

	builders := make(map[string]*objectSegmentBuilder)
	if len(readers) == 0 {
		return builders, nil
	}
	if len(readers) == 1 {
		readerBuilders, err := buildPersistedSidecarBuildersForReader(
			ctx,
			mp,
			readers[0],
			readAttrs,
			colTypes,
			tableDef,
			indexDef,
			pkType,
			param,
		)
		if err != nil {
			return nil, err
		}
		mergeObjectSegmentBuilders(builders, readerBuilders)
		return builders, nil
	}

	type buildResult struct {
		builders map[string]*objectSegmentBuilder
		err      error
	}
	results := make(chan buildResult, len(readers))
	var wg sync.WaitGroup
	for _, reader := range readers {
		wg.Add(1)
		go func(reader engine.Reader) {
			defer wg.Done()
			readerBuilders, readErr := buildPersistedSidecarBuildersForReader(
				ctx,
				mp,
				reader,
				readAttrs,
				colTypes,
				tableDef,
				indexDef,
				pkType,
				param,
			)
			results <- buildResult{builders: readerBuilders, err: readErr}
		}(reader)
	}
	go func() {
		wg.Wait()
		close(results)
	}()

	for result := range results {
		if result.err != nil {
			return nil, result.err
		}
		mergeObjectSegmentBuilders(builders, result.builders)
	}
	return builders, nil
}

func persistedSidecarBuildParallelism(relData engine.RelData) int {
	parallelism := runtime.GOMAXPROCS(0)
	if parallelism < 1 {
		parallelism = 1
	}
	if parallelism > maxPersistedSidecarBuildReaders {
		parallelism = maxPersistedSidecarBuildReaders
	}
	if relData != nil && relData.DataCnt() > 0 && parallelism > relData.DataCnt() {
		parallelism = relData.DataCnt()
	}
	if parallelism < 1 {
		parallelism = 1
	}
	return parallelism
}

func buildPersistedSidecarBuildersForReader(
	ctx context.Context,
	mp *mpool.MPool,
	reader engine.Reader,
	readAttrs []string,
	colTypes []types.Type,
	tableDef *plan.TableDef,
	indexDef *plan.IndexDef,
	pkType types.T,
	param fulltext.FullTextParserParam,
) (map[string]*objectSegmentBuilder, error) {
	builders := make(map[string]*objectSegmentBuilder)
	readBatch := batch.NewWithSize(len(readAttrs))
	readBatch.SetAttributes(readAttrs)
	for i := range readAttrs {
		readBatch.Vecs[i] = vector.NewVec(colTypes[i])
	}
	resolved, err := resolveRowIDIndexBatchAttrs(readBatch, tableDef.Pkey.PkeyColName, indexDef.Parts)
	if err != nil {
		readBatch.Clean(mp)
		reader.Close()
		return nil, err
	}
	defer readBatch.Clean(mp)
	defer reader.Close()

	for {
		isEnd, readErr := reader.Read(ctx, readAttrs, nil, mp, readBatch)
		if readErr != nil {
			return nil, readErr
		}
		if isEnd {
			return builders, nil
		}
		if readBatch.RowCount() == 0 {
			readBatch.CleanOnlyData()
			continue
		}
		readErr = appendRowIDIndexBatch(builders, readBatch, resolved, pkType, param)
		readBatch.CleanOnlyData()
		if readErr != nil {
			return nil, readErr
		}
	}
}

func mergeObjectSegmentBuilders(dst, src map[string]*objectSegmentBuilder) {
	for key, builder := range src {
		if builder == nil {
			continue
		}
		if existing := dst[key]; existing != nil {
			mergeBuilder(existing.builder, builder.builder)
			continue
		}
		dst[key] = builder
	}
}

func mergeBuilder(dst, src *Builder) {
	if dst == nil || src == nil {
		return
	}
	dst.docCount += src.docCount
	dst.tokenSum += src.tokenSum
	for term, postingsByRow := range src.terms {
		dstPostings := dst.terms[term]
		if dstPostings == nil {
			dstPostings = make(map[string]*postingBuilder, len(postingsByRow))
			dst.terms[term] = dstPostings
		}
		for rowKey, posting := range postingsByRow {
			if posting == nil {
				continue
			}
			dstPostings[rowKey] = clonePostingBuilder(posting)
		}
	}
}

func clonePostingBuilder(src *postingBuilder) *postingBuilder {
	if src == nil {
		return nil
	}
	return &postingBuilder{
		ref:       cloneRowRef(src.ref),
		docLen:    src.docLen,
		positions: append([]int32(nil), src.positions...),
	}
}

func buildVisibleObjectRelData(
	ctx context.Context,
	rel PersistedSidecarRelation,
	baseRelData engine.RelData,
) (engine.RelData, bool, error) {
	var (
		stats []objectio.ObjectStats
		err   error
		ok    bool
	)
	if visibleRel, yes := rel.(visibleObjectStatsRelation); yes {
		stats, err = visibleRel.GetVisibleObjectStats(ctx)
		if err != nil {
			return nil, false, err
		}
		ok = true
	} else if visibleRel, yes := rel.(nonAppendableObjectStatsRelation); yes {
		stats, err = visibleRel.GetNonAppendableObjectStats(ctx)
		if err != nil {
			return nil, false, err
		}
		ok = true
	}
	if !ok {
		return nil, false, nil
	}
	if len(stats) == 0 {
		return nil, true, nil
	}
	if baseRelData == nil {
		return nil, true, nil
	}
	relData := baseRelData.BuildEmptyRelData(len(stats))
	relData.AppendBlockInfoSlice(objectio.MultiObjectStatsToBlockInfoSlice(stats, false))
	return relData, true, nil
}

func ReadPublishedSidecar(
	ctx context.Context,
	fs fileservice.FileService,
	objName objectio.ObjectName,
	indexTableName string,
) (*Segment, bool, error) {
	filePath, exists, err := resolvePublishedSidecarPath(ctx, fs, objName.String(), indexTableName)
	if err != nil || !exists {
		return nil, false, err
	}
	seg, exists, err := readSidecarFile(ctx, fs, filePath)
	if err != nil {
		return nil, false, err
	}
	if !exists {
		markMissingSidecar(filePath)
		return nil, false, nil
	}
	clearMissingSidecar(filePath)
	return seg, true, nil
}

func ReadSidecar(ctx context.Context, fs fileservice.FileService, objName objectio.ObjectName, indexTableName string) (*Segment, bool, error) {
	return readSidecarFile(ctx, fs, SidecarPath(objName.String(), indexTableName))
}

func readSidecarFile(ctx context.Context, fs fileservice.FileService, filePath string) (*Segment, bool, error) {
	if seg, ok := lookupCachedSidecar(filePath); ok {
		return seg, true, nil
	}
	prefix, exists, err := readSidecarRange(ctx, fs, filePath, 0, int64(segmentPrefixLen))
	if err != nil {
		return nil, false, err
	}
	if !exists {
		return nil, false, nil
	}
	magic, headerLen, err := parseSegmentPrefix(prefix)
	if err != nil {
		return nil, false, err
	}
	if magic == segmentMagicV4 {
		headerBytes, exists, err := readSidecarRange(ctx, fs, filePath, int64(segmentPrefixLen), int64(headerLen))
		if err != nil {
			return nil, false, err
		}
		if !exists {
			return nil, false, moerr.NewFileNotFoundNoCtx(filePath)
		}
		seg := &Segment{}
		header, err := readSegmentHeaderV4(headerBytes, seg)
		if err != nil {
			return nil, false, err
		}
		var dirBytes []byte
		if header.DirectorySize > 0 {
			dirOffset := int64(segmentPrefixLen) + int64(headerLen)
			dirBytes, exists, err = readSidecarRange(ctx, fs, filePath, dirOffset, int64(header.DirectorySize))
			if err != nil {
				return nil, false, err
			}
			if !exists {
				return nil, false, moerr.NewFileNotFoundNoCtx(filePath)
			}
		}
		if err := indexSegmentDirectoryV4(dirBytes, header.TermCount, seg); err != nil {
			return nil, false, err
		}
		lazyReadCtx := context.Background()
		seg.lazyLoader = func(offset, size int64) ([]byte, error) {
			data, exists, err := readSidecarRange(lazyReadCtx, fs, filePath, offset, size)
			if err != nil {
				return nil, err
			}
			if !exists {
				return nil, moerr.NewFileNotFoundNoCtx(filePath)
			}
			return data, nil
		}
		seg.lazyBatchLoader = func(requests []segmentTermLoadRequest) (map[string][]byte, error) {
			blobs, exists, err := readSidecarRanges(lazyReadCtx, fs, filePath, requests)
			if err != nil {
				return nil, err
			}
			if !exists {
				return nil, moerr.NewFileNotFoundNoCtx(filePath)
			}
			return blobs, nil
		}
		cacheSidecar(filePath, seg)
		return seg, true, nil
	}

	vec := &fileservice.IOVector{
		FilePath: filePath,
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   -1,
		}},
	}
	if err := fs.Read(ctx, vec); err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	seg, err := UnmarshalBinary(vec.Entries[0].Data)
	if err != nil {
		return nil, false, err
	}
	cacheSidecar(filePath, seg)
	return seg, true, nil
}

func resolvePublishedSidecarPath(
	ctx context.Context,
	fs fileservice.FileService,
	objectPath string,
	indexTableName string,
) (string, bool, error) {
	if set, ok := LookupRuntimeSidecars(objectPath); ok {
		if entry, ok := set.Entries[indexTableName]; ok && entry.SidecarPath != "" {
			if hasMissingSidecar(entry.SidecarPath) {
				return "", false, nil
			}
			return entry.SidecarPath, true, nil
		}
	}

	if hasMissingLocator(objectPath) {
		return "", false, nil
	}
	locator, exists, err := ReadSidecarLocator(ctx, fs, objectPath)
	if err != nil {
		return "", false, err
	}
	if !exists {
		markMissingLocator(objectPath)
		return "", false, err
	}
	clearMissingLocator(objectPath)
	for _, entry := range locator.Entries {
		if entry.IndexTable != indexTableName || entry.FilePath == "" {
			continue
		}
		if hasMissingSidecar(entry.FilePath) {
			return "", false, nil
		}
		return entry.FilePath, true, nil
	}
	return "", false, nil
}

func readSidecarRange(
	ctx context.Context,
	fs fileservice.FileService,
	filePath string,
	offset int64,
	size int64,
) ([]byte, bool, error) {
	vec := &fileservice.IOVector{
		FilePath: filePath,
		Entries: []fileservice.IOEntry{{
			Offset: offset,
			Size:   size,
		}},
	}
	if err := fs.Read(ctx, vec); err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return vec.Entries[0].Data, true, nil
}

func readSidecarRanges(
	ctx context.Context,
	fs fileservice.FileService,
	filePath string,
	requests []segmentTermLoadRequest,
) (map[string][]byte, bool, error) {
	vec := &fileservice.IOVector{
		FilePath: filePath,
		Entries:  make([]fileservice.IOEntry, len(requests)),
	}
	for i, request := range requests {
		vec.Entries[i] = fileservice.IOEntry{
			Offset: request.meta.offset,
			Size:   request.meta.size,
		}
	}
	if err := fs.Read(ctx, vec); err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	blobs := make(map[string][]byte, len(requests))
	for i, request := range requests {
		blobs[request.term] = vec.Entries[i].Data
	}
	return blobs, true, nil
}

func buildRowIDIndexReadAttrs(
	tableDef *plan.TableDef,
	parts []string,
) ([]string, []types.Type, types.T, error) {
	pkIdx, ok := tableDef.Name2ColIndex[tableDef.Pkey.PkeyColName]
	if !ok {
		return nil, nil, 0, moerr.NewInternalErrorNoCtx("native fulltext backfill missing primary key column")
	}
	pkType := types.T(tableDef.Cols[pkIdx].Typ.Id)
	readAttrs := make([]string, 0, len(parts)+2)
	colTypes := make([]types.Type, 0, len(parts)+2)
	seen := make(map[string]struct{}, len(parts)+2)
	appendAttr := func(name string, typ types.Type) {
		key := strings.ToLower(name)
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		readAttrs = append(readAttrs, name)
		colTypes = append(colTypes, typ)
	}
	appendAttr(pkgcatalog.Row_ID, types.T_Rowid.ToType())
	appendAttr(
		tableDef.Pkey.PkeyColName,
		types.New(
			types.T(tableDef.Cols[pkIdx].Typ.Id),
			tableDef.Cols[pkIdx].Typ.Width,
			tableDef.Cols[pkIdx].Typ.Scale,
		),
	)
	for _, part := range parts {
		idx, ok := tableDef.Name2ColIndex[part]
		if !ok {
			return nil, nil, 0, moerr.NewInternalErrorNoCtx("native fulltext backfill missing indexed column")
		}
		appendAttr(
			part,
			types.New(
				types.T(tableDef.Cols[idx].Typ.Id),
				tableDef.Cols[idx].Typ.Width,
				tableDef.Cols[idx].Typ.Scale,
			),
		)
	}
	return readAttrs, colTypes, pkType, nil
}

func resolveRowIDIndexBatchAttrs(
	bat *batch.Batch,
	pkName string,
	parts []string,
) (rowIDIndexBatchAttrs, error) {
	attrMap := make(map[string]int, len(bat.Attrs))
	for i, attr := range bat.Attrs {
		attrMap[strings.ToLower(attr)] = i
	}
	rowIDIdx, ok := attrMap[strings.ToLower(pkgcatalog.Row_ID)]
	if !ok {
		return rowIDIndexBatchAttrs{}, moerr.NewInternalErrorNoCtx("native fulltext backfill missing rowid column in batch")
	}
	pkIdx, ok := attrMap[strings.ToLower(pkName)]
	if !ok {
		return rowIDIndexBatchAttrs{}, moerr.NewInternalErrorNoCtx("native fulltext backfill missing primary key column in batch")
	}

	partIdxes := make([]int, 0, len(parts))
	partTypes := make([]types.T, 0, len(parts))
	for _, part := range parts {
		colIdx, ok := attrMap[strings.ToLower(part)]
		if !ok {
			return rowIDIndexBatchAttrs{}, moerr.NewInternalErrorNoCtx("native fulltext backfill missing indexed column in batch")
		}
		partIdxes = append(partIdxes, colIdx)
		partTypes = append(partTypes, bat.Vecs[colIdx].GetType().Oid)
	}

	return rowIDIndexBatchAttrs{
		rowIDIdx:  rowIDIdx,
		pkIdx:     pkIdx,
		partIdxes: partIdxes,
		partTypes: partTypes,
	}, nil
}

func appendRowIDIndexBatch(
	builders map[string]*objectSegmentBuilder,
	bat *batch.Batch,
	resolved rowIDIndexBatchAttrs,
	pkType types.T,
	param fulltext.FullTextParserParam,
) error {
	if bat == nil || bat.RowCount() == 0 {
		return nil
	}
	for row := 0; row < bat.RowCount(); row++ {
		values, ok, err := collectIndexValues(bat, row, resolved.partIdxes, resolved.partTypes)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		rowID := vector.GetFixedAtNoTypeCheck[types.Rowid](bat.Vecs[resolved.rowIDIdx], row)
		objName := objectio.BuildObjectNameWithObjectID(rowID.BorrowObjectID())
		objKey := objName.String()
		objectBuilder := builders[objKey]
		if objectBuilder == nil {
			objectBuilder = &objectSegmentBuilder{
				name:    objName,
				builder: NewBuilder(param, nil),
			}
			builders[objKey] = objectBuilder
		}
		pkBytes := types.EncodeValue(vector.GetAny(bat.Vecs[resolved.pkIdx], row, true), pkType)
		if err := objectBuilder.builder.Add(Document{
			Block:  rowID.GetBlockOffset(),
			Row:    rowID.GetRowOffset(),
			PK:     pkBytes,
			Values: values,
		}); err != nil {
			return err
		}
	}
	return nil
}

func publishBuiltSidecars(
	ctx context.Context,
	fs fileservice.FileService,
	tableID uint64,
	indexTableName string,
	builders map[string]*objectSegmentBuilder,
) error {
	if len(builders) == 0 {
		return nil
	}
	keys := make([]string, 0, len(builders))
	for key := range builders {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		seg := builders[key].builder.Build()
		if seg.DocCount == 0 {
			continue
		}
		if _, err := publishSidecarSegment(ctx, fs, tableID, builders[key].name, indexTableName, seg); err != nil {
			return err
		}
	}
	return nil
}

func publishSidecarSegment(
	ctx context.Context,
	fs fileservice.FileService,
	tableID uint64,
	objName objectio.ObjectName,
	indexTableName string,
	seg *Segment,
) (PublishedSidecar, error) {
	buf, err := seg.MarshalBinary()
	if err != nil {
		return PublishedSidecar{}, err
	}
	sidecarPath := SidecarPath(objName.String(), indexTableName)
	if err := fs.Write(ctx, fileservice.IOVector{
		FilePath: sidecarPath,
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   int64(len(buf)),
			Data:   buf,
		}},
	}); err != nil {
		return PublishedSidecar{}, err
	}

	locatorEntries := make(map[string]SidecarLocatorEntry)
	locator, exists, err := ReadSidecarLocator(ctx, fs, objName.String())
	if err != nil {
		return PublishedSidecar{}, err
	}
	if exists {
		for _, entry := range locator.Entries {
			if entry.IndexTable == "" || entry.FilePath == "" {
				continue
			}
			locatorEntries[entry.IndexTable] = entry
		}
	}
	clearMissingLocator(objName.String())
	locatorEntries[indexTableName] = SidecarLocatorEntry{
		IndexTable: indexTableName,
		FilePath:   sidecarPath,
	}
	mergedLocatorEntries := make([]SidecarLocatorEntry, 0, len(locatorEntries))
	for _, entry := range locatorEntries {
		mergedLocatorEntries = append(mergedLocatorEntries, entry)
	}
	if err := WriteSidecarLocator(ctx, fs, objName.String(), mergedLocatorEntries); err != nil {
		return PublishedSidecar{}, err
	}

	published := PublishedSidecar{
		IndexTable:     indexTableName,
		SidecarPath:    sidecarPath,
		LocatorPath:    SidecarLocatorPath(objName.String()),
		SegmentVersion: CurrentSegmentVersion,
		DocCount:       seg.DocCount,
		Flags:          SidecarFlagLocatorWritten,
	}
	mergedPublished := make(map[string]PublishedSidecar)
	if set, ok := LookupRuntimeSidecars(objName.String()); ok {
		if tableID == 0 {
			tableID = set.TableID
		}
		for indexTable, entry := range set.Entries {
			mergedPublished[indexTable] = entry
		}
	}
	mergedPublished[indexTableName] = published
	entries := make([]PublishedSidecar, 0, len(mergedPublished))
	for _, entry := range mergedPublished {
		entries = append(entries, entry)
	}
	PublishRuntimeSidecars(tableID, objName.String(), entries)
	return published, nil
}

func AppendQueryBatch(
	builder *Builder,
	bat *batch.Batch,
	pkName string,
	pkType types.T,
	parts []string,
	nextDoc uint64,
) (uint64, error) {
	if builder == nil || bat == nil || bat.RowCount() == 0 {
		return nextDoc, nil
	}

	attrMap := make(map[string]int, len(bat.Attrs))
	for i, attr := range bat.Attrs {
		attrMap[strings.ToLower(attr)] = i
	}
	pkIdx, ok := attrMap[strings.ToLower(pkName)]
	if !ok {
		return nextDoc, moerr.NewInternalErrorNoCtx("native fulltext tail scan missing primary key column in batch")
	}

	partIdxes := make([]int, 0, len(parts))
	partTypes := make([]types.T, 0, len(parts))
	for _, part := range parts {
		colIdx, ok := attrMap[strings.ToLower(part)]
		if !ok {
			return nextDoc, moerr.NewInternalErrorNoCtx("native fulltext tail scan missing indexed column in batch")
		}
		partIdxes = append(partIdxes, colIdx)
		partTypes = append(partTypes, bat.Vecs[colIdx].GetType().Oid)
	}

	for row := 0; row < bat.RowCount(); row++ {
		block := nextDoc / objectio.BlockMaxRows
		if block > math.MaxUint16 {
			return nextDoc, moerr.NewInternalErrorNoCtx("native fulltext tail scan exceeded synthetic block range")
		}
		values, ok, err := collectIndexValues(bat, row, partIdxes, partTypes)
		if err != nil {
			return nextDoc, err
		}
		if !ok {
			nextDoc++
			continue
		}
		pkBytes := types.EncodeValue(vector.GetAny(bat.Vecs[pkIdx], row, true), pkType)
		if err := builder.Add(Document{
			Block:  uint16(block),
			Row:    uint32(nextDoc % objectio.BlockMaxRows),
			PK:     pkBytes,
			Values: values,
		}); err != nil {
			return nextDoc, err
		}
		nextDoc++
	}
	return nextDoc, nil
}

func parseIndexParam(idx *plan.IndexDef) (fulltext.FullTextParserParam, error) {
	var param fulltext.FullTextParserParam
	if len(idx.IndexAlgoParams) == 0 {
		return param, nil
	}
	return param, json.Unmarshal([]byte(idx.IndexAlgoParams), &param)
}

func containsDatalink(schema *catalog.Schema, parts []string) bool {
	for _, part := range parts {
		colIdx, ok := schema.NameMap[part]
		if !ok {
			continue
		}
		if schema.ColDefs[colIdx].Type.Oid == types.T_datalink {
			return true
		}
	}
	return false
}

func hasDatalinkPartInPlan(tableDef *plan.TableDef, parts []string) bool {
	for _, part := range parts {
		colIdx, ok := tableDef.Name2ColIndex[part]
		if !ok {
			continue
		}
		if types.T(tableDef.Cols[colIdx].Typ.Id) == types.T_datalink {
			return true
		}
	}
	return false
}

func (o *ObjectIndexer) resolveBatch(bat *batch.Batch) ([]resolvedIndex, int, error) {
	attrMap := make(map[string]int, len(bat.Attrs))
	for i, attr := range bat.Attrs {
		attrMap[strings.ToLower(attr)] = i
	}
	pkIdx, ok := attrMap[strings.ToLower(o.pkName)]
	if !ok {
		return nil, -1, moerr.NewInternalErrorNoCtx("native fulltext sidecar missing primary key column in batch")
	}

	resolved := make([]resolvedIndex, 0, len(o.indexes))
	for _, idx := range o.indexes {
		builder, ok := o.builders[idx.TableName]
		if !ok {
			continue
		}
		partIdxes := make([]int, 0, len(idx.Parts))
		partTypes := make([]types.T, 0, len(idx.Parts))
		for _, part := range idx.Parts {
			colIdx, ok := attrMap[strings.ToLower(part)]
			if !ok {
				return nil, -1, moerr.NewInternalErrorNoCtx("native fulltext sidecar missing indexed column in batch")
			}
			partIdxes = append(partIdxes, colIdx)
			partTypes = append(partTypes, bat.Vecs[colIdx].GetType().Oid)
		}
		resolved = append(resolved, resolvedIndex{
			def:       idx,
			builder:   builder,
			partIdxes: partIdxes,
			partTypes: partTypes,
		})
	}
	return resolved, pkIdx, nil
}

func collectIndexValues(bat *batch.Batch, row int, partIdxes []int, partTypes []types.T) ([]fulltext.IndexValue, bool, error) {
	values := make([]fulltext.IndexValue, 0, len(partIdxes))
	for i, partIdx := range partIdxes {
		vec := bat.Vecs[partIdx]
		if vec.IsNull(uint64(row)) {
			continue
		}
		values = append(values, fulltext.IndexValue{
			Text: vec.GetStringAt(row),
			Raw:  vec.GetRawBytesAt(row),
			Type: partTypes[i],
		})
	}
	if len(values) == 0 {
		return nil, false, nil
	}
	return values, true, nil
}
