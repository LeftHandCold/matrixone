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

package table_function

import (
	"context"
	"sort"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fulltext"
	ftnative "github.com/matrixorigin/matrixone/pkg/fulltext/native"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type nativePreparedScan struct {
	rel         engine.Relation
	pkType      types.T
	indexTable  string
	objects     []nativeObjectSegment
	complete    bool
	tombstones  engine.Tombstoner
	snapshot    types.TS
	fs          fileservice.FileService
	totalDocs   int64
	totalTokens int64
}

type visibleObjectStatsRelation interface {
	GetVisibleObjectStats(ctx context.Context) ([]objectio.ObjectStats, error)
}

type nativeObjectSegment struct {
	key             string
	name            objectio.ObjectName
	segment         *ftnative.Segment
	applyTombstones bool
}

type nativeDocState struct {
	pk              any
	docLen          int32
	ref             ftnative.RowRef
	obj             objectio.ObjectName
	segmentKey      string
	applyTombstones bool
	counts          []uint16
}

type nativeDocKey struct {
	segmentKey string
	block      uint16
	row        uint32
}

type nativeDocSet map[nativeDocKey]struct{}

type nativeDeleteCache struct {
	hasBlock map[string]bool
	deleted  map[string]map[uint32]bool
}

type nativeTailBatchAttrs struct {
	rowIDIdx  int
	pkIdx     int
	partIdxes []int
	partTypes []types.T
}

type nativeTailSegmentBuilder struct {
	name    objectio.ObjectName
	builder *ftnative.Builder
}

func fulltextIndexMatchNative(
	u *fulltextState,
	proc *process.Process,
	s *fulltext.SearchAccum,
	srctbl, tblname string,
) (bool, error) {
	supported := nativeQuerySupported(s)
	if !supported {
		return validateNativeScanUsage(proc.Ctx, u.param, supported, nil)
	}

	scan, err := prepareNativeScan(proc, srctbl, tblname, u.param)
	if err != nil {
		return false, err
	}
	used, err := validateNativeScanUsage(proc.Ctx, u.param, supported, scan)
	if err != nil || !used {
		return used, err
	}
	applyNativeSegmentStats(u, s, scan)

	if s.Mode == int64(tree.FULLTEXT_DEFAULT) || s.Mode == int64(tree.FULLTEXT_NL) {
		return true, populatePhraseCompat(u, proc, s, scan, s.Pattern)
	}
	if len(s.Pattern) == 1 && s.Pattern[0].Operator == fulltext.PHRASE {
		return true, populatePhraseCompat(u, proc, s, scan, s.Pattern[0].Children)
	}
	return true, populateBooleanNative(u, proc, s, scan)
}

func nativeQuerySupported(s *fulltext.SearchAccum) bool {
	switch s.Mode {
	case int64(tree.FULLTEXT_DEFAULT), int64(tree.FULLTEXT_NL):
		for _, p := range s.Pattern {
			if p.Operator != fulltext.TEXT {
				return false
			}
		}
		return true
	case int64(tree.FULLTEXT_BOOLEAN):
		for _, p := range s.Pattern {
			if !nativePatternSupported(p) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func nativePatternSupported(p *fulltext.Pattern) bool {
	switch p.Operator {
	case fulltext.TEXT, fulltext.STAR, fulltext.PLUS, fulltext.MINUS,
		fulltext.LESSTHAN, fulltext.GREATERTHAN, fulltext.RANKLESS,
		fulltext.GROUP, fulltext.PHRASE, fulltext.JOIN:
	default:
		return false
	}
	for _, child := range p.Children {
		if !nativePatternSupported(child) {
			return false
		}
	}
	return true
}

func validateNativeScanUsage(
	ctx context.Context,
	param fulltext.FullTextParserParam,
	supported bool,
	scan *nativePreparedScan,
) (bool, error) {
	if !supported {
		if param.NativeOnly() {
			return false, moerr.NewNotSupported(ctx, "native-only fulltext query pattern is not supported by the native path")
		}
		return false, nil
	}
	if scan == nil {
		if param.NativeOnly() {
			return false, moerr.NewNotSupported(ctx, "native-only fulltext query is unavailable for this index")
		}
		return false, nil
	}
	if !scan.complete {
		if param.NativeOnly() {
			return false, moerr.NewNotSupported(ctx, "native-only fulltext query cannot run because native sidecars are incomplete")
		}
		return false, nil
	}
	return true, nil
}

func prepareNativeScan(
	proc *process.Process,
	srctbl, tblname string,
	param fulltext.FullTextParserParam,
) (*nativePreparedScan, error) {
	if len(param.Parts) == 0 {
		return nil, nil
	}

	dbName, tableName, err := parseQualifiedTableName(srctbl)
	if err != nil {
		return nil, err
	}
	_, indexTableName, err := parseQualifiedTableName(tblname)
	if err != nil {
		return nil, err
	}

	e := proc.Ctx.Value(defines.EngineKey{}).(engine.Engine)
	db, err := e.Database(proc.Ctx, dbName, proc.GetTxnOperator())
	if err != nil {
		return nil, err
	}
	rel, err := db.Relation(proc.Ctx, tableName, nil)
	if err != nil {
		return nil, err
	}
	tableDef := rel.GetTableDef(proc.Ctx)
	if hasDatalinkPart(tableDef, param.Parts) {
		return nil, nil
	}
	objectFS, err := colexec.GetObjectFSFromProc(proc)
	if err != nil {
		return nil, err
	}

	visibleInfos, err := rel.GetColumMetadataScanInfo(proc.Ctx, param.Parts[0], false)
	if err != nil {
		return nil, err
	}
	visible := make(map[string]struct{}, len(visibleInfos))
	for _, info := range visibleInfos {
		visible[info.ObjectName] = struct{}{}
	}

	stats, err := getNativeVisibleObjectStats(proc.Ctx, rel)
	if err != nil {
		return nil, err
	}

	objects := make([]nativeObjectSegment, 0, len(stats))
	totalDocs := int64(0)
	totalTokens := int64(0)
	incomplete := false
	for i := range stats {
		name := stats[i].ObjectName()
		nameStr := name.String()
		if _, ok := visible[nameStr]; !ok {
			continue
		}
		delete(visible, nameStr)
		seg, exists, err := ftnative.ReadPublishedSidecar(proc.Ctx, objectFS, name, indexTableName)
		if err != nil {
			return nil, err
		}
		if !exists {
			incomplete = true
			continue
		}
		objects = append(objects, nativeObjectSegment{
			key:             nameStr,
			name:            name,
			segment:         seg,
			applyTombstones: true,
		})
		totalDocs += seg.DocCount
		totalTokens += seg.TokenSum
	}
	if len(visible) > 0 {
		incomplete = true
	}

	tailObjects, tailDocs, tailTokens, err := buildNativeTailSegments(proc, rel, tableDef, param)
	if err != nil {
		return nil, err
	}
	if len(tailObjects) > 0 {
		objects = append(objects, tailObjects...)
		totalDocs += tailDocs
		totalTokens += tailTokens
	}
	if len(objects) == 0 {
		return nil, nil
	}

	tombstones, err := rel.CollectTombstones(proc.Ctx, 0, engine.Policy_CollectAllTombstones)
	if err != nil {
		return nil, err
	}

	pkColIdx, ok := tableDef.Name2ColIndex[tableDef.Pkey.PkeyColName]
	if !ok {
		return nil, moerr.NewInternalErrorNoCtx("native fulltext scan missing primary key column")
	}
	return &nativePreparedScan{
		rel:         rel,
		pkType:      types.T(tableDef.Cols[pkColIdx].Typ.Id),
		indexTable:  indexTableName,
		objects:     objects,
		complete:    !incomplete,
		tombstones:  tombstones,
		snapshot:    types.TimestampToTS(proc.GetTxnOperator().SnapshotTS()),
		fs:          objectFS,
		totalDocs:   totalDocs,
		totalTokens: totalTokens,
	}, nil
}

func applyNativeSegmentStats(u *fulltextState, s *fulltext.SearchAccum, scan *nativePreparedScan) {
	s.Nrow = scan.totalDocs
	if scan.totalDocs > 0 {
		s.AvgDocLen = float64(scan.totalTokens) / float64(scan.totalDocs)
	} else {
		s.AvgDocLen = 0
	}
	if s.ScoreAlgo == fulltext.ALGO_BM25 && s.AvgDocLen == 0 {
		s.ScoreAlgo = fulltext.ALGO_TFIDF
	}
	u.statsLoaded = true
}

func populatePhraseCompat(
	u *fulltextState,
	proc *process.Process,
	s *fulltext.SearchAccum,
	scan *nativePreparedScan,
	patterns []*fulltext.Pattern,
) error {
	if len(patterns) == 0 {
		return nil
	}
	tokens := make([]ftnative.PhraseToken, 0, len(patterns))
	for _, p := range patterns {
		if p.Operator != fulltext.TEXT {
			return nil
		}
		tokens = append(tokens, ftnative.PhraseToken{
			Word: p.Text,
			Pos:  p.Position,
		})
	}

	states := make([]*nativeDocState, 0, 64)
	for _, obj := range scan.objects {
		matches, err := obj.segment.SearchPhrase(tokens)
		if err != nil {
			return err
		}
		for _, match := range matches {
			states = append(states, &nativeDocState{
				pk:              decodeNativePK(match.Ref.PK, scan.pkType),
				docLen:          match.DocLen,
				ref:             match.Ref,
				obj:             obj.name,
				segmentKey:      obj.key,
				applyTombstones: obj.applyTombstones,
			})
		}
	}
	liveStates, err := filterLiveNativeDocStates(proc.Ctx, proc.GetService(), scan, newNativeDeleteCache(), states)
	if err != nil {
		return err
	}
	count := int64(len(liveStates))
	for _, state := range liveStates {
		addr, buf, err := u.mpool.NewItem()
		if err != nil {
			return err
		}
		for i := 0; i < s.Nkeywords; i++ {
			buf[i] = 1
		}
		u.agghtab[state.pk] = addr
		u.docLenMap[state.pk] = state.docLen
		markNativeOwned(u, state.pk)
	}
	for i := range u.aggcnt {
		u.aggcnt[i] = count
	}
	return nil
}

func populateBooleanNative(
	u *fulltextState,
	proc *process.Process,
	s *fulltext.SearchAccum,
	scan *nativePreparedScan,
) error {
	leafs := make(map[int32]*fulltext.Pattern, s.Nkeywords)
	phrases := make([]*fulltext.Pattern, 0, 4)
	collectNativePatterns(s.Pattern, leafs, &phrases)
	hasPlus := s.PatternAnyPlus()
	anchorLeafs := collectNativeAnchorLeafIndexes(s.Pattern, hasPlus)
	negativeLeafs := collectNativeNegativeLeafIndexes(s.Pattern)

	docs := make(map[nativeDocKey]*nativeDocState, 1024)
	leafSets := make(map[int32]nativeDocSet, len(leafs))
	for i := 0; i < s.Nkeywords; i++ {
		leafSets[int32(i)] = make(nativeDocSet)
	}

	for _, obj := range scan.objects {
		if err := prefetchBooleanNativeTerms(obj.segment, leafs, phrases); err != nil {
			return err
		}
		for idx, leaf := range leafs {
			postings, err := nativeLookupLeaf(obj.segment, leaf)
			if err != nil {
				return err
			}
			for _, posting := range postings {
				key := makeNativeDocKey(obj.key, posting.Ref)
				_, isAnchorLeaf := anchorLeafs[idx]
				if _, negative := negativeLeafs[idx]; !negative {
					state := docs[key]
					if state == nil {
						if hasPlus && !isAnchorLeaf {
							leafSets[idx][key] = struct{}{}
							continue
						}
						state = &nativeDocState{
							pk:              decodeNativePK(posting.Ref.PK, scan.pkType),
							docLen:          posting.DocLen,
							ref:             posting.Ref,
							obj:             obj.name,
							segmentKey:      obj.key,
							applyTombstones: obj.applyTombstones,
							counts:          make([]uint16, s.Nkeywords),
						}
						docs[key] = state
					}
					tf := uint16(len(posting.Positions))
					if tf == 0 {
						tf = 1
					}
					state.counts[int(idx)] += tf
				}
				leafSets[idx][key] = struct{}{}
			}
		}
	}
	for i := range u.aggcnt {
		u.aggcnt[i] = int64(len(leafSets[int32(i)]))
	}

	phraseSets := make(map[*fulltext.Pattern]nativeDocSet, len(phrases))
	for _, phrase := range phrases {
		phraseSets[phrase] = make(nativeDocSet)
	}
	for _, obj := range scan.objects {
		for _, phrase := range phrases {
			tokens := make([]ftnative.PhraseToken, 0, len(phrase.Children))
			for _, child := range phrase.Children {
				tokens = append(tokens, ftnative.PhraseToken{
					Word: child.Text,
					Pos:  child.Position,
				})
			}
			matches, err := obj.segment.SearchPhrase(tokens)
			if err != nil {
				return err
			}
			for _, match := range matches {
				phraseSets[phrase][makeNativeDocKey(obj.key, match.Ref)] = struct{}{}
			}
		}
	}

	candidates := nativeCandidateSet(s, leafSets, phraseSets)
	states := make([]*nativeDocState, 0, len(candidates))
	for key := range candidates {
		if state := docs[key]; state != nil {
			states = append(states, state)
		}
	}
	liveStates, err := filterLiveNativeDocStates(proc.Ctx, proc.GetService(), scan, newNativeDeleteCache(), states)
	if err != nil {
		return err
	}
	for _, state := range liveStates {
		addr, buf, err := u.mpool.NewItem()
		if err != nil {
			return err
		}
		for i, cnt := range state.counts {
			if cnt > 255 {
				buf[i] = 255
			} else {
				buf[i] = uint8(cnt)
			}
		}
		u.agghtab[state.pk] = addr
		u.docLenMap[state.pk] = state.docLen
		markNativeOwned(u, state.pk)
	}
	return nil
}

func getNativeVisibleObjectStats(ctx context.Context, rel engine.Relation) ([]objectio.ObjectStats, error) {
	if visibleRel, ok := rel.(visibleObjectStatsRelation); ok {
		return visibleRel.GetVisibleObjectStats(ctx)
	}
	return rel.GetNonAppendableObjectStats(ctx)
}

func prefetchBooleanNativeTerms(
	seg *ftnative.Segment,
	leafs map[int32]*fulltext.Pattern,
	phrases []*fulltext.Pattern,
) error {
	terms := make([]string, 0, len(leafs)+len(phrases)*2)
	seen := make(map[string]struct{}, len(leafs)+len(phrases)*2)
	appendTerm := func(term string) {
		if term == "" {
			return
		}
		if _, ok := seen[term]; ok {
			return
		}
		seen[term] = struct{}{}
		terms = append(terms, term)
	}
	for _, leaf := range leafs {
		collectNativeLeafExactTerms(leaf, appendTerm)
	}
	for _, phrase := range phrases {
		for _, child := range phrase.Children {
			appendTerm(child.Text)
		}
	}
	if len(terms) == 0 {
		return nil
	}
	return seg.PrefetchTerms(terms)
}

func markNativeOwned(u *fulltextState, pk any) {
	if u.nativeOwned == nil {
		u.nativeOwned = make(map[any]struct{}, 1024)
	}
	u.nativeOwned[pk] = struct{}{}
}

func collectNativePatterns(patterns []*fulltext.Pattern, leafs map[int32]*fulltext.Pattern, phrases *[]*fulltext.Pattern) {
	for _, p := range patterns {
		switch p.Operator {
		case fulltext.TEXT, fulltext.STAR, fulltext.JOIN:
			leafs[p.Index] = p
		case fulltext.PHRASE:
			*phrases = append(*phrases, p)
			collectNativePatterns(p.Children, leafs, phrases)
		default:
			collectNativePatterns(p.Children, leafs, phrases)
		}
	}
}

func collectNativeAnchorLeafIndexes(patterns []*fulltext.Pattern, hasPlus bool) map[int32]struct{} {
	if !hasPlus || len(patterns) == 0 {
		return nil
	}
	anchor := make(map[int32]struct{})
	collectNativeLeafIndexes(patterns[0], anchor)
	return anchor
}

func collectNativeNegativeLeafIndexes(patterns []*fulltext.Pattern) map[int32]struct{} {
	negative := make(map[int32]struct{})
	for _, p := range patterns {
		if p.Operator != fulltext.MINUS || len(p.Children) == 0 {
			continue
		}
		collectNativeLeafIndexes(p.Children[0], negative)
	}
	return negative
}

func collectNativeLeafIndexes(pattern *fulltext.Pattern, out map[int32]struct{}) {
	if pattern == nil {
		return
	}
	switch pattern.Operator {
	case fulltext.TEXT, fulltext.STAR, fulltext.JOIN:
		out[pattern.Index] = struct{}{}
	default:
		for _, child := range pattern.Children {
			collectNativeLeafIndexes(child, out)
		}
	}
}

func nativeLookupLeaf(seg *ftnative.Segment, leaf *fulltext.Pattern) ([]ftnative.Posting, error) {
	switch leaf.Operator {
	case fulltext.TEXT:
		return seg.Lookup(leaf.Text)
	case fulltext.STAR:
		prefix := strings.TrimSuffix(leaf.Text, "*")
		return seg.LookupPrefix(prefix)
	case fulltext.JOIN:
		return nativeLookupJoin(seg, leaf)
	default:
		return nil, nil
	}
}

func collectNativeLeafExactTerms(leaf *fulltext.Pattern, appendTerm func(string)) {
	if leaf == nil {
		return
	}
	switch leaf.Operator {
	case fulltext.TEXT:
		appendTerm(leaf.Text)
	case fulltext.JOIN:
		for _, child := range leaf.Children {
			collectNativeLeafExactTerms(nativeJoinValuePattern(child), appendTerm)
		}
	}
}

func nativeJoinValuePattern(pattern *fulltext.Pattern) *fulltext.Pattern {
	if pattern == nil {
		return nil
	}
	if pattern.Operator == fulltext.PLUS && len(pattern.Children) == 1 {
		return pattern.Children[0]
	}
	return pattern
}

type nativeJoinMatch struct {
	ref    ftnative.RowRef
	docLen int32
	tf     int
}

type nativeJoinDocKey struct {
	block uint16
	row   uint32
}

func nativeLookupJoin(seg *ftnative.Segment, leaf *fulltext.Pattern) ([]ftnative.Posting, error) {
	if leaf == nil || len(leaf.Children) == 0 {
		return nil, nil
	}
	var matches map[nativeJoinDocKey]nativeJoinMatch
	for i, child := range leaf.Children {
		value := nativeJoinValuePattern(child)
		if value == nil {
			return nil, nil
		}
		postings, err := nativeLookupLeaf(seg, value)
		if err != nil {
			return nil, err
		}
		if len(postings) == 0 {
			return nil, nil
		}
		next := make(map[nativeJoinDocKey]nativeJoinMatch, len(postings))
		for _, posting := range postings {
			tf := len(posting.Positions)
			if tf == 0 {
				tf = 1
			}
			key := nativeJoinDocKey{block: posting.Ref.Block, row: posting.Ref.Row}
			if i == 0 {
				next[key] = nativeJoinMatch{
					ref:    posting.Ref,
					docLen: posting.DocLen,
					tf:     tf,
				}
				continue
			}
			prev, ok := matches[key]
			if !ok {
				continue
			}
			prev.tf += tf
			next[key] = prev
		}
		matches = next
		if len(matches) == 0 {
			return nil, nil
		}
	}
	keys := make([]nativeJoinDocKey, 0, len(matches))
	for key := range matches {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].block != keys[j].block {
			return keys[i].block < keys[j].block
		}
		return keys[i].row < keys[j].row
	})
	postings := make([]ftnative.Posting, 0, len(keys))
	for _, key := range keys {
		match := matches[key]
		postings = append(postings, ftnative.Posting{
			Ref:       match.ref,
			DocLen:    match.docLen,
			Positions: make([]int32, match.tf),
		})
	}
	return postings, nil
}

func nativeCandidateSet(
	s *fulltext.SearchAccum,
	leafSets map[int32]nativeDocSet,
	phraseSets map[*fulltext.Pattern]nativeDocSet,
) nativeDocSet {
	var result nativeDocSet
	hasPlus := s.PatternAnyPlus()
	for _, p := range s.Pattern {
		arg := nativePatternSet(p, leafSets, phraseSets)
		switch p.Operator {
		case fulltext.MINUS:
			if result != nil {
				nativeDifference(result, arg)
			} else {
				result = make(nativeDocSet)
			}
		case fulltext.PLUS, fulltext.JOIN:
			if result == nil {
				result = nativeCloneSet(arg)
			} else {
				result = nativeIntersect(result, arg)
			}
		default:
			if !hasPlus {
				if result == nil {
					result = nativeCloneSet(arg)
				} else {
					nativeUnion(result, arg)
				}
			} else if result == nil {
				result = nativeCloneSet(arg)
			}
		}
	}
	if result == nil {
		return make(nativeDocSet)
	}
	return result
}

func nativePatternSet(
	p *fulltext.Pattern,
	leafSets map[int32]nativeDocSet,
	phraseSets map[*fulltext.Pattern]nativeDocSet,
) nativeDocSet {
	switch p.Operator {
	case fulltext.TEXT, fulltext.STAR, fulltext.JOIN:
		return leafSets[p.Index]
	case fulltext.PHRASE:
		return phraseSets[p]
	case fulltext.PLUS, fulltext.MINUS, fulltext.LESSTHAN, fulltext.GREATERTHAN, fulltext.RANKLESS:
		if len(p.Children) == 0 {
			return make(nativeDocSet)
		}
		return nativePatternSet(p.Children[0], leafSets, phraseSets)
	case fulltext.GROUP:
		ret := make(nativeDocSet)
		for _, child := range p.Children {
			nativeUnion(ret, nativePatternSet(child, leafSets, phraseSets))
		}
		return ret
	default:
		return make(nativeDocSet)
	}
}

func newNativeDeleteCache() *nativeDeleteCache {
	return &nativeDeleteCache{
		hasBlock: make(map[string]bool),
		deleted:  make(map[string]map[uint32]bool),
	}
}

type nativeDeleteGroup struct {
	blockKey string
	obj      objectio.ObjectName
	block    uint16
	indexes  []int
}

func filterLiveNativeDocStates(
	ctx context.Context,
	service string,
	scan *nativePreparedScan,
	cache *nativeDeleteCache,
	states []*nativeDocState,
) ([]*nativeDocState, error) {
	if len(states) == 0 || scan == nil || scan.tombstones == nil {
		return states, nil
	}
	if !scan.tombstones.HasAnyInMemoryTombstone() && !scan.tombstones.HasAnyTombstoneFile() {
		return states, nil
	}

	live := make([]bool, len(states))
	groups := make(map[string]*nativeDeleteGroup)
	for i, state := range states {
		if state == nil {
			continue
		}
		if !state.applyTombstones {
			live[i] = true
			continue
		}
		blockKey := nativeBlockKey(state.segmentKey, state.ref.Block)
		group := groups[blockKey]
		if group == nil {
			group = &nativeDeleteGroup{
				blockKey: blockKey,
				obj:      state.obj,
				block:    state.ref.Block,
			}
			groups[blockKey] = group
		}
		group.indexes = append(group.indexes, i)
	}

	if scan.tombstones.HasAnyTombstoneFile() && len(groups) > 0 {
		bids := make([]objectio.Blockid, 0, len(groups))
		for _, group := range groups {
			bid := objectio.NewBlockidWithObjectID(group.obj.ObjectId(), group.block)
			bids = append(bids, bid)
		}
		scan.tombstones.PrefetchTombstones(service, scan.fs, bids)
	}

	for _, group := range groups {
		if err := filterLiveNativeDeleteGroup(ctx, scan, cache, states, live, group); err != nil {
			return nil, err
		}
	}

	out := make([]*nativeDocState, 0, len(states))
	for i, state := range states {
		if live[i] {
			out = append(out, state)
		}
	}
	return out, nil
}

func filterLiveNativeDeleteGroup(
	ctx context.Context,
	scan *nativePreparedScan,
	cache *nativeDeleteCache,
	states []*nativeDocState,
	live []bool,
	group *nativeDeleteGroup,
) error {
	if group == nil {
		return nil
	}

	has, ok := cache.hasBlock[group.blockKey]
	if !ok {
		bid := objectio.NewBlockidWithObjectID(group.obj.ObjectId(), group.block)
		var err error
		has, err = scan.tombstones.HasBlockTombstone(ctx, &bid, scan.fs)
		if err != nil {
			return err
		}
		cache.hasBlock[group.blockKey] = has
	}

	rowCache := cache.deleted[group.blockKey]
	if rowCache == nil {
		rowCache = make(map[uint32]bool)
		cache.deleted[group.blockKey] = rowCache
	}
	if !has {
		for _, idx := range group.indexes {
			row := states[idx].ref.Row
			rowCache[row] = false
			live[idx] = true
		}
		return nil
	}

	rowIndexes := make(map[uint32][]int, len(group.indexes))
	pendingRows := make([]uint32, 0, len(group.indexes))
	for _, idx := range group.indexes {
		row := states[idx].ref.Row
		if deleted, ok := rowCache[row]; ok {
			if !deleted {
				live[idx] = true
			}
			continue
		}
		if _, ok := rowIndexes[row]; !ok {
			pendingRows = append(pendingRows, row)
		}
		rowIndexes[row] = append(rowIndexes[row], idx)
	}
	if len(pendingRows) == 0 {
		return nil
	}

	sort.Slice(pendingRows, func(i, j int) bool {
		return pendingRows[i] < pendingRows[j]
	})
	rows := make([]int64, len(pendingRows))
	for i, row := range pendingRows {
		rows[i] = int64(row)
	}

	bid := objectio.NewBlockidWithObjectID(group.obj.ObjectId(), group.block)
	rows = scan.tombstones.ApplyInMemTombstones(&bid, rows, nil)
	if len(rows) > 0 {
		var err error
		rows, err = scan.tombstones.ApplyPersistedTombstones(
			ctx,
			scan.fs,
			&scan.snapshot,
			&bid,
			rows,
			nil,
		)
		if err != nil {
			return err
		}
	}

	liveRows := make(map[uint32]struct{}, len(rows))
	for _, row := range rows {
		liveRows[uint32(row)] = struct{}{}
	}
	for _, row := range pendingRows {
		_, ok := liveRows[row]
		rowCache[row] = !ok
		if !ok {
			continue
		}
		for _, idx := range rowIndexes[row] {
			live[idx] = true
		}
	}
	return nil
}

func isNativeDeleted(
	ctx context.Context,
	scan *nativePreparedScan,
	cache *nativeDeleteCache,
	obj nativeObjectSegment,
	ref ftnative.RowRef,
) (bool, error) {
	if !obj.applyTombstones {
		return false, nil
	}
	bid := objectio.NewBlockidWithObjectID(obj.name.ObjectId(), ref.Block)
	blockKey := nativeBlockKey(obj.key, ref.Block)
	has, ok := cache.hasBlock[blockKey]
	if !ok {
		var err error
		has, err = scan.tombstones.HasBlockTombstone(ctx, &bid, scan.fs)
		if err != nil {
			return false, err
		}
		cache.hasBlock[blockKey] = has
	}
	if !has {
		return false, nil
	}

	if cache.deleted[blockKey] == nil {
		cache.deleted[blockKey] = make(map[uint32]bool)
	}
	if deleted, ok := cache.deleted[blockKey][ref.Row]; ok {
		return deleted, nil
	}

	rows := []int64{int64(ref.Row)}
	rows = scan.tombstones.ApplyInMemTombstones(&bid, rows, nil)
	if len(rows) > 0 {
		var err error
		rows, err = scan.tombstones.ApplyPersistedTombstones(
			ctx,
			scan.fs,
			&scan.snapshot,
			&bid,
			rows,
			nil,
		)
		if err != nil {
			return false, err
		}
	}
	deleted := len(rows) == 0
	cache.deleted[blockKey][ref.Row] = deleted
	return deleted, nil
}

func buildNativeTailSegments(
	proc *process.Process,
	rel engine.Relation,
	tableDef *pbplan.TableDef,
	param fulltext.FullTextParserParam,
) ([]nativeObjectSegment, int64, int64, error) {
	readAttrs, colTypes, pkType, err := buildNativeTailReadAttrs(tableDef, param.Parts)
	if err != nil {
		return nil, 0, 0, err
	}
	relData, err := rel.Ranges(proc.Ctx, engine.RangesParam{
		PreAllocBlocks:     2,
		TxnOffset:          0,
		Policy:             engine.Policy_CollectCommittedInmemData | engine.Policy_CollectUncommittedData,
		DontSupportRelData: false,
	})
	if err != nil {
		return nil, 0, 0, err
	}
	readers, err := rel.BuildReaders(
		proc.Ctx,
		proc,
		nil,
		relData,
		1,
		0,
		false,
		engine.Policy_CheckAll,
		engine.FilterHint{},
	)
	if err != nil {
		return nil, 0, 0, err
	}

	builders := make(map[string]*nativeTailSegmentBuilder)
	for _, reader := range readers {
		readBatch := batch.NewWithSize(len(readAttrs))
		readBatch.SetAttributes(readAttrs)
		for i := range readAttrs {
			readBatch.Vecs[i] = vector.NewVec(colTypes[i])
		}
		resolved, err := resolveNativeTailBatchAttrs(readBatch, tableDef.Pkey.PkeyColName, param.Parts)
		if err != nil {
			readBatch.Clean(proc.Mp())
			reader.Close()
			return nil, 0, 0, err
		}
		func() {
			defer readBatch.Clean(proc.Mp())
			defer reader.Close()
			for {
				isEnd, readErr := reader.Read(proc.Ctx, readAttrs, nil, proc.Mp(), readBatch)
				if readErr != nil {
					err = readErr
					return
				}
				if isEnd {
					return
				}
				if readBatch.RowCount() == 0 {
					readBatch.CleanOnlyData()
					continue
				}
				readErr = appendNativeTailBatch(
					builders,
					readBatch,
					resolved,
					pkType,
					param,
				)
				readBatch.CleanOnlyData()
				if readErr != nil {
					err = readErr
					return
				}
			}
		}()
		if err != nil {
			return nil, 0, 0, err
		}
	}
	objects, totalDocs, totalTokens := buildNativeTailSegmentsFromBuilders(builders)
	return objects, totalDocs, totalTokens, nil
}

func buildNativeTailReadAttrs(
	tableDef *pbplan.TableDef,
	parts []string,
) ([]string, []types.Type, types.T, error) {
	pkIdx, ok := tableDef.Name2ColIndex[tableDef.Pkey.PkeyColName]
	if !ok {
		return nil, nil, 0, moerr.NewInternalErrorNoCtx("native fulltext tail scan missing primary key column")
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
	appendAttr(catalog.Row_ID, types.T_Rowid.ToType())
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
			return nil, nil, 0, moerr.NewInternalErrorNoCtx("native fulltext tail scan missing indexed column")
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

func resolveNativeTailBatchAttrs(
	bat *batch.Batch,
	pkName string,
	parts []string,
) (nativeTailBatchAttrs, error) {
	attrMap := make(map[string]int, len(bat.Attrs))
	for i, attr := range bat.Attrs {
		attrMap[strings.ToLower(attr)] = i
	}

	rowIDIdx, ok := attrMap[strings.ToLower(catalog.Row_ID)]
	if !ok {
		return nativeTailBatchAttrs{}, moerr.NewInternalErrorNoCtx("native fulltext tail scan missing rowid column in batch")
	}
	pkIdx, ok := attrMap[strings.ToLower(pkName)]
	if !ok {
		return nativeTailBatchAttrs{}, moerr.NewInternalErrorNoCtx("native fulltext tail scan missing primary key column in batch")
	}

	partIdxes := make([]int, 0, len(parts))
	partTypes := make([]types.T, 0, len(parts))
	for _, part := range parts {
		colIdx, ok := attrMap[strings.ToLower(part)]
		if !ok {
			return nativeTailBatchAttrs{}, moerr.NewInternalErrorNoCtx("native fulltext tail scan missing indexed column in batch")
		}
		partIdxes = append(partIdxes, colIdx)
		partTypes = append(partTypes, bat.Vecs[colIdx].GetType().Oid)
	}

	return nativeTailBatchAttrs{
		rowIDIdx:  rowIDIdx,
		pkIdx:     pkIdx,
		partIdxes: partIdxes,
		partTypes: partTypes,
	}, nil
}

func appendNativeTailBatch(
	builders map[string]*nativeTailSegmentBuilder,
	bat *batch.Batch,
	resolved nativeTailBatchAttrs,
	pkType types.T,
	param fulltext.FullTextParserParam,
) error {
	if bat == nil || bat.RowCount() == 0 {
		return nil
	}
	for row := 0; row < bat.RowCount(); row++ {
		values, ok := collectNativeTailIndexValues(bat, row, resolved.partIdxes, resolved.partTypes)
		if !ok {
			continue
		}
		rowID := vector.GetFixedAtNoTypeCheck[types.Rowid](bat.Vecs[resolved.rowIDIdx], row)
		objName := objectio.BuildObjectNameWithObjectID(rowID.BorrowObjectID())
		objKey := objName.String()
		tailBuilder := builders[objKey]
		if tailBuilder == nil {
			tailBuilder = &nativeTailSegmentBuilder{
				name:    objName,
				builder: ftnative.NewBuilder(param, nil),
			}
			builders[objKey] = tailBuilder
		}
		pkBytes := types.EncodeValue(vector.GetAny(bat.Vecs[resolved.pkIdx], row, true), pkType)
		if err := tailBuilder.builder.Add(ftnative.Document{
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

func collectNativeTailIndexValues(
	bat *batch.Batch,
	row int,
	partIdxes []int,
	partTypes []types.T,
) ([]fulltext.IndexValue, bool) {
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
		return nil, false
	}
	return values, true
}

func buildNativeTailSegmentsFromBuilders(
	builders map[string]*nativeTailSegmentBuilder,
) ([]nativeObjectSegment, int64, int64) {
	if len(builders) == 0 {
		return nil, 0, 0
	}
	keys := make([]string, 0, len(builders))
	for key := range builders {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	objects := make([]nativeObjectSegment, 0, len(keys))
	totalDocs := int64(0)
	totalTokens := int64(0)
	for _, key := range keys {
		seg := builders[key].builder.Build()
		if seg.DocCount == 0 {
			continue
		}
		objects = append(objects, nativeObjectSegment{
			key:             key,
			name:            builders[key].name,
			segment:         seg,
			applyTombstones: true,
		})
		totalDocs += seg.DocCount
		totalTokens += seg.TokenSum
	}
	return objects, totalDocs, totalTokens
}

func hasDatalinkPart(tableDef *pbplan.TableDef, parts []string) bool {
	for _, part := range parts {
		idx, ok := tableDef.Name2ColIndex[part]
		if !ok {
			continue
		}
		if types.T(tableDef.Cols[idx].Typ.Id) == types.T_datalink {
			return true
		}
	}
	return false
}

func parseQualifiedTableName(name string) (string, string, error) {
	parts := strings.Split(strings.TrimSpace(name), ".")
	if len(parts) != 2 {
		return "", "", moerr.NewInternalErrorNoCtx("invalid fulltext table name")
	}
	return trimQuotedIdent(parts[0]), trimQuotedIdent(parts[1]), nil
}

func trimQuotedIdent(s string) string {
	return strings.Trim(strings.TrimSpace(s), "`")
}

func decodeNativePK(raw []byte, typ types.T) any {
	v := types.DecodeValue(raw, typ)
	if bs, ok := v.([]byte); ok {
		return string(bs)
	}
	return v
}

func makeNativeDocKey(objKey string, ref ftnative.RowRef) nativeDocKey {
	return nativeDocKey{
		segmentKey: objKey,
		block:      ref.Block,
		row:        ref.Row,
	}
}

func nativeBlockKey(objKey string, blk uint16) string {
	return objKey + "#" + strconv.FormatUint(uint64(blk), 10)
}

func nativeCloneSet(src nativeDocSet) nativeDocSet {
	dst := make(nativeDocSet, len(src))
	for k := range src {
		dst[k] = struct{}{}
	}
	return dst
}

func nativeUnion(dst, src nativeDocSet) {
	for k := range src {
		dst[k] = struct{}{}
	}
}

func nativeIntersect(dst, src nativeDocSet) nativeDocSet {
	ret := make(nativeDocSet)
	for k := range dst {
		if _, ok := src[k]; ok {
			ret[k] = struct{}{}
		}
	}
	return ret
}

func nativeDifference(dst, src nativeDocSet) {
	for k := range src {
		delete(dst, k)
	}
}
