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
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fulltext"
)

var (
	segmentMagicV1 = [8]byte{'M', 'O', 'F', 'T', 'S', 'N', '1', 0}
	segmentMagicV2 = [8]byte{'M', 'O', 'F', 'T', 'S', 'N', '2', 0}
	segmentMagicV3 = [8]byte{'M', 'O', 'F', 'T', 'S', 'N', '3', 0}
	segmentMagicV4 = [8]byte{'M', 'O', 'F', 'T', 'S', 'N', '4', 0}
)

const (
	segmentPrefixLen    = uint32(12)
	segmentHeaderLenV3  = uint32(16)
	segmentHeaderLenV4  = uint32(24)
	segmentMaxHeaderLen = uint32(1 << 20)
)

type RowRef struct {
	Block uint16
	Row   uint32
	PK    []byte
}

type Posting struct {
	Ref       RowRef
	DocLen    int32
	Positions []int32
}

type Match struct {
	Ref       RowRef
	DocLen    int32
	Positions map[string][]int32
}

type PhraseToken struct {
	Word string
	Pos  int32
}

type Document struct {
	Block  uint16
	Row    uint32
	PK     []byte
	Values []fulltext.IndexValue
}

type Segment struct {
	Terms    map[string][]Posting
	DocCount int64
	TokenSum int64

	mu              sync.RWMutex
	termOrder       []string
	lazyData        []byte
	lazyTerms       map[string]segmentTermMeta
	lazyLoader      segmentTermLoader
	lazyBatchLoader segmentTermBatchLoader
}

type segmentTermMeta struct {
	offset       int64
	size         int64
	postingCount uint32
}

type segmentTermLoader func(offset, size int64) ([]byte, error)

type segmentTermBatchLoader func(requests []segmentTermLoadRequest) (map[string][]byte, error)

type segmentTermLoadRequest struct {
	term string
	meta segmentTermMeta
}

type segmentHeaderV4 struct {
	DocCount      int64
	TokenSum      int64
	TermCount     uint32
	DirectorySize uint32
}

type Builder struct {
	param          fulltext.FullTextParserParam
	resolveDatalnk fulltext.DatalinkTextResolver
	terms          map[string]map[string]*postingBuilder
	docCount       int64
	tokenSum       int64
}

type postingBuilder struct {
	ref       RowRef
	docLen    int32
	positions []int32
}

type rowKey struct {
	block uint16
	row   uint32
	pk    string
}

func NewBuilder(
	param fulltext.FullTextParserParam,
	resolveDatalink fulltext.DatalinkTextResolver,
) *Builder {
	return &Builder{
		param:          param,
		resolveDatalnk: resolveDatalink,
		terms:          make(map[string]map[string]*postingBuilder),
	}
}

func (b *Builder) Add(doc Document) error {
	tokens, err := fulltext.TokenizeIndexValues(b.param, doc.Values, b.resolveDatalnk)
	if err != nil {
		return err
	}
	if len(tokens) == 0 {
		return nil
	}

	grouped := make(map[string][]int32)
	for _, token := range tokens {
		grouped[token.Word] = append(grouped[token.Word], token.Pos)
	}

	key := encodeRowKey(rowKey{
		block: doc.Block,
		row:   doc.Row,
		pk:    string(doc.PK),
	})
	ref := RowRef{
		Block: doc.Block,
		Row:   doc.Row,
		PK:    bytes.Clone(doc.PK),
	}
	docLen := int32(len(tokens))
	b.docCount++
	b.tokenSum += int64(docLen)
	for term, positions := range grouped {
		if _, ok := b.terms[term]; !ok {
			b.terms[term] = make(map[string]*postingBuilder)
		}
		b.terms[term][key] = &postingBuilder{
			ref:       cloneRowRef(ref),
			docLen:    docLen,
			positions: append([]int32(nil), positions...),
		}
	}
	return nil
}

func (b *Builder) Build() *Segment {
	segment := &Segment{
		Terms:    make(map[string][]Posting, len(b.terms)),
		DocCount: b.docCount,
		TokenSum: b.tokenSum,
	}
	for term, postingsByRow := range b.terms {
		postings := make([]Posting, 0, len(postingsByRow))
		for _, posting := range postingsByRow {
			postings = append(postings, Posting{
				Ref:       cloneRowRef(posting.ref),
				DocLen:    posting.docLen,
				Positions: append([]int32(nil), posting.positions...),
			})
		}
		sortPostings(postings)
		segment.Terms[term] = postings
	}
	return segment
}

func (s *Segment) Lookup(term string) ([]Posting, error) {
	postings, err := s.loadTermPostings(term)
	if err != nil {
		return nil, err
	}
	if len(postings) == 0 {
		return nil, nil
	}
	return clonePostings(postings), nil
}

func (s *Segment) PrefetchTerms(terms []string) error {
	return s.ensureTermsLoaded(terms)
}

func (s *Segment) LookupPrefix(prefix string) ([]Posting, error) {
	terms := s.termNames()
	matchedTerms := make([]string, 0, 8)
	for _, term := range terms {
		if strings.HasPrefix(term, prefix) {
			matchedTerms = append(matchedTerms, term)
		}
	}
	if len(matchedTerms) == 0 {
		return nil, nil
	}
	if err := s.ensureTermsLoaded(matchedTerms); err != nil {
		return nil, err
	}
	postings := make([]Posting, 0, 8)
	for _, term := range matchedTerms {
		termPostings, err := s.loadTermPostings(term)
		if err != nil {
			return nil, err
		}
		postings = append(postings, termPostings...)
	}
	if len(postings) == 0 {
		return nil, nil
	}
	sortPostings(postings)
	return clonePostings(postings), nil
}

func (s *Segment) SearchAll(words []string) ([]Match, error) {
	if len(words) == 0 {
		return nil, nil
	}

	type searchTerm struct {
		word         string
		postingCount uint32
	}
	ordered := make([]searchTerm, 0, len(words))
	for _, word := range words {
		postingCount, ok := s.termPostingCount(word)
		if !ok || postingCount == 0 {
			return nil, nil
		}
		ordered = append(ordered, searchTerm{
			word:         word,
			postingCount: postingCount,
		})
	}
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].postingCount != ordered[j].postingCount {
			return ordered[i].postingCount < ordered[j].postingCount
		}
		return ordered[i].word < ordered[j].word
	})
	orderedWords := make([]string, 0, len(ordered))
	for _, term := range ordered {
		orderedWords = append(orderedWords, term.word)
	}
	if err := s.ensureTermsLoaded(orderedWords); err != nil {
		return nil, err
	}

	candidates := make(map[string]*Match)
	for i, term := range ordered {
		postings, err := s.loadTermPostings(term.word)
		if err != nil {
			return nil, err
		}
		if len(postings) == 0 {
			return nil, nil
		}
		if i == 0 {
			for _, posting := range postings {
				key := encodeRowKey(rowKey{
					block: posting.Ref.Block,
					row:   posting.Ref.Row,
					pk:    string(posting.Ref.PK),
				})
				candidates[key] = &Match{
					Ref:    cloneRowRef(posting.Ref),
					DocLen: posting.DocLen,
					Positions: map[string][]int32{
						term.word: append([]int32(nil), posting.Positions...),
					},
				}
			}
			continue
		}

		next := make(map[string]*Match)
		for _, posting := range postings {
			key := encodeRowKey(rowKey{
				block: posting.Ref.Block,
				row:   posting.Ref.Row,
				pk:    string(posting.Ref.PK),
			})
			match := candidates[key]
			if match == nil {
				continue
			}
			clone := cloneMatch(match)
			clone.Positions[term.word] = append([]int32(nil), posting.Positions...)
			next[key] = clone
		}
		candidates = next
		if len(candidates) == 0 {
			return nil, nil
		}
	}

	keys := make([]string, 0, len(candidates))
	for key := range candidates {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	matches := make([]Match, 0, len(keys))
	for _, key := range keys {
		matches = append(matches, *candidates[key])
	}
	return matches, nil
}

func (s *Segment) SearchPhrase(tokens []PhraseToken) ([]Match, error) {
	if len(tokens) == 0 {
		return nil, nil
	}

	unique := make([]string, 0, len(tokens))
	seen := make(map[string]struct{}, len(tokens))
	for _, token := range tokens {
		if _, ok := seen[token.Word]; ok {
			continue
		}
		seen[token.Word] = struct{}{}
		unique = append(unique, token.Word)
	}

	candidates, err := s.SearchAll(unique)
	if err != nil {
		return nil, err
	}
	if len(candidates) == 0 {
		return nil, nil
	}

	matches := make([]Match, 0, len(candidates))
	for _, candidate := range candidates {
		if hasPhrase(candidate.Positions, tokens) {
			matches = append(matches, candidate)
		}
	}
	return matches, nil
}

func hasPhrase(positionsByWord map[string][]int32, tokens []PhraseToken) bool {
	first := positionsByWord[tokens[0].Word]
	if len(first) == 0 {
		return false
	}

	lookups := make(map[string]map[int32]struct{}, len(positionsByWord))
	for word, positions := range positionsByWord {
		set := make(map[int32]struct{}, len(positions))
		for _, pos := range positions {
			set[pos] = struct{}{}
		}
		lookups[word] = set
	}

	baseOffset := tokens[0].Pos
	for _, basePos := range first {
		found := true
		for _, token := range tokens[1:] {
			expected := basePos + (token.Pos - baseOffset)
			if _, ok := lookups[token.Word][expected]; !ok {
				found = false
				break
			}
		}
		if found {
			return true
		}
	}
	return false
}

func (s *Segment) MarshalBinary() ([]byte, error) {
	if s == nil {
		return nil, nil
	}

	terms := s.termNames()
	postingBlobs := make(map[string][]byte, len(terms))
	directorySize := uint32(0)
	for _, term := range terms {
		postings, err := s.loadTermPostings(term)
		if err != nil {
			return nil, err
		}
		postings = append([]Posting(nil), postings...)
		sortPostings(postings)
		blob, err := encodePostings(postings)
		if err != nil {
			return nil, err
		}
		postingBlobs[term] = blob
		directorySize += uint32(4 + len(term) + 4 + 8 + 8)
	}

	postingsStart := int64(segmentPrefixLen + segmentHeaderLenV4 + directorySize)
	var buf bytes.Buffer
	if _, err := buf.Write(segmentMagicV4[:]); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.LittleEndian, segmentHeaderLenV4); err != nil {
		return nil, err
	}
	header := segmentHeaderV4{
		DocCount:      s.DocCount,
		TokenSum:      s.TokenSum,
		TermCount:     uint32(len(terms)),
		DirectorySize: directorySize,
	}
	if err := binary.Write(&buf, binary.LittleEndian, header.DocCount); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.LittleEndian, header.TokenSum); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.LittleEndian, header.TermCount); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.LittleEndian, header.DirectorySize); err != nil {
		return nil, err
	}

	postingOffset := postingsStart
	for _, term := range terms {
		if err := writeBytes(&buf, []byte(term)); err != nil {
			return nil, err
		}
		postingCount, ok := s.termPostingCount(term)
		if !ok {
			return nil, moerr.NewInternalErrorNoCtx("native fulltext segment missing posting count")
		}
		if err := binary.Write(&buf, binary.LittleEndian, postingCount); err != nil {
			return nil, err
		}
		if err := binary.Write(&buf, binary.LittleEndian, uint64(postingOffset)); err != nil {
			return nil, err
		}
		blob := postingBlobs[term]
		if err := binary.Write(&buf, binary.LittleEndian, uint64(len(blob))); err != nil {
			return nil, err
		}
		postingOffset += int64(len(blob))
	}
	for _, term := range terms {
		if _, err := buf.Write(postingBlobs[term]); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func UnmarshalBinary(data []byte) (*Segment, error) {
	magic, headerLen, err := parseSegmentPrefix(data)
	if err != nil {
		return nil, err
	}

	reader := bytes.NewReader(data[8:])
	segment := &Segment{}
	switch magic {
	case segmentMagicV4:
		if len(data) < int(segmentPrefixLen+headerLen) {
			return nil, moerr.NewInternalErrorNoCtx("native fulltext segment v4 header exceeds payload")
		}
		headerBytes := data[segmentPrefixLen : segmentPrefixLen+headerLen]
		header, err := readSegmentHeaderV4(headerBytes, segment)
		if err != nil {
			return nil, err
		}
		dirStart := int(segmentPrefixLen + headerLen)
		dirEnd := dirStart + int(header.DirectorySize)
		if dirEnd > len(data) {
			return nil, moerr.NewInternalErrorNoCtx("native fulltext segment v4 directory exceeds payload")
		}
		if err := indexSegmentDirectoryV4(data[dirStart:dirEnd], header.TermCount, segment); err != nil {
			return nil, err
		}
		segment.lazyData = data
	case segmentMagicV3:
		if err := readSegmentHeaderV3(reader, segment); err != nil {
			return nil, err
		}
	case segmentMagicV2:
		if err := binary.Read(reader, binary.LittleEndian, &segment.DocCount); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.LittleEndian, &segment.TokenSum); err != nil {
			return nil, err
		}
	case segmentMagicV1:
	default:
		return nil, moerr.NewInternalErrorNoCtx("invalid native fulltext segment magic")
	}

	if magic != segmentMagicV4 {
		termPayload := data[len(data)-reader.Len():]
		if err := indexSegmentTerms(termPayload, segment); err != nil {
			return nil, err
		}
	}
	if magic == segmentMagicV1 {
		segment.rebuildStats()
	}
	return segment, nil
}

func readSegmentHeaderV3(reader *bytes.Reader, segment *Segment) error {
	var headerLen uint32
	if err := binary.Read(reader, binary.LittleEndian, &headerLen); err != nil {
		return err
	}
	if headerLen < segmentHeaderLenV3 {
		return moerr.NewInternalErrorNoCtx("invalid native fulltext segment header length")
	}
	if headerLen > segmentMaxHeaderLen {
		return moerr.NewInternalErrorNoCtx("native fulltext segment header too large")
	}
	if int(headerLen) > reader.Len() {
		return moerr.NewInternalErrorNoCtx("native fulltext segment header exceeds payload")
	}
	header := make([]byte, headerLen)
	if _, err := io.ReadFull(reader, header); err != nil {
		return err
	}
	headerReader := bytes.NewReader(header)
	if err := binary.Read(headerReader, binary.LittleEndian, &segment.DocCount); err != nil {
		return err
	}
	if err := binary.Read(headerReader, binary.LittleEndian, &segment.TokenSum); err != nil {
		return err
	}
	return nil
}

func parseSegmentPrefix(data []byte) ([8]byte, uint32, error) {
	var magic [8]byte
	if len(data) < 8 {
		return magic, 0, io.ErrUnexpectedEOF
	}
	copy(magic[:], data[:8])
	if magic == segmentMagicV3 || magic == segmentMagicV4 {
		if len(data) < int(segmentPrefixLen) {
			return magic, 0, io.ErrUnexpectedEOF
		}
		return magic, binary.LittleEndian.Uint32(data[8:12]), nil
	}
	return magic, 0, nil
}

func readSegmentHeaderV4(header []byte, segment *Segment) (segmentHeaderV4, error) {
	if len(header) < int(segmentHeaderLenV4) {
		return segmentHeaderV4{}, moerr.NewInternalErrorNoCtx("native fulltext segment v4 header too small")
	}
	reader := bytes.NewReader(header)
	var out segmentHeaderV4
	if err := binary.Read(reader, binary.LittleEndian, &out.DocCount); err != nil {
		return segmentHeaderV4{}, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &out.TokenSum); err != nil {
		return segmentHeaderV4{}, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &out.TermCount); err != nil {
		return segmentHeaderV4{}, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &out.DirectorySize); err != nil {
		return segmentHeaderV4{}, err
	}
	segment.DocCount = out.DocCount
	segment.TokenSum = out.TokenSum
	return out, nil
}

func indexSegmentDirectoryV4(data []byte, termCount uint32, segment *Segment) error {
	reader := bytes.NewReader(data)
	segment.Terms = make(map[string][]Posting)
	segment.termOrder = make([]string, 0, termCount)
	segment.lazyTerms = make(map[string]segmentTermMeta, termCount)
	for i := uint32(0); i < termCount; i++ {
		termBytes, err := readBytes(reader)
		if err != nil {
			return err
		}
		term := string(termBytes)
		var postingCount uint32
		if err := binary.Read(reader, binary.LittleEndian, &postingCount); err != nil {
			return err
		}
		var offset uint64
		if err := binary.Read(reader, binary.LittleEndian, &offset); err != nil {
			return err
		}
		var size uint64
		if err := binary.Read(reader, binary.LittleEndian, &size); err != nil {
			return err
		}
		segment.termOrder = append(segment.termOrder, term)
		segment.lazyTerms[term] = segmentTermMeta{
			offset:       int64(offset),
			size:         int64(size),
			postingCount: postingCount,
		}
	}
	if reader.Len() != 0 {
		return moerr.NewInternalErrorNoCtx("native fulltext segment v4 directory length mismatch")
	}
	return nil
}

func indexSegmentTerms(data []byte, segment *Segment) error {
	reader := bytes.NewReader(data)
	var termCount uint32
	if err := binary.Read(reader, binary.LittleEndian, &termCount); err != nil {
		return err
	}
	segment.Terms = make(map[string][]Posting)
	segment.termOrder = make([]string, 0, termCount)
	segment.lazyData = data
	segment.lazyTerms = make(map[string]segmentTermMeta, termCount)
	for i := uint32(0); i < termCount; i++ {
		termBytes, err := readBytes(reader)
		if err != nil {
			return err
		}
		term := string(termBytes)

		var postingCount uint32
		if err := binary.Read(reader, binary.LittleEndian, &postingCount); err != nil {
			return err
		}
		start := int(reader.Size()) - reader.Len()
		if err := skipEncodedPostings(reader, postingCount); err != nil {
			return err
		}
		end := int(reader.Size()) - reader.Len()
		segment.termOrder = append(segment.termOrder, term)
		segment.lazyTerms[term] = segmentTermMeta{
			offset:       int64(start),
			size:         int64(end - start),
			postingCount: postingCount,
		}
	}
	return nil
}

func SidecarPath(objectName string, indexName string) string {
	sum := sha1.Sum([]byte(indexName))
	return fmt.Sprintf("%s.fts.%x.seg", objectName, sum[:8])
}

func cloneRowRef(ref RowRef) RowRef {
	return RowRef{
		Block: ref.Block,
		Row:   ref.Row,
		PK:    bytes.Clone(ref.PK),
	}
}

func cloneMatch(match *Match) *Match {
	positions := make(map[string][]int32, len(match.Positions))
	for word, pos := range match.Positions {
		positions[word] = append([]int32(nil), pos...)
	}
	return &Match{
		Ref:       cloneRowRef(match.Ref),
		DocLen:    match.DocLen,
		Positions: positions,
	}
}

func clonePostings(postings []Posting) []Posting {
	out := make([]Posting, 0, len(postings))
	for _, posting := range postings {
		out = append(out, Posting{
			Ref:       cloneRowRef(posting.Ref),
			DocLen:    posting.DocLen,
			Positions: append([]int32(nil), posting.Positions...),
		})
	}
	return out
}

func sortPostings(postings []Posting) {
	sort.Slice(postings, func(i, j int) bool {
		if postings[i].Ref.Block != postings[j].Ref.Block {
			return postings[i].Ref.Block < postings[j].Ref.Block
		}
		if postings[i].Ref.Row != postings[j].Ref.Row {
			return postings[i].Ref.Row < postings[j].Ref.Row
		}
		return bytes.Compare(postings[i].Ref.PK, postings[j].Ref.PK) < 0
	})
}

func writeBytes(buf *bytes.Buffer, data []byte) error {
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(data))); err != nil {
		return err
	}
	_, err := buf.Write(data)
	return err
}

func readBytes(reader *bytes.Reader) ([]byte, error) {
	var length uint32
	if err := binary.Read(reader, binary.LittleEndian, &length); err != nil {
		return nil, err
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(reader, data); err != nil {
		return nil, err
	}
	return data, nil
}

func skipVarBytes(reader *bytes.Reader) error {
	var length uint32
	if err := binary.Read(reader, binary.LittleEndian, &length); err != nil {
		return err
	}
	return skipBytes(reader, int(length))
}

func skipBytes(reader *bytes.Reader, length int) error {
	if length < 0 || length > reader.Len() {
		return io.ErrUnexpectedEOF
	}
	_, err := reader.Seek(int64(length), io.SeekCurrent)
	return err
}

func skipEncodedPostings(reader *bytes.Reader, postingCount uint32) error {
	for i := uint32(0); i < postingCount; i++ {
		if err := skipEncodedPosting(reader); err != nil {
			return err
		}
	}
	return nil
}

func skipEncodedPosting(reader *bytes.Reader) error {
	var block uint16
	if err := binary.Read(reader, binary.LittleEndian, &block); err != nil {
		return err
	}
	var row uint32
	if err := binary.Read(reader, binary.LittleEndian, &row); err != nil {
		return err
	}
	if err := skipVarBytes(reader); err != nil {
		return err
	}
	var docLen int32
	if err := binary.Read(reader, binary.LittleEndian, &docLen); err != nil {
		return err
	}
	var posCount uint32
	if err := binary.Read(reader, binary.LittleEndian, &posCount); err != nil {
		return err
	}
	return skipBytes(reader, int(posCount)*4)
}

func readPosting(reader *bytes.Reader) (Posting, error) {
	var posting Posting
	if err := binary.Read(reader, binary.LittleEndian, &posting.Ref.Block); err != nil {
		return Posting{}, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &posting.Ref.Row); err != nil {
		return Posting{}, err
	}
	pk, err := readBytes(reader)
	if err != nil {
		return Posting{}, err
	}
	posting.Ref.PK = pk
	if err := binary.Read(reader, binary.LittleEndian, &posting.DocLen); err != nil {
		return Posting{}, err
	}
	var posCount uint32
	if err := binary.Read(reader, binary.LittleEndian, &posCount); err != nil {
		return Posting{}, err
	}
	posting.Positions = make([]int32, 0, posCount)
	for i := uint32(0); i < posCount; i++ {
		var pos int32
		if err := binary.Read(reader, binary.LittleEndian, &pos); err != nil {
			return Posting{}, err
		}
		posting.Positions = append(posting.Positions, pos)
	}
	return posting, nil
}

func encodePostings(postings []Posting) ([]byte, error) {
	var buf bytes.Buffer
	for _, posting := range postings {
		if err := binary.Write(&buf, binary.LittleEndian, posting.Ref.Block); err != nil {
			return nil, err
		}
		if err := binary.Write(&buf, binary.LittleEndian, posting.Ref.Row); err != nil {
			return nil, err
		}
		if err := writeBytes(&buf, posting.Ref.PK); err != nil {
			return nil, err
		}
		if err := binary.Write(&buf, binary.LittleEndian, posting.DocLen); err != nil {
			return nil, err
		}
		if err := binary.Write(&buf, binary.LittleEndian, uint32(len(posting.Positions))); err != nil {
			return nil, err
		}
		for _, pos := range posting.Positions {
			if err := binary.Write(&buf, binary.LittleEndian, pos); err != nil {
				return nil, err
			}
		}
	}
	return buf.Bytes(), nil
}

func decodeEncodedPostings(data []byte, postingCount uint32) ([]Posting, error) {
	reader := bytes.NewReader(data)
	postings := make([]Posting, 0, postingCount)
	for i := uint32(0); i < postingCount; i++ {
		posting, err := readPosting(reader)
		if err != nil {
			return nil, err
		}
		postings = append(postings, posting)
	}
	if reader.Len() != 0 {
		return nil, moerr.NewInternalErrorNoCtx("native fulltext segment postings length mismatch")
	}
	return postings, nil
}

func encodeRowKey(key rowKey) string {
	return fmt.Sprintf("%d/%d/%x", key.block, key.row, key.pk)
}

func (s *Segment) termNames() []string {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	if len(s.termOrder) > 0 {
		names := append([]string(nil), s.termOrder...)
		s.mu.RUnlock()
		return names
	}
	names := make([]string, 0, len(s.Terms))
	for term := range s.Terms {
		names = append(names, term)
	}
	s.mu.RUnlock()
	sort.Strings(names)
	return names
}

func (s *Segment) termPostingCount(term string) (uint32, bool) {
	if s == nil {
		return 0, false
	}
	s.mu.RLock()
	if postings, ok := s.Terms[term]; ok {
		s.mu.RUnlock()
		return uint32(len(postings)), true
	}
	meta, ok := s.lazyTerms[term]
	s.mu.RUnlock()
	if !ok {
		return 0, false
	}
	return meta.postingCount, true
}

func (s *Segment) ensureTermsLoaded(terms []string) error {
	if s == nil || len(terms) == 0 {
		return nil
	}
	s.mu.RLock()
	if len(s.lazyData) > 0 || s.lazyBatchLoader == nil {
		s.mu.RUnlock()
		for _, term := range terms {
			if _, err := s.loadTermPostings(term); err != nil {
				return err
			}
		}
		return nil
	}
	requests := make([]segmentTermLoadRequest, 0, len(terms))
	seen := make(map[string]struct{}, len(terms))
	for _, term := range terms {
		if _, ok := seen[term]; ok {
			continue
		}
		seen[term] = struct{}{}
		if _, ok := s.Terms[term]; ok {
			continue
		}
		meta, ok := s.lazyTerms[term]
		if !ok {
			continue
		}
		requests = append(requests, segmentTermLoadRequest{
			term: term,
			meta: meta,
		})
	}
	batchLoader := s.lazyBatchLoader
	s.mu.RUnlock()
	if len(requests) == 0 {
		return nil
	}
	blobs, err := batchLoader(requests)
	if err != nil {
		return err
	}
	decoded := make(map[string][]Posting, len(requests))
	for _, request := range requests {
		data, ok := blobs[request.term]
		if !ok {
			return moerr.NewInternalErrorNoCtx("native fulltext segment batch loader missing term payload")
		}
		postings, err := decodeEncodedPostings(data, request.meta.postingCount)
		if err != nil {
			return err
		}
		decoded[request.term] = postings
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for term, postings := range decoded {
		if _, ok := s.Terms[term]; ok {
			continue
		}
		s.Terms[term] = postings
	}
	return nil
}

func (s *Segment) loadTermPostings(term string) ([]Posting, error) {
	if s == nil {
		return nil, nil
	}
	s.mu.RLock()
	if postings, ok := s.Terms[term]; ok {
		s.mu.RUnlock()
		return postings, nil
	}
	meta, ok := s.lazyTerms[term]
	lazyData := s.lazyData
	lazyLoader := s.lazyLoader
	s.mu.RUnlock()
	if !ok {
		return nil, nil
	}
	var payload []byte
	if len(lazyData) > 0 {
		if meta.offset < 0 || meta.size < 0 || meta.offset+meta.size > int64(len(lazyData)) {
			return nil, moerr.NewInternalErrorNoCtx("native fulltext segment term offset out of range")
		}
		start := int(meta.offset)
		end := int(meta.offset + meta.size)
		payload = lazyData[start:end]
	} else {
		if lazyLoader == nil {
			return nil, moerr.NewInternalErrorNoCtx("native fulltext segment missing lazy loader")
		}
		var err error
		payload, err = lazyLoader(meta.offset, meta.size)
		if err != nil {
			return nil, err
		}
	}
	postings, err := decodeEncodedPostings(payload, meta.postingCount)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	if existing, ok := s.Terms[term]; ok {
		s.mu.Unlock()
		return existing, nil
	}
	s.Terms[term] = postings
	s.mu.Unlock()
	return postings, nil
}

func (s *Segment) rebuildStats() {
	if s == nil {
		return
	}
	docLens := make(map[string]int32)
	for _, term := range s.termNames() {
		postings, err := s.loadTermPostings(term)
		if err != nil {
			return
		}
		for _, posting := range postings {
			key := encodeRowKey(rowKey{
				block: posting.Ref.Block,
				row:   posting.Ref.Row,
				pk:    string(posting.Ref.PK),
			})
			if _, ok := docLens[key]; ok {
				continue
			}
			docLens[key] = posting.DocLen
			s.DocCount++
			s.TokenSum += int64(posting.DocLen)
		}
	}
}
