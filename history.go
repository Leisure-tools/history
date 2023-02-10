// Package history represents the history of a document as a DAG of operations
// Each document has a unique ID
// Session objects hold peer ID and track pending changes to a document
package history

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"

	doc "github.com/leisure-tools/document"
)

var ErrCollab = errors.New("Collaboration error")
var ErrDivergentBlock = fmt.Errorf("%w, divergent block", ErrCollab)

type document = doc.Document
type Replacement = doc.Replacement
type Sha = [sha256.Size]byte
type Twosha = [sha256.Size * 2]byte
type UUID [16]byte

// History of a document
type History struct {
	DocID         UUID
	Source        *OpBlock
	Latest        map[string]*OpBlock // peer -> block
	Blocks        map[Sha]*OpBlock
	PendingOn     map[Sha]doc.Set[*OpBlock]
	PendingBlocks map[Sha]*OpBlock
	Storage       DocStorage
	LCAs          map[Twosha]*LCA // 2-block LCAs
	BlockOrder    []Sha
	DirtyBlocks   []*OpBlock // stored at the end of a commit
}

type DocStorage interface {
	GetBlock(hash Sha) *OpBlock
	HasBlock(hash Sha) bool
	StoreBlocks(blks []*OpBlock) // removes from pending and pendingOn
	HasPendingBlock(hash Sha) bool
	GetPendingBlock(hash Sha) *OpBlock
	StorePendingBlock(*OpBlock)
	StoreParameters(latest map[string]Sha, pendingOps []Replacement)
	PendingOn(blk Sha, pendingBlock *OpBlock)
	StoreBlockDoc(blk *OpBlock)
}

type Session struct {
	Peer       string
	PendingOps []Replacement
	History    *History
}

// UUIDs
func newUUID() UUID {
	var uuid UUID
	rand.Read(uuid[:])
	return uuid
}

// a block the editing dag
// blocks do not point to each other, they use shas to support deferring to storage
// using descendants and order To find LCA, as in
// [Kowaluk & Lingas (2005)](https://people.cs.nctu.edu.tw/~tjshen/doc/fulltext.pdf)
// All public fields except Hash are transmitted (hash can be computed)
type OpBlock struct {
	Peer             string
	Hash             Sha // not transmitted; computed upon reception
	Nonce            int
	Parents          []Sha
	Replacements     []Replacement
	SelectionOffset  int
	SelectionLength  int
	document         *document // simple cache to speed up successive document edits
	documentAncestor Sha
	blockDoc         *document // frozen document for this block
	children         []Sha
	descendants      doc.Set[Sha]
	order            int // block's index in the session's blockOrder list
}

type LCA struct {
	blkA     Sha // blkA.order < blkB.order
	orderA   int
	blkB     Sha
	orderB   int
	ancestor Sha
}

// this is the default storage
type MemoryStorage struct {
	blocks        map[Sha]*OpBlock
	pendingBlocks map[Sha]*OpBlock
	pendingOn     map[Sha]doc.Set[*OpBlock]
	blockDocs     map[Sha]string
}

///
/// basic storage
///

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		blocks:        map[Sha]*OpBlock{},
		pendingBlocks: map[Sha]*OpBlock{},
		pendingOn:     map[Sha]doc.Set[*OpBlock]{},
		blockDocs:     map[Sha]string{},
	}
}

func (st *MemoryStorage) HasBlock(hash Sha) bool {
	return st.blocks[hash] != nil
}

func (st *MemoryStorage) GetBlock(hash Sha) *OpBlock {
	return st.blocks[hash]
}

func (st *MemoryStorage) StoreBlocks(blks []*OpBlock) {
	for _, blk := range blks {
		st.blocks[blk.Hash] = blk.clean()
		delete(st.pendingBlocks, blk.Hash)
		delete(st.pendingOn, blk.Hash)
	}
}

func (st *MemoryStorage) HasPendingBlock(hash Sha) bool {
	return st.pendingBlocks[hash] != nil
}

func (st *MemoryStorage) GetPendingBlock(hash Sha) *OpBlock {
	return st.pendingBlocks[hash]
}

func (st *MemoryStorage) StorePendingBlock(blk *OpBlock) {
	st.pendingBlocks[blk.Hash] = blk
}

func (st *MemoryStorage) StoreParameters(latest map[string]Sha, pendingOps []Replacement) {
}

func (st *MemoryStorage) Dirty(blk *OpBlock) {}

func (st *MemoryStorage) PendingOn(hash Sha, pendingBlock *OpBlock) {
	if st.pendingOn[hash] == nil {
		st.pendingOn[hash] = doc.Set[*OpBlock]{}
	}
	st.pendingOn[hash].Add(pendingBlock)
}

func (st *MemoryStorage) StoreBlockDoc(blk *OpBlock) {
	st.blockDocs[blk.Hash] = blk.blockDoc.String()
}

///
/// opBlock
///

func newOpBlock(peer string, nonce int, parents []Sha, repl []Replacement, selOff, selLen int) *OpBlock {
	blk := &OpBlock{
		Peer:            peer,
		Nonce:           nonce,
		Parents:         parents,
		Replacements:    repl,
		SelectionOffset: selOff,
		SelectionLength: selLen,
		children:        make([]Sha, 0, 4),
	}
	blk.computeHash()
	blk.descendants = doc.NewSet(blk.Hash)
	return blk
}

func (blk *OpBlock) clean() *OpBlock {
	return &OpBlock{
		Hash:            blk.Hash,
		Peer:            blk.Peer,
		Nonce:           blk.Nonce,
		Parents:         blk.Parents,
		Replacements:    blk.Replacements,
		SelectionOffset: blk.SelectionOffset,
		SelectionLength: blk.SelectionLength,
	}
}

func (blk *OpBlock) isSource() bool {
	return len(blk.Parents) == 0
}

func (blk *OpBlock) computeHash() {
	var blank Sha
	if blk.Hash == blank {
		b := &strings.Builder{}
		fmt.Fprintln(b, blk.Peer)
		fmt.Fprintln(b, blk.Hash)
		fmt.Fprintln(b, blk.Nonce)
		for _, hash := range blk.Parents {
			fmt.Fprintf(b, "%x\n", hash)
		}
		for _, r := range blk.Replacements {
			fmt.Fprintln(b, r.Offset)
			fmt.Fprintln(b, r.Length)
			fmt.Fprintln(b, r.Text)
		}
		fmt.Fprintln(b, blk.SelectionOffset)
		fmt.Fprintln(b, blk.SelectionLength)
		hash := sha256.Sum256([]byte(b.String()))
		blk.Hash = hash
	}
}

func (blk *OpBlock) addToDescendants(s *History, descendant Sha, seen doc.Set[Sha]) {
	if seen.Has(blk.Hash) {
		return
	}
	seen.Add(blk.Hash)
	blk.descendants.Add(descendant)
	for _, parent := range blk.Parents {
		s.getBlock(parent).addToDescendants(s, descendant, seen)
	}
}

// parent for the same peer, or source block if there is none
func (blk *OpBlock) peerParent(s *History) *OpBlock {
	for _, hash := range blk.Parents {
		parent := s.getBlock(hash)
		if parent.Peer == blk.Peer {
			return parent
		}
	}
	return s.Source
}

// parent for the same peer
func (blk *OpBlock) peerChild(s *History) *OpBlock {
	for _, hash := range blk.children {
		child := s.getBlock(hash)
		if child.Peer == blk.Peer {
			return child
		}
	}
	return nil
}

// get the frozen document for this block
func (blk *OpBlock) GetDocument(s *History) *document {
	if blk.blockDoc == nil && blk.isSource() {
		blk.blockDoc = doc.NewDocument(blk.Replacements[0].Text)
	} else if blk.blockDoc == nil {
		blk.blockDoc = blk.getDocumentForAncestor(s, s.lca(blk.Parents)).Freeze()
	}
	return blk.blockDoc.Copy()
}

func (blk *OpBlock) getDocumentForAncestor(s *History, ancestor *OpBlock) *document {
	s.getBlockOrder()
	verbose("%d get document for %d\n", blk.order, ancestor.order)
	if ancestor == blk {
		return blk.GetDocument(s)
	} else if blk.document == nil || blk.documentAncestor != ancestor.Hash {
		doc := ancestor.GetDocument(s).Copy()
		verbose("ANCESTOR-DOC: %v\n", doc)
		//verbose("HISTORY-DOC: (%d ancestors)\n%s", len(blk.Parents), doc.Changes("  "))
		for _, hash := range blk.Parents {
			parent := s.getBlock(hash)
			if parent == ancestor {
				continue
			}
			parentDoc := parent.getDocumentForAncestor(s, ancestor)
			verbose("MERGING-PARENT-DOC: %s\n%s", parent.Peer, parentDoc.Changes("  "))
			doc.Merge(parentDoc)
		}
		verbose("MERGED-DOC:\n%s", doc.Changes("  "))
		verbose("REPLACEMENTS: %v\n", blk.Replacements)
		blk.applyTo(doc)
		verbose("RESULT-DOC: %s\n%s", blk.Peer, doc.Changes("  "))
		blk.documentAncestor = ancestor.Hash
		blk.document = doc
	}
	return blk.document.Copy()
}

// blocks come in any order because of pubsub
func (blk *OpBlock) checkPending(s *History) {
	if s.PendingBlocks[blk.Hash] != nil {
		pending := false
		delete(s.PendingBlocks, blk.Hash)
		for _, hash := range blk.Parents {
			if !s.hasBlock(hash) {
				pending = true
				if s.PendingOn[hash] == nil {
					s.PendingOn[hash] = doc.Set[*OpBlock]{}
				}
				s.PendingOn[hash].Add(blk)
			}
		}
		if !pending {
			s.addBlock(blk)
			for blk := range s.PendingOn[blk.Hash] {
				blk.checkPending(s)
			}
			delete(s.PendingOn, blk.Hash)
		}
	}
}

// applyTo replacements to document
func (blk *OpBlock) applyTo(doc *document) {
	for _, op := range blk.Replacements {
		doc.Replace(blk.Peer, op.Offset, op.Length, op.Text)
	}
}

// compute edits to reconstruct merged document for a block
// this is for when the peer just committed the replacements, so
// it's document state is prevBlock + replacements. Compute the
// edits necessary to transform it to the merged document.
func (blk *OpBlock) edits(s *History) ([]Replacement, int, int) {
	// to get to the merged doc from peer's current doc:
	// reverse(parent->current)
	// + reverse(ancestor -> parent)
	// + get ancestor -> merged
	verbose("GETTING-EDITS-FOR-BLOCK")
	parent := blk.peerParent(s)
	ancestor := s.lca(blk.Parents)
	parentToCurrent := parent.GetDocument(s)
	blk.applyTo(parentToCurrent)
	ancestorToParent := parent.getDocumentForAncestor(s, ancestor)
	ancestorToMerged := blk.getDocumentForAncestor(s, ancestor)
	fmt.Printf("ancestorToMerged: %v\n", ancestorToMerged.Edits())
	peerDoc := parentToCurrent.Freeze()
	selection(peerDoc, blk.Peer, blk.SelectionOffset, blk.SelectionLength)
	peerDoc.Apply(blk.Peer, parentToCurrent.ReverseEdits())
	peerDoc.Apply(blk.Peer, ancestorToParent.ReverseEdits())
	peerDoc.Apply(blk.Peer, ancestorToMerged.Edits())
	peerDoc.Simplify()
	offset, length := getSelection(peerDoc, blk.Peer)
	return peerDoc.Edits(), offset, length
}

///
/// LCA
///

func newTwosha(h1 Sha, h2 Sha) Twosha {
	hashes := []Sha{h1, h2}
	sortHashes(hashes)
	var result Twosha
	copy(result[:], hashes[0][:])
	copy(result[len(hashes[0]):], hashes[1][:])
	return result
}

///
/// Session
///

func NewSession(peer string, history *History) *Session {
	return &Session{
		Peer:       peer,
		PendingOps: make([]Replacement, 0, 8),
		History:    history,
	}
}

///
/// History
///

func NewHistory(docId, text string, storage DocStorage) *History {
	src := newOpBlock("", 0, []Sha{}, []Replacement{{Offset: 0, Length: 0, Text: text}}, 0, 0)
	src.order = 0
	s := &History{
		DocID:         newUUID(),
		Source:        src,
		Latest:        map[string]*OpBlock{},
		Blocks:        map[Sha]*OpBlock{src.Hash: src},
		PendingBlocks: map[Sha]*OpBlock{},
		PendingOn:     map[Sha]doc.Set[*OpBlock]{},
		Storage:       storage,
		BlockOrder:    append(make([]Sha, 0, 8), src.Hash),
	}
	s.LCAs = map[Twosha]*LCA{}
	storage.StoreBlocks([]*OpBlock{src})
	return s
}

func (s *History) recomputeOrder() {
	// number blocks in breadth-first order from the source by children
	cur := make([]Sha, 0, 8)
	next := append(make([]Sha, 0, 8), s.Source.Hash)
	seen := doc.Set[Sha]{}
	s.BlockOrder = s.BlockOrder[:0]
	for len(next) > 0 {
		cur, next = next, cur[:0]
		for _, hash := range cur {
			if seen.Has(hash) {
				continue
			}
			seen.Add(hash)
			blk := s.getBlock(hash)
			blk.order = len(s.BlockOrder)
			s.BlockOrder = append(s.BlockOrder, hash)
			sortHashes(blk.children)
			next = append(next, blk.children...)
		}
	}
	// clear LCA cache
	s.LCAs = map[Twosha]*LCA{}
}

// these are cached; when new blocks come in from outside,
// they can cause renumbering, which clears the cache
func (s *History) lca2(blkA *OpBlock, blkB *OpBlock) *OpBlock {
	s.getBlockOrder()
	// ensure blkA is the lower block
	if blkB.order < blkA.order {
		blkA, blkB = blkB, blkA
	}
	key := newTwosha(blkA.Hash, blkB.Hash)
	lca := s.LCAs[key]
	if lca != nil && lca.blkA == blkA.Hash && blkA.order == lca.orderA && blkB.order == lca.orderB {
		return s.getBlock(lca.ancestor)
	}
	// start with the lowest block
	for i := blkA.order; i >= 0; i-- {
		anc := s.getBlock(s.BlockOrder[i])
		if anc.descendants.Has(blkA.Hash) && anc.descendants.Has(blkB.Hash) {
			s.LCAs[key] = &LCA{
				blkA:     blkA.Hash,
				orderA:   blkA.order,
				blkB:     blkB.Hash,
				orderB:   blkB.order,
				ancestor: anc.Hash,
			}
			return anc
		}
	}
	return nil
}

// LCA for several nodes (e.g. the parents of a block)
func (s *History) lca(hashes []Sha) *OpBlock {
	blocks := make([]*OpBlock, 0, len(hashes))
	for _, block := range hashes {
		blocks = append(blocks, s.getBlock(block))
	}
	if len(blocks) == 1 {
		return blocks[0]
	} else if len(blocks) == 2 {
		return s.lca2(blocks[0], blocks[1])
	}
	// more than 2 inputs
	// the result cannot be higher than any pairwise LCA
	// find highest block with all inputs as descendants, starting with the first pairwise LCA
	// (hashes tends to have sorted shas, which are reasonably random)
LCA:
	for index := s.lca2(blocks[0], blocks[1]).order; index >= 0; index-- {
		anc := s.getBlock(s.BlockOrder[index])
		for _, hash := range hashes {
			if !anc.descendants.Has(hash) {
				continue LCA
			}
		}
		return anc
	}
	return nil
}

func (s *History) getBlock(hash Sha) *OpBlock {
	if s.Blocks[hash] != nil {
		return s.Blocks[hash]
	} else if s.PendingBlocks[hash] != nil {
		return s.PendingBlocks[hash]
	}
	if blk := s.Storage.GetBlock(hash); blk != nil {
		return blk
	}
	return s.Storage.GetPendingBlock(hash)
}

func (s *History) hasBlock(hash Sha) bool {
	return s.Blocks[hash] != nil || s.Storage.HasBlock(hash)
}

func (s *History) hasPendingBlock(hash Sha) bool {
	return s.PendingBlocks[hash] != nil || s.Storage.HasPendingBlock(hash)
}

func sortHashes(hashes []Sha) {
	sort.Slice(hashes, func(i, j int) bool {
		return bytes.Compare(hashes[i][:], hashes[j][:]) < 0
	})
}

// sorted hashes of the most recent blocks in the known chains
func (s *History) latestHashes() []Sha {
	if len(s.Latest) == 0 {
		return []Sha{s.Source.Hash}
	}
	var hashes []Sha
	hashes = make([]Sha, len(s.Latest))
	pos := 0
	for peer := range s.Latest {
		hashes[pos] = s.Latest[peer].Hash
		pos++
	}
	sortHashes(hashes)
	return hashes
}

// add a replacement to pendingOps
func (s *Session) Replace(offset int, length int, text string) {
	s.PendingOps = append(s.PendingOps, Replacement{Offset: offset, Length: length, Text: text})
}

// add a replacement to pendingOps
func (s *Session) ReplaceAll(replacements []Replacement) {
	for _, repl := range replacements {
		s.Replace(repl.Offset, repl.Length, repl.Text)
	}
}

func (s *History) addBlock(blk *OpBlock) {
	seen := doc.NewSet(blk.Hash)
	for _, parentHash := range blk.Parents {
		parent := s.getBlock(parentHash)
		parent.children = append(parent.children, blk.Hash)
		parent.addToDescendants(s, blk.Hash, seen)
	}
	s.Latest[blk.Peer] = blk
	s.Blocks[blk.Hash] = blk
	for _, hash := range blk.Parents {
		if s.getBlock(hash).order == len(s.Blocks)-1 {
			blk.order = len(s.Blocks)
			if s.BlockOrder != nil {
				s.BlockOrder = append(s.BlockOrder, blk.Hash)
			}
			return
		}
	}
	// none of the parents had order == len(s.blocks)-1
	s.BlockOrder = nil
}

func (s *History) getBlockOrder() []Sha {
	if s.BlockOrder == nil {
		s.recomputeOrder()
	}
	return s.BlockOrder
}

func (s *History) addIncomingBlock(blk *OpBlock) error {
	if s.hasBlock(blk.Hash) || s.hasPendingBlock(blk.Hash) {
		//fmt.Println("Already has block", blk.Hash)
		return nil
	}
	prev := blk.peerParent(s)
	if prev == s.Source && s.Latest[blk.Peer] != nil || prev != s.Source && prev.peerChild(s) != nil {
		return ErrDivergentBlock
	}
	s.PendingBlocks[blk.Hash] = blk
	blk.checkPending(s)
	return nil
}

// commit pending ops into an opBlock, get its document, and return the replacements
// these will unwind the current document to the common ancestor and replay to the current version
func (s *Session) Commit(selOff int, selLen int) ([]Replacement, int, int) {
	latest := s.History.Latest[s.Peer]
	if latest != nil && len(s.PendingOps) == 0 {
		hashes := s.History.latestHashes()
		same := len(hashes) == len(latest.Parents)
		for i, hash := range hashes {
			if bytes.Compare(hash[:], latest.Parents[i][:]) != 0 {
				same = false
				break
			}
		}
		if same {
			return []Replacement{}, selOff, selLen
		}
	}
	repl := make([]Replacement, len(s.PendingOps))
	copy(repl, s.PendingOps)
	s.PendingOps = s.PendingOps[:0]
	blk := newOpBlock(s.Peer, len(s.History.Blocks), s.History.latestHashes(), repl, selOff, selLen)
	s.History.addBlock(blk)
	return blk.edits(s.History)
}

// set the Selection for peer
func selection(d *document, peer string, start int, length int) {
	// remove old selection for peer if there is one
	tree := doc.RemoveMarker(d.Ops, selectionStart(peer))
	tree = doc.RemoveMarker(d.Ops, selectionEnd(peer))
	left, right := tree.Split(func(m doc.Measure) bool {
		return m.NewLen > start
	})
	mid, end := right.Split(func(m doc.Measure) bool {
		return m.NewLen > length
	})
	d.Ops = left.
		AddLast(doc.NewMarkerOp(selectionStart(peer))).
		Concat(mid).
		AddLast(doc.NewMarkerOp(selectionEnd(peer))).
		Concat(end)
}

func selectionStart(peer string) string {
	return fmt.Sprint(peer, ".sel.start")
}

func selectionEnd(peer string) string {
	return fmt.Sprint(peer, ".sel.end")
}

func getSelection(d *document, peer string) (int, int) {
	left, right := d.SplitOnMarker(selectionStart(peer))
	if !right.IsEmpty() {
		if _, ok := right.PeekFirst().(*doc.MarkerOp); ok {
			mid, end := d.SplitOnMarker(selectionEnd(peer))
			if _, ok := end.PeekFirst().(*doc.MarkerOp); ok {
				return left.Measure().NewLen, mid.Measure().NewLen
			}
		}
	}
	return -1, -1
}

// UTILS
type config struct {
	verbose bool
}

var cfg config

func verbose(format string, args ...any) {
	if cfg.verbose {
		fmt.Printf(format, args...)
	}
}
