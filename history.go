// Package history represents the history of a document as a DAG of operations
// Each document has a unique ID
// Session objects hold peer ID and track pending changes to a document
package history

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"

	doc "github.com/leisure-tools/document"
	ft "github.com/leisure-tools/lazyfingertree"
)

var ErrCollab = errors.New("Collaboration error")
var ErrDivergentBlock = fmt.Errorf("%w, divergent block", ErrCollab)

type document = doc.Document
type Replacement = doc.Replacement
type Sha = [sha256.Size]byte
type Twosha = [sha256.Size * 2]byte
type UUID [16]byte

var EMPTY_SHA Sha

// History of a document
type History struct {
	Source        *OpBlock
	Latest        map[string]*OpBlock // peer -> block
	Blocks        map[Sha]*OpBlock
	PendingOn     map[Sha]doc.Set[Sha]
	PendingBlocks map[Sha]*OpBlock
	Storage       DocStorage
	LCAs          map[Twosha]*LCA // 2-block LCAs
	BlockOrder    []Sha
}

type DocStorage interface {
	GetBlockCount() int
	GetSource() *OpBlock
	GetLatest() map[string]*OpBlock
	SetLatest(peer string, latest *OpBlock)
	GetBlock(hash Sha) *OpBlock
	HasBlock(hash Sha) bool
	StoreBlock(blks *OpBlock) // removes from pending and pendingOn
	AddChild(blk *OpBlock, child Sha)
	HasPendingBlock(hash Sha) bool
	GetPendingBlock(hash Sha) *OpBlock
	StorePendingBlock(*OpBlock)
	GetPendingOn(hash Sha) []Sha
	StorePendingOn(blk Sha, pendingBlock *OpBlock)
	GetDocument(docHash Sha) string
	StoreDocument(doc string)
}

//// temporary connection information, not persisted to storage
//type Session struct {
//	*History
//	Peer      string
//	SessionId string
//	//SessionId  string
//	PendingOps []Replacement
//	Follow     string
//}

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
	Peer             string // to track peer changes and authorship
	SessionId        string // for determining parent blocks
	Hash             Sha    // not transmitted; computed upon reception
	Nonce            int
	Parents          []Sha
	Replacements     []Replacement
	SelectionOffset  int
	SelectionLength  int
	document         *document // simple cache to speed up successive document edits
	documentAncestor Sha
	blockDoc         *document // frozen document for this block
	blockDocHash     Sha
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
	sourceBlock   *OpBlock
	latest        map[string]Sha
	blocks        map[Sha]*OpBlock
	pendingBlocks map[Sha]*OpBlock
	pendingOn     map[Sha]doc.Set[Sha]
	documents     map[Sha]string
}

///
/// basic storage
///

func NewMemoryStorage(source string) *MemoryStorage {
	src := newOpBlock("", "", 0, []Sha{}, []Replacement{{Offset: 0, Length: 0, Text: source}}, 0, 0)
	return &MemoryStorage{
		sourceBlock:   src,
		latest:        map[string]Sha{},
		blocks:        map[Sha]*OpBlock{src.Hash: src},
		pendingBlocks: map[Sha]*OpBlock{},
		pendingOn:     map[Sha]doc.Set[Sha]{},
		documents:     map[Sha]string{},
	}
}

func (st *MemoryStorage) GetBlockCount() int {
	return len(st.blocks)
}

func (st *MemoryStorage) GetSource() *OpBlock {
	return st.sourceBlock
}

func (st *MemoryStorage) GetLatest() map[string]*OpBlock {
	latest := map[string]*OpBlock{}
	for peer, hash := range st.latest {
		latest[peer] = st.GetBlock(hash)
	}
	return latest
}

func (st *MemoryStorage) SetLatest(peer string, blk *OpBlock) {
	st.latest[peer] = blk.Hash
}

func (st *MemoryStorage) HasBlock(hash Sha) bool {
	return st.blocks[hash] != nil
}

func (st *MemoryStorage) GetBlock(hash Sha) *OpBlock {
	return st.blocks[hash]
}

func (st *MemoryStorage) AddChild(blk *OpBlock, child Sha) {
	b := st.blocks[blk.Hash]
	b.children = append(b.children, child)
}

func (st *MemoryStorage) StoreBlock(blk *OpBlock) {
	st.blocks[blk.Hash] = blk.Clean()
	delete(st.pendingBlocks, blk.Hash)
	delete(st.pendingOn, blk.Hash)
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

func (st *MemoryStorage) Dirty(blk *OpBlock) {}

func (st *MemoryStorage) StorePendingOn(hash Sha, pendingBlock *OpBlock) {
	if st.pendingOn[hash] == nil {
		st.pendingOn[hash] = doc.Set[Sha]{}
	}
	st.pendingOn[hash].Add(pendingBlock.Hash)
}

func (st *MemoryStorage) GetPendingOn(hash Sha) []Sha {
	return st.pendingOn[hash].ToSlice()
}

func (st *MemoryStorage) GetDocument(docHash Sha) string {
	return st.documents[docHash]
}

func (st *MemoryStorage) StoreDocument(doc string) {
	st.documents[sha256.Sum256([]byte(doc))] = doc
}

///
/// opBlock
///

func newOpBlock(peer, session string, nonce int, parents []Sha, repl []Replacement, selOff, selLen int) *OpBlock {
	blk := &OpBlock{
		Peer:            peer,
		SessionId:       session,
		Nonce:           nonce,
		Parents:         parents,
		Replacements:    repl,
		SelectionOffset: selOff,
		SelectionLength: selLen,
	}
	blk.computeHash()
	blk.descendants = doc.NewSet(blk.Hash)
	return blk
}

func (blk *OpBlock) GetDescendants() doc.Set[Sha] {
	return blk.descendants
}

func (blk *OpBlock) GetOrder() int {
	return blk.order
}

func (blk *OpBlock) Clean() *OpBlock {
	return &OpBlock{
		Hash:            blk.Hash,
		Peer:            blk.Peer,
		SessionId:       blk.SessionId,
		Nonce:           blk.Nonce,
		Parents:         ft.Dup(blk.Parents),
		Replacements:    ft.Dup(blk.Replacements),
		SelectionOffset: blk.SelectionOffset,
		SelectionLength: blk.SelectionLength,
	}
}

func (blk *OpBlock) isSource() bool {
	return len(blk.Parents) == 0
}

func (blk *OpBlock) replId() string {
	return hex.EncodeToString(blk.Hash[:])
}

func (blk *OpBlock) computeHash() {
	var blank Sha
	if blk.Hash == blank {
		b := &strings.Builder{}
		fmt.Fprintln(b, blk.Peer)
		fmt.Fprintln(b, blk.SessionId)
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
	if blk.descendants == nil {
		blk.descendants = make(doc.Set[Sha])
	}
	blk.descendants.Add(descendant)
	for _, parent := range blk.Parents {
		s.GetBlock(parent).addToDescendants(s, descendant, seen)
	}
}

// most recent ancestor for the peer, or source block if there is none
func (blk *OpBlock) SessionParent(h *History) *OpBlock {
	order := h.GetBlockOrder()
	for i := blk.order - 1; i >= 0; i-- {
		oblk := h.GetBlock(order[i])
		if oblk.SessionId == blk.SessionId {
			return oblk
		}
	}
	return h.Source
}

// parent for the same peer
func (blk *OpBlock) SessionChild(s *History) *OpBlock {
	for _, hash := range blk.children {
		child := s.GetBlock(hash)
		if child.SessionId == blk.SessionId {
			return child
		}
	}
	return nil
}

func (blk *OpBlock) setBlockDoc(bdoc *document) {
	blk.blockDoc = bdoc
	blk.blockDocHash = sha256.Sum256([]byte(bdoc.String()))
}

func (blk *OpBlock) GetDocumentHash(s *History) Sha {
	blk.GetDocument(s)
	return blk.blockDocHash
}

// get the frozen document for this block
func (blk *OpBlock) GetDocument(s *History) *document {
	if blk.blockDoc == nil && blk.isSource() {
		blk.setBlockDoc(doc.NewDocument(blk.Replacements[0].Text))
	} else if blk.blockDoc == nil {
		if blk.blockDocHash != EMPTY_SHA {
			blk.setBlockDoc(doc.NewDocument(s.Storage.GetDocument(blk.blockDocHash)))
		} else {
			blk.setBlockDoc(blk.getDocumentForAncestor(s, s.lca(blk.Parents), false).Freeze())
			s.Storage.StoreDocument(blk.blockDoc.String())
		}
	}
	return blk.blockDoc.Copy()
}

func (blk *OpBlock) getDocumentForAncestor(h *History, ancestor *OpBlock, sel bool) *document {
	h.GetBlockOrder()
	if ancestor.Hash == blk.Hash {
		return blk.GetDocument(h)
	} else if blk.document == nil || blk.documentAncestor != ancestor.Hash {
		peerParent := blk.SessionParent(h)
		doc := ancestor.GetDocument(h).Copy()
		applied := blk.applyToParent(peerParent, ancestor, doc, sel)
		for _, hash := range blk.Parents {
			parent := h.GetBlock(hash)
			if parent != ancestor {
				parentDoc := parent.getDocumentForAncestor(h, ancestor, false)
				applied = applied || blk.applyToParent(peerParent, parent, parentDoc, sel)
				doc.Merge(parentDoc)
			}
		}
		if !applied {
			blk.applyToParent(peerParent, peerParent, doc, sel)
		}
		blk.documentAncestor = ancestor.Hash
		blk.document = doc
	}
	return blk.document.Copy()
}

func (blk *OpBlock) applyToParent(parent, ancestor *OpBlock, doc *document, sel bool) bool {
	if ancestor == parent {
		doc.Apply(blk.replId(), 0, blk.Replacements)
		if sel && blk.SelectionOffset > -1 {
			selection(doc, blk.SessionId, blk.SelectionOffset, blk.SelectionLength)
		}
	}
	return ancestor == parent
}

// blocks come in any order because of pubsub
func (blk *OpBlock) checkPending(h *History) {
	if h.hasPendingBlock(blk.Hash) {
		pending := false
		delete(h.PendingBlocks, blk.Hash)
		for _, hash := range blk.Parents {
			if !h.hasBlock(hash) {
				pending = true
				if h.PendingOn[hash] == nil {
					h.PendingOn[hash] = doc.Set[Sha]{}
				}
				h.PendingOn[hash].Add(blk.Hash)
				h.Storage.StorePendingOn(hash, blk)
			}
		}
		if !pending {
			h.addBlock(blk)
			for blk := range h.getPendingOn(blk.Hash) {
				blk.checkPending(h)
			}
			delete(h.PendingOn, blk.Hash)
		}
	}
}

// applyTo replacements to document
func (blk *OpBlock) applyTo(doc *document) {
	id := hex.EncodeToString(blk.Hash[:])
	pos := 0
	for _, op := range blk.Replacements {
		doc.Replace(id, pos, op.Offset, op.Length, op.Text)
		pos += len(op.Text)
	}
}

// compute edits to reconstruct merged document for a block
// this is for when the peer just committed the replacements, so
// it's document state is prevBlock + replacements. Compute the
// edits necessary to transform it to the merged document.
func (blk *OpBlock) edits(h *History) ([]Replacement, int, int) {
	// to get to the merged doc from peer's current doc:
	// reverse(parent->current)
	// + reverse(ancestor -> parent)
	// + get ancestor -> merged
	h.GetBlockOrder()
	parent := blk.SessionParent(h)
	parents := blk.Parents
	if parent == h.Source {
		parents = make([]Sha, 0, len(parents)+1)
		parents = append(make([]Sha, 0, len(parents)+1), blk.Parents...)
		parents = append(parents, h.Source.Hash)
	}
	ancestor := h.lca(parents)
	if parent.order < ancestor.order {
		ancestor = parent
	}
	parentToCurrent := parent.GetDocument(h)
	blk.applyTo(parentToCurrent)
	ancestorToParent := parent.getDocumentForAncestor(h, ancestor, false)
	ancestorToMerged := blk.getDocumentForAncestor(h, ancestor, true)
	peerDoc := parentToCurrent.Freeze()
	selection(peerDoc, blk.SessionId, blk.SelectionOffset, blk.SelectionLength)
	//fmt.Printf("STARTING-PEER-DOC:\n%s\n", peerDoc.Changes("  "))
	e := editor{peerDoc, blk.replId(), 0}
	e.Apply(parentToCurrent.ReverseEdits())
	e.Apply(ancestorToParent.ReverseEdits())
	e.Apply(ancestorToMerged.Edits())
	////call parentToCurrent.Reversed(parent.SessionId, blk.SessionId).OpString(false)
	//peerDoc.Merge(parentToCurrent.Reversed(parent.replId(), blk.replId()))
	////call ancestorToParent.Reversed(ancestor.SessionId, parent.SessionId).OpString(false)
	//peerDoc.Merge(ancestorToParent.Reversed(ancestor.replId(), blk.replId()))
	//peerDoc.Merge(ancestorToMerged)
	offset, length := getSelection(ancestorToMerged, blk.SessionId)
	peerDoc.Simplify()
	//selection(peerDoc, blk.SessionId, offset, length) // diag
	//fmt.Printf("FINAL-PEER-DOC:\n%s\n", peerDoc.Changes("  "))
	return peerDoc.Edits(), offset, length
}

type editor struct {
	doc    *doc.Document
	id     string
	offset int
}

func (e *editor) Apply(repl []Replacement) *editor {
	e.doc.Apply(e.id, e.offset, repl)
	e.offset += doc.Width(repl)
	return e
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

/////
///// Session
/////
//
//func NewSession(peer, sessionId string, history *History, follow string) *Session {
//	return &Session{
//		Peer:       peer,
//		SessionId:  sessionId,
//		PendingOps: make([]Replacement, 0, 8),
//		History:    history,
//		Follow:     follow,
//	}
//}

///
/// History
///

func NewHistory(storage DocStorage, text string) *History {
	src := newOpBlock("", "", 0, []Sha{}, []Replacement{{Offset: 0, Length: 0, Text: text}}, 0, 0)
	src.order = 0
	s := &History{
		Source:        src,
		Latest:        map[string]*OpBlock{},
		Blocks:        map[Sha]*OpBlock{src.Hash: src},
		PendingBlocks: map[Sha]*OpBlock{},
		PendingOn:     map[Sha]doc.Set[Sha]{},
		Storage:       storage,
		BlockOrder:    append(make([]Sha, 0, 8), src.Hash),
	}
	s.LCAs = map[Twosha]*LCA{}
	storage.StoreBlock(src)
	return s
}

func (s *History) GetDocument(hash Sha) string {
	return s.Storage.GetDocument(hash)
}

func (s *History) GetLatestDocument() *document {
	latest := s.LatestHashes()
	ancestor := s.lca(latest)
	return s.GetDocumentForBlocks(ancestor, latest)
}

func (s *History) GetDocumentForBlocks(ancestor *OpBlock, hashes []Sha) *document {
	doc := ancestor.GetDocument(s).Copy()
	for _, hash := range hashes {
		parent := s.GetBlock(hash)
		if parent != ancestor {
			parentDoc := parent.getDocumentForAncestor(s, ancestor, false)
			doc.Merge(parentDoc)
		}
	}
	return doc
}

// deterministically compute block order
// children are sorted by hash at each step
func (s *History) recomputeBlockOrder() {
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
			blk := s.GetBlock(hash)
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
	s.GetBlockOrder()
	// ensure blkA is the lower block
	if blkB.order < blkA.order {
		blkA, blkB = blkB, blkA
	}
	key := newTwosha(blkA.Hash, blkB.Hash)
	lca := s.LCAs[key]
	if lca != nil && lca.blkA == blkA.Hash && blkA.order == lca.orderA && blkB.order == lca.orderB {
		return s.GetBlock(lca.ancestor)
	}
	// start with the lowest block
	for i := blkA.order; i >= 0; i-- {
		anc := s.GetBlock(s.BlockOrder[i])
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
		blocks = append(blocks, s.GetBlock(block))
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
		anc := s.GetBlock(s.BlockOrder[index])
		for _, hash := range hashes {
			if !anc.descendants.Has(hash) {
				continue LCA
			}
		}
		return anc
	}
	return nil
}

func (s *History) GetBlock(hash Sha) *OpBlock {
	if s.Blocks[hash] != nil {
		return s.Blocks[hash]
	} else if s.PendingBlocks[hash] != nil {
		return s.PendingBlocks[hash]
	} else if blk := s.Storage.GetBlock(hash); blk != nil {
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

func (s *History) getPendingOn(hash Sha) doc.Set[*OpBlock] {
	if s.PendingOn[hash] == nil {
		pending := s.Storage.GetPendingOn(hash)
		if pending != nil {
			blocks := make(doc.Set[Sha], len(pending))
			for _, hash := range pending {
				blocks.Add(hash)
			}
			s.PendingOn[hash] = blocks
		}
	}
	result := make(doc.Set[*OpBlock], len(s.PendingOn[hash]))
	for hash := range s.PendingOn[hash] {
		result.Add(s.GetBlock(hash))
	}
	return result
}

func sortHashes(hashes []Sha) {
	sort.Slice(hashes, func(i, j int) bool {
		return bytes.Compare(hashes[i][:], hashes[j][:]) < 0
	})
}

// sorted hashes of the most recent blocks in the known chains
func (s *History) LatestHashes() []Sha {
	if len(s.Latest) == 0 {
		return []Sha{s.Source.Hash}
	}
	hashSet := doc.NewSet[Sha]()
	for _, blk := range s.Latest {
		hashSet.Add(blk.Hash)
	}
	hashes := hashSet.ToSlice()
	sortHashes(hashes)
	return hashes
}

//// add a replacement to pendingOps
//func (s *Session) Replace(offset int, length int, text string) {
//	s.PendingOps = append(s.PendingOps, Replacement{Offset: offset, Length: length, Text: text})
//}

//// add a replacement to pendingOps
//func (s *Session) ReplaceAll(replacements []Replacement) {
//	for _, repl := range replacements {
//		s.Replace(repl.Offset, repl.Length, repl.Text)
//	}
//}

func (s *History) addBlock(blk *OpBlock) {
	if len(blk.Parents) == 0 {
		panic("Adding block with not parents")
	}
	seen := doc.NewSet(blk.Hash)
	s.Blocks[blk.Hash] = blk
	s.Storage.StoreBlock(blk)
	for _, parentHash := range blk.Parents {
		parent := s.GetBlock(parentHash)
		parent.children = append(parent.children, blk.Hash)
		s.Storage.AddChild(parent, blk.Hash)
		parent.addToDescendants(s, blk.Hash, seen)
	}
	s.Latest[blk.SessionId] = blk
	s.Storage.SetLatest(blk.SessionId, blk)
	count := s.Storage.GetBlockCount()
	for _, hash := range blk.Parents {
		if s.GetBlock(hash).order == count-1 {
			blk.order = count
			if s.BlockOrder != nil {
				s.BlockOrder = append(s.BlockOrder, blk.Hash)
			}
			return
		}
	}
	// none of the parents had order == len(s.blocks)-1
	s.BlockOrder = s.BlockOrder[:0]
}

func (s *History) GetBlockOrder() []Sha {
	if len(s.BlockOrder) == 0 {
		s.recomputeBlockOrder()
	}
	return s.BlockOrder
}

func (s *History) addIncomingBlock(blk *OpBlock) error {
	if s.hasBlock(blk.Hash) || s.hasPendingBlock(blk.Hash) {
		//fmt.Println("Already has block", blk.Hash)
		return nil
	}
	prev := blk.SessionParent(s)
	if prev.isSource() && s.Latest[blk.SessionId] != nil || !prev.isSource() && prev.SessionChild(s) != nil {
		return ErrDivergentBlock
	}
	s.PendingBlocks[blk.Hash] = blk
	s.Storage.StorePendingBlock(blk)
	blk.checkPending(s)
	return nil
}

func SameHashes(a, b []Sha) bool {
	if len(a) != len(b) {
		return false
	}
	for i, hashA := range a {
		if bytes.Compare(hashA[:], b[i][:]) != 0 {
			return false
		}
	}
	return true
}

//func (s *Session) latestNonTrivialHashes() []Sha {
//	latestHashes := s.LatestHashes()
//	parentSet := doc.NewSet(latestHashes...)
//	// for non-peer blocks, replace each null block with its first non-null ancestor
//	for _, b := range latestHashes {
//		blk := s.GetBlock(b)
//		if blk.SessionId != s.SessionId {
//			for len(blk.Replacements) == 0 && len(blk.Parents) == 1 {
//				parentSet.Remove(blk.Hash)
//				blk = s.GetBlock(blk.Parents[0])
//				parentSet.Add(blk.Hash)
//			}
//		}
//	}
//	return parentSet.ToSlice()
//}

//// commit pending ops into an opBlock, get its document, and return the replacements
//// these will unwind the current document to the common ancestor and replay to the current version
//func (s *Session) Commit(selOff int, selLen int) ([]Replacement, int, int) {
//	repls := ft.Dup(s.PendingOps)
//	s.PendingOps = s.PendingOps[:0]
//	return s.History.Commit(s.Peer, s.SessionId, repls, selOff, selLen)
//}

// commit pending ops into an opBlock, get its document, and return the replacements
// these will unwind the current document to the common ancestor and replay to the current version
func (h *History) Commit(peer, sessionId string, repls []Replacement, selOff int, selLen int) ([]Replacement, int, int) {
	parent := h.Latest[sessionId]
	latestHashes := h.LatestHashes()
	if parent != nil && len(repls) == 0 {
		if SameHashes(latestHashes, parent.Parents) {
			// no changes
			return []Replacement{}, selOff, selLen
		}
	}
	//latestHashes = s.latestNonTrivialHashes()
	excluded := doc.Set[Sha]{}
	for i, p := range latestHashes {
		if !excluded.Has(p) {
			parent := h.GetBlock(p)
			for _, c := range latestHashes[i+1:] {
				if !excluded.Has(c) {
					child := h.GetBlock(c)
					if child.descendants.Has(p) {
						excluded.Add(c)
					} else if parent.descendants.Has(c) {
						excluded.Add(p)
					}
				}
			}
		}
	}
	parents := make([]Sha, 0, len(latestHashes)-len(excluded))
	for _, p := range latestHashes {
		if !excluded.Has(p) {
			parents = append(parents, p)
		}
	}
	sortHashes(parents)
	blk := newOpBlock(peer, sessionId, h.Storage.GetBlockCount(), parents, repls, selOff, selLen)
	h.addBlock(blk)
	return blk.edits(h)
}

// set the Selection for peer
func selection(d *document, session string, start int, length int) {
	d.Mark(selectionStart(session), start)
	d.Mark(selectionEnd(session), start+length)
}

func selectionStart(session string) string {
	return fmt.Sprint(session, ".sel.start")
}

func selectionEnd(peer string) string {
	return fmt.Sprint(peer, ".sel.end")
}

func getSelection(d *document, peer string) (int, int) {
	left, right := d.SplitOnMarker(selectionStart(peer))
	if !right.IsEmpty() {
		selOff := left.Measure().Width + right.PeekFirst().Measure().Width
		mid, rest := doc.SplitOnMarker(right.RemoveLast(), selectionEnd(peer))
		if !rest.IsEmpty() {
			selLen := mid.Measure().Width + rest.PeekFirst().Measure().Width
			return selOff, selLen
		}
	}
	return -1, -1
}
