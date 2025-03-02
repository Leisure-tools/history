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
	"os"

	//"math/rand"
	"crypto/rand"
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
	Source        *OpBlock             // the first block
	Latest        map[string]*OpBlock  // peer -> block
	Blocks        map[Sha]*OpBlock     // blocks by Sha -- Storage is a fallback for missing ones
	PendingBlocks map[Sha]*OpBlock     // pending blocks whose parent has not been received
	PendingOn     map[Sha]doc.Set[Sha] // unreceived parents for pending blocks
	Storage       DocStorage           // storage (optionally external)
	LCAs          map[Twosha]*LCA      // cache of 2-block latest common ancestors
	BlockOrder    []Sha
	Listeners     []HistoryListener
	Verbosity     int
	// TODO need to move to Heads instead of Latest
	//   - currently we use Heads() which computes it from Latest
	//   - heads should accumulate when new blocks come in
	//   - remove ancesctors from the Heads list
	//   - commits should merge Heads and replace them with the new block
	//	Heads         []*OpBlock // current DAG heads -- commits should merge these
}

type HistoryListener interface {
	NewHeads(service *History)
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
	Replacements     []Replacement // replacements applied to the parents
	SelectionOffset  int
	SelectionLength  int
	document         *document // simple cache to speed up successive document edits
	documentAncestor Sha
	blockDoc         *document // frozen document for this block, used for computing edits
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
	src := newOpBlock("SOURCE", "SOURCE", 0, []Sha{}, []Replacement{{Offset: 0, Length: 0, Text: source}}, 0, 0)
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

func (blk *OpBlock) getDocumentAncestor(h *History) *OpBlock {
	if blk.documentAncestor != EMPTY_SHA {
		return h.GetBlock(blk.documentAncestor)
	}
	return h.lca(blk.Parents)
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

// most recent ancestor for the session, or source block if there is none
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
// history arg is for computing it if there is none
func (blk *OpBlock) GetDocument(s *History) *document {
	if blk.blockDoc == nil && blk.isSource() {
		blk.setBlockDoc(doc.NewDocument(blk.Replacements[0].Text))
	} else if blk.blockDoc == nil {
		// no blockDoc yet, compute it
		if blk.blockDocHash != EMPTY_SHA {
			blk.setBlockDoc(doc.NewDocument(s.Storage.GetDocument(blk.blockDocHash)))
		} else {
			doc := s.GetDocumentForBlocks(blk.getDocumentAncestor(s), blk.Parents)
			doc.Apply(blk.replId(), 0, blk.Replacements)
			blk.setBlockDoc(doc.Freeze())
			s.Storage.StoreDocument(blk.blockDoc.String())
		}
	}
	return blk.blockDoc.Copy()
}

func (blk *OpBlock) GetRawDocument(s *History) *document {
	if blk.blockDocHash != EMPTY_SHA {
		return doc.NewDocument(s.Storage.GetDocument(blk.blockDocHash))
	}
	return blk.getDocumentForAncestor(s, blk.getDocumentAncestor(s), false)
}

// get the document for an ancestor
func (blk *OpBlock) getDocumentForAncestor(h *History, ancestor *OpBlock, sel bool) *document {
	if blk.Hash == ancestor.Hash {
		return blk.GetDocument(h)
	}
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
			markSelection(doc, blk.SessionId, blk.SelectionOffset, blk.SelectionLength)
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
	doc.Apply(id, 0, blk.Replacements)
}

func (parent *OpBlock) computeBlock(h *History, peer, sessionId string, parents []Sha, repl []doc.Replacement, selOff, selLen int) (*OpBlock, []doc.Replacement, error) {
	if len(parents) == 1 && parent.Hash == parents[0] {
		if len(repl) == 0 {
			h.Verbose("NO EDITS TO PARENT")
			return parent, []doc.Replacement{}, nil
		}
		h.Verbose("ONLY EDITS TO PARENT")
		return newOpBlock(peer, sessionId, h.Storage.GetBlockCount(), parents, repl, selOff, selLen), []doc.Replacement{}, nil
	}
	hashStrs := make([]string, len(parents))
	for i, hash := range parents {
		hashStrs[i] = hex.EncodeToString(hash[:])
	}
	h.Verbose("COMPUTING BLOCK\n  PARENT: %#v\n  PARENTS: %#v\n  REPLS: %#v\n",
		parent.replId(),
		hashStrs,
		repl,
	)
	h.GetBlockOrder()
	parentSet := doc.NewSet(parents...)
	parentSet.Add(parent.Hash)
	ancestor := h.lca(parentSet.ToSlice())
	if ancestor == nil {
		return nil, nil, fmt.Errorf("Could not compute block, parents do not have a common ancestor")
	}
	// identify edits from merging
	editId := "edit-" + parent.replId()
	// save parentPeerDoc for later edit computation
	h.Verbose("Ancestor %s\n Parent: %s\n", ancestor.replId(), parent.replId())
	peerDoc := parent.getDocumentForAncestor(h, ancestor, false)
	h.Verbose("PARENT DOC\n%s\n", peerDoc.Changes("  "))
	originalIds := peerDoc.GetOps().Measure().Ids
	original := peerDoc.Freeze()
	original.Apply(editId, 0, repl)
	reverse := original.ReverseEdits()
	editorDoc := original.Freeze()
	peerDoc.Apply(editId, 0, repl)
	h.Verbose("EDITED PARENT DOC\n%s\n", peerDoc.Changes("  "))
	var pdoc *doc.Document
	// merge in other edits
	for _, hash := range parents {
		if hash == parent.Hash {
			continue
		}
		hdoc := h.GetBlock(hash).getDocumentForAncestor(h, ancestor, false)
		if pdoc == nil {
			pdoc = hdoc
		} else {
			pdoc.Merge(hdoc)
		}
	}
	var editRepl []Replacement
	h.Verbose("Merged parents:\n%s\n", pdoc.Changes("  "))
	// mark peer's selection (if any)
	if selOff != -1 {
		markSelection(peerDoc, editId, selOff, selLen)
	}
	peerDoc.Merge(pdoc)
	peerDoc.Simplify()
	h.Verbose("FINAL DOC:\n%s\n", peerDoc.Changes("  "))
	h.Verbose("REVERSE EDIT: %#v\n", reverse)
	orepl := repl
	// transform the replacements
	ids := peerDoc.GetOps().Measure().Ids.Copy()
	ids.Subtract(originalIds)
	repl, _ = peerDoc.EditsFor(doc.NewSet(editId), nil)
	var erepl []Replacement
	erepl, selOff, selLen = getReplsForEdits(peerDoc, editId, ids)
	editRepl = make([]Replacement, 0, len(reverse)+len(repl))
	editRepl = append(editRepl, reverse...)
	editRepl = append(editRepl, erepl...)
	h.Verbose("COMPUTED REVISED EDITs\n  GIVEN: %#v\n  BLOCK: %#v\n  REVERSE: %#v\n  EDITREPL: %#v\n", orepl, repl, reverse, editRepl)
	blk := newOpBlock(peer, sessionId, h.Storage.GetBlockCount(), parents, repl, selOff, selLen)
	if blk.GetDocument(h).String() != peerDoc.String() {
		h.Verbose("BAD DOCUMENT, EXPECTED:\n%s\nBUT GOT:\n%s\n",
			blk.GetDocument(h).String(),
			peerDoc.String())
		panic("ERROR COMPUTING DOCUMENT")
	}
	h.Verbose("APPLY FINAL EDIT TO PEER DOC:\n%s\n", editorDoc.String())
	editorDoc.Apply("edit", 0, editRepl)
	h.Verbose("FINAL PEER DOC:\n%s\n", editorDoc.Changes("  "))
	return blk, editRepl, nil
}

func getReplsForEdits(document *doc.Document, editId string, ids doc.Set[string]) ([]doc.Replacement, int, int) {
	startM := selectionStart(editId)
	endM := selectionEnd(editId)
	markers := doc.NewSet(startM, endM)
	result, resultMarkers := document.EditsFor(ids, markers)
	resultOff := -1
	resultLen := -1
	if document.HasMarker(startM) {
		resultOff = resultMarkers[startM]
		resultLen = resultMarkers[endM] - resultOff
	}
	return result, resultOff, resultLen
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

///
/// History
///

func NewHistory(storage DocStorage, text string) *History {
	src := newOpBlock("SOURCE", "SOURCE", 0, []Sha{}, []Replacement{{Offset: 0, Length: 0, Text: text}}, 0, 0)
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

func (s *History) Verbose(format string, args ...any) { s.VerboseN(1, format, args...) }

func (s *History) VerboseN(level int, format string, args ...any) {
	if level <= s.Verbosity {
		if format[len(format)-1] != '\n' {
			format += "\n"
		}
		fmt.Fprintf(os.Stderr, format, args...)
	}
}

func (s *History) AddListener(l HistoryListener) {
	if s.Listeners == nil {
		s.Listeners = make([]HistoryListener, 0, 8)
	}
	s.Listeners = append(s.Listeners, l)
}

func (s *History) fireNewHeads() {
	for _, l := range s.Listeners {
		l.NewHeads(s)
	}
}

func (s *History) LatestBlock(sessionId string) *OpBlock {
	blk := s.Latest[sessionId]
	if blk == nil {
		blk = s.Source
	}
	return blk
}

func (s *History) GetDocument(hash Sha) string {
	return s.Storage.GetDocument(hash)
}

func (s *History) GetLatestDocument() *document {
	latest := s.Heads()
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
	var chosen *OpBlock
	if blkA.descendants.Has(blkB.Hash) {
		chosen = blkA
	} else if blkB.descendants.Has(blkA.Hash) {
		chosen = blkB
	} else {
		// start with the lowest block
		for i := blkA.order; i >= 0; i-- {
			anc := s.GetBlock(s.BlockOrder[i])
			if anc.descendants.Has(blkA.Hash) && anc.descendants.Has(blkB.Hash) {
				chosen = anc
				break
			}
		}
	}
	if chosen != nil {
		s.LCAs[key] = &LCA{
			blkA:     blkA.Hash,
			orderA:   blkA.order,
			blkB:     blkB.Hash,
			orderB:   blkB.order,
			ancestor: chosen.Hash,
		}
	}
	return chosen
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
	s.fireNewHeads()
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

func SameHashesNoExclude(a, b []Sha) bool {
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

func SameHashes(a, b []Sha, exclude Sha) bool {
	if len(a) != len(b)+1 {
		return false
	}
	pos := 0
	for _, hashA := range a {
		if pos >= len(b) {
			return false
		} else if bytes.Compare(hashA[:], b[pos][:]) != 0 {
			if bytes.Compare(hashA[:], exclude[:]) == 0 {
				continue
			}
			return false
		}
		pos++
	}
	return true
}

func (h *History) isAncestor(bl1, bl2 *OpBlock) bool {
	if bl1.order > bl2.order {
		return false
	}
	h2 := bl2.Hash[:]
	for _, ch := range bl1.children {
		if bytes.Compare(ch[:], h2) == 0 {
			return true
		} else if h.isAncestor(h.GetBlock(ch), bl2) {
			return true
		}
	}
	return false
}

func (h *History) Heads() []Sha {
	if len(h.Latest) == 0 {
		return []Sha{h.Source.Hash}
	}
	blocks := make([]*OpBlock, 0, len(h.Latest))
	for _, blk := range h.Latest {
		blocks = append(blocks, blk)
	}
	h.GetBlockOrder()
	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].order < blocks[j].order
	})
	chosen := doc.NewSet(blocks...)
	for i, block := range blocks {
		for j := i + 1; j < len(blocks); j++ {
			if h.isAncestor(block, blocks[j]) {
				chosen.Remove(block)
				break
			}
		}
	}
	result := make([]Sha, 0, len(chosen))
	for c := range chosen {
		result = append(result, c.Hash)
	}
	sortHashes(result)
	return result
}

// commit pending ops into an opBlock, get its document, and return the replacements
// these will unwind the current document to the common ancestor and replay to the current version
func (h *History) Commit(peer, sessionId string, repls []Replacement, selOff int, selLen int) ([]Replacement, int, int, error) {
	parent := h.Latest[sessionId]
	if parent == nil {
		parent = h.Source
		h.Latest[sessionId] = parent
	}
	latestHashes := h.Heads()
	if len(repls) == 0 {
		if (parent == nil && len(h.Latest) == 0) || parent != nil && SameHashes(latestHashes, parent.Parents, parent.Hash) {
			// no changes
			h.Verbose("NO CHANGES")
			return []Replacement{}, selOff, selLen, nil
		}
	}
	parents := h.Heads()
	blk, edits, err := parent.computeBlock(h, peer, sessionId, parents, repls, selOff, selLen)
	if err != nil {
		return nil, 0, 0, err
	} else if blk == parent {
		h.Verbose("NO EDITS")
		return nil, 0, 0, nil
	}
	h.Verbose("EDITED, REPL: %#v\n", edits)
	h.addBlock(blk)
	h.fireNewHeads()
	return edits, blk.SelectionOffset, blk.SelectionLength, nil
}

// set the Selection for peer
func markSelection(d *document, session string, start int, length int) {
	if start != -1 {
		d.Mark(selectionStart(session), start)
		d.Mark(selectionEnd(session), start+length)
	}
}

func selectionStart(session string) string {
	return fmt.Sprint(session, ".sel.start")
}

func selectionEnd(peer string) string {
	return fmt.Sprint(peer, ".sel.end")
}

func getSelection(d *document, session string) (int, int) {
	left, right := d.SplitOnMarker(selectionStart(session))
	if !right.IsEmpty() {
		endMarker := selectionEnd(session)
		selOff := left.Measure().Width
		if right.PeekFirst().(*doc.MarkerOp).Names.Has(endMarker) {
			return selOff, 0
		}
		mid, rest := doc.SplitOnMarker(right.RemoveFirst(), endMarker)
		if !rest.IsEmpty() {
			selLen := mid.Measure().Width
			return selOff, selLen
		}
	}
	return -1, -1
}
