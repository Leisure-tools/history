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
	//	Heads         []*OpBlock // current DAG heads -- edits should merge these
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

// parents hashes most likely includes parent.Hash
func (parent *OpBlock) computeBlock(h *History, peer, sessionId string, parents []Sha, repl []doc.Replacement, selOff, selLen int) (*OpBlock, []doc.Replacement, error) {
	if len(parents) == 1 && parent.Hash == parents[0] {
		return newOpBlock(peer, sessionId, h.Storage.GetBlockCount(), parents, repl, selOff, selLen), []doc.Replacement{}, nil
	}
	hashStrs := make([]string, len(parents))
	for i, hash := range parents {
		hashStrs[i] = hex.EncodeToString(hash[:])
	}
	//fmt.Printf("COMPUTING BLOCK\n  PARENT: %#v\n  PARENTS: %#v\n  REPLS: %#v\n",
	//	parent.replId(),
	//	hashStrs,
	//	repl,
	//)
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
	//fmt.Printf("Ancestor %s\n Parent: %s\n", ancestor.replId(), parent.replId())
	peerDoc := parent.getDocumentForAncestor(h, ancestor, false)
	//fmt.Printf("PARENT DOC\n%s\n", peerDoc.Changes("  "))
	original := peerDoc.Freeze()
	original.Apply(editId, 0, repl)
	reverse := original.ReverseEdits()
	editorDoc := peerDoc.Copy()
	peerDoc.Apply(editId, 0, repl)
	originalIds := peerDoc.GetOps().Measure().Ids
	//fmt.Printf("EDITED PARENT DOC\n%s\n", peerDoc.Changes("  "))
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
	// no alterations are needed unless merging occurred
	editRepl := []doc.Replacement{}
	if pdoc != nil {
		//fmt.Printf("Merged parents:\n%s\n", pdoc.Changes("  "))
		// mark peer's selection (if any)
		if selOff != -1 {
			markSelection(peerDoc, editId, selOff, selLen)
		}
		peerDoc.Merge(pdoc)
		peerDoc.Simplify()
		//fmt.Printf("FINAL DOC:\n%s\n", peerDoc.Changes("  "))
		//fmt.Printf("REVERSE EDIT: %#v\n", reverse)
		orepl := repl
		// transform the replacements
		repl, selOff, selLen = getReplsForEdits(peerDoc, editId)

		editorId := "\x00edit-repl-" + parent.replId()
		//fmt.Printf("EDITOR DOC - INITIAL\n%s", editorDoc.Changes("  "))
		editorDoc.Apply(editorId, 0, orepl)
		//fmt.Printf("EDITOR DOC - AFTER ORIGINAL REPL\n%s", editorDoc.Changes("  "))
		originalIds = editorDoc.GetOps().Measure().Ids
		editorId2 := "\x00reverse-edit-repl-" + parent.replId()
		editorDoc.Apply(editorId2, 0, reverse)
		//fmt.Printf("EDITOR DOC - AFTER REVERSE REPL\n%s", editorDoc.Changes("  "))
		editorId3 := "\x00forward-edit-repl-" + parent.replId()
		editorDoc.Apply(editorId3, 0, orepl)
		//fmt.Printf("EDITOR DOC - AFTER FORWARD REPL\n%s", editorDoc.Changes("  "))
		editorDoc.Merge(pdoc)
		//fmt.Printf("EDITOR DOC - AFTER MERGE\n%s", editorDoc.Changes("  "))
		editorDoc.Simplify()
		//fmt.Printf("EDITOR DOC - AFTER SIMPLIFY\n%s", editorDoc.Changes("  "))
		editRepl, _ = editorDoc.EditsFor(editorDoc.GetOps().Measure().Ids.Complement(originalIds), nil)
		//fmt.Printf("COMPUTED REVISED EDITs\n  GIVEN: %#v\n  BLOCK: %#v\n  EDITREPL: %#v\n  REVERSE: %#v\n", orepl, repl, editRepl, reverse)
	}
	blk := newOpBlock(peer, sessionId, h.Storage.GetBlockCount(), parents, repl, selOff, selLen)
	return blk, editRepl, nil
}

//func cleanRevisedRepl(orig, revised []doc.Replacement) []doc.Replacement {
//	res := make([]doc.Replacement, 0, len(revised))
//	oi := 0
//	ri := 0
//	offset := 0
//	for oi < len(orig) {
//		if orig[oi] == revised[ri] {
//			offset += len(revised[ri].Text) - revised[ri].Length
//		} else {
//			repl := revised[ri]
//			repl.Offset += offset
//			res = append(res, repl)
//		}
//		oi++
//		ri++
//	}
//}

func getReplsForEdits(document *doc.Document, editId string) ([]doc.Replacement, int, int) {
	startM := selectionStart(editId)
	endM := selectionEnd(editId)
	markers := doc.NewSet(startM, endM)
	result, resultMarkers := document.EditsFor(doc.NewSet(editId), markers)
	resultOff := -1
	resultLen := -1
	if document.HasMarker(startM) {
		resultOff = resultMarkers[startM]
		resultLen = resultMarkers[endM] - resultOff
	}
	return result, resultOff, resultLen
}

// compute edits to reconstruct merged document for a block
// this is for when the peer just committed the replacements, so
// it's document state is prevBlock + replacements. Compute the
// edits necessary to transform it to the merged document.
func (blk *OpBlock) old_edits(h *History, selOff, selLen int) ([]Replacement, int, int) {
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
	ancestor := blk.getDocumentAncestor(h)
	if parent.order < ancestor.order {
		ancestor = parent
	}
	// start at parent and apply replacements to produce current session's doc
	parentToCurrent := parent.GetDocument(h)
	blk.applyTo(parentToCurrent)
	// snapshot current session's document
	peerDoc := parentToCurrent.Freeze()

	// if there's a selection, add markers to the current session's document
	//if blk.SelectionOffset != -1 {
	if selOff != -1 {
		//markSelection(peerDoc, blk.SessionId, blk.SelectionOffset, blk.SelectionLength)
		markSelection(peerDoc, blk.SessionId, selOff, selLen)
		so, sl := getSelection(peerDoc, blk.SessionId)
		//if so != blk.SelectionOffset || sl != blk.SelectionLength {
		if so != selOff || sl != selLen {
			println("DOCUMENT\n", peerDoc.Changes("  "))
			//panic(fmt.Sprintf("BAD SELECTION, EXPECTED %d LEN %d BUT GOT %d LEN %d", blk.SelectionOffset, blk.SelectionLength, so, sl))
			panic(fmt.Sprintf("BAD SELECTION, EXPECTED %d LEN %d BUT GOT %d LEN %d", selOff, selLen, so, sl))
		}
	}
	//fmt.Printf("STARTING-PEER-DOC:\n%s\n", peerDoc.Changes("  "))
	// make an editor object to help track changes
	e := editor{peerDoc, blk.replId(), 0}
	// reverse session's edits to move selection back to parent
	e.Apply(parentToCurrent.ReverseEdits())
	if ancestor.Hash != parent.Hash {
		ancestorToParent := parent.getDocumentForAncestor(h, ancestor, false)
		e.Apply(ancestorToParent.ReverseEdits())
	}
	ancestorToMerged := blk.getDocumentForAncestor(h, ancestor, true)
	e.Apply(ancestorToMerged.Edits())
	offset, length := getSelection(peerDoc, blk.SessionId)
	peerDoc.Simplify()
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

// commit pending ops into an opBlock, get its document, and return the replacements
// these will unwind the current document to the common ancestor and replay to the current version
func (h *History) Commit(peer, sessionId string, repls []Replacement, selOff int, selLen int) ([]Replacement, int, int, error) {
	parent := h.Latest[sessionId]
	if parent == nil {
		parent = h.Source
		h.Latest[sessionId] = parent
	}
	latestHashes := h.LatestHashes()
	if len(repls) == 0 {
		if (parent == nil && len(h.Latest) == 0) || parent != nil && SameHashes(latestHashes, parent.Parents, parent.Hash) {
			// no changes
			return []Replacement{}, selOff, selLen, nil
		}
	}
	excluded := doc.Set[Sha]{}
	for i, p := range latestHashes {
		if !excluded.Has(p) {
			par := h.GetBlock(p)
			for _, c := range latestHashes[i+1:] {
				if !excluded.Has(c) {
					child := h.GetBlock(c)
					if child.descendants.Has(p) {
						excluded.Add(c)
					} else if par.descendants.Has(c) {
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
	if len(repls) == 0 && parent != nil {
		grandSet := doc.NewSet(parent.Parents...)
		parentSet := doc.NewSet(parents...)
		parentSet.Remove(parent.Hash)
		parentSet.Subtract(grandSet)
		if len(parentSet) == 0 {
			// no changes
			return []Replacement{}, selOff, selLen, nil
		}
	}
	sortHashes(parents)
	blk, edits, err := parent.computeBlock(h, peer, sessionId, parents, repls, selOff, selLen)
	if err != nil {
		return nil, 0, 0, err
	}
	fmt.Printf("EDITED, REPL: %#v\n", edits)
	h.addBlock(blk)
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
