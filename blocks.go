package peerot

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
	"strings"
)

var ErrCollab = errors.New("Collaboration error")
var ErrDivergentBlock = fmt.Errorf("%w, divergent block", ErrCollab)

type sha = [sha256.Size]byte
type twosha = [sha256.Size * 2]byte

// checkpoints are stored by the most recent block hash of each peer and also by the
// hash of all of the blocks
type Session struct {
	peer          string
	source        sha
	latest        map[string]*OpBlock // peer -> block
	blocks        map[sha]*OpBlock
	pendingOn     map[sha]set[*OpBlock]
	pendingBlocks map[sha]*OpBlock
	storage       DocStorage
	pendingOps    []Replacement
	lcas          map[twosha]*LCA // 2-block LCAs
	blockOrder    []sha
}

type DocStorage interface {
	HasPendingBlock(hash sha) bool
	StoreBlock(blk *OpBlock) // removes from pending
	GetBlock(hash sha) *OpBlock
	HasBlock(hash sha) bool
	StorePendingBlock(*OpBlock)
	GetPendingBlocks() []*OpBlock
	StoreParameters(latest map[string]sha, pendingOps []Replacement)
	Pending(blk sha)
	PendingOn(blk sha, pendingBlock sha)
	Dirty(blk *OpBlock)
}

// a block the editing dag
// blocks do not point to each other, they use shas to support deferring to storage
// using descendants and order To find LCA, as in
// [Kowaluk & Lingas (2005)](https://people.cs.nctu.edu.tw/~tjshen/doc/fulltext.pdf)
// All public fields except Hash are transmitted (hash can be computed)
type OpBlock struct {
	Peer             string
	Hash             sha // not transmitted; computed upon reception
	Nonce            int
	Parents          []sha
	Replacements     []Replacement
	SelectionOffset  int
	SelectionLength  int
	document         *document // simple cache to speed up successive document edits
	documentAncestor sha
	blockDoc         *document // frozen document for this block
	children         []sha
	descendants      set[sha]
	order            int // block's index in the session's blockOrder list
}

type LCA struct {
	blkA     sha // blkA.order < blkB.order
	orderA   int
	blkB     sha
	orderB   int
	ancestor sha
}

type Replacement struct {
	Offset int
	Length int
	Text   string
}

// this is the default storage
type MemoryStorage struct {
	blocks        map[sha]*OpBlock
	pendingBlocks map[sha]*OpBlock
}

///
/// basic storage
///

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{map[sha]*OpBlock{}, map[sha]*OpBlock{}}
}

func (st *MemoryStorage) GetBlock(hash sha) *OpBlock {
	return st.blocks[hash]
}

func (st *MemoryStorage) HasPendingBlock(hash sha) bool {
	return st.pendingBlocks[hash] != nil
}

func (st *MemoryStorage) GetPendingBlocks() []*OpBlock {
	blocks := make([]*OpBlock, 0, len(st.pendingBlocks))
	for _, blk := range st.pendingBlocks {
		blocks = append(blocks, blk)
	}
	return blocks
}

func (st *MemoryStorage) StorePendingBlock(blk *OpBlock) {
	st.pendingBlocks[blk.Hash] = blk
}

func (st *MemoryStorage) HasBlock(hash sha) bool {
	return st.blocks[hash] != nil
}

func (st *MemoryStorage) StoreBlock(blk *OpBlock) {
	st.blocks[blk.Hash] = blk
}

func (st *MemoryStorage) StoreParameters(latest map[string]sha, pendingOps []Replacement) {
}

func (st *MemoryStorage) Dirty(blk *OpBlock) {}

func (st *MemoryStorage) Pending(blk sha) {}

func (st *MemoryStorage) PendingOn(blk sha, pendingBlock sha) {}

///
/// opBlock
///

func newOpBlock(peer string, nonce int, parents []sha, repl []Replacement, selOff, selLen int) *OpBlock {
	blk := &OpBlock{
		Peer:            peer,
		Nonce:           nonce,
		Parents:         parents,
		Replacements:    repl,
		SelectionOffset: selOff,
		SelectionLength: selLen,
		children:        make([]sha, 0, 4),
	}
	blk.computeHash()
	blk.descendants = newSet(blk.Hash)
	return blk
}

func (blk *OpBlock) isSource() bool {
	return len(blk.Parents) == 0
}

func (blk *OpBlock) computeHash() {
	var blank sha
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

func (blk *OpBlock) addToDescendants(s *Session, descendant sha, seen set[sha]) {
	if seen.has(blk.Hash) {
		return
	}
	seen.add(blk.Hash)
	blk.descendants.add(descendant)
	for _, parent := range blk.Parents {
		s.getBlock(parent).addToDescendants(s, descendant, seen)
	}
}

// parent for the same peer
func (blk *OpBlock) peerParent(s *Session) *OpBlock {
	for _, hash := range blk.Parents {
		parent := s.getBlock(hash)
		if parent.Peer == blk.Peer {
			return parent
		}
	}
	return nil
}

// parent for the same peer
func (blk *OpBlock) peerChild(s *Session) *OpBlock {
	for _, hash := range blk.children {
		child := s.getBlock(hash)
		if child.Peer == blk.Peer {
			return child
		}
	}
	return nil
}

func (blk *OpBlock) getDocument(s *Session) *document {
	if blk.document != nil && blk.documentAncestor == blk.Hash {
		return blk.document
	} else if blk.isSource() {
		blk.blockDoc = newDocument(blk.Replacements[0].Text)
		return blk.blockDoc
	}
	blk.documentAncestor = blk.Hash
	blk.document = blk.getDocumentForAncestor(s, s.lca(blk.Parents))
	return blk.document
}

func (blk *OpBlock) getDocumentForAncestor(s *Session, ancestor *OpBlock) *document {
	if ancestor == blk && blk.blockDoc != nil {
		return blk.blockDoc
	} else if ancestor == blk {
		// haven't cached a blockDoc yet
		blk.blockDoc = blk.getDocument(s).Freeze()
		return blk.blockDoc
	} else if blk.document != nil && blk.documentAncestor == ancestor.Hash {
		return blk.document
	}
	doc := ancestor.getDocumentForAncestor(s, ancestor).Copy()
	for _, hash := range blk.Parents {
		doc.merge(s.getBlock(hash).getDocumentForAncestor(s, ancestor))
	}
	blk.apply(doc)
	blk.documentAncestor = ancestor.Hash
	blk.document = doc
	return doc
}

// blocks come in any order because of pubsub
func (blk *OpBlock) checkPending(s *Session) {
	if s.pendingBlocks[blk.Hash] != nil {
		pending := false
		delete(s.pendingBlocks, blk.Hash)
		for _, hash := range blk.Parents {
			if !s.hasBlock(hash) {
				pending = true
				if s.pendingOn[hash] == nil {
					s.pendingOn[hash] = set[*OpBlock]{}
				}
				s.pendingOn[hash].add(blk)
			}
		}
		if !pending {
			s.addBlock(blk)
			for blk := range s.pendingOn[blk.Hash] {
				blk.checkPending(s)
			}
			delete(s.pendingOn, blk.Hash)
		}
	}
}

// apply replacements to document
func (blk *OpBlock) apply(doc *document) {
	for _, op := range blk.Replacements {
		doc.replace(blk.Peer, op.Offset, op.Length, op.Text)
	}
}

// compute edits to reconstruct merged document for a block
// this is for when the peer just committed the replacements, so
// it's document state is prevBlock + replacements. Compute the
// edits necessary to transform it to the merged document.
func (blk *OpBlock) edits(s *Session) ([]Replacement, int, int) {
	// The peer document = parent + edits but it needs the merged state.
	// It needs to unwind back to the ancestor and then forward to the merged current state.
	// 1. start with current doc
	// 2. transform backwards to parent's state by reversing the current edits
	// 3. transform backwards to ancestor's state by reversing the parent's document from the ancestor
	// 4. transform forward to current doc by editing forward to the merged document
	ancestor := s.lca(blk.Parents)
	parent := blk.peerParent(s)
	// The client's document is only based on the parent's merged state and its own edits.
	// We need to compute the edits required to move the client's state to the new merged state.
	// CurrentDoc models the client's document.
	// All of CurrentDoc's edits will be returned as the result.
	currentDoc := blk.getDocumentForAncestor(s, blk)                         // get current doc
	currentDoc.selection(blk.Peer, blk.SelectionOffset, blk.SelectionLength) // record selection
	parentDoc := parent.getDocumentForAncestor(s, parent)                    // get previous doc
	blk.apply(parentDoc)                                                     // edit parent ->  current
	currentDoc.apply(blk.Peer, parentDoc.reverseEdits())                     // edit current ->  parent
	parentToAncestor := parent.getDocumentForAncestor(s, ancestor)           // get edits ancestor -> parent
	currentDoc.apply(blk.Peer, parentToAncestor.reverseEdits())              // edit current -> ancestor
	ancestorToCurrent := blk.getDocumentForAncestor(s, ancestor)             // get edits ancestor -> current
	currentDoc.apply(blk.Peer, ancestorToCurrent.edits())                    // edit current -> merged
	off, len := currentDoc.getSelection(blk.Peer)
	return currentDoc.edits(), off, len
}

///
/// LCA
///

func newTwosha(h1 sha, h2 sha) twosha {
	hashes := []sha{h1, h2}
	sortHashes(hashes)
	var result twosha
	copy(result[:], hashes[0][:])
	copy(result[len(hashes[0]):], hashes[1][:])
	return result
}

///
/// session
///

func newSession(peer string, text string, storage DocStorage) *Session {
	src := newOpBlock(peer, 0, []sha{}, []Replacement{{Offset: 0, Length: 0, Text: text}}, 0, 0)
	src.order = 0
	s := &Session{
		latest:        map[string]*OpBlock{},
		blocks:        map[sha]*OpBlock{src.Hash: src},
		pendingBlocks: map[sha]*OpBlock{},
		pendingOn:     map[sha]set[*OpBlock]{},
		pendingOps:    make([]Replacement, 0, 8),
		storage:       storage,
		blockOrder:    append(make([]sha, 0, 8), src.Hash),
	}
	s.lcas = map[twosha]*LCA{}
	storage.StoreBlock(src)
	return s
}

func (s *Session) recomputeOrder() {
	// number blocks in breadth-first order from the source by children
	cur := make([]sha, 0, 8)
	next := append(make([]sha, 0, 8), s.source)
	seen := set[sha]{}
	s.blockOrder = s.blockOrder[:0]
	for len(next) > 0 {
		cur, next = next, cur[:0]
		for _, child := range cur {
			if seen.has(child) {
				continue
			}
			seen.add(child)
			blk := s.getBlock(child)
			blk.order = len(s.blockOrder)
			s.blockOrder = append(s.blockOrder, child)
			next = append(next, blk.children...)
		}
	}
	// clear LCA cache
	s.lcas = map[twosha]*LCA{}
}

// these are cached; when new blocks come in from outside,
// they can cause renumbering, which clears the cache
func (s *Session) lca2(blkA *OpBlock, blkB *OpBlock) *OpBlock {
	// ensure blkA is the lower block
	if blkB.order < blkA.order {
		blkA, blkB = blkB, blkA
	}
	key := newTwosha(blkA.Hash, blkB.Hash)
	lca := s.lcas[key]
	if lca != nil && lca.blkA == blkA.Hash && blkA.order == lca.orderA && blkB.order == lca.orderB {
		return s.getBlock(lca.ancestor)
	}
	// start with the lowest block
	for i := blkA.order; i >= 0; i-- {
		anc := s.getBlock(s.blockOrder[i])
		if anc.descendants.has(blkA.Hash) && anc.descendants.has(blkB.Hash) {
			s.lcas[key] = &LCA{
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
func (s *Session) lca(hashes []sha) *OpBlock {
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
		anc := s.getBlock(s.blockOrder[index])
		for _, hash := range hashes {
			if !anc.descendants.has(hash) {
				continue LCA
			}
		}
		return anc
	}
	return nil
}

func (s *Session) getBlock(hash sha) *OpBlock {
	if s.blocks[hash] != nil {
		return s.blocks[hash]
	}
	return s.pendingBlocks[hash]
}

func (s *Session) hasBlock(hash sha) bool {
	return s.blocks[hash] != nil || s.pendingBlocks[hash] != nil
}

func sortHashes(hashes []sha) {
	sort.Slice(hashes, func(i, j int) bool {
		return bytes.Compare(hashes[i][:], hashes[j][:]) < 0
	})
}

// sorted hashes of the most recent blocks in the known chains
func (s *Session) latestHashes() []sha {
	var hashes []sha
	hashes = make([]sha, len(s.latest))
	pos := 0
	for peer := range s.latest {
		hashes[pos] = s.latest[peer].Hash
		pos++
	}
	sortHashes(hashes)
	return hashes
}

// add a replacement to pendingOps
func (s *Session) Replace(offset int, length int, text string) {
	s.pendingOps = append(s.pendingOps, Replacement{offset, length, text})
}

// add a replacement to pendingOps
func (s *Session) ReplaceAll(replacements []Replacement) {
	for _, repl := range replacements {
		s.Replace(repl.Offset, repl.Length, repl.Text)
	}
}

func (s *Session) addBlock(blk *OpBlock) {
	seen := newSet(blk.Hash)
	for _, parentHash := range blk.Parents {
		parent := s.getBlock(parentHash)
		parent.children = append(parent.children, blk.Hash)
		parent.addToDescendants(s, blk.Hash, seen)
	}
	s.latest[blk.Peer] = blk
	s.blocks[blk.Hash] = blk
	for _, hash := range blk.Parents {
		if s.getBlock(hash).order == len(s.blocks)-1 {
			blk.order = len(s.blocks)
			s.blockOrder = append(s.blockOrder, blk.Hash)
			return
		}
	}
	// none of the parents had order == len(s.blocks)-1
	s.recomputeOrder()
}

func (s *Session) addIncomingBlock(blk *OpBlock) error {
	if s.hasBlock(blk.Hash) {
		return nil
	}
	prev := blk.peerParent(s)
	if prev == nil && s.latest[blk.Peer] != s.getBlock(s.source) || prev.peerChild(s) != nil {
		return ErrDivergentBlock
	}
	s.pendingBlocks[blk.Hash] = blk
	blk.checkPending(s)
	return nil
}

// commit pending ops into an opBlock, get its document, and return the replacements
// these will unwind the current document to the common ancestor and replay to the current version
func (s *Session) Commit(selOff int, selLen int) ([]Replacement, int, int) {
	repl := make([]Replacement, len(s.pendingOps))
	copy(repl, s.pendingOps)
	s.pendingOps = s.pendingOps[:0]
	blk := newOpBlock(s.peer, len(s.blockOrder), s.latestHashes(), repl, selOff, selLen)
	s.addBlock(blk)
	if len(blk.Parents) == 1 {
		return blk.Replacements, selOff, selLen
	}
	return blk.edits(s)
}

func Apply(str string, repl []Replacement) string {
	sb := &strings.Builder{}
	pos := 0
	for _, r := range repl {
		if r.Offset > pos {
			sb.WriteString(str[pos:r.Offset])
		}
		sb.WriteString(r.Text)
		pos += r.Length
	}
	if pos < len(str) {
		sb.WriteString(str[pos:])
	}
	return sb.String()
}
