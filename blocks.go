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
	pendingOn     map[sha][]*OpBlock
	pendingBlocks map[sha]*OpBlock
	storage       DocStorage
	pendingOps    []*Replacement
	lcas          map[twosha]*LCA
	blockOrder    []sha
	//checkpoints   map[sha]*lcaContent // latest checkpoint for a block hash or peer hash
}

//type lcaContent struct {
//	content string
//}

type DocStorage interface {
	EachBlockHash(func(sha))
	HasPendingBlock(hash sha) bool
	StoreBlock(blk *OpBlock) // removes from pending
	GetBlock(hash sha) *OpBlock
	HasBlock(hash sha) bool
	StorePendingBlock(*OpBlock)
	GetPendingBlocks() []*OpBlock
	//StoreLcaContent(chp *lcaContent)
	StoreParameters(latest map[string]sha, pendingOps []*Replacement)
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
	//cp               *lcaContent // cached at each LCA
	children    []sha
	descendants set[sha]
	order       int // block's index in the session's blockOrder list
}

type LCA struct {
	blkA     sha
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
	//checkpoints   map[sha]*lcaContent
}

///
/// basic storage
///

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{map[sha]*OpBlock{}, map[sha]*OpBlock{}}
}

func (st MemoryStorage) EachBlockHash(fn func(sha)) {
	for hash := range st.blocks {
		fn(hash)
	}
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

//func (st *MemoryStorage) StoreLcaContent(chp *lcaContent) {
//	st.checkpoints[chp.hash()] = chp
//}

func (st *MemoryStorage) StoreParameters(latest map[string]sha, pendingOps []*Replacement) {
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

//func (blk *OpBlock) computeOrder(s *Session) {
//	for _, hash := range blk.Parents {
//		if s.getBlock(hash).order == len(s.blocks)-1 {
//			blk.order = len(s.blocks)
//			return
//		}
//	}
//	s.recomputeOrder()
//}

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
					s.pendingOn[hash] = make([]*OpBlock, 0, 8)
				}
				s.pendingOn[hash] = append(s.pendingOn[hash], blk)
			}
		}
		if !pending {
			s.addBlock(blk)
			for _, blk := range s.pendingOn[blk.Hash] {
				blk.checkPending(s)
			}
			delete(s.pendingOn, blk.Hash)
		}
	}
}

//func (blk *OpBlock) recentCheckpoint(s *Session) *lcaContent {
//	//for blk != nil {
//	//	if ch := s.checkpoints[blk.Hash]; ch != nil {
//	//		return ch
//	//	}
//	//	blk = s.getBlock(blk.PrevHash)
//	//}
//	return s.checkpoints[sha{}]
//}

//func (blk *OpBlock) prev(s *Session) *OpBlock {
//	return s.getBlock(blk.PrevHash)
//}

//func (blk *OpBlock) blocksAfter(s *Session, chp *checkpoint) []*OpBlock {
//	blocks := make([]*OpBlock, 0, 8)
//	for blk != nil {
//		if (chp.hashes[blk.Peer] != sha{}) {
//			break
//		}
//		blocks = append(blocks, blk)
//		blk = blk.prev(s)
//	}
//	return blocks
//}

//func (blk *OpBlock) addChild(hash sha) {
//	blk.children = append(blk.children, hash)
//}

func (blk *OpBlock) computeBlockDoc(s *Session) *document {
	if blk.document != nil {
		return blk.document
	}
	//blk.document = s.mergeBlocks(blk.InputHashes).Copy()
	blk.apply(blk.document)
	return blk.document
}

// apply replacements to document
func (blk *OpBlock) apply(doc *document) {
	for _, op := range blk.Replacements {
		doc.replace(blk.Peer, op.Offset, op.Length, op.Text)
	}
}

// compute a document starting from a checkpoint
//func (blk *OpBlock) docFrom(s *Session, chp *checkpoint) *document {
//	blocks := blk.blocksAfter(s, chp)
//	doc := newDocument(chp.content)
//	for i := len(blocks) - 1; i >= 0; i-- {
//		blocks[i].apply(doc)
//	}
//	return doc
//}

///
/// checkpoint
///

//func newCheckpoint(s *Session, hashes map[string]sha, content string) *lcaContent {
//	return &lcaContent{
//		//hashes:  hashes,
//		content: content,
//		//serial:  len(s.checkpoints),
//	}
//}

//func (chp *checkpoint) has(blk *OpBlock) bool {
//	for _, hash := range chp.hashes {
//		if hash == blk.Hash {
//			return true
//		}
//	}
//	return false
//}

//func (chp *checkpoint) hash() sha {
//	hashes := make([]sha, len(chp.hashes))
//	pos := 0
//	for peer := range chp.hashes {
//		hashes[pos] = chp.hashes[peer]
//		pos++
//	}
//	sortHashes(hashes)
//	return hashHash(hashes)
//}

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

// blkA.order < blkB.order
func newLCA(blkA *OpBlock, blkB *OpBlock, ancestor sha) (twosha, *LCA) {
	key := newTwosha(blkA.Hash, blkB.Hash)
	return key, &LCA{
		blkA:     blkA.Hash,
		orderA:   blkA.order,
		blkB:     blkB.Hash,
		orderB:   blkB.order,
		ancestor: ancestor,
	}
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
		pendingOn:     map[sha][]*OpBlock{},
		//checkpoints:   map[sha]*lcaContent{},
		pendingOps: make([]*Replacement, 0, 8),
		storage:    storage,
		blockOrder: append(make([]sha, 0, 8), src.Hash),
	}
	s.lcas = map[twosha]*LCA{}
	storage.StoreBlock(src)
	//s.checkpoints[sha{}] = newCheckpoint(s, map[string]sha{}, text)
	return s
}

func (s *Session) eachBlock(fn func(*OpBlock)) {
	for _, block := range s.blocks {
		fn(block)
	}
}

func (s *Session) recomputeOrder() {
	// number blocks in breadth-first order from the source by children
	cur := make([]sha, 0, 8)
	next := append(make([]sha, 0, 8), s.source)
	seen := set[sha]{}
	order := 0
	s.blockOrder = s.blockOrder[:]
	for len(next) > 0 {
		cur, next = next, cur[:0]
		for _, child := range cur {
			if seen.has(child) {
				continue
			}
			seen.add(child)
			s.blockOrder = append(s.blockOrder, child)
			blk := s.getBlock(child)
			blk.order = order
			order++
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

func hashHash(hashes []sha) sha {
	hashDoc := &strings.Builder{}
	for _, hash := range hashes {
		fmt.Fprintln(hashDoc, hash)
	}
	return sha256.Sum256([]byte(hashDoc.String()))
}

//// return whether there has been incoming activity since the last checkpoint
//func (s *Session) hasActivity() bool {
//	return s.checkpoints[hashHash(s.latestHashes())] == nil
//}

// produce a document from an ancestor by merging in edits from blocks
//func (s *Session) docFromCheckpoint(ancestor *checkpoint, blocks []*OpBlock) *document {
//	content := s.base
//	if ancestor != nil {
//		content = ancestor.content
//	}
//	doc := newDocument(content)
//	for _, blk := range blocks {
//		if ancestor != nil && ancestor.has(blk) {
//			continue
//		}
//		doc.merge(blk.docFrom(s, ancestor))
//	}
//	return doc
//}

// compute a new checkpoint if there has been any activity
// return the checkpoint and a document containing edits from the previous checkpoint
//func (s *Session) checkpoint() (*checkpoint, *document) {
//	if ch := s.checkpoints[hashHash(s.chainHashes(true))]; ch != nil {
//		// no activity
//		return nil, nil
//	}
//	ancestor := s.chains[s.peer].recentCheckpoint(s)
//	blocks := make([]*OpBlock, 0, len(s.chains))
//	for _, blk := range s.chains {
//		blocks = append(blocks, blk)
//	}
//	doc := s.docFromCheckpoint(ancestor, blocks)
//	cpHashes := map[string]sha{}
//	chp := newCheckpoint(s, cpHashes, doc.String())
//	s.checkpoints[hashHash(s.chainHashes(true))] = chp
//	for peer := range s.peers {
//		cpHashes[peer] = s.chains[peer].Hash
//		s.checkpoints[s.chains[peer].Hash] = chp
//	}
//	s.storage.StoreCheckpoint(chp)
//	s.pruneBefore(ancestor)
//	return ancestor, doc
//}

//// remove blocks before checkpoint from memory
//func (s *Session) pruneBefore(chp *lcaContent) {
//}

// add a replacement to pendingOps
func (s *Session) Replace(offset int, length int, text string) {
	s.pendingOps = append(s.pendingOps, &Replacement{offset, length, text})
}

// compute edits to reconstruct merged document for a block
func (s *Session) edits(blk *OpBlock) ([]Replacement, int, int) {
	// the client has the current doc state
	// in order to merge properly, it needs to unwind back to the ancestor and
	// then back to the merged current state
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

func (s *Session) addBlock(blk *OpBlock) {
	seen := newSet(blk.Hash)
	for _, parentHash := range blk.Parents {
		parent := s.getBlock(parentHash)
		parent.children = append(parent.children, blk.Hash)
		parent.addToDescendants(s, blk.Hash, seen)
	}
	s.latest[s.peer] = blk
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
	//prev := s.chains[s.peer]
	repl := make([]Replacement, len(s.pendingOps))
	for i := range repl {
		repl[i] = *s.pendingOps[i]
	}
	blk := newOpBlock(s.peer, len(s.blockOrder), s.latestHashes(), repl, selOff, selLen)
	s.addBlock(blk)
	if len(blk.Parents) == 1 {
		return blk.Replacements, selOff, selLen
	}
	return s.edits(blk)
}

//func (s *Session) UpdateLatest() []*OpBlock {
//	ops := make([]*OpBlock, 0, 8)
//	for blk := s.latest; blk != nil; blk = s.getBlock(s.nextBlock[blk.Hash]) {
//		ops = append(ops, blk)
//	}
//	s.latest = ops[len(ops)-1]
//	return ops
//}

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
