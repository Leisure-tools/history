package history

import (
	"fmt"
	"os"
	"runtime/debug"
	"strings"
	"testing"

	doc "github.com/leisure-tools/document"
)

func replace(t *testing.T, tree *document, peer string, start int, length int, text string) {
	str := tree.String()
	tree.Replace(peer, start, length, text)
	expect := fmt.Sprintf("%s%s%s", str[0:start], text, str[start+length:])
	if tree.String() != expect {
		fmt.Printf("Bad tree '%s', expected '%s'\n", tree.String(), expect)
		t.FailNow()
	}
}

func testEqual(t *testing.T, actual any, expected any, msg string) {
	failIfNot(t, actual == expected, fmt.Sprintf("%s: expected <%v> but got <%v>", msg, expected, actual))
}

func testEqualRepls(t *testing.T, repl1, repl2 []Replacement, msg string) {
	testEqual(t, len(repl1), len(repl2), msg)
	for i, repl := range repl1 {
		testEqual(t, repl, repl2[i], msg)
	}
}

func failIfNot(t *testing.T, cond bool, msg string) {
	if !cond {
		t.Log(msg)
		fmt.Fprintln(os.Stderr, msg)
		debug.PrintStack()
		t.FailNow()
	}
}

func failIfErrNow(t *testing.T, err any) {
	if err != nil {
		t.Log(err)
		fmt.Fprintln(os.Stderr, err)
		debug.PrintStack()
		t.FailNow()
	}
}

const docString1 = `line one
line two
line three`

const docMerged = `line ONE
line TWO
line three
line four
line five`

func index(str string, line, col int) int {
	i := 0
	for line > 0 {
		index := strings.IndexByte(str, '\n') + 1
		i += index
		str = str[index:]
		line--
	}
	return i + col
}

func docONE(t *testing.T, peer string) *document {
	d := doc.NewDocument(docString1)
	replace(t, d, peer, index(docString1, 0, 5), 3, "ONE")
	replace(t, d, peer, index(docString1, 2, 10), 0, "\nline four")
	return d
}

func docTWO(t *testing.T, peer string) *document {
	d := doc.NewDocument(docString1)
	replace(t, d, peer, index(docString1, 1, 5), 3, "TWO")
	replace(t, d, peer, index(docString1, 2, 10), 0, "\nline five")
	return d
}

func docs(t *testing.T) (*document, *document) {
	return docONE(t, "peer1"), docTWO(t, "peer2")
}

func TestMerge(t *testing.T) {
	a, b := docs(t)
	a.Merge(b)
	testEqual(t, a.String(), docMerged, "unsuccessful merge")
	a, b = docs(t)
	b.Merge(a)
	bDoc := b.String()
	testEqual(t, bDoc, docMerged, "unsuccessful merge")
	revDoc := doc.NewDocument(bDoc)
	for _, r := range b.ReverseEdits() {
		replace(t, revDoc, "peer1", r.Offset, r.Length, r.Text)
	}
	testEqual(t, revDoc.String(), docString1, "unsuccessful reversal")
}

func commitEdits(t *testing.T, s *Session, doc *document, expected []Replacement) {
	commitReplacements(t, s, doc.Edits(), expected)
}

func commitReplacements(t *testing.T, s *Session, edits []Replacement, expected []Replacement) {
	s.ReplaceAll(edits)
	s.Commit(0, 0)
	//repl, _, _ := s.Commit(0, 0)
	//testEqualRepls(t, repl, expected, "replacements did not match after commit")
	latest := s.History.Latest[s.Peer]
	prev := latest.peerParent(s.History)
	if prev == nil {
		prev = s.History.Source
	}
	anc := latest.getDocumentForAncestor(s.History, prev)
	testEqualRepls(t, anc.Edits(), expected, "ancestor edits did not match")
}

func addBlock(t *testing.T, s *Session, blk *OpBlock, expected string) {
	l := s.History.Storage.GetBlockCount()
	failIfErrNow(t, s.History.addIncomingBlock(blk))
	testBlockOrder(t, s, l+1, 1)
	prev := blk.peerParent(s.History)
	if prev == nil {
		prev = s.History.Source
	}
	ancestor := s.History.lca(blk.Parents)
	ancestorDoc := ancestor.getDocumentForAncestor(s.History, ancestor)
	anc := s.History.Latest[blk.Peer].getDocumentForAncestor(s.History, prev)
	testEqual(t, anc.String(), expected, "ancestor doc did not match")
	testEqual(t, anc.OriginalString(), ancestorDoc.String(), "ancestor doc did not match")
}

func testCommit(t *testing.T, s *Session, startDoc, expected string) {
	repl, _, _ := s.Commit(0, 0)
	str := doc.NewDocument(startDoc)
	str.Apply(s.Peer, repl)
	testEqual(t, str.String(), expected, "Result did not match expected")
}

// strip out local data
func outgoing(s *Session) *OpBlock {
	blk := s.History.Latest[s.Peer]
	return newOpBlock(
		blk.Peer,
		blk.Nonce,
		blk.Parents,
		blk.Replacements,
		blk.SelectionOffset,
		blk.SelectionLength,
	)
}

func testBlockOrder(t *testing.T, s *Session, expected, additional int) {
	s.History.getBlockOrder()
	lenBlocks := s.History.Storage.GetBlockCount()
	testEqual(t, lenBlocks, expected,
		fmt.Sprintf("session expected len(blocks) = %d but got %d\n", expected, lenBlocks))
	lenOrder := len(s.History.getBlockOrder())
	testEqual(t, lenOrder, expected,
		fmt.Sprintf("session expected len(blockOrder) = %d but got %d\n", expected, len(s.History.BlockOrder)))
	for _, hash := range s.History.BlockOrder[lenOrder-additional:] {
		failIfNot(t, s.History.getBlock(hash) != nil, fmt.Sprintf("Could not find block for hash in block order"))
	}
}

func clearHistoryCache(histories ...*History) {
	for _, h := range histories {
		h.Blocks = make(map[Sha]*OpBlock)
		h.PendingOn = make(map[Sha]doc.Set[Sha])
		h.PendingBlocks = make(map[Sha]*OpBlock)
		h.LCAs = make(map[Twosha]*LCA)
		h.BlockOrder = h.BlockOrder[:0]
		h.Blocks[h.Source.Hash] = h.Source
		for _, blk := range h.Latest {
			h.Blocks[blk.Hash] = blk
		}
	}
}

func testSession(t *testing.T, peer string, doc string) *Session {
	ch := NewHistory(NewMemoryStorage(doc), docString1)
	s := NewSession(peer, ch)
	testBlockOrder(t, s, 1, 1)
	clearHistoryCache(ch)
	return s
}

func TestEditing(t *testing.T) {
	s1 := testSession(t, "peer1", docString1)
	s2 := testSession(t, "peer2", docString1)
	d1 := doc.NewDocument(docString1)
	d2 := docTWO(t, "peer2")
	doc2 := d2.String()
	commitEdits(t, s2, d2, []Replacement{
		{
			Offset: 14,
			Length: 3,
			Text:   "TWO"},
		{
			Offset: 28,
			Length: 0,
			Text: `
line five`},
	})
	blk2 := outgoing(s2)
	addBlock(t, s1, blk2, doc2)
	commitReplacements(t, s1, []Replacement{}, []Replacement{
		{
			Offset: 14,
			Length: 3,
			Text:   "TWO"},
		{
			Offset: 28,
			Length: 0,
			Text: `
line five`},
	})
	s1 = testSession(t, "peer1", docString1)
	s2 = testSession(t, "peer2", docString1)
	testEqual(t, s1.History.Source.Hash, s2.History.Source.Hash, "source hashes are not identical")
	d1, d2 = docs(t)
	doc1 := d1.String()
	doc2 = d2.String()
	clearHistoryCache(s1.History, s2.History)
	commitEdits(t, s1, d1, []Replacement{
		{
			Offset: 5,
			Length: 3,
			Text:   "ONE"},
		{
			Offset: 28,
			Length: 0,
			Text: `
line four`},
	})
	clearHistoryCache(s1.History, s2.History)
	testBlockOrder(t, s1, 2, 1)
	clearHistoryCache(s1.History, s2.History)
	commitEdits(t, s2, d2, []Replacement{
		{
			Offset: 14,
			Length: 3,
			Text:   "TWO"},
		{
			Offset: 28,
			Length: 0,
			Text: `
line five`},
	})
	clearHistoryCache(s1.History, s2.History)
	testBlockOrder(t, s2, 2, 1)
	clearHistoryCache(s1.History, s2.History)
	blk1 := outgoing(s1)
	clearHistoryCache(s1.History, s2.History)
	blk2 = outgoing(s2)
	clearHistoryCache(s1.History, s2.History)
	addBlock(t, s1, blk2, doc2)
	clearHistoryCache(s1.History, s2.History)
	testCommit(t, s1, doc1, docMerged)
	clearHistoryCache(s1.History, s2.History)
	addBlock(t, s2, blk1, doc1)
	clearHistoryCache(s1.History, s2.History)
	testCommit(t, s2, doc2, docMerged)
}
