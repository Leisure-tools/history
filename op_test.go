package peerot

import (
	"fmt"
	"runtime/debug"
	"strings"
	"testing"
)

func replace(t *testing.T, tree *document, peer string, start int, length int, text string) {
	str := tree.String()
	tree.replace(peer, start, length, text)
	expect := fmt.Sprintf("%s%s%s", str[0:start], text, str[start+length:])
	if tree.String() != expect {
		fmt.Printf("Bad tree '%s', expected '%s'\n", tree.String(), expect)
		t.FailNow()
	}
	//fmt.Println("Tree:", tree)
	//fmt.Println("Ops:", tree.opString())
}

func testEqual(t *testing.T, actual any, expected any, msg string) {
	failIfNot(t, actual == expected, fmt.Sprintf("%s: expected <%v> but got <%v>", msg, expected, actual))
}

func failIfNot(t *testing.T, cond bool, msg string) {
	if !cond {
		t.Log(msg)
		t.Fail()
	}
}

func failIfErrNow(t *testing.T, err any) {
	if err != nil {
		t.Log(err)
		debug.PrintStack()
		t.FailNow()
	}
}

func nth(tree opTree, n int) operation {
	count := 0
	var item operation
	tree.Each(func(op operation) bool {
		if count == n {
			item = op
			return false
		}
		count++
		return true
	})
	return item
}

func TestDoc(t *testing.T) {
	doc := newDocument("aaaaa")
	replace(t, doc, "peer", 0, 0, "hello")
	replace(t, doc, "peer", 1, 2, "d")
	doc = newDocument("aaaaa")
	replace(t, doc, "peer", 3, 1, "hello")
	replace(t, doc, "peer", 2, 2, "")
	replace(t, doc, "peer", 0, 0, "bbb")
}

const doc1 = `line one
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
	doc := newDocument(doc1)
	replace(t, doc, peer, index(doc1, 0, 5), 3, "ONE")
	replace(t, doc, peer, index(doc1, 2, 10), 0, "\nline four")
	return doc
}

func docTWO(t *testing.T, peer string) *document {
	doc := newDocument(doc1)
	replace(t, doc, peer, index(doc1, 1, 5), 3, "TWO")
	replace(t, doc, peer, index(doc1, 2, 10), 0, "\nline five")
	return doc
}

func docs(t *testing.T) (*document, *document) {
	return docONE(t, "peer1"), docTWO(t, "peer2")
}

func TestMerge(t *testing.T) {
	a, b := docs(t)
	a.merge(b)
	//fmt.Println("Merged:", a)
	//fmt.Println("Ops:", a.opString())
	testEqual(t, a.String(), docMerged, "unsuccessful merge")
	a, b = docs(t)
	b.merge(a)
	//fmt.Println("Merged:", b)
	//fmt.Println("Ops:", b.opString())
	bDoc := b.String()
	testEqual(t, bDoc, docMerged, "unsuccessful merge")
	revDoc := newDocument(bDoc)
	for _, r := range b.reverseEdits() {
		replace(t, revDoc, "peer1", r.Offset, r.Length, r.Text)
	}
	testEqual(t, revDoc.String(), doc1, "unsuccessful reversal")
}

func commitEdits(t *testing.T, s *Session, doc *document, expected []Replacement) {
	commitReplacements(t, s, doc.edits(), expected)
}

func commitReplacements(t *testing.T, s *Session, edits []Replacement, expected []Replacement) {
	s.ReplaceAll(edits)
	repl, _, _ := s.Commit(0, 0)
	testEqual(t, len(repl), len(expected), "replacements did not match after commit")
	for i := range repl {
		testEqual(t, repl[i], expected[i], "replacements did not match after commit")
	}
}

func mergeBlock(t *testing.T, peer string, s *Session, blk *OpBlock, startDoc, expected string) {
	l := len(s.blockOrder)
	s.addIncomingBlock(blk)
	testBlockOrder(t, s, l+1, 1)
	repl, _, _ := s.Commit(0, 0)
	str := Apply(startDoc, repl)
	testEqual(t, str, expected, "Result did not match expected")
}

// strip out local data
func outgoing(s *Session, peer string) *OpBlock {
	blk := s.latest[peer]
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
	lenBlocks := len(s.blocks)
	testEqual(t, lenBlocks, expected,
		fmt.Sprintf("session %s expected len(blocks) = %d but got %d\n", s.peer, expected, len(s.blocks)))
	lenOrder := len(s.blockOrder)
	testEqual(t, lenOrder, expected,
		fmt.Sprintf("session %s expected len(blockOrder) = %d but got %d\n", s.peer, expected, len(s.blockOrder)))
	for _, hash := range s.blockOrder[lenOrder-additional:] {
		failIfNot(t, s.getBlock(hash) != nil, fmt.Sprintf("Could not find block for hash in block order"))
	}
}

func TestEditing(t *testing.T) {
	s1 := newSession("peer1", doc1, NewMemoryStorage())
	testBlockOrder(t, s1, 1, 1)
	s2 := newSession("peer2", doc1, NewMemoryStorage())
	testBlockOrder(t, s2, 1, 1)
	d1, d2 := docs(t)
	commitEdits(t, s1, d1, d1.edits())
	testBlockOrder(t, s1, 2, 1)
	commitEdits(t, s2, d2, d2.edits())
	testBlockOrder(t, s2, 2, 1)
	mergeBlock(t, "peer1", s1, outgoing(s2, s2.peer), d1.String(), docMerged)
	mergeBlock(t, "peer2", s2, outgoing(s1, s1.peer), d2.String(), docMerged)
}
