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

func docs(t *testing.T) (*document, *document) {
	a := newDocument(doc1)
	replace(t, a, "peer1", index(doc1, 0, 5), 3, "ONE")
	replace(t, a, "peer1", index(doc1, 2, 10), 0, "\nline four")
	b := newDocument(doc1)
	replace(t, b, "peer2", index(doc1, 1, 5), 3, "TWO")
	replace(t, b, "peer2", index(doc1, 2, 10), 0, "\nline five")
	return a, b
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
