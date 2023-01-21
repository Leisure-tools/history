package peerot

import (
	"fmt"
	"runtime/debug"
	"strings"
	"testing"
)

func replace(t *testing.T, tree *document, start int, length int, text string) {
	str := tree.String()
	tree.replace(start, length, text)
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
	doc := newDocument("peer", "aaaaa")
	replace(t, doc, 0, 0, "hello")
	replace(t, doc, 1, 2, "d")
	doc = newDocument("peer", "aaaaa")
	replace(t, doc, 3, 1, "hello")
	replace(t, doc, 2, 2, "")
	replace(t, doc, 0, 0, "bbb")
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
	a := newDocument("peer1", doc1)
	replace(t, a, index(doc1, 0, 5), 3, "ONE")
	replace(t, a, index(doc1, 2, 10), 0, "\nline four")
	b := newDocument("peer2", doc1)
	replace(t, b, index(doc1, 1, 5), 3, "TWO")
	replace(t, b, index(doc1, 2, 10), 0, "\nline five")
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
	testEqual(t, b.String(), docMerged, "unsuccessful merge")
}
