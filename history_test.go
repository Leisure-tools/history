package history

import (
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"runtime/debug"
	"strings"
	"testing"

	doc "github.com/leisure-tools/document"
	diff "github.com/sergi/go-diff/diffmatchpatch"
)

// diagnostics
var verbosity = 0
var inspector historyInspector
var block historyBlock

const docString0 = `line one
line two
line three`

const docString1 = `line ONE
line two
line three
line four`

const docString2 = `line one
line TWO
line three
line five`

const docMerged = `line ONE
line TWO
line three
line four
line five`

type historyInspector struct {
	*History
	latest []historyBlock
}

type historyBlock struct {
	sessionId string
	order     int
	parents   []historyBlock
	OpBlock
}

type myT struct {
	*testing.T
}

type twoSessions struct {
	myT
	history    *History
	s1         *testSession
	s2         *testSession
	blockNames map[Sha]string
}

type testSession struct {
	myT
	*twoSessions
	*History
	peer      string
	sessionId string
	nonce     int
}

func verbose(n int, format string, args ...any) {
	if n <= verbosity {
		fmt.Printf(format, args...)
	}
}

func Inspect(h *History, blk *OpBlock) historyBlock {
	if h == nil {
		return historyBlock{sessionId: ""}
	}
	i := historyInspector{History: h}
	l := len(h.Latest)
	if l == 0 {
		l = 1
	}
	result := make([]historyBlock, 0, l)
	blocks := map[Sha]*historyBlock{}
	if len(h.Latest) == 0 {
		result = append(result, *addInspect(h, h.Source, blocks))
	} else {
		for _, b := range h.Latest {
			result = append(result, *addInspect(h, b, blocks))
		}
	}
	i.latest = result
	inspector = i
	block = *blocks[blk.Hash]
	return block
}

func addInspect(h *History, blk *OpBlock, blocks map[Sha]*historyBlock) *historyBlock {
	if blocks[blk.Hash] != nil {
		return blocks[blk.Hash]
	}
	hb := historyBlock{
		sessionId: blk.SessionId,
		order:     blk.order,
		parents:   make([]historyBlock, len(blk.Parents)),
		OpBlock:   *blk,
	}
	for i, parent := range blk.Parents {
		hb.parents[i] = *addInspect(h, h.GetBlock(parent), blocks)
	}
	blocks[blk.Hash] = &hb
	return &hb
}

///

func replace(t myT, doc *document, owner string, offset, start int, length int, text string) {
	str := doc.String()
	doc.Replace(owner, offset, start, length, text)
	expect := fmt.Sprintf("%s%s%s", str[0:start], text, str[start+length:])
	if doc.String() != expect {
		fmt.Printf("Bad tree '%s', expected '%s'\n", doc.String(), expect)
		t.FailNow()
	}
}

func (t myT) testEqual(actual any, expected any, msg string) {
	testEqual(t, actual, expected, msg)
}

func testEqual(t myT, actual any, expected any, msg string) {
	if s, ok := actual.(string); ok {
		actual = strings.ReplaceAll(s, "\n", "\\n")
	}
	if s, ok := expected.(string); ok {
		expected = strings.ReplaceAll(s, "\n", "\\n")
	}
	t.failIfNot(actual == expected, fmt.Sprintf("%s: expected <%v> but got <%v>", msg, expected, actual))
}

func testEqualRepls(t myT, repl1, repl2 []Replacement, msg string) {
	testEqual(t, len(repl1), len(repl2), msg)
	for i, repl := range repl1 {
		testEqual(t, repl, repl2[i], msg)
	}
}

func (t myT) failIfNot(cond bool, format string, args ...any) {
	if !cond {
		t.failNow(fmt.Sprintf(format, args...))
	}
}

func failIfErrNow(t myT, err any) {
	if err != nil {
		t.failNow(err)
	}
}

func (t myT) failNow(msg any) {
	t.Log(msg)
	fmt.Fprintln(os.Stderr, msg)
	debug.PrintStack()
	t.FailNow()
}

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

func docONE(t myT, session string) *document {
	d := doc.NewDocument(docString0)
	replace(t, d, session, 0, index(docString0, 0, 5), 3, "ONE")
	replace(t, d, session, 3, index(docString0, 2, 10), 0, "\nline four")
	return d
}

func docTWO(t myT, session string) *document {
	d := doc.NewDocument(docString0)
	replace(t, d, session, 0, index(docString0, 1, 5), 3, "TWO")
	replace(t, d, session, 3, index(docString0, 2, 10), 0, "\nline five")
	return d
}

func docs(t myT) (*document, *document) {
	return docONE(t, "session1"), docTWO(t, "session2")
}

func changes(d *document) {
	fmt.Println(d.Changes(""))
}

func ops(d *document) {
	fmt.Println(d.OpString(true))
}

func treeOps(ops doc.OpTree) {
	fmt.Println(doc.OpString(ops, true))
}

func toSlice(d *document) []doc.Operation {
	return d.GetOps().ToSlice()
}

func values(v ...any) []any {
	result := make([]any, 0, len(v))
	for _, obj := range v {
		result = append(result, reflect.ValueOf(obj))
	}
	return result
}

func Init() {
	fmt.Println("HELLO")
	//fmt.Printf("%v %v %v", values(changes, ops, treeOps, toSlice, ft.Len[doc.OpMeasurer, doc.Operation, doc.Measure])...)
	fmt.Printf("%v %v %v", values(changes, ops, treeOps, toSlice, doc.LenTree)...)
}

func TestMerge(tt *testing.T) {
	// uncomment this to allow changes, ops, etc. in debugging
	//Init()
	t := myT{tt}
	Inspect(nil, nil)
	a, b := docs(t)
	a.Merge(b)
	testEqual(t, a.String(), docMerged, "unsuccessful merge")
	a, b = docs(t)
	b.Merge(a)
	bDoc := b.String()
	testEqual(t, bDoc, docMerged, "unsuccessful merge")
	revDoc := doc.NewDocument(bDoc)
	pos := 0
	for _, r := range b.ReverseEdits() {
		replace(t, revDoc, "session1", pos, r.Offset, r.Length, r.Text)
		pos += len(r.Text)
	}
	testEqual(t, revDoc.String(), docString0, "unsuccessful reversal")
}

func commitEdits(t myT, s *testSession, doc *document, expected []Replacement, expectedDoc string) {
	commitReplacements(t, s, doc.Edits(), expected, expectedDoc)
}

func commitReplacements(t myT, p *testSession, edits []Replacement, expected []Replacement, expectedDoc string) {
	//repl, _, _ := p.Commit(edits, 0, 0)
	//testEqualRepls(t, repl, expected, "replacements did not match after commit")

	editDoc := p.doc().Freeze()
	repl, _, _ := p.Commit(edits, 0, 0)
	editDoc.Apply("edits", 0, edits)
	editDoc.Apply("result", 0, repl)
	fmt.Printf("EDITS\n  INPUT: %#v\n  OUTPUT: %#v\n", edits, repl)
	testEqual(t, editDoc.String(), expectedDoc, "replacements did not match after commit")

	//p.Commit(edits, 0, 0)
	//latest := p.History.Latest[p.sessionId]
	//prev := latest.SessionParent(p.History)
	//if prev == nil {
	//	prev = p.History.Source
	//}
	//anc := latest.getDocumentForAncestor(p.History, prev, false)
	//testEqualRepls(t, anc.Edits(), expected, "ancestor edits did not match")
	//testEqual(t, anc.String(), expectedDoc, "committed doc did not match")
}

func addBlock(t myT, s *testSession, blk *OpBlock, expected string) {
	l := s.History.Storage.GetBlockCount()
	failIfErrNow(t, s.History.addIncomingBlock(blk))
	testBlockOrder(t, s, l+1, 1)
	prev := blk.SessionParent(s.History)
	if prev == nil {
		prev = s.History.Source
	}
	ancestor := s.History.lca(blk.Parents)
	ancestorDoc := ancestor.getDocumentForAncestor(s.History, ancestor, false)
	anc := s.History.Latest[blk.SessionId].getDocumentForAncestor(s.History, prev, false)
	testEqual(t, anc.String(), expected, "ancestor doc did not match")
	testEqual(t, anc.OriginalString(), ancestorDoc.String(), "ancestor doc did not match")
}

func (s *testSession) checkError(err error, msg string) {
	s.testEqual(err, nil, fmt.Sprintf("%s: %s", msg, fmt.Sprint(err)))
}

func (s *testSession) Commit(repls []Replacement, selOff, selLen int) ([]Replacement, int, int) {
	repl, selOff, selLen, err := s.History.Commit(s.peer, s.sessionId, repls, selOff, selLen)
	s.checkError(err, "Error committing replacements")
	s.verifyEdits()
	return repl, selOff, selLen
}

func (s *testSession) verifyEdits() {
	//}
	//func (s *testSession) real_verifyEdits() {
	blk := s.History.Latest[s.sessionId]
	if blk == nil || len(blk.Parents) == 0 {
		return
	}
	anc := s.History.lca(blk.Parents)
	firstParent := s.History.GetBlock(blk.Parents[0])
	result := firstParent.getDocumentForAncestor(s.History, anc, false)
	for _, parentHash := range blk.Parents[1:] {
		parent := s.History.GetBlock(parentHash)
		result.Merge(parent.getDocumentForAncestor(s.History, anc, false))
	}
	prev := result.Copy()
	result.Apply("", 0, blk.Replacements)
	expected := blk.GetRawDocument(s.History).String()
	eq := result.String() == expected
	if !eq {
		println()
		fmt.Println("REPL ID: ", blk.replId())
		fmt.Println("PARENT ID: ", blk.SessionParent(s.History).replId())
		fmt.Println("OLD REPLS: ", blk.Replacements)
		fmt.Println("NEW REPLS: ", blk.Replacements)
		fmt.Println("EXPECTED TREE:\n", doc.Changes("  ", blk.GetRawDocument(s.History).GetOps()))
		fmt.Println("ACTUAL TREE BEFORE REPL:\n", doc.Changes("  ", prev.GetOps()))
		fmt.Println("ACTUAL TREE:\n", doc.Changes("  ", result.GetOps()))
		fmt.Println("EXPECTED:\n", expected, "\nGOT:\n", result)
		os.Exit(1)
	} else if !sameRepls(blk.Replacements, blk.Replacements) {
		fmt.Println("OLD REPLS: ", blk.Replacements)
		fmt.Println("NEW REPLS: ", blk.Replacements)
	}
	s.testEqual(eq, true, "BAD NEW REPLACEMENTS")
}

func sameRepls(a, b []doc.Replacement) bool {
	if len(a) != len(b) {
		return false
	}
	for i, ra := range a {
		rb := b[i]
		if ra.Offset != rb.Offset || ra.Length != rb.Length || ra.Text != rb.Text {
			return false
		}
	}
	return true
}

func testCommit(t myT, s *testSession, startDoc, expected string) {
	repl, _, _ := s.Commit(nil, 0, 0)
	str := doc.NewDocument(startDoc)
	str.Apply(s.sessionId, 0, repl)
	testEqual(t, str.String(), expected, "Result did not match expected")
}

// strip out local data
func outgoing(s *testSession) *OpBlock {
	blk := s.History.Latest[s.sessionId]
	return newOpBlock(
		blk.Peer,
		blk.SessionId,
		blk.Nonce,
		blk.Parents,
		blk.Replacements,
		blk.SelectionOffset,
		blk.SelectionLength,
	)
}

func testBlockOrder(t myT, s *testSession, expected, additional int) {
	s.History.GetBlockOrder()
	lenBlocks := s.History.Storage.GetBlockCount()
	testEqual(t, lenBlocks, expected,
		fmt.Sprintf("session expected len(blocks) = %d but got %d\n", expected, lenBlocks))
	lenOrder := len(s.History.GetBlockOrder())
	testEqual(t, lenOrder, expected,
		fmt.Sprintf("session expected len(blockOrder) = %d but got %d\n", expected, len(s.History.BlockOrder)))
	for _, hash := range s.History.BlockOrder[lenOrder-additional:] {
		t.failIfNot(s.History.GetBlock(hash) != nil, fmt.Sprintf("Could not find block for hash in block order"))
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

func newTestPeer(t myT, peer, sessionId, doc string) *testSession {
	ch := NewHistory(NewMemoryStorage(doc), doc)
	p := &testSession{
		myT:       t,
		History:   ch,
		peer:      peer,
		sessionId: sessionId,
		nonce:     0,
	}
	testBlockOrder(t, p, 1, 1)
	clearHistoryCache(ch)
	ch.Latest[sessionId] = ch.Source
	testEqual(t, p.doc().String(), doc, fmt.Sprintf("Bad document, expected <%s> but got <%s>\n", doc, p.doc()))
	return p
}

func TestBasic(tt *testing.T) {
	t := myT{tt}
	tp := t.newTwoSessions("session1", "session2", "one\ntwo\n")
	commitReplacements(t, tp.s1,
		[]Replacement{{Offset: 4, Length: 4, Text: "THREE\n"}},
		[]Replacement{},
		"one\nTHREE\n")
	t.testEqual(tp.history.Latest["session1"].Hash, tp.history.BlockOrder[len(tp.history.BlockOrder)-1],
		"Bad latest block for session1")
	t.testEqual(tp.s1.doc().String(), "one\nTHREE\n",
		"Bad document for session1",
	)
	t.testEqual(tp.s2.doc().String(), "one\ntwo\n",
		"Bad document for session1",
	)
	commitReplacements(t, tp.s2,
		[]Replacement{{Offset: 4, Length: 4, Text: "FOUR\n"}},
		[]Replacement{},
		"one\nTHREE\nFOUR\n")
}

func TestEditing(tt *testing.T) {
	// uncomment this to make diagnostic function available during debugging
	Init()
	t := myT{tt}
	p1 := newTestPeer(t, "", "session1", docString0)
	p2 := newTestPeer(t, "", "session2", docString0)
	d1 := doc.NewDocument(docString0)
	d2 := docTWO(t, "session2")
	doc2 := d2.String()
	commitEdits(t, p2, d2, []Replacement{
		{
			Offset: 14,
			Length: 3,
			Text:   "TWO"},
		{
			Offset: 28,
			Length: 0,
			Text: `
line five`},
	},
		docString2)
	blk2 := outgoing(p2)
	addBlock(t, p1, blk2, doc2)
	commitReplacements(t, p1, []Replacement{}, []Replacement{
		{
			Offset: 14,
			Length: 3,
			Text:   "TWO"},
		{
			Offset: 28,
			Length: 0,
			Text: `
line five`},
	},
		docString2)
	p1 = newTestPeer(t, "", "session1", docString0)
	p2 = newTestPeer(t, "", "session2", docString0)
	testEqual(t, p1.History.Source.Hash, p2.History.Source.Hash, "source hashes are not identical")
	d1, d2 = docs(t)
	doc1 := d1.String()
	doc2 = d2.String()
	clearHistoryCache(p1.History, p2.History)
	commitEdits(t, p1, d1, []Replacement{
		{
			Offset: 5,
			Length: 3,
			Text:   "ONE"},
		{
			Offset: 28,
			Length: 0,
			Text: `
line four`},
	},
		docString1)
	clearHistoryCache(p1.History, p2.History)
	testBlockOrder(t, p1, 2, 1)
	clearHistoryCache(p1.History, p2.History)
	commitEdits(t, p2, d2, []Replacement{
		{
			Offset: 14,
			Length: 3,
			Text:   "TWO"},
		{
			Offset: 28,
			Length: 0,
			Text: `
line five`},
	},
		docString2)
	clearHistoryCache(p1.History, p2.History)
	testBlockOrder(t, p2, 2, 1)
	clearHistoryCache(p1.History, p2.History)
	blk1 := outgoing(p1)
	clearHistoryCache(p1.History, p2.History)
	blk2 = outgoing(p2)
	clearHistoryCache(p1.History, p2.History)
	addBlock(t, p1, blk2, doc2)
	clearHistoryCache(p1.History, p2.History)
	testCommit(t, p1, doc1, docMerged)
	clearHistoryCache(p1.History, p2.History)
	addBlock(t, p2, blk1, doc1)
	clearHistoryCache(p1.History, p2.History)
	testCommit(t, p2, doc2, docMerged)
}

func (t myT) newTwoSessions(session1, session2, doc string) *twoSessions {
	h := NewHistory(NewMemoryStorage(doc), doc)
	blockNames := map[Sha]string{}
	tp := &twoSessions{
		myT:        t,
		history:    h,
		blockNames: blockNames,
	}
	tp.s1 = &testSession{
		myT:         t,
		twoSessions: tp,
		History:     h,
		sessionId:   session1,
	}
	tp.s2 = &testSession{
		myT:         t,
		twoSessions: tp,
		History:     h,
		sessionId:   session2,
	}
	tp.addBlock(h.Source)
	h.Latest[session1] = h.Source
	h.Latest[session2] = h.Source
	t.testEqual(tp.s1.doc().String(), doc, "bad document for session "+session1)
	t.testEqual(tp.s2.doc().String(), doc, "bad document for session "+session2)
	return tp
}

func (s *testSession) doc() *doc.Document {
	return s.Latest[s.sessionId].GetDocument(s.History)
}

func (tp *twoSessions) addBlock(blk *OpBlock) {
	if tp.blockNames[blk.Hash] != "" {
		return
	}
	n := len(tp.blockNames)
	sb := strings.Builder{}
	for {
		i := n % 52
		c := 'a' + i
		if i >= 26 {
			c = 'A' + (i - 26)
		}
		fmt.Fprintf(&sb, "%c", c)
		n = n / 52
		if n == 0 {
			break
		}
	}
	tp.blockNames[blk.Hash] = sb.String()
}

func (tp *twoSessions) block(blk *OpBlock) string {
	return tp.blockNames[blk.Hash]
}

func (tp *twoSessions) blocks(hashes ...Sha) string {
	sb := strings.Builder{}
	for _, hash := range hashes {
		fmt.Fprintf(&sb, " %s", tp.block(tp.history.GetBlock(hash)))
	}
	return sb.String()
}

func (tp *twoSessions) printBlock(blk *OpBlock) {
	sb := &strings.Builder{}
	fmt.Fprintf(sb, "Block: %s[%s]:", tp.block(blk), blk.SessionId)
	for _, p := range blk.Parents {
		parent := tp.history.GetBlock(p)
		fmt.Fprintf(sb, " %s[%s]", tp.blockNames[p], parent.SessionId)
	}
	fmt.Println(sb.String())
}

func (tp *twoSessions) printBlockOrder() {
	sb := strings.Builder{}
	for _, hash := range tp.history.BlockOrder {
		block := tp.history.GetBlock(hash)
		fmt.Fprintf(&sb, " %s[%s]", tp.blockNames[hash], block.SessionId)
	}
	fmt.Printf("BLOCK-ORDER:%s\n", sb.String())
}

func (s *testSession) latest() *OpBlock {
	l := s.History.Latest[s.sessionId]
	if l == nil {
		return s.History.Source
	}
	return l
}

func (s *testSession) latestRepls(prefix ...any) string {
	newPrefix := append(append(make([]any, 0, len(prefix)+1), prefix...), s.latest().Replacements)
	return s.repls(newPrefix...)
}

func (s *testSession) repls(prefixes ...any) string {
	repls := prefixes[len(prefixes)-1].([]Replacement)
	prefixes = prefixes[:len(prefixes)-1]
	sb := strings.Builder{}
	for _, repl := range repls {
		fmt.Fprint(&sb, prefixes...)
		fmt.Fprintf(&sb, "{%d %d %s}\n", repl.Offset, repl.Length, repl.Text)
	}
	return sb.String()
}

func sameRepl(r1, r2 *Replacement) bool {
	return r1.Offset == r2.Offset && r1.Length == r2.Length && r1.Text == r2.Text
}

func (t myT) failIfNotSameRepl(r1, r2 *Replacement) {
	t.failIfNot(sameRepl(r1, r2), "bad replacement")
}

type edit struct {
	selOffset int
	selLength int
	repls     []Replacement
}

func replacement(selOffset, selLength int, repls ...any) *edit {
	r := make([]Replacement, 0, len(repls)/3)
	for pos := 0; pos+2 < len(repls); pos += 3 {
		r = append(r, doc.Replacement{
			Offset: doc.As[int](repls[pos]),
			Length: doc.As[int](repls[pos+1]),
			Text:   doc.As[string](repls[pos+2]),
		})
	}
	return &edit{selOffset, selLength, r}
}

func (s *testSession) commit(anEdit, expected *edit) int {
	verbose(1, "latest-block: [%s] %s\n", s.sessionId, s.block(s.latest()))
	// delta is the change in size based on both anEdit and the edits from the commit
	delta := 0
	for _, repl := range anEdit.repls {
		delta += len(repl.Text) - repl.Length
	}
	prevBlock := s.latest()
	result, _, _ := s.Commit(anEdit.repls, anEdit.selOffset, anEdit.selLength)
	s.nonce++
	verbose(1, "NEW-REPLACEMENTS:\n%s", s.repls(" ", s.latest().Replacements))
	s.twoSessions.addBlock(s.latest())
	verbose(1, "new-block: [%s] %s\nnew-parents: %s\n", s.sessionId, s.block(s.latest()), s.blocks(s.latest().Parents...))
	if verbosity > 0 {
		s.printBlock(s.latest())
		s.printBlockOrder()
	}
	s.failIfNot(len(result) == len(expected.repls), "bad-replacement: [%s] expected %d replacements but got %d:\n%sbad-repls-latest:\n%s", s.sessionId, len(expected.repls), len(result), s.repls(" ", result), s.latestRepls(" "))
	bad := -1
	for i, repl := range result {
		delta += len(repl.Text) - repl.Length
		if !sameRepl(&repl, &expected.repls[i]) {
			bad = i
		}
	}
	if bad > -1 {
		prevDoc := prevBlock.GetDocument(s.history).String()
		expectedDoc := s.latest().GetDocument(s.history).String()
		newDoc := doc.NewDocument(prevDoc)
		newDoc.Apply(s.sessionId, 0, result)
		// sometimes returned replacements are not exactly as expected but the result is still correct
		if newDoc.String() != expectedDoc {
			fmt.Printf("Expected\n %v\n but got\n %v\n", result[bad], expected.repls[bad])
			diag := doc.NewDocument(expectedDoc)
			dmp := diff.New()
			pos := 0
			off := 0
			for _, dif := range dmp.DiffMain(expectedDoc, newDoc.String(), true) {
				switch dif.Type {
				case diff.DiffDelete:
					diag.Replace(s.sessionId, off, pos, len(dif.Text), "")
				case diff.DiffEqual:
					pos += len(dif.Text)
				case diff.DiffInsert:
					diag.Replace(s.sessionId, off, pos, 0, dif.Text)
					off += len(dif.Text)
				}
			}
			fmt.Println("Bad replacement:\n", diag.Changes("  "))
			s.failNow("bad replacement from commit")
		}
	}
	return delta
}

func (s *testSession) change(newEdit, expected *edit) *document {
	l := s.latest()
	doc := l.GetDocument(s.history)
	verbose(1, "EDIT: %+v\n", newEdit)
	delta := s.commit(newEdit, expected)
	verbose(1, "Delta: %d\n", delta)
	newDoc := s.latest().GetDocument(s.history)
	verbose(1, "DOC:\n%s\n", doc.Changes("  "))
	verbose(1, "NEW-DOC:\n%s\n", newDoc.Changes("  "))
	verbose(1, "OLD == NEW: %v\n", l == s.latest())
	s.failIfNot(doc.String() != newDoc.String(), "document is unchanged")
	s.failIfNot(len(doc.String())+delta == len(newDoc.String()), "new document for %s size is wrong, expected delta %d\n (%d) '%s' but got\n (%d) '%s'", s.sessionId, delta, len(doc.String())+delta, doc.String(), len(newDoc.String()), newDoc.String())
	return newDoc
}

func (tp *twoSessions) change(offset, length int, text string) {
	verbose(1, "replace: %d %d %s\n", offset, length, text)
	verbose(1, "=========EMACS==========\n")
	newDoc1 := tp.s1.change(replacement(0, 0, offset, length, text), replacement(-1, -1))
	verbose(1, "DOC:\n  '%s'\n", strings.Join(strings.Split(newDoc1.String(), "\n"), "'\n  '"))
	verbose(1, "--------VS CODE---------\n")
	newDoc2 := tp.s2.change(replacement(0, 0), replacement(-1, -1, offset, length, text))
	tp.failIfNot(newDoc1.String() == newDoc2.String(), "documents are not equal")
}

func TestPeerEdits(tt *testing.T) {
	lines := 2
	word := "hello "
	words := 2
	// these each produce the same error
	inserts := []int{17, 15, 23}
	//inserts := []int{1, 1, 1}
	docBuf := strings.Builder{}
	for line := 0; line < lines; line++ {
		for wordNum := 0; wordNum < words; wordNum++ {
			docBuf.WriteString(word)
		}
		docBuf.WriteString("\n")
	}
	tp := myT{tt}.newTwoSessions("emacs", "vscode", docBuf.String())
	for i, offset := range inserts {
		verbose(1, "Replacement: %d\n", i+1)
		tp.change(offset, 0, "a")
	}
}

func TestRandomEdits(tt *testing.T) {
	lines := 2000
	//lines := 3
	word := "hello "
	words := 20
	//words := 5
	docBuf := strings.Builder{}
	for line := 0; line < lines; line++ {
		for wordNum := 0; wordNum < words; wordNum++ {
			docBuf.WriteString(word)
		}
		docBuf.WriteString("\n")
	}
	docStr := docBuf.String()
	docLen := len(docStr)
	tp := myT{tt}.newTwoSessions("emacs", "vscode", docStr)
	for edit := 0; edit < 1000; edit++ {
		if edit%100 == 0 {
			fmt.Printf("Testing edit %d\n", edit)
		}
		i := rand.Intn(docLen)
		verbose(1, "Replacement: %d\n", i+1)
		if i+3 < docLen && rand.Intn(100) < 50 {
			tp.change(i, 3, "a")
			docLen -= 2
		} else {
			tp.change(i, 0, "a")
			docLen++
		}
	}
}

func (server *twoSessions) checkSimpleChange(edit, expected *edit) {
	doc := server.history.GetLatestDocument().String()
	server.s1.change(edit, expected)
	for _, repl := range edit.repls {
		doc = doc[:repl.Offset] + repl.Text + doc[repl.Offset+repl.Length:]
	}
	testEqual(server.myT, server.history.GetLatestDocument().String(), doc, "latest document is wrong")
}

func TestLatestDocument(tt *testing.T) {
	verbosity = 2
	t := myT{tt}
	server := t.newTwoSessions("emacs", "vscode", docString0)
	l := len(server.history.GetBlockOrder())
	server.s1.commit(replacement(0, 0), replacement(-1, -1))
	server.s1.commit(replacement(0, 0), replacement(-1, -1))
	server.s1.commit(replacement(0, 0), replacement(-1, -1))
	testEqual(t, len(server.history.GetBlockOrder()), l, "inserted redundant blocks")
	server.checkSimpleChange(replacement(0, 0, 5, 3, "ONE"), replacement(-1, -1))
	server.s2.change(replacement(0, 0), replacement(0, 0, 5, 3, "ONE"))
	server.checkSimpleChange(replacement(0, 0, 28, 0, "\nline four"), replacement(-1, -1))
	server.checkSimpleChange(replacement(0, 0, 5, 3, "TWO"), replacement(-1, -1))
	server.s2.change(replacement(0, 0), replacement(0, 0, 28, 0, "\nline four", 5, 3, "TWO"))
	server.checkSimpleChange(replacement(0, 0, 38, 0, "\nline five"), replacement(-1, -1))
	server.s2.change(replacement(0, 0), replacement(0, 0, 38, 0, "\nline five"))
	server.checkSimpleChange(replacement(0, 0, 48, 0, "\nline sixA"), replacement(-1, -1))
	server.s2.change(replacement(0, 0), replacement(0, 0, 48, 0, "\nline sixA"))
}

func newOffsets(doc []string, off []int) {
	tot := 0
	for i, v := range doc {
		off[i] = tot
		tot += len(v)
	}
}

func TestSet(tt *testing.T) {
	doc0 := []string{`
#+NAME: one
#+begin_src yaml
`, // 0
		`a: 1`, // 1
		`
#+end_src
#+NAME: two
#+begin_src yaml
`, // 2
		`b: 2`, // 3
		`
#+end_src
#+NAME: three
#+begin_src yaml
`, // 4
		`c: 3`, //5
		`
#+end_src
`} // 6
	offsets := make([]int, len(doc0))
	doc1 := make([]string, len(doc0))
	//verbosity = 2
	t := myT{tt}
	s := newTestPeer(t, "", "session1", strings.Join(doc0, ""))
	testReplace := func(pos int, newText string) {
		newOffsets(doc0, offsets)
		copy(doc1, doc0)
		s.Commit([]Replacement{doc.Replacement{
			Offset: offsets[pos],
			Length: len(doc0[pos]),
			Text:   newText,
		}}, -1, -1)
		doc0[pos] = newText
		latest := s.GetLatestDocument().String()
		t.testEqual(latest, strings.Join(doc0, ""), "Replacement mismatch")
	}
	testReplace(5, `6`)
	testReplace(3, `5`)
	testReplace(1, `4`)
}

func TestComplexEditing(tt *testing.T) {
	t := myT{tt}
	p1 := newTestPeer(t, "", "session1", docString0)
	p2 := newTestPeer(t, "", "session2", docString0)
	p3 := newTestPeer(t, "", "session3", docString0)
	p2.Commit([]Replacement{Replacement{0, 0, "line ZERO\n"}}, -1, -1)
	p3.Commit([]Replacement{Replacement{23, 5, "THREE"}}, -1, -1)
	blk2 := outgoing(p2)
	doc2 := p2.GetLatestDocument().String()
	t.testEqual(doc2, `line ZERO
line one
line two
line three`, "Doc 2 is bad")
	blk3 := outgoing(p3)
	doc3 := p3.GetLatestDocument().String()
	t.testEqual(doc3, `line one
line two
line THREE`, "Doc 3 is bad")
	addBlock(t, p1, blk2, doc2)
	addBlock(t, p1, blk3, doc3)
	p1.Commit([]Replacement{Replacement{5, 3, "ONE"}}, -1, -1)
	doc1 := p1.GetLatestDocument().String()
	t.testEqual(doc1, `line ZERO
line ONE
line two
line THREE`, "Doc 1 is bad")
}
