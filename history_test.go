package history

import (
	"fmt"
	"math/rand"
	"os"
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

type historyInspector struct {
	*History
	latest []historyBlock
}

type historyBlock struct {
	peer    string
	order   int
	parents []historyBlock
	OpBlock
}

func verbose(n int, format string, args ...any) {
	if n <= verbosity {
		fmt.Printf(format, args...)
	}
}

func Inspect(h *History, blk *OpBlock) historyBlock {
	if h == nil {
		return historyBlock{peer: ""}
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
		peer:    blk.Peer,
		order:   blk.order,
		parents: make([]historyBlock, len(blk.Parents)),
		OpBlock: *blk,
	}
	for i, parent := range blk.Parents {
		hb.parents[i] = *addInspect(h, h.GetBlock(parent), blocks)
	}
	blocks[blk.Hash] = &hb
	return &hb
}

///

func replace(t myT, tree *document, peer string, start int, length int, text string) {
	str := tree.String()
	tree.Replace(peer, start, length, text)
	expect := fmt.Sprintf("%s%s%s", str[0:start], text, str[start+length:])
	if tree.String() != expect {
		fmt.Printf("Bad tree '%s', expected '%s'\n", tree.String(), expect)
		t.FailNow()
	}
}

func testEqual(t myT, actual any, expected any, msg string) {
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

func docONE(t myT, peer string) *document {
	d := doc.NewDocument(docString1)
	replace(t, d, peer, index(docString1, 0, 5), 3, "ONE")
	replace(t, d, peer, index(docString1, 2, 10), 0, "\nline four")
	return d
}

func docTWO(t myT, peer string) *document {
	d := doc.NewDocument(docString1)
	replace(t, d, peer, index(docString1, 1, 5), 3, "TWO")
	replace(t, d, peer, index(docString1, 2, 10), 0, "\nline five")
	return d
}

func docs(t myT) (*document, *document) {
	return docONE(t, "peer1"), docTWO(t, "peer2")
}

func TestMerge(tt *testing.T) {
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
	for _, r := range b.ReverseEdits() {
		replace(t, revDoc, "peer1", r.Offset, r.Length, r.Text)
	}
	testEqual(t, revDoc.String(), docString1, "unsuccessful reversal")
}

func commitEdits(t myT, s *Session, doc *document, expected []Replacement) {
	commitReplacements(t, s, doc.Edits(), expected)
}

func commitReplacements(t myT, s *Session, edits []Replacement, expected []Replacement) {
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

func addBlock(t myT, s *Session, blk *OpBlock, expected string) {
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

func testCommit(t myT, s *Session, startDoc, expected string) {
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

func testBlockOrder(t myT, s *Session, expected, additional int) {
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

func testSession(t myT, peer string, doc string) *Session {
	ch := NewHistory(NewMemoryStorage(doc), docString1)
	s := NewSession(peer, ch)
	testBlockOrder(t, s, 1, 1)
	clearHistoryCache(ch)
	return s
}

func TestEditing(tt *testing.T) {
	t := myT{tt}
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

type myT struct {
	*testing.T
}

type twoPeers struct {
	myT
	history    *History
	p1         *testPeer
	p2         *testPeer
	blockNames map[Sha]string
}

type testPeer struct {
	*twoPeers
	*Session
}

func (t myT) newTwoPeers(peer1, peer2, doc string) *twoPeers {
	h := NewHistory(NewMemoryStorage(doc), doc)
	blockNames := map[Sha]string{}
	tp := &twoPeers{
		myT:        t,
		history:    h,
		blockNames: blockNames,
	}
	tp.p1 = &testPeer{
		twoPeers: tp,
		Session:  NewSession(peer1, h),
	}
	tp.p2 = &testPeer{
		twoPeers: tp,
		Session:  NewSession(peer2, h),
	}
	tp.addBlock(h.Source)
	return tp
}

func (tp *twoPeers) addBlock(blk *OpBlock) {
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

func (tp *twoPeers) block(blk *OpBlock) string {
	return tp.blockNames[blk.Hash]
}

func (tp *twoPeers) blocks(hashes ...Sha) string {
	sb := strings.Builder{}
	for _, hash := range hashes {
		fmt.Fprintf(&sb, " %s", tp.block(tp.history.GetBlock(hash)))
	}
	return sb.String()
}

func (tp *twoPeers) printBlock(blk *OpBlock) {
	sb := &strings.Builder{}
	fmt.Fprintf(sb, "Block: %s[%s]:", tp.block(blk), blk.Peer)
	for _, p := range blk.Parents {
		parent := tp.history.GetBlock(p)
		fmt.Fprintf(sb, " %s[%s]", tp.blockNames[p], parent.Peer)
	}
	fmt.Println(sb.String())
}

func (tp *twoPeers) printBlockOrder() {
	sb := strings.Builder{}
	for _, hash := range tp.history.BlockOrder {
		block := tp.history.GetBlock(hash)
		fmt.Fprintf(&sb, " %s[%s]", tp.blockNames[hash], block.Peer)
	}
	fmt.Printf("BLOCK-ORDER:%s\n", sb.String())
}

func (p *testPeer) latest() *OpBlock {
	l := p.History.Latest[p.Peer]
	if l == nil {
		return p.History.Source
	}
	return l
}

func (p *testPeer) latestRepls(prefix ...any) string {
	newPrefix := append(append(make([]any, 0, len(prefix)+1), prefix...), p.latest().Replacements)
	return p.repls(newPrefix...)
}

func (p *testPeer) repls(prefixes ...any) string {
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
	for pos := 0; pos+2 < len(repls); pos++ {
		r = append(r, doc.Replacement{
			Offset: doc.As[int](repls[pos]),
			Length: doc.As[int](repls[pos+1]),
			Text:   doc.As[string](repls[pos+2]),
		})
	}
	return &edit{selOffset, selLength, r}
}

func (p *testPeer) commit(anEdit, expected *edit) int {
	verbose(1, "latest-block: [%s] %s\n", p.Peer, p.block(p.latest()))
	// delta is the change in size based on both anEdit and the edits from the commit
	delta := 0
	for _, repl := range anEdit.repls {
		delta += len(repl.Text) - repl.Length
		p.Replace(repl.Offset, repl.Length, repl.Text)
	}
	prevBlock := p.latest()
	result, _, _ := p.Commit(anEdit.selOffset, anEdit.selLength)
	verbose(1, "NEW-REPLACEMENTS:\n%s", p.repls(" ", p.latest().Replacements))
	p.addBlock(p.latest())
	verbose(1, "new-block: [%s] %s\nnew-parents: %s\n", p.Peer, p.block(p.latest()), p.blocks(p.latest().Parents...))
	if verbosity > 0 {
		p.printBlock(p.latest())
		p.printBlockOrder()
	}
	p.failIfNot(len(result) == len(expected.repls), "bad-replacement: [%s] expected %d replacements but got %d:\n%sbad-repls-latest:\n%s", p.Peer, len(expected.repls), len(result), p.repls(" ", result), p.latestRepls(" "))
	bad := -1
	for i, repl := range result {
		delta += len(repl.Text) - repl.Length
		if !sameRepl(&repl, &expected.repls[i]) {
			bad = i
		}
	}
	if bad > -1 {
		prevDoc := prevBlock.GetDocument(p.history).String()
		expectedDoc := p.latest().GetDocument(p.history).String()
		newDoc := doc.NewDocument(prevDoc)
		newDoc.Apply(p.Peer, result)
		// sometimes returned replacements are not exactly as expected but the result is still correct
		if newDoc.String() != expectedDoc {
			fmt.Printf("Expected\n %v\n but got\n %v\n", result[bad], expected.repls[bad])
			diag := doc.NewDocument(expectedDoc)
			dmp := diff.New()
			pos := 0
			for _, dif := range dmp.DiffMain(expectedDoc, newDoc.String(), true) {
				switch dif.Type {
				case diff.DiffDelete:
					diag.Replace(p.Peer, pos, len(dif.Text), "")
				case diff.DiffEqual:
					pos += len(dif.Text)
				case diff.DiffInsert:
					diag.Replace(p.Peer, pos, 0, dif.Text)
				}
			}
			fmt.Println("Bad replacement:\n", diag.Changes("  "))
			p.failNow("bad replacement from commit")
		}
	}
	return delta
}

func (p *testPeer) change(newEdit, expected *edit) *document {
	doc := p.latest().GetDocument(p.history)
	delta := p.commit(newEdit, expected)
	verbose(1, "Delta: %d\n", delta)
	newDoc := p.latest().GetDocument(p.history)
	p.failIfNot(doc.String() != newDoc.String(), "document is unchanged")
	p.failIfNot(len(doc.String())+delta == len(newDoc.String()), "new document for %s size is wrong, expected delta %d\n (%d) '%s' but got\n (%d) '%s'", p.Peer, delta, len(doc.String())+delta, doc.String(), len(newDoc.String()), newDoc.String())
	return newDoc
}

func (tp *twoPeers) change(offset, length int, text string) {
	verbose(1, "replace: %d %d %s\n", offset, length, text)
	verbose(1, "=========EMACS==========")
	newDoc1 := tp.p1.change(replacement(0, 0, offset, length, text), replacement(-1, -1))
	verbose(1, "DOC:\n  '%s'\n", strings.Join(strings.Split(newDoc1.String(), "\n"), "'\n  '"))
	verbose(1, "--------VS CODE---------")
	newDoc2 := tp.p2.change(replacement(0, 0), replacement(-1, -1, offset, length, text))
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
	tp := myT{tt}.newTwoPeers("emacs", "vscode", docBuf.String())
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
	tp := myT{tt}.newTwoPeers("emacs", "vscode", docStr)
	for edit := 0; edit < 1000; edit++ {
		fmt.Printf("Testing edit %d\n", edit)
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
