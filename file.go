package peerot

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"time"

	diff "github.com/sergi/go-diff/diffmatchpatch"
)

var ErrPeer = errors.New("Peer error")
var ErrBadPath = fmt.Errorf("%w, bad file path", ErrPeer)
var ErrWatcher = fmt.Errorf("%w watching", ErrPeer)
var ErrWatcherCreate = fmt.Errorf("%w, could not create watcher", ErrPeer)
var ErrWatcherPath = fmt.Errorf("%w, could not watch path", ErrPeer)

type filePeer struct {
	path         string
	content      string
	session      *Session
	changed      func()
	service      chanSvc // only this service can change the filePeer
	lastModTime  time.Time
	shutdownHook []func()
}

// opaque type to separate service API from internal API so no scramble brains
type filePeerService filePeer

func NewFilePeer(s *Session, file string, shutdownHook func()) (*filePeerService, error) {
	file = path.Clean(file)
	if !path.IsAbs(file) {
		dir, err := os.Getwd()
		if err != nil {
			return nil, err
		}
		file = path.Join(dir, file)
	}
	hooks := []func(){}
	if shutdownHook != nil {
		hooks = []func(){shutdownHook}
	}
	service := make(chanSvc)
	runSvc(service)
	return (*filePeerService)(&filePeer{
		path:         file,
		changed:      func() {},
		service:      service,
		session:      s,
		shutdownHook: hooks,
	}), nil
}

///
/// filePeer service API
///

func (s *filePeerService) fileChanged(changed chan bool) {
	svc(s.service, func() {
		fmt.Println("Signalling change")
		changed <- (*filePeer)(s).fileChanged()
	})
}

func (s *filePeerService) shutdown(args ...any) {
	(*filePeer)(s).shutdown(args...)
}

func (s *filePeerService) GetUpdates() []Replacement {
	return svcSync(s.service, func() []Replacement {
		//repl, _, _ := (*filePeer)(s).session.Commit(0, 0)
		//return repl
		return []Replacement{}
	})
}

///
/// filePeer internal API (can only be called by service)
///

func (p *filePeer) shutdown(args ...any) {
	if len(args) > 0 {
		log.Fatal(fmt.Sprint(args...))
	}
	close(p.service)
	for _, f := range p.shutdownHook {
		f()
	}
}

func (p *filePeer) checkFile() *os.File {
	file, err := os.Open(p.path)
	done := func(args ...any) *os.File {
		if file != nil {
			file.Close()
		}
		p.shutdown(args...)
		return nil
	}
	if err != nil {
		return done("Error opening peer file", p.path)
	}
	stat, err := file.Stat()
	if err != nil {
		return done("Error statting file:", err)
	}
	modTime := stat.ModTime()
	if modTime == p.lastModTime {
		return done()
	}
	p.lastModTime = modTime
	return file
}

// write content to a temporary file and return the name
func (p *filePeer) writeFile(content string) (string, error) {
	file, err := os.CreateTemp(path.Dir(p.path), "peerot*")
	if err != nil {
		return "", err
	}
	_, err = io.WriteString(file, content)
	if err != nil {
		return "", err
	}
	err = file.Close()
	return file.Name(), err
}

// does not check whether the file changed in the background
func (p *filePeer) fileChanged() bool {
	file := p.checkFile()
	if file == nil {
		return false
	}
	contentBytes, err := io.ReadAll(file)
	if err != nil {
		p.shutdown("Error reading file:", err)
		return false
	}
	content := string(contentBytes)
	dmp := diff.New()
	pos := 0
	for _, dif := range dmp.DiffMain(p.content, content, true) {
		switch dif.Type {
		case diff.DiffDelete:
			p.session.Replace(pos, len(dif.Text), "")
		case diff.DiffEqual:
			pos += len(dif.Text)
		case diff.DiffInsert:
			p.session.Replace(pos, 0, dif.Text)
		}
	}
	//repl, _, _ := p.session.Commit(0, 0)
	//newContent := doc.Apply(p.session.peer, content, repl)
	//if content == newContent {
	//	return false
	//}
	//p.content = newContent
	return true
}

func (s *filePeerService) Monitor(changed chan bool, control func()) error {
	//// safe to mess with the peer because the service has not started yet
	//p := (*filePeer)(s)
	//file, err := os.ReadFile(p.path)
	//if err != nil {
	//	return fmt.Errorf("%w: %s", ErrBadPath, p.path)
	//}
	//p.session = NewChanges(p.peer, string(file), NewMemoryStorage())
	//watcher, err := fsnotify.NewWatcher()
	//if err != nil {
	//	return ErrWatcherCreate
	//}
	//defer watcher.Close()
	//dir := path.Dir(p.path)
	////filename := path.Base(p.path)
	//// Start listening for events.
	//go func() {
	//	for {
	//		select {
	//		case event, ok := <-watcher.Events:
	//			if !ok {
	//				return
	//			}
	//			log.Println("event:", event, "name:", event.Name)
	//			if event.Has(fsnotify.Write) && event.Name == p.path {
	//				fmt.Println(p.path, "changed")
	//				s.fileChanged(changed)
	//			}
	//		case err, ok := <-watcher.Errors:
	//			if !ok {
	//				log.Fatal(err)
	//				s.shutdown()
	//				return
	//			}
	//			log.Println("error:", err)
	//		}
	//	}
	//}()
	//err = watcher.Add(dir)
	//if err != nil {
	//	log.Fatal(err)
	//}
	control()
	return nil
}
