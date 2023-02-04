package peerot

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
)

type WebService struct {
	id       string
	serial   int
	sessions map[string]*History // changes by doc ID
}

const (
	VERSION = "/v1"
	LIST    = VERSION + "/list"
	CREATE  = VERSION + "/create"  // /create/NAME
	CONNECT = VERSION + "/connect" // /connect/NAME
	UPDATES = VERSION + "/updates"
)

func (sv *WebService) updates(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		// ...
	}
	defer c.Close(websocket.StatusInternalError, "the sky is falling")
	ctx, cancel := context.WithTimeout(r.Context(), time.Second*10)
	defer cancel()
	var v interface{}
	err = wsjson.Read(ctx, c, &v)
	if err != nil {
		// ...
	}
	log.Printf("received: %v", v)

	c.Close(websocket.StatusNormalClosure, "")
}

func isGood(w http.ResponseWriter, err any, msg any, status int) bool {
	if err != nil {
		w.Write([]byte(fmt.Sprint(msg)))
		w.WriteHeader(status)
		return false
	}
	return true
}

func readJson(w http.ResponseWriter, r *http.Request) (any, bool) {
	if bytes, err := io.ReadAll(r.Body); isGood(w, err, "bad request format", http.StatusNotAcceptable) {
		var msg any
		if isGood(w, json.Unmarshal(bytes, &msg), "bad request format", http.StatusNotAcceptable) {
			return &msg, true
		}
	}
	return nil, false
}

// URL: /create/NAME
// creates a new session with peer name = sv.id + sv.serial.
// starts by copying one of the other sessions (they should all have the same trees
func (sv *WebService) create(w http.ResponseWriter, r *http.Request) {

}

// /list
func (sv *WebService) list(w http.ResponseWriter, r *http.Request) {
	sessions := []string{}
	for name := range sv.sessions {
		sessions = append(sessions, name)
	}
	if data, err := json.Marshal(sessions); isGood(w, err, "internal error", http.StatusInternalServerError) {
		w.Write(data)
	}
}

// /connect/NAME
func (sv *WebService) connect(w http.ResponseWriter, r *http.Request) {
}

func jsonFunc(fn func(w http.ResponseWriter, msg any)) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if msg, ok := readJson(w, r); ok {
			fn(w, msg)
		}
	}
}

func start() {
	sv := &WebService{}
	http.HandleFunc(CREATE, sv.connect)
	http.HandleFunc(LIST, sv.connect)
	http.HandleFunc(CONNECT, sv.connect)
	http.HandleFunc(UPDATES, sv.updates)
	log.Fatal(http.ListenAndServe(":8080", nil))
}
