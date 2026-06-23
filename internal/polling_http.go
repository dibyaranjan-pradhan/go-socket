// Copyright (c) 2026 Dibyaranjan Pradhan. All rights reserved.

package internal

import (
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/dibyaranjan-pradhan/go-socket/internal/engineio"
)

// PollAttachFunc starts application pumps after a polling handshake succeeds.
type PollAttachFunc func(transport Transport, r *http.Request)

// PollHandler serves Engine.IO v4 HTTP long-polling. It is mounted separately
// from the WebSocket handler so existing clients stay untouched.
type PollHandler struct {
	manager *pollManager
	attach  PollAttachFunc
	closed  func() bool
}

// NewPollHandler builds a handler for Engine.IO polling transport.
func NewPollHandler(pingInterval, pingTimeout time.Duration, maxPayload int64, attach PollAttachFunc, closed func() bool) *PollHandler {
	return &PollHandler{
		manager: newPollManager(pingInterval, pingTimeout, maxPayload),
		attach:  attach,
		closed:  closed,
	}
}

// ServeHTTP implements Engine.IO polling: handshake GET, long-poll GET, POST send.
func (h *PollHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.closed != nil && h.closed() {
		http.Error(w, "server closed", http.StatusServiceUnavailable)
		return
	}
	if r.URL.Query().Get("EIO") != engineio.Version {
		http.Error(w, "unsupported EIO version", http.StatusBadRequest)
		return
	}
	if r.URL.Query().Get("transport") != engineio.TransportPolling {
		http.Error(w, "unsupported transport", http.StatusBadRequest)
		return
	}

	sid := r.URL.Query().Get("sid")
	if sid == "" {
		h.serveHandshake(w, r)
		return
	}
	pt, ok := h.manager.get(sid)
	if !ok {
		http.Error(w, "unknown sid", http.StatusBadRequest)
		return
	}
	switch r.Method {
	case http.MethodGet:
		h.servePoll(w, pt)
	case http.MethodPost:
		h.servePost(w, r, pt)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (h *PollHandler) serveHandshake(w http.ResponseWriter, r *http.Request) {
	sess, pt := h.manager.create()
	openWire, err := engineio.MarshalOpenPacket(engineio.NewHandshake(
		sess.ID, sess.PingInterval, sess.PingTimeout, sess.MaxPayload,
	))
	if err != nil {
		http.Error(w, "handshake failed", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=UTF-8")
	_, _ = w.Write(openWire)
	if h.attach != nil {
		go h.attach(pt, r)
	}
}

func (h *PollHandler) servePoll(w http.ResponseWriter, pt *pollingTransport) {
	wait := pt.sess.PingTimeout
	if wait <= 0 {
		wait = 20 * time.Second
	}
	body, err := pt.poll(wait)
	if err != nil {
		if err == io.EOF {
			http.Error(w, "session closed", http.StatusBadRequest)
			return
		}
		http.Error(w, "poll failed", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=UTF-8")
	if len(body) > 0 {
		_, _ = w.Write(body)
	}
}

func (h *PollHandler) servePost(w http.ResponseWriter, r *http.Request, pt *pollingTransport) {
	raw, err := io.ReadAll(io.LimitReader(r.Body, pt.sess.MaxPayload+1))
	if err != nil {
		http.Error(w, "read body", http.StatusBadRequest)
		return
	}
	if int64(len(raw)) > pt.sess.MaxPayload {
		http.Error(w, "payload too large", http.StatusRequestEntityTooLarge)
		return
	}
	packets, err := engineio.DecodeBatch(raw)
	if err != nil {
		http.Error(w, "bad packet batch", http.StatusBadRequest)
		return
	}
	if err := pt.handleInbound(packets); err != nil {
		if err == io.EOF {
			http.Error(w, "session closed", http.StatusBadRequest)
			return
		}
		http.Error(w, "handle packets", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=UTF-8")
	_, _ = w.Write([]byte("ok"))
}

// SessionCount exposes active polling sessions (for tests).
func (h *PollHandler) SessionCount() int {
	return h.manager.count()
}

// IsPollingRequest reports whether r targets Engine.IO polling on this handler.
func IsPollingRequest(r *http.Request) bool {
	return strings.EqualFold(r.URL.Query().Get("transport"), engineio.TransportPolling) &&
		r.URL.Query().Get("EIO") == engineio.Version
}
