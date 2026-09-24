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

// EngineIOHandler serves Engine.IO v4 polling and polling→WebSocket upgrade.
type EngineIOHandler struct {
	manager *pollManager
	attach  PollAttachFunc
	closed  func() bool
	wsUp    TransportUpgrader
}

// NewEngineIOHandler builds a handler for Engine.IO polling and upgrade.
func NewEngineIOHandler(pingInterval, pingTimeout time.Duration, maxPayload int64, wsUp TransportUpgrader, attach PollAttachFunc, closed func() bool) *EngineIOHandler {
	return &EngineIOHandler{
		manager: newPollManager(pingInterval, pingTimeout, maxPayload),
		attach:  attach,
		closed:  closed,
		wsUp:    wsUp,
	}
}

// ServeHTTP routes Engine.IO transports: polling (default) and websocket upgrade.
func (h *EngineIOHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.closed != nil && h.closed() {
		http.Error(w, "server closed", http.StatusServiceUnavailable)
		return
	}
	if r.URL.Query().Get("EIO") != engineio.Version {
		http.Error(w, "unsupported EIO version", http.StatusBadRequest)
		return
	}

	transport := r.URL.Query().Get("transport")
	switch transport {
	case engineio.TransportPolling:
		h.servePolling(w, r)
	case engineio.TransportWebSocket:
		h.serveWebSocketUpgrade(w, r)
	default:
		http.Error(w, "unsupported transport", http.StatusBadRequest)
	}
}

func (h *EngineIOHandler) servePolling(w http.ResponseWriter, r *http.Request) {
	sid := r.URL.Query().Get("sid")
	if sid == "" {
		h.serveHandshake(w, r)
		return
	}
	pt, ok := h.manager.getPolling(sid)
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

func (h *EngineIOHandler) serveHandshake(w http.ResponseWriter, r *http.Request) {
	sess, st := h.manager.create()
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
		go h.attach(st, r)
	}
}

func (h *EngineIOHandler) serveWebSocketUpgrade(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	sid := r.URL.Query().Get("sid")
	if sid == "" {
		http.Error(w, "sid required", http.StatusBadRequest)
		return
	}
	if !h.manager.readyForUpgrade(sid) {
		http.Error(w, "upgrade not ready", http.StatusBadRequest)
		return
	}
	raw, err := h.wsUp.Upgrade(w, r)
	if err != nil {
		return
	}
	if _, ok := h.manager.applyUpgrade(sid, raw); !ok {
		_ = raw.Close()
	}
}

func (h *EngineIOHandler) servePoll(w http.ResponseWriter, pt *pollingTransport) {
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

func (h *EngineIOHandler) servePost(w http.ResponseWriter, r *http.Request, pt *pollingTransport) {
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
func (h *EngineIOHandler) SessionCount() int {
	return h.manager.count()
}

// IsPollingRequest reports whether r targets Engine.IO polling on this handler.
func IsPollingRequest(r *http.Request) bool {
	return strings.EqualFold(r.URL.Query().Get("transport"), engineio.TransportPolling) &&
		r.URL.Query().Get("EIO") == engineio.Version
}

// PollHandler is an alias kept for tests referencing the M2 name.
type PollHandler = EngineIOHandler

// NewPollHandler forwards to NewEngineIOHandler for backward-compatible tests.
func NewPollHandler(pingInterval, pingTimeout time.Duration, maxPayload int64, attach PollAttachFunc, closed func() bool) *PollHandler {
	return NewEngineIOHandler(pingInterval, pingTimeout, maxPayload, NewWSUpgrader(0, 0, nil), attach, closed)
}
