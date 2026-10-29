// Copyright (c) 2026 Dibyaranjan Pradhan. All rights reserved.

package gosocket

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/dibyaranjan-pradhan/go-socket/internal"
)

// Session recovery wire contract. Everything here is opt-in via
// Config.SessionRecovery; with it disabled none of these names appear on the wire.
const (
	// EventSession is the control frame sent first on every recovery-enabled
	// connection: {"sessionId", "resumed", "replayed", "lastSeq", "reason"?}.
	EventSession = "$session"
	// EventAck is the client->server control event that releases buffered frames:
	// {"event":"$ack","payload":{"seq":N}}.
	EventAck = "$ack"

	// QuerySession and QuerySeq carry the resume request on the handshake URL
	// (works for WebSocket and for the Engine.IO handshake).
	QuerySession = "gs_session"
	QuerySeq     = "gs_seq"
	// HeaderSession and HeaderSeq are the header equivalents; they take precedence
	// and keep the bearer token out of access logs for non-browser clients.
	HeaderSession = "X-GoSocket-Session"
	HeaderSeq     = "X-GoSocket-Seq"

	// DefaultReplayBufferMessages is the per-session cap on buffered frames.
	DefaultReplayBufferMessages = 256
	// DefaultReplayBufferBytes is the per-session cap on buffered bytes (256 KiB).
	DefaultReplayBufferBytes = 256 << 10
	// DefaultSessionTTL is how long a disconnected session stays resumable.
	DefaultSessionTTL = 2 * time.Minute
	// DefaultMaxParkedSessions caps disconnected sessions held in memory.
	DefaultMaxParkedSessions = 10000
	// DefaultIdempotencyWindow is the number of inbound keys remembered per session.
	DefaultIdempotencyWindow = 1024
)

// SessionRecoveryConfig enables connection-state recovery. Worst-case replay
// memory is (live sessions + MaxParkedSessions) x ReplayBufferBytes; frames
// broadcast to many sessions share one backing array, so real usage is far lower.
// State is per process: resume only works against the node that holds the session.
type SessionRecoveryConfig struct {
	// Enabled turns the feature on. Off by default; existing deployments see no change.
	Enabled bool
	// ReplayBufferMessages caps frames kept per session. Zero uses DefaultReplayBufferMessages.
	ReplayBufferMessages int
	// ReplayBufferBytes caps bytes kept per session. Zero uses DefaultReplayBufferBytes.
	ReplayBufferBytes int64
	// SessionTTL is how long a disconnected session stays resumable. Zero uses DefaultSessionTTL.
	SessionTTL time.Duration
	// MaxParkedSessions caps disconnected sessions; the oldest are evicted first.
	// Zero uses DefaultMaxParkedSessions.
	MaxParkedSessions int
	// IdempotencyWindow is how many inbound idempotency keys each session remembers.
	// Zero uses DefaultIdempotencyWindow.
	IdempotencyWindow int
	// OwnerFunc derives the principal (e.g. user ID from a verified token) for a
	// handshake. A session can only be resumed by a request yielding the same owner,
	// which stops a leaked token from being replayed by another user. Nil means the
	// token alone is the credential.
	OwnerFunc func(r *http.Request) string
}

// BackpressurePolicy chooses what happens to an outbound event when the peer's
// queue is full.
type BackpressurePolicy int

const (
	// BackpressureDefault defers to Config.DefaultBackpressure.
	BackpressureDefault BackpressurePolicy = iota
	// BackpressureDisconnect disconnects the slow peer (the historical behaviour).
	// With session recovery the event is buffered first, so the peer replays it.
	BackpressureDisconnect
	// BackpressureDrop discards this event and keeps the connection.
	BackpressureDrop
	// BackpressureVolatile is best-effort: dropped when the peer is not ready, never
	// sequenced, and never replayed after a reconnect.
	BackpressureVolatile
)

// EmitOptions tunes a single emit.
type EmitOptions struct {
	// Policy overrides the server default for this emit.
	Policy BackpressurePolicy
	// IdempotencyKey is echoed as "idempotencyKey" so clients can dedupe
	// at-least-once delivery (replay overlap, retried server jobs).
	IdempotencyKey string
}

// SessionInfo describes a recovery session in lifecycle callbacks.
type SessionInfo struct {
	// SessionID is the resume token's identity; treat as secret.
	SessionID string
	// ClientID is the last connection that held the session.
	ClientID string
	// Owner is the principal bound by SessionRecoveryConfig.OwnerFunc.
	Owner string
}

func (p BackpressurePolicy) internal() internal.SendPolicy {
	switch p {
	case BackpressureDrop:
		return internal.PolicyDrop
	case BackpressureVolatile:
		return internal.PolicyVolatile
	}
	return internal.PolicyDisconnect
}

func (s *Server) resolvePolicy(p BackpressurePolicy) internal.SendPolicy {
	if p == BackpressureDefault {
		p = s.cfg.DefaultBackpressure
	}
	return p.internal()
}

func (s *Server) resumeRequest(r *http.Request) internal.ResumeRequest {
	req := internal.ResumeRequest{}
	if s.cfg.SessionRecovery.OwnerFunc != nil && r != nil {
		req.Owner = s.cfg.SessionRecovery.OwnerFunc(r)
	}
	if r == nil {
		return req
	}
	sid := r.Header.Get(HeaderSession)
	if sid == "" {
		sid = r.URL.Query().Get(QuerySession)
	}
	if sid == "" {
		return req
	}
	req.Requested = true
	req.SessionID = sid

	seq := r.Header.Get(HeaderSeq)
	if seq == "" {
		seq = r.URL.Query().Get(QuerySeq)
	}
	if seq == "" {
		return req
	}
	n, err := strconv.ParseUint(seq, 10, 64)
	if err != nil {
		req.Invalid = true
		return req
	}
	req.LastSeq = n
	return req
}

func (s *Server) newSessionStore() *internal.SessionStore {
	rc := s.cfg.SessionRecovery
	return internal.NewSessionStore(internal.SessionOptions{
		ReplayMessages: rc.ReplayBufferMessages,
		ReplayBytes:    rc.ReplayBufferBytes,
		TTL:            rc.SessionTTL,
		MaxParked:      rc.MaxParkedSessions,
		KeyWindow:      rc.IdempotencyWindow,
		Release:        s.releaseSession,
	})
}

// releaseSession drops a discarded session's hub membership and, for sessions that
// really went away (not replaced by a fresh one from the same peer), notifies the app.
func (s *Server) releaseSession(sess *internal.Session, why internal.ReleaseReason) {
	m := sess.Member()
	if m != nil {
		s.hub.Unregister(m)
	}
	if why == internal.ReleaseReplaced {
		return
	}
	info := SessionInfo{SessionID: sess.ID(), Owner: sess.Owner()}
	if m != nil {
		info.ClientID = m.ID()
	}
	s.mu.RLock()
	cbs := make([]func(SessionInfo), len(s.onSessionExpire))
	copy(cbs, s.onSessionExpire)
	s.mu.RUnlock()
	for _, fn := range cbs {
		func() {
			defer func() {
				if rec := recover(); rec != nil {
					s.diag.Printf("gosocket: session expire handler panic: %v", rec)
				}
			}()
			fn(info)
		}()
	}
}

// OnSessionExpire registers a callback for sessions that were parked and never
// resumed (TTL elapsed or evicted). Use it for "truly offline" presence: with
// recovery enabled, OnDisconnect fires on every transport drop, including brief ones.
func (s *Server) OnSessionExpire(fn func(SessionInfo)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onSessionExpire = append(s.onSessionExpire, fn)
}

// openSession binds a freshly authenticated client to a new or resumed session.
func (s *Server) openSession(ic *internal.Client, r *http.Request) {
	res := s.sessions.Open(ic, s.resumeRequest(r))
	ic.SetResumed(res.Resumed)
	if res.Prev != nil {
		ic.CopyMetaFrom(res.Prev)
		s.hub.Transfer(res.Prev, ic)
	}
	if res.Displaced != nil {
		res.Displaced.ShutdownResumable()
	}
}

// handleAck processes {"event":"$ack","payload":{"seq":N}}.
func (s *Server) handleAck(sess *internal.Session, payload json.RawMessage) {
	var p struct {
		Seq uint64 `json:"seq"`
	}
	if json.Unmarshal(payload, &p) == nil {
		sess.Ack(p.Seq)
	}
}
