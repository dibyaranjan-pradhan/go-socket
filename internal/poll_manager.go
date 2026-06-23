// Copyright (c) 2026 Dibyaranjan Pradhan. All rights reserved.

package internal

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"

	"github.com/dibyaranjan-pradhan/go-socket/internal/engineio"
)

// pollManager tracks live Engine.IO polling sessions keyed by sid.
type pollManager struct {
	mu       sync.RWMutex
	sessions map[string]*pollSession
	cfg      engineio.Config
}

type pollSession struct {
	engineio.Session
	transport *pollingTransport
}

func newPollManager(pingInterval, pingTimeout time.Duration, maxPayload int64) *pollManager {
	return &pollManager{
		sessions: make(map[string]*pollSession),
		cfg: engineio.Config{
			PingInterval: pingInterval,
			PingTimeout:  pingTimeout,
			MaxPayload:   maxPayload,
		},
	}
}

func (m *pollManager) create() (*engineio.Session, *pollingTransport) {
	sid := randomPollSID()
	sess := engineio.Session{
		ID:           sid,
		PingInterval: m.cfg.PingInterval,
		PingTimeout:  m.cfg.PingTimeout,
		MaxPayload:   m.cfg.MaxPayload,
		CreatedAt:    time.Now(),
	}
	pt := newPollingTransport(&sess)
	ps := &pollSession{Session: sess, transport: pt}
	pt.onClose = func() { m.remove(sid) }

	m.mu.Lock()
	m.sessions[sid] = ps
	m.mu.Unlock()
	return &sess, pt
}

func (m *pollManager) get(sid string) (*pollingTransport, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ps, ok := m.sessions[sid]
	if !ok || ps.transport == nil {
		return nil, false
	}
	return ps.transport, true
}

func (m *pollManager) remove(sid string) {
	m.mu.Lock()
	delete(m.sessions, sid)
	m.mu.Unlock()
}

func (m *pollManager) count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.sessions)
}

func randomPollSID() string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "sid-fallback"
	}
	return hex.EncodeToString(b[:])
}
