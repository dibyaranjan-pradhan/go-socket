// Copyright (c) 2026 Dibyaranjan Pradhan. All rights reserved.

package internal

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"

	"github.com/dibyaranjan-pradhan/go-socket/internal/engineio"
)

// pollManager tracks live Engine.IO sessions keyed by sid.
type pollManager struct {
	mu       sync.RWMutex
	sessions map[string]*pollSession
	cfg      engineio.Config
}

type pollSession struct {
	engineio.Session
	polling   *pollingTransport
	sessionT  *sessionTransport
	probed    bool
	upgradeOK bool
	upgraded  bool
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

func (m *pollManager) create() (*engineio.Session, *sessionTransport) {
	sid := randomPollSID()
	sess := engineio.Session{
		ID:           sid,
		PingInterval: m.cfg.PingInterval,
		PingTimeout:  m.cfg.PingTimeout,
		MaxPayload:   m.cfg.MaxPayload,
		CreatedAt:    time.Now(),
	}
	pt := newPollingTransport(&sess)
	st := newSessionTransport(pt)
	ps := &pollSession{Session: sess, polling: pt, sessionT: st}

	pt.onClose = func() { m.remove(sid) }
	pt.onProbe = func() { m.markProbed(sid) }
	pt.onUpgradePacket = func() { m.markUpgrade(sid) }

	m.mu.Lock()
	m.sessions[sid] = ps
	m.mu.Unlock()
	return &sess, st
}

func (m *pollManager) getPolling(sid string) (*pollingTransport, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ps, ok := m.sessions[sid]
	if !ok || ps.polling == nil || ps.upgraded {
		return nil, false
	}
	return ps.polling, true
}

func (m *pollManager) markProbed(sid string) {
	m.mu.Lock()
	if ps, ok := m.sessions[sid]; ok {
		ps.probed = true
	}
	m.mu.Unlock()
}

func (m *pollManager) markUpgrade(sid string) {
	m.mu.Lock()
	if ps, ok := m.sessions[sid]; ok {
		ps.upgradeOK = true
	}
	m.mu.Unlock()
}

func (m *pollManager) readyForUpgrade(sid string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ps, ok := m.sessions[sid]
	return ok && ps.probed && ps.upgradeOK && !ps.upgraded
}

// applyUpgrade swaps the session transport to WebSocket without restarting client pumps.
func (m *pollManager) applyUpgrade(sid string, ws Transport) (*sessionTransport, bool) {
	m.mu.Lock()
	ps, ok := m.sessions[sid]
	if !ok || !ps.probed || !ps.upgradeOK || ps.upgraded {
		m.mu.Unlock()
		return nil, false
	}
	ps.upgraded = true
	st := ps.sessionT
	pt := ps.polling
	m.mu.Unlock()

	eioWS := newEIOWebSocketTransport(ws)
	st.Swap(eioWS)
	if noop, err := engineio.Encode(engineio.Packet{Type: engineio.Noop}); err == nil {
		_ = pt.enqueueOutbound(noop)
	}
	pt.signalUpgrade()
	return st, true
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
