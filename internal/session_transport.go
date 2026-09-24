// Copyright (c) 2026 Dibyaranjan Pradhan. All rights reserved.

package internal

import (
	"errors"
	"sync"
	"time"
)

// sessionTransport wraps the active Transport for an Engine.IO session so polling
// can be replaced with WebSocket without restarting client pumps.
type sessionTransport struct {
	mu      sync.RWMutex
	current Transport
}

func newSessionTransport(initial Transport) *sessionTransport {
	return &sessionTransport{current: initial}
}

// Swap replaces the underlying transport; callers must close the old transport after swapping.
func (s *sessionTransport) Swap(next Transport) {
	s.mu.Lock()
	s.current = next
	s.mu.Unlock()
}

func (s *sessionTransport) active() Transport {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.current
}

func (s *sessionTransport) Name() string { return s.active().Name() }

func (s *sessionTransport) SetReadLimit(limit int64) { s.active().SetReadLimit(limit) }

func (s *sessionTransport) SetReadDeadline(t time.Time) error {
	return s.active().SetReadDeadline(t)
}

func (s *sessionTransport) SetWriteDeadline(t time.Time) error {
	return s.active().SetWriteDeadline(t)
}

func (s *sessionTransport) OnPong(handler func()) { s.active().OnPong(handler) }

func (s *sessionTransport) Read() ([]byte, error) {
	for {
		data, err := s.active().Read()
		if errors.Is(err, errTransportUpgraded) {
			continue
		}
		return data, err
	}
}

func (s *sessionTransport) Write(data []byte) error { return s.active().Write(data) }

func (s *sessionTransport) Ping() error { return s.active().Ping() }

func (s *sessionTransport) SendClose() error { return s.active().SendClose() }

func (s *sessionTransport) Close() error { return s.active().Close() }
