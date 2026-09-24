// Copyright (c) 2026 Dibyaranjan Pradhan. All rights reserved.

package internal

import (
	"net/http"
	"time"

	"github.com/dibyaranjan-pradhan/go-socket/internal/engineio"
	"github.com/dibyaranjan-pradhan/go-socket/internal/nativews"
)

// WebSocketBackend selects the WebSocket implementation used by Handler() and Engine.IO upgrades.
type WebSocketBackend int

const (
	// WebSocketBackendGorilla uses gorilla/websocket (default, production path).
	WebSocketBackendGorilla WebSocketBackend = iota
	// WebSocketBackendNative uses the in-tree RFC 6455 codec (alpha).
	WebSocketBackendNative
)

// TransportUpgrader turns an HTTP request into a Transport.
type TransportUpgrader interface {
	Upgrade(w http.ResponseWriter, r *http.Request) (Transport, error)
}

// NewTransportUpgrader picks gorilla or native based on backend.
func NewTransportUpgrader(backend WebSocketBackend, readBufferSize, writeBufferSize int, checkOrigin func(*http.Request) bool) TransportUpgrader {
	if backend == WebSocketBackendNative {
		return NewNativeUpgrader(checkOrigin)
	}
	return NewWSUpgrader(readBufferSize, writeBufferSize, checkOrigin)
}

type nativeUpgrader struct {
	checkOrigin func(*http.Request) bool
}

// NewNativeUpgrader builds an upgrader backed by the alpha RFC 6455 codec.
func NewNativeUpgrader(checkOrigin func(*http.Request) bool) TransportUpgrader {
	if checkOrigin == nil {
		checkOrigin = func(*http.Request) bool { return true }
	}
	return &nativeUpgrader{checkOrigin: checkOrigin}
}

func (u *nativeUpgrader) Upgrade(w http.ResponseWriter, r *http.Request) (Transport, error) {
	conn, err := nativews.Upgrade(w, r, u.checkOrigin)
	if err != nil {
		return nil, err
	}
	return &nativeTransport{conn: conn}, nil
}

// nativeTransport adapts nativews.Conn to Transport.
type nativeTransport struct {
	conn *nativews.Conn
}

func (t *nativeTransport) Name() string { return engineio.TransportWebSocket }

func (t *nativeTransport) SetReadLimit(limit int64) { t.conn.SetReadLimit(limit) }

func (t *nativeTransport) SetReadDeadline(tm time.Time) error { return t.conn.SetReadDeadline(tm) }

func (t *nativeTransport) SetWriteDeadline(tm time.Time) error { return t.conn.SetWriteDeadline(tm) }

func (t *nativeTransport) OnPong(handler func()) {
	t.conn.SetPongHandler(func(string) error {
		handler()
		return nil
	})
}

func (t *nativeTransport) Read() ([]byte, error) {
	_, data, err := t.conn.ReadMessage()
	return data, err
}

func (t *nativeTransport) Write(data []byte) error {
	return t.conn.WriteMessage(0x1, data)
}

func (t *nativeTransport) Ping() error {
	return t.conn.WriteControl(0x9, nil)
}

func (t *nativeTransport) SendClose() error {
	return t.conn.WriteControl(0x8, []byte{0x03, 0xe8})
}

func (t *nativeTransport) Close() error { return t.conn.Close() }
