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
	// WebSocketBackendNative uses the in-tree RFC 6455 codec.
	WebSocketBackendNative
)

// TransportUpgrader turns an HTTP request into a Transport.
type TransportUpgrader interface {
	Upgrade(w http.ResponseWriter, r *http.Request) (Transport, error)
}

// NewTransportUpgrader picks gorilla or native based on backend.
func NewTransportUpgrader(backend WebSocketBackend, readBufferSize, writeBufferSize int, checkOrigin func(*http.Request) bool) TransportUpgrader {
	if backend == WebSocketBackendNative {
		return NewNativeUpgrader(readBufferSize, checkOrigin)
	}
	return NewWSUpgrader(readBufferSize, writeBufferSize, checkOrigin)
}

type nativeUpgrader struct {
	up nativews.Upgrader
}

// NewNativeUpgrader builds an upgrader backed by the in-tree RFC 6455 codec. A nil
// checkOrigin allows connections from any origin.
func NewNativeUpgrader(readBufferSize int, checkOrigin func(*http.Request) bool) TransportUpgrader {
	return &nativeUpgrader{up: nativews.Upgrader{ReadBufferSize: readBufferSize, CheckOrigin: checkOrigin}}
}

func (u *nativeUpgrader) Upgrade(w http.ResponseWriter, r *http.Request) (Transport, error) {
	conn, err := u.up.Upgrade(w, r)
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
	return t.conn.WriteMessage(nativews.TextMessage, data)
}

func (t *nativeTransport) Ping() error {
	return t.conn.WriteControl(nativews.PingMessage, nil)
}

func (t *nativeTransport) SendClose() error {
	return t.conn.WriteControl(nativews.CloseMessage, nativews.FormatCloseMessage(nativews.CloseNormalClosure, ""))
}

func (t *nativeTransport) Close() error { return t.conn.Close() }
