// Copyright (c) 2026 Dibyaranjan Pradhan. All rights reserved.

package internal

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dibyaranjan-pradhan/go-socket/internal/engineio"
	"github.com/gorilla/websocket"
)

func TestNewTransportUpgraderSelectsBackend(t *testing.T) {
	g := NewTransportUpgrader(WebSocketBackendGorilla, 0, 0, nil)
	if g == nil {
		t.Fatal("nil gorilla upgrader")
	}
	n := NewTransportUpgrader(WebSocketBackendNative, 0, 0, nil)
	if _, ok := n.(*nativeUpgrader); !ok {
		t.Fatalf("native backend = %T", n)
	}
}

func TestNativeUpgraderRoundTrip(t *testing.T) {
	up := NewNativeUpgrader(nil)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tr, err := up.Upgrade(w, r)
		if err != nil {
			t.Error(err)
			return
		}
		defer tr.Close()
		tr.SetReadLimit(1024)
		_ = tr.SetReadDeadline(time.Now().Add(time.Second))
		_ = tr.SetWriteDeadline(time.Now().Add(time.Second))
		ponged := false
		tr.OnPong(func() { ponged = true })
		data, err := tr.Read()
		if err != nil {
			t.Error(err)
			return
		}
		if string(data) != "ping" {
			t.Fatalf("data = %q", data)
		}
		if err := tr.Write([]byte("pong")); err != nil {
			t.Error(err)
		}
		_ = tr.Ping()
		_ = tr.SendClose()
		_ = ponged
	}))
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.WriteMessage(websocket.TextMessage, []byte("ping")); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	_, reply, err := conn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if string(reply) != "pong" {
		t.Fatalf("reply = %q", reply)
	}
}

func TestNativeTransportName(t *testing.T) {
	up := NewNativeUpgrader(nil)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tr, err := up.Upgrade(w, r)
		if err != nil {
			return
		}
		if tr.Name() != engineio.TransportWebSocket {
			t.Fatalf("name = %q", tr.Name())
		}
		_ = tr.Close()
	}))
	defer ts.Close()
	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
}

func TestNativeUpgraderRejectsOrigin(t *testing.T) {
	up := NewNativeUpgrader(func(*http.Request) bool { return false })
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	req.Header.Set("Sec-WebSocket-Version", "13")
	rec := httptest.NewRecorder()
	if _, err := up.Upgrade(rec, req); err == nil {
		t.Fatal("expected error")
	}
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d", rec.Code)
	}
}
