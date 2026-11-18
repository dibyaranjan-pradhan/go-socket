// Copyright (c) 2026 Dibyaranjan Pradhan. All rights reserved.

package nativews_test

import (
	"bufio"
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dibyaranjan-pradhan/go-socket/internal/nativews"
)

func wsURL(ts *httptest.Server) string { return "ws" + strings.TrimPrefix(ts.URL, "http") }

func dial(t *testing.T, url string, h http.Header) (*nativews.Conn, *http.Response, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return nativews.Dial(ctx, url, h)
}

// echoServer upgrades with u and echoes messages until the peer goes away.
func echoServer(t *testing.T, u *nativews.Upgrader) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := u.Upgrade(w, r)
		if err != nil {
			return
		}
		defer func() { _ = c.Close() }()
		for {
			op, msg, err := c.ReadMessage()
			if err != nil {
				return
			}
			if err := c.WriteMessage(op, msg); err != nil {
				return
			}
		}
	}))
	t.Cleanup(ts.Close)
	return ts
}

func TestDialUpgradeEcho(t *testing.T) {
	for _, size := range []int{0, 64} {
		ts := echoServer(t, &nativews.Upgrader{ReadBufferSize: size})
		c, resp, err := dial(t, wsURL(ts), http.Header{"X-Probe": {"1"}})
		if err != nil {
			t.Fatalf("ReadBufferSize=%d: %v", size, err)
		}
		if resp.StatusCode != http.StatusSwitchingProtocols {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		if err := c.WriteMessage(nativews.TextMessage, []byte("ping")); err != nil {
			t.Fatal(err)
		}
		if _, msg, err := c.ReadMessage(); err != nil || string(msg) != "ping" {
			t.Fatalf("msg=%q err=%v", msg, err)
		}
		_ = c.Close()
	}
}

func TestDialPassesHeaders(t *testing.T) {
	got := make(chan string, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got <- r.Header.Get("Authorization")
		var u nativews.Upgrader
		if c, err := u.Upgrade(w, r); err == nil {
			_ = c.Close()
		}
	}))
	defer ts.Close()
	c, _, err := dial(t, wsURL(ts), http.Header{"Authorization": {"Bearer t"}})
	if err != nil {
		t.Fatal(err)
	}
	_ = c.Close()
	if h := <-got; h != "Bearer t" {
		t.Fatalf("Authorization = %q", h)
	}
}

func TestUpgradeRejects(t *testing.T) {
	valid := func() *http.Request {
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.Header.Set("Connection", "keep-alive, Upgrade")
		r.Header.Set("Upgrade", "websocket")
		r.Header.Set("Sec-WebSocket-Version", "13")
		r.Header.Set("Sec-WebSocket-Key", nativews.GenerateKey())
		return r
	}
	tests := []struct {
		name       string
		mutate     func(*http.Request)
		origin     func(*http.Request) bool
		wantStatus int
	}{
		{name: "post", mutate: func(r *http.Request) { r.Method = http.MethodPost }, wantStatus: http.StatusMethodNotAllowed},
		{name: "plain get", mutate: func(r *http.Request) { r.Header.Del("Upgrade") }, wantStatus: http.StatusBadRequest},
		{name: "no connection upgrade", mutate: func(r *http.Request) { r.Header.Set("Connection", "close") }, wantStatus: http.StatusBadRequest},
		{name: "old version", mutate: func(r *http.Request) { r.Header.Set("Sec-WebSocket-Version", "8") }, wantStatus: http.StatusBadRequest},
		{name: "bad key", mutate: func(r *http.Request) { r.Header.Set("Sec-WebSocket-Key", "short") }, wantStatus: http.StatusBadRequest},
		{name: "origin", origin: func(*http.Request) bool { return false }, wantStatus: http.StatusForbidden},
		// httptest.ResponseRecorder cannot be hijacked.
		{name: "not hijackable", wantStatus: http.StatusInternalServerError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := valid()
			if tt.mutate != nil {
				tt.mutate(r)
			}
			rec := httptest.NewRecorder()
			u := nativews.Upgrader{CheckOrigin: tt.origin}
			if _, err := u.Upgrade(rec, r); err == nil {
				t.Fatal("expected error")
			}
			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tt.wantStatus)
			}
		})
	}
}

func TestUpgradeAdvertisesVersion(t *testing.T) {
	ts := echoServer(t, &nativews.Upgrader{})
	req, _ := http.NewRequest(http.MethodGet, ts.URL, nil)
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Sec-WebSocket-Version", "8")
	req.Header.Set("Sec-WebSocket-Key", nativews.GenerateKey())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest || resp.Header.Get("Sec-WebSocket-Version") != "13" {
		t.Fatalf("status=%d version=%q", resp.StatusCode, resp.Header.Get("Sec-WebSocket-Version"))
	}
}

func TestDialRefusedUpgradeReturnsResponse(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "no", http.StatusUnauthorized)
	}))
	defer ts.Close()
	_, resp, err := dial(t, wsURL(ts), nil)
	if !errors.Is(err, nativews.ErrBadHandshake) || resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("err=%v resp=%v", err, resp)
	}
}

func TestDialErrors(t *testing.T) {
	closed := httptest.NewServer(http.NotFoundHandler())
	closedURL := wsURL(closed)
	closed.Close()
	hangup := hangupServer(t)
	for name, url := range map[string]string{
		"bad url":      "ws://[::1",
		"wss":          "wss://example.com",
		"bad method":   "ws://host/\x7f",
		"refused":      closedURL,
		"no response":  "ws://" + hangup,
		"default port": "ws://127.0.0.1.invalid",
	} {
		if _, _, err := dial(t, url, nil); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

// hangupServer accepts TCP connections and closes them before any HTTP reply.
func hangupServer(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()
	return ln.Addr().String()
}

// A client may pipeline its first frame behind the handshake; those bytes already
// sit in net/http's buffer and must not be lost when ReadBufferSize is set.
func TestUpgradeKeepsPipelinedFrame(t *testing.T) {
	got := make(chan string, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u := nativews.Upgrader{ReadBufferSize: 64}
		c, err := u.Upgrade(w, r)
		if err != nil {
			return
		}
		defer func() { _ = c.Close() }()
		_, msg, _ := c.ReadMessage()
		got <- string(msg)
	}))
	defer ts.Close()
	nc, err := net.Dial("tcp", strings.TrimPrefix(ts.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = nc.Close() }()
	frame := []byte{0x81, 0x82, 0, 0, 0, 0, 'h', 'i'} // masked with a zero key
	req := "GET / HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
		"Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: " + nativews.GenerateKey() + "\r\n\r\n"
	if _, err := nc.Write(append([]byte(req), frame...)); err != nil {
		t.Fatal(err)
	}
	if line, _ := bufio.NewReader(nc).ReadString('\n'); !strings.Contains(line, "101") {
		t.Fatalf("status line = %q", line)
	}
	if msg := <-got; msg != "hi" {
		t.Fatalf("msg = %q", msg)
	}
}

func TestAcceptKeyMatchesRFCVector(t *testing.T) {
	ts := echoServer(t, &nativews.Upgrader{})
	nc, err := net.Dial("tcp", strings.TrimPrefix(ts.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = nc.Close() }()
	// RFC 6455 §1.3 sample key and accept value.
	_, _ = nc.Write([]byte("GET / HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
		"Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"))
	resp, err := http.ReadResponse(bufio.NewReader(nc), nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := resp.Header.Get("Sec-WebSocket-Accept"); got != "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=" {
		t.Fatalf("accept = %q", got)
	}
}
