// Copyright (c) 2026 Dibyaranjan Pradhan. All rights reserved.

package nativews

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

var (
	// ErrNotWebSocket is returned when a request is not a valid opening handshake.
	ErrNotWebSocket = errors.New("nativews: not a websocket handshake")
	// ErrBadHandshake is returned by Dial when the server does not accept the upgrade.
	ErrBadHandshake = errors.New("nativews: bad handshake")
)

// Upgrader completes the server side of the opening handshake (RFC 6455 §4.2).
type Upgrader struct {
	// ReadBufferSize sizes the connection's read buffer. Zero keeps net/http's buffer.
	ReadBufferSize int
	// CheckOrigin rejects the request with 403 when it returns false. Nil allows every origin.
	CheckOrigin func(r *http.Request) bool
}

// Upgrade hijacks the request's connection. On failure it has already written the
// HTTP error response, so callers only need to return.
func (u *Upgrader) Upgrade(w http.ResponseWriter, r *http.Request) (*Conn, error) {
	if r.Method != http.MethodGet {
		return nil, reject(w, http.StatusMethodNotAllowed)
	}
	if !headerHasToken(r.Header, "Connection", "upgrade") || !headerHasToken(r.Header, "Upgrade", "websocket") {
		return nil, reject(w, http.StatusBadRequest)
	}
	if r.Header.Get("Sec-WebSocket-Version") != "13" {
		w.Header().Set("Sec-WebSocket-Version", "13") // RFC 6455 §4.4: advertise the version we speak
		return nil, reject(w, http.StatusBadRequest)
	}
	key := r.Header.Get("Sec-WebSocket-Key")
	if k, err := base64.StdEncoding.DecodeString(key); err != nil || len(k) != 16 {
		return nil, reject(w, http.StatusBadRequest)
	}
	if u.CheckOrigin != nil && !u.CheckOrigin(r) {
		return nil, reject(w, http.StatusForbidden)
	}
	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return nil, errors.New("nativews: response writer does not support hijacking")
	}
	nc, rw, err := hj.Hijack()
	if err != nil {
		return nil, err
	}
	// http.Server read/write timeouts would otherwise kill a long-lived socket; the
	// caller's pumps own deadlines from here on.
	_ = nc.SetDeadline(time.Time{})
	br := rw.Reader
	// Bytes the client pipelined behind the handshake already sit in net/http's buffer.
	if u.ReadBufferSize > 0 && br.Buffered() == 0 {
		br = bufio.NewReaderSize(nc, u.ReadBufferSize)
	}
	_, _ = rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + acceptKey(key) + "\r\n\r\n")
	if err := rw.Flush(); err != nil {
		_ = nc.Close()
		return nil, err
	}
	return newConn(nc, br, rw.Writer, false), nil
}

func reject(w http.ResponseWriter, status int) error {
	http.Error(w, http.StatusText(status), status)
	return ErrNotWebSocket
}

// Dial opens a client connection to a ws:// URL; wss:// is not supported. It exists
// for go-socket's own tests and tools. When the server refuses the upgrade it
// returns ErrBadHandshake together with the response, so callers can check the status.
func Dial(ctx context.Context, rawURL string, header http.Header) (*Conn, *http.Response, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, nil, err
	}
	if u.Scheme != "ws" {
		return nil, nil, fmt.Errorf("nativews: unsupported scheme %q", u.Scheme)
	}
	u.Scheme = "http"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, nil, err
	}
	for k, v := range header {
		req.Header[k] = v
	}
	key := GenerateKey()
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Sec-WebSocket-Key", key)
	req.Header.Set("Sec-WebSocket-Version", "13")

	addr := u.Host
	if u.Port() == "" {
		addr = net.JoinHostPort(u.Hostname(), "80")
	}
	var d net.Dialer
	nc, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, nil, err
	}
	if dl, ok := ctx.Deadline(); ok {
		_ = nc.SetDeadline(dl)
	}
	br := bufio.NewReader(nc)
	err = req.Write(nc)
	var resp *http.Response
	if err == nil {
		resp, err = http.ReadResponse(br, req)
	}
	if err != nil {
		_ = nc.Close()
		return nil, nil, err
	}
	if resp.StatusCode != http.StatusSwitchingProtocols || resp.Header.Get("Sec-WebSocket-Accept") != acceptKey(key) {
		_ = nc.Close()
		return nil, resp, ErrBadHandshake
	}
	_ = nc.SetDeadline(time.Time{})
	return newConn(nc, br, bufio.NewWriter(nc), true), resp, nil
}

// acceptKey uses SHA-1 because RFC 6455 §4.2.2 mandates it; it proves protocol
// awareness, not integrity.
func acceptKey(key string) string {
	h := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	return base64.StdEncoding.EncodeToString(h[:])
}

func headerHasToken(h http.Header, name, token string) bool {
	for _, v := range h[name] {
		for part := range strings.SplitSeq(v, ",") {
			if strings.EqualFold(strings.TrimSpace(part), token) {
				return true
			}
		}
	}
	return false
}

// GenerateKey returns a random Sec-WebSocket-Key.
func GenerateKey() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return base64.StdEncoding.EncodeToString(b[:])
}
