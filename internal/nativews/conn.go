// Package nativews is go-socket's alpha server-side WebSocket codec (RFC 6455).
// It exists so the library can drop gorilla/websocket once Autobahn coverage is green.
package nativews

import (
	"bufio"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	opcodeContinuation = 0x0
	opcodeText         = 0x1
	opcodeBinary       = 0x2
	opcodeClose        = 0x8
	opcodePing         = 0x9
	opcodePong         = 0xA

	maxControlPayload = 125
)

var (
	// ErrNotWebSocket is returned when the request is not a valid upgrade.
	ErrNotWebSocket = errors.New("nativews: not a websocket handshake")
	// ErrHandshakeComplete means Upgrade was already called on this connection.
	ErrHandshakeComplete = errors.New("nativews: handshake already complete")
)

// Conn is a server-side WebSocket after the HTTP upgrade handshake.
type Conn struct {
	br          *bufio.Reader
	w           io.Writer
	closer      io.Closer
	readLimit   int64
	readMu      sync.Mutex
	writeMu     sync.Mutex
	pongHandler func(string) error
}

// Upgrade hijacks w and completes the WebSocket opening handshake.
func Upgrade(w http.ResponseWriter, r *http.Request, checkOrigin func(*http.Request) bool) (*Conn, error) {
	if !headerTokenContains(r.Header, "Connection", "upgrade") ||
		!strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		return nil, ErrNotWebSocket
	}
	if r.Method != http.MethodGet {
		return nil, ErrNotWebSocket
	}
	if checkOrigin != nil && !checkOrigin(r) {
		http.Error(w, "origin not allowed", http.StatusForbidden)
		return nil, ErrNotWebSocket
	}
	key := r.Header.Get("Sec-WebSocket-Key")
	if key == "" {
		return nil, ErrNotWebSocket
	}
	if r.Header.Get("Sec-WebSocket-Version") != "13" {
		http.Error(w, "unsupported websocket version", http.StatusBadRequest)
		return nil, ErrNotWebSocket
	}

	hj, ok := w.(http.Hijacker)
	if !ok {
		return nil, errors.New("nativews: ResponseWriter does not support hijacking")
	}
	conn, bufrw, err := hj.Hijack()
	if err != nil {
		return nil, err
	}
	accept := computeAccept(key)
	response := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + accept + "\r\n\r\n"
	if _, err := bufrw.WriteString(response); err != nil {
		_ = conn.Close()
		return nil, err
	}
	if err := bufrw.Flush(); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return &Conn{br: bufrw.Reader, w: conn, closer: conn}, nil
}

func computeAccept(key string) string {
	const magic = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
	h := sha1.Sum([]byte(key + magic))
	return base64.StdEncoding.EncodeToString(h[:])
}

func headerTokenContains(h http.Header, name, token string) bool {
	for _, v := range h[name] {
		for _, part := range strings.Split(v, ",") {
			if strings.EqualFold(strings.TrimSpace(part), token) {
				return true
			}
		}
	}
	return false
}

// SetReadLimit caps the payload size of a single message after reassembly.
func (c *Conn) SetReadLimit(limit int64) { c.readLimit = limit }

// SetReadDeadline sets the read deadline on the underlying connection when supported.
func (c *Conn) SetReadDeadline(t time.Time) error {
	if nc, ok := c.closer.(net.Conn); ok {
		return nc.SetReadDeadline(t)
	}
	return nil
}

// SetWriteDeadline sets the write deadline on the underlying connection when supported.
func (c *Conn) SetWriteDeadline(t time.Time) error {
	if nc, ok := c.closer.(net.Conn); ok {
		return nc.SetWriteDeadline(t)
	}
	return nil
}

// SetPongHandler registers a callback for incoming pong control frames.
func (c *Conn) SetPongHandler(fn func(string) error) { c.pongHandler = fn }

// ReadMessage reads one complete text or binary application message.
func (c *Conn) ReadMessage() (opcode int, payload []byte, err error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()

	var (
		msgOpcode int
		buf       []byte
	)
	for {
		fin, op, data, err := c.readFrame()
		if err != nil {
			return 0, nil, err
		}
		switch op {
		case opcodePing:
			if err := c.writeFrame(true, opcodePong, data); err != nil {
				return 0, nil, err
			}
			continue
		case opcodePong:
			if c.pongHandler != nil {
				_ = c.pongHandler(string(data))
			}
			continue
		case opcodeClose:
			_ = c.writeFrame(true, opcodeClose, nil)
			return 0, nil, io.EOF
		}

		if msgOpcode == 0 {
			msgOpcode = op
		} else if op != opcodeContinuation {
			return 0, nil, fmt.Errorf("nativews: unexpected opcode %d during fragmentation", op)
		}
		buf = append(buf, data...)
		if c.readLimit > 0 && int64(len(buf)) > c.readLimit {
			return 0, nil, fmt.Errorf("nativews: message exceeds read limit")
		}
		if fin {
			if msgOpcode != opcodeText && msgOpcode != opcodeBinary {
				return 0, nil, fmt.Errorf("nativews: fragmented control frame")
			}
			return msgOpcode, buf, nil
		}
	}
}

// WriteMessage sends one text or binary message (single frame, FIN set).
func (c *Conn) WriteMessage(opcode int, data []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.writeFrame(true, opcode, data)
}

// WriteControl sends a small control frame (ping/pong/close).
func (c *Conn) WriteControl(opcode int, data []byte) error {
	if len(data) > maxControlPayload {
		return fmt.Errorf("nativews: control frame too large")
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.writeFrame(true, opcode, data)
}

// Close closes the underlying connection.
func (c *Conn) Close() error {
	if c.closer == nil {
		return nil
	}
	return c.closer.Close()
}

func (c *Conn) readFrame() (fin bool, opcode int, payload []byte, err error) {
	b1, err := c.br.ReadByte()
	if err != nil {
		return false, 0, nil, err
	}
	b2, err := c.br.ReadByte()
	if err != nil {
		return false, 0, nil, err
	}
	fin = b1&0x80 != 0
	opcode = int(b1 & 0x0F)
	masked := b2&0x80 != 0
	length := int64(b2 & 0x7F)

	switch length {
	case 126:
		var ext [2]byte
		if _, err := io.ReadFull(c.br, ext[:]); err != nil {
			return false, 0, nil, err
		}
		length = int64(ext[0])<<8 | int64(ext[1])
	case 127:
		var ext [8]byte
		if _, err := io.ReadFull(c.br, ext[:]); err != nil {
			return false, 0, nil, err
		}
		length = 0
		for i := 0; i < 8; i++ {
			length = length<<8 | int64(ext[i])
		}
	}

	if !masked {
		return false, 0, nil, fmt.Errorf("nativews: client frame must be masked")
	}
	var maskKey [4]byte
	if _, err := io.ReadFull(c.br, maskKey[:]); err != nil {
		return false, 0, nil, err
	}
	payload = make([]byte, length)
	if _, err := io.ReadFull(c.br, payload); err != nil {
		return false, 0, nil, err
	}
	for i := range payload {
		payload[i] ^= maskKey[i%4]
	}

	return fin, opcode, payload, nil
}

func (c *Conn) writeFrame(fin bool, opcode int, payload []byte) error {
	var header []byte
	b0 := byte(opcode)
	if fin {
		b0 |= 0x80
	}
	header = append(header, b0)

	n := len(payload)
	switch {
	case n <= 125:
		header = append(header, byte(n))
	case n <= 65535:
		header = append(header, 126, byte(n>>8), byte(n))
	default:
		header = append(header, 127,
			0, 0, 0, 0,
			byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
	}

	if _, err := c.w.Write(header); err != nil {
		return err
	}
	if n > 0 {
		_, err := c.w.Write(payload)
		return err
	}
	return nil
}

// GenerateKey produces a valid Sec-WebSocket-Key for tests.
func GenerateKey() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return base64.StdEncoding.EncodeToString(b[:])
}
