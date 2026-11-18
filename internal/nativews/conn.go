// Copyright (c) 2026 Dibyaranjan Pradhan. All rights reserved.

// Package nativews is go-socket's RFC 6455 WebSocket codec, built on the standard
// library only so the core module carries no third-party dependency. It negotiates
// no extensions or subprotocols; permessage-deflate is planned for M7.
package nativews

import (
	"bufio"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"sync"
	"time"
	"unicode/utf8"
)

// Message and control opcodes (RFC 6455 §5.2).
const (
	TextMessage   = 1
	BinaryMessage = 2
	CloseMessage  = 8
	PingMessage   = 9
	PongMessage   = 10

	opContinuation    = 0
	maxControlPayload = 125

	// defaultReadLimit applies when SetReadLimit was never called, so a peer-declared
	// frame length can never drive an unbounded allocation.
	defaultReadLimit = 32 << 20
	readChunk        = 64 << 10
)

// Close codes (RFC 6455 §7.4.1).
const (
	CloseNormalClosure    = 1000
	CloseProtocolError    = 1002
	CloseNoStatusReceived = 1005
	CloseInvalidPayload   = 1007
	CloseMessageTooBig    = 1009
)

// ErrCloseSent is returned by writes after a close frame went out; RFC 6455 forbids
// sending anything else on the connection after that.
var ErrCloseSent = errors.New("nativews: close frame already sent")

// CloseError is returned by ReadMessage when the peer closes the connection.
// Code is CloseNoStatusReceived when the close frame carried no status.
type CloseError struct {
	Code int
	Text string
}

func (e *CloseError) Error() string {
	return fmt.Sprintf("nativews: peer closed (%d) %s", e.Code, e.Text)
}

// protocolError is a peer violation; ReadMessage answers it with close code before returning it.
type protocolError struct {
	code   int
	reason string
}

func (e *protocolError) Error() string { return "nativews: " + e.reason }

// Conn is one WebSocket connection after the opening handshake. One goroutine may
// read while others write: the reader answers pings and closes itself, so all
// writes share writeMu to keep frames from interleaving.
type Conn struct {
	nc     net.Conn
	br     *bufio.Reader
	client bool // client conns mask outbound frames and require unmasked inbound ones

	readMu      sync.Mutex
	readErr     error // sticky: a failed read may have consumed part of a frame
	readLimit   int64
	pongHandler func(string) error

	writeMu   sync.Mutex
	bw        *bufio.Writer
	closeSent bool
}

func newConn(nc net.Conn, br *bufio.Reader, bw *bufio.Writer, client bool) *Conn {
	return &Conn{nc: nc, br: br, bw: bw, client: client}
}

// SetReadLimit caps the size of one reassembled message. A larger message closes
// the connection with CloseMessageTooBig.
func (c *Conn) SetReadLimit(limit int64) { c.readLimit = limit }

// SetReadDeadline bounds the next read on the underlying connection.
func (c *Conn) SetReadDeadline(t time.Time) error { return c.nc.SetReadDeadline(t) }

// SetWriteDeadline bounds the next write on the underlying connection.
func (c *Conn) SetWriteDeadline(t time.Time) error { return c.nc.SetWriteDeadline(t) }

// SetPongHandler registers a callback for pong frames. A non-nil error from fn is
// returned by ReadMessage.
func (c *Conn) SetPongHandler(fn func(string) error) { c.pongHandler = fn }

// Close closes the underlying connection without a close handshake.
func (c *Conn) Close() error { return c.nc.Close() }

// ReadMessage returns the next complete text or binary message. Control frames are
// handled inline; a peer close is answered and returned as *CloseError. Errors are
// permanent, including deadline timeouts, because the stream position is unknown
// afterwards (same contract as gorilla). Only one goroutine may read at a time.
func (c *Conn) ReadMessage() (opcode int, payload []byte, err error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	if c.readErr != nil {
		return 0, nil, c.readErr
	}
	opcode, payload, err = c.readMessage()
	var pe *protocolError
	if errors.As(err, &pe) {
		_ = c.writeClose(pe.code)
	}
	c.readErr = err
	return opcode, payload, err
}

func (c *Conn) readMessage() (int, []byte, error) {
	limit := c.readLimit
	if limit <= 0 {
		limit = defaultReadLimit
	}
	var (
		op    int
		buf   []byte
		valid int // bytes of buf already verified as UTF-8
	)
	for {
		h, err := c.readHeader()
		if err != nil {
			return 0, nil, err
		}
		if h.opcode >= CloseMessage {
			payload, err := c.readPayload(h, nil)
			if err != nil {
				return 0, nil, err
			}
			if err := c.handleControl(h.opcode, payload); err != nil {
				return 0, nil, err
			}
			continue
		}
		switch {
		case h.opcode == opContinuation && op == 0:
			return 0, nil, &protocolError{CloseProtocolError, "continuation frame without a message"}
		case h.opcode != opContinuation && op != 0:
			return 0, nil, &protocolError{CloseProtocolError, "new message inside a fragmented message"}
		case h.opcode != opContinuation:
			op = h.opcode
		}
		// Subtract instead of add: len(buf)+h.length overflows for a forged 63-bit length.
		if h.length > limit-int64(len(buf)) {
			return 0, nil, &protocolError{CloseMessageTooBig, "message exceeds read limit"}
		}
		if buf, err = c.readPayload(h, buf); err != nil {
			return 0, nil, err
		}
		// Validating per fragment lets an invalid text message fail as soon as the
		// bad bytes arrive instead of after the whole message (Autobahn 6.x "fail fast").
		if op == TextMessage {
			n, ok := utf8Prefix(buf[valid:])
			valid += n
			if !ok || h.fin && valid != len(buf) {
				return 0, nil, &protocolError{CloseInvalidPayload, "invalid UTF-8 in text message"}
			}
		}
		if h.fin {
			return op, buf, nil
		}
	}
}

type frameHeader struct {
	fin    bool
	opcode int
	length int64
	masked bool
	mask   [4]byte
}

func (c *Conn) readHeader() (frameHeader, error) {
	var b [8]byte
	if _, err := io.ReadFull(c.br, b[:2]); err != nil {
		return frameHeader{}, err
	}
	h := frameHeader{
		fin:    b[0]&0x80 != 0,
		opcode: int(b[0] & 0x0f),
		length: int64(b[1] & 0x7f),
		masked: b[1]&0x80 != 0,
	}
	switch {
	case b[0]&0x70 != 0:
		return h, &protocolError{CloseProtocolError, "reserved bits set without a negotiated extension"}
	case h.opcode > BinaryMessage && h.opcode < CloseMessage || h.opcode > PongMessage:
		return h, &protocolError{CloseProtocolError, "reserved opcode"}
	case h.opcode >= CloseMessage && (!h.fin || h.length > maxControlPayload):
		return h, &protocolError{CloseProtocolError, "fragmented or oversized control frame"}
	case h.masked == c.client:
		return h, &protocolError{CloseProtocolError, "frame masking does not match the peer role"}
	}
	switch h.length {
	case 126:
		if _, err := io.ReadFull(c.br, b[:2]); err != nil {
			return h, err
		}
		h.length = int64(binary.BigEndian.Uint16(b[:2]))
	case 127:
		if _, err := io.ReadFull(c.br, b[:]); err != nil {
			return h, err
		}
		n := binary.BigEndian.Uint64(b[:])
		if n>>63 != 0 {
			return h, &protocolError{CloseProtocolError, "payload length has the most significant bit set"}
		}
		h.length = int64(n)
	}
	if h.masked {
		if _, err := io.ReadFull(c.br, h.mask[:]); err != nil {
			return h, err
		}
	}
	return h, nil
}

// readPayload appends the frame payload to dst, unmasked. Callers bound h.length
// first. It grows dst as bytes arrive, so a peer that declares a large frame and
// then stalls cannot pin memory it never sent.
func (c *Conn) readPayload(h frameHeader, dst []byte) ([]byte, error) {
	start := len(dst)
	for remaining := int(h.length); remaining > 0; {
		n := min(remaining, readChunk)
		dst = slices.Grow(dst, n)
		if _, err := io.ReadFull(c.br, dst[len(dst):len(dst)+n]); err != nil {
			return nil, err
		}
		dst = dst[:len(dst)+n]
		remaining -= n
	}
	if h.masked {
		maskBytes(h.mask, dst[start:])
	}
	return dst, nil
}

func (c *Conn) handleControl(op int, payload []byte) error {
	switch op {
	case PingMessage:
		if err := c.writeFrame(PongMessage, payload); err != nil && err != ErrCloseSent {
			return err
		}
	case PongMessage:
		if c.pongHandler != nil {
			return c.pongHandler(string(payload))
		}
	default:
		code, text, err := parseClose(payload)
		if err != nil {
			return err
		}
		_ = c.writeClose(code)
		return &CloseError{Code: code, Text: text}
	}
	return nil
}

func parseClose(p []byte) (int, string, error) {
	switch {
	case len(p) == 0:
		return CloseNoStatusReceived, "", nil
	case len(p) == 1:
		return 0, "", &protocolError{CloseProtocolError, "truncated close status"}
	}
	code := int(binary.BigEndian.Uint16(p))
	if !validCloseCode(code) {
		return 0, "", &protocolError{CloseProtocolError, "invalid close status"}
	}
	if !utf8.Valid(p[2:]) {
		return 0, "", &protocolError{CloseInvalidPayload, "invalid UTF-8 in close reason"}
	}
	return code, string(p[2:]), nil
}

// validCloseCode accepts the codes a peer may send: 1004-1006 and 1015 are reserved
// for local reporting, and 1012-2999 are unassigned in RFC 6455.
func validCloseCode(code int) bool {
	return code >= 1000 && code <= 1003 || code >= 1007 && code <= 1011 || code >= 3000 && code <= 4999
}

// FormatCloseMessage builds a close frame payload.
func FormatCloseMessage(code int, text string) []byte {
	return append(binary.BigEndian.AppendUint16(nil, uint16(code)), text...)
}

func (c *Conn) writeClose(code int) error {
	var p []byte
	if code != CloseNoStatusReceived {
		p = FormatCloseMessage(code, "")
	}
	return c.writeFrame(CloseMessage, p)
}

// WriteMessage sends one text or binary message as a single frame.
func (c *Conn) WriteMessage(opcode int, data []byte) error {
	if opcode != TextMessage && opcode != BinaryMessage {
		return fmt.Errorf("nativews: opcode %d is not a data message", opcode)
	}
	return c.writeFrame(opcode, data)
}

// WriteControl sends a ping, pong or close frame of at most 125 bytes.
func (c *Conn) WriteControl(opcode int, data []byte) error {
	if opcode < CloseMessage || opcode > PongMessage || len(data) > maxControlPayload {
		return fmt.Errorf("nativews: invalid control frame (opcode %d, %d bytes)", opcode, len(data))
	}
	return c.writeFrame(opcode, data)
}

func (c *Conn) writeFrame(op int, payload []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.closeSent {
		return ErrCloseSent
	}
	if op == CloseMessage {
		c.closeSent = true
	}
	var hdr [14]byte
	hdr[0] = 0x80 | byte(op)
	n, i := len(payload), 2
	switch {
	case n <= 125:
		hdr[1] = byte(n)
	case n <= 0xffff:
		hdr[1] = 126
		binary.BigEndian.PutUint16(hdr[2:], uint16(n))
		i = 4
	default:
		hdr[1] = 127
		binary.BigEndian.PutUint64(hdr[2:], uint64(n))
		i = 10
	}
	if c.client {
		hdr[1] |= 0x80
		_, _ = rand.Read(hdr[i : i+4])
		payload = slices.Clone(payload)
		maskBytes([4]byte(hdr[i:i+4]), payload)
		i += 4
	}
	if _, err := c.bw.Write(hdr[:i]); err != nil {
		return err
	}
	if _, err := c.bw.Write(payload); err != nil {
		return err
	}
	return c.bw.Flush()
}

func maskBytes(key [4]byte, b []byte) {
	for i := range b {
		b[i] ^= key[i&3]
	}
}

// utf8Prefix validates p up to a trailing rune that later bytes could still
// complete. It returns the length of the verified part.
func utf8Prefix(p []byte) (int, bool) {
	i := 0
	for i < len(p) {
		if p[i] < utf8.RuneSelf {
			i++
			continue
		}
		if !utf8.FullRune(p[i:]) {
			return i, true
		}
		r, size := utf8.DecodeRune(p[i:])
		if r == utf8.RuneError && size == 1 {
			return i, false
		}
		i += size
	}
	return i, true
}
