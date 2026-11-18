// Copyright (c) 2026 Dibyaranjan Pradhan. All rights reserved.

package nativews

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"
)

// clientFrame encodes one masked client-to-server frame.
func clientFrame(fin bool, op byte, payload []byte) []byte {
	b0 := op
	if fin {
		b0 |= 0x80
	}
	out := []byte{b0}
	switch n := len(payload); {
	case n <= 125:
		out = append(out, 0x80|byte(n))
	case n <= 0xffff:
		out = binary.BigEndian.AppendUint16(append(out, 0x80|126), uint16(n))
	default:
		out = binary.BigEndian.AppendUint64(append(out, 0x80|127), uint64(n))
	}
	mask := [4]byte{0x37, 0xfa, 0x21, 0x3d}
	masked := bytes.Clone(payload)
	maskBytes(mask, masked)
	return append(append(out, mask[:]...), masked...)
}

type exchangeResult struct {
	op    int
	msg   []byte
	err   error
	reply []byte // raw bytes the server wrote back
}

// exchange feeds raw client bytes to a server Conn, runs one ReadMessage, and
// returns what the server sent back.
func exchange(t *testing.T, input []byte, limit int64) exchangeResult {
	t.Helper()
	srv, peer := net.Pipe()
	c := newConn(srv, bufio.NewReader(srv), bufio.NewWriter(srv), false)
	c.SetReadLimit(limit)
	go func() { _, _ = peer.Write(input) }()
	replies := make(chan []byte)
	go func() {
		b, _ := io.ReadAll(peer)
		replies <- b
	}()
	var res exchangeResult
	res.op, res.msg, res.err = c.ReadMessage()
	_ = c.Close()
	res.reply = <-replies
	_ = peer.Close()
	return res
}

// closeCode returns the status of the first frame in b when it is a close frame, or 0.
func closeCode(b []byte) int {
	if len(b) < 2 || b[0]&0x0f != CloseMessage {
		return 0
	}
	if b[1]&0x7f < 2 {
		return CloseNoStatusReceived
	}
	return int(binary.BigEndian.Uint16(b[2:4]))
}

func TestReadMessage(t *testing.T) {
	long := bytes.Repeat([]byte("a"), 70000)
	cat := func(frames ...[]byte) []byte { return bytes.Join(frames, nil) }
	tests := []struct {
		name      string
		input     []byte
		limit     int64
		wantOp    int
		wantMsg   string
		wantClose int // close status the server must send; 0 means none
	}{
		{name: "text", input: clientFrame(true, TextMessage, []byte("hello")), wantOp: TextMessage, wantMsg: "hello"},
		{name: "binary", input: clientFrame(true, BinaryMessage, []byte{0, 0xff}), wantOp: BinaryMessage, wantMsg: "\x00\xff"},
		{name: "empty text", input: clientFrame(true, TextMessage, nil), wantOp: TextMessage},
		{name: "16-bit length", input: clientFrame(true, TextMessage, long[:300]), wantOp: TextMessage, wantMsg: string(long[:300])},
		{name: "64-bit length", input: clientFrame(true, BinaryMessage, long), wantOp: BinaryMessage, wantMsg: string(long)},
		{
			name: "fragments with interleaved ping",
			input: cat(clientFrame(false, TextMessage, []byte("hel")), clientFrame(true, PingMessage, []byte("p")),
				clientFrame(true, opContinuation, []byte("lo"))),
			wantOp: TextMessage, wantMsg: "hello",
		},
		{
			name:   "rune split across fragments",
			input:  cat(clientFrame(false, TextMessage, []byte("\xce")), clientFrame(true, opContinuation, []byte("\xba"))),
			wantOp: TextMessage, wantMsg: "κ",
		},
		{name: "reserved bits", input: []byte{0xc1, 0x80, 0, 0, 0, 0}, wantClose: CloseProtocolError},
		{name: "reserved data opcode", input: clientFrame(true, 3, nil), wantClose: CloseProtocolError},
		{name: "reserved control opcode", input: clientFrame(true, 0xb, nil), wantClose: CloseProtocolError},
		{name: "fragmented ping", input: clientFrame(false, PingMessage, nil), wantClose: CloseProtocolError},
		{name: "oversized ping", input: clientFrame(true, PingMessage, long[:126]), wantClose: CloseProtocolError},
		{name: "unmasked frame", input: []byte{0x81, 0x01, 'a'}, wantClose: CloseProtocolError},
		{name: "continuation first", input: clientFrame(true, opContinuation, []byte("x")), wantClose: CloseProtocolError},
		{
			name:      "new message mid-fragment",
			input:     cat(clientFrame(false, TextMessage, []byte("a")), clientFrame(true, TextMessage, []byte("b"))),
			wantClose: CloseProtocolError,
		},
		{name: "length msb set", input: []byte{0x82, 0xff, 0x80, 0, 0, 0, 0, 0, 0, 0}, wantClose: CloseProtocolError},
		{
			// buffered bytes + max int64 length must not overflow past the limit check
			name: "max length after a fragment",
			input: append(clientFrame(false, BinaryMessage, []byte("a")),
				0x80, 0xff, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0),
			wantClose: CloseMessageTooBig,
		},
		{name: "over read limit", input: clientFrame(true, TextMessage, []byte("12345")), limit: 4, wantClose: CloseMessageTooBig},
		{
			name:      "over read limit across fragments",
			input:     cat(clientFrame(false, TextMessage, []byte("123")), clientFrame(true, opContinuation, []byte("45"))),
			limit:     4,
			wantClose: CloseMessageTooBig,
		},
		{name: "invalid utf8", input: clientFrame(true, TextMessage, []byte{0xff}), wantClose: CloseInvalidPayload},
		{name: "invalid utf8 fails fast", input: clientFrame(false, TextMessage, []byte("ok\xc0\xaf")), wantClose: CloseInvalidPayload},
		{name: "truncated rune at end", input: clientFrame(true, TextMessage, []byte("\xce")), wantClose: CloseInvalidPayload},
		{name: "close status 1", input: clientFrame(true, CloseMessage, []byte{3}), wantClose: CloseProtocolError},
		{name: "close status reserved", input: clientFrame(true, CloseMessage, FormatCloseMessage(1005, "")), wantClose: CloseProtocolError},
		{name: "close reason not utf8", input: clientFrame(true, CloseMessage, FormatCloseMessage(1000, "\xff")), wantClose: CloseInvalidPayload},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := exchange(t, tt.input, tt.limit)
			if tt.wantClose != 0 {
				var pe *protocolError
				if !errors.As(res.err, &pe) || pe.code != tt.wantClose || !strings.HasPrefix(pe.Error(), "nativews: ") {
					t.Fatalf("err = %v, want protocol error %d", res.err, tt.wantClose)
				}
				if got := closeCode(res.reply); got != tt.wantClose {
					t.Fatalf("sent close %d, want %d (reply % x)", got, tt.wantClose, res.reply)
				}
				return
			}
			if res.err != nil || res.op != tt.wantOp || string(res.msg) != tt.wantMsg {
				t.Fatalf("got op=%d len=%d err=%v", res.op, len(res.msg), res.err)
			}
		})
	}
}

func TestReadMessageAnswersPing(t *testing.T) {
	in := append(clientFrame(true, PingMessage, []byte("hb")), clientFrame(true, TextMessage, []byte("x"))...)
	res := exchange(t, in, 0)
	if res.err != nil {
		t.Fatal(res.err)
	}
	if want := []byte{0x80 | PongMessage, 2, 'h', 'b'}; !bytes.Equal(res.reply, want) {
		t.Fatalf("reply = % x, want % x", res.reply, want)
	}
}

func TestReadMessagePeerClose(t *testing.T) {
	tests := []struct {
		name      string
		payload   []byte
		wantCode  int
		wantText  string
		wantReply []byte
	}{
		{name: "with status", payload: FormatCloseMessage(4000, "bye"), wantCode: 4000, wantText: "bye", wantReply: []byte{0x88, 2, 0x0f, 0xa0}},
		{name: "without status", wantCode: CloseNoStatusReceived, wantReply: []byte{0x88, 0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := exchange(t, clientFrame(true, CloseMessage, tt.payload), 0)
			var ce *CloseError
			if !errors.As(res.err, &ce) || ce.Code != tt.wantCode || ce.Text != tt.wantText {
				t.Fatalf("err = %v", res.err)
			}
			if !strings.Contains(ce.Error(), "peer closed") {
				t.Fatalf("Error() = %q", ce.Error())
			}
			if !bytes.Equal(res.reply, tt.wantReply) {
				t.Fatalf("reply = % x, want % x", res.reply, tt.wantReply)
			}
		})
	}
}

// readTruncated reads from a peer that sends data and then hangs up.
func readTruncated(data []byte) error {
	srv, peer := net.Pipe()
	c := newConn(srv, bufio.NewReader(srv), bufio.NewWriter(srv), false)
	defer func() { _ = c.Close() }()
	go func() { _, _ = peer.Write(data); _ = peer.Close() }()
	_, _, err := c.ReadMessage()
	return err
}

func TestReadMessageTruncatedInput(t *testing.T) {
	text := clientFrame(true, TextMessage, bytes.Repeat([]byte("z"), 300))
	long := clientFrame(true, BinaryMessage, bytes.Repeat([]byte("z"), 70000))
	ping := clientFrame(true, PingMessage, []byte("abc"))
	for name, in := range map[string][]byte{
		"header": text[:1], "16-bit length": text[:3], "64-bit length": long[:5],
		"mask": text[:6], "payload": text[:20], "control payload": ping[:7],
	} {
		var pe *protocolError
		if err := readTruncated(in); err == nil || errors.As(err, &pe) {
			t.Errorf("%s: err = %v, want an I/O error", name, err)
		}
	}
}

func TestReadErrorIsSticky(t *testing.T) {
	srv, peer := net.Pipe()
	defer func() { _ = peer.Close() }()
	c := newConn(srv, bufio.NewReader(srv), bufio.NewWriter(srv), false)
	defer func() { _ = c.Close() }()
	frame := clientFrame(true, TextMessage, []byte("hello"))
	go func() { _, _ = peer.Write(frame[:4]) }() // stall mid-frame
	_ = c.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
	_, _, first := c.ReadMessage()
	var ne net.Error
	if !errors.As(first, &ne) || !ne.Timeout() {
		t.Fatalf("first err = %v, want timeout", first)
	}
	_ = c.SetReadDeadline(time.Time{})
	go func() { _, _ = peer.Write(frame[4:]) }()
	if _, _, err := c.ReadMessage(); err != first {
		t.Fatalf("second err = %v, want the first error again", err)
	}
}

func TestDeclaredLengthIsNotPreallocated(t *testing.T) {
	srv, peer := net.Pipe()
	c := newConn(srv, bufio.NewReader(srv), bufio.NewWriter(srv), false)
	c.SetReadLimit(1 << 30)
	defer func() { _ = c.Close() }()
	hdr := []byte{0x82, 0xff, 0, 0, 0, 0, 0x20, 0, 0, 0, 0, 0, 0, 0} // claims 512 MiB, sends nothing
	go func() { _, _ = peer.Write(hdr); _ = peer.Close() }()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, _, _ = c.ReadMessage()
	runtime.ReadMemStats(&after)
	if grew := after.TotalAlloc - before.TotalAlloc; grew > 1<<20 {
		t.Fatalf("allocated %d bytes for a frame that sent none", grew)
	}
}

func TestPongHandler(t *testing.T) {
	srv, peer := net.Pipe()
	defer func() { _ = peer.Close() }()
	c := newConn(srv, bufio.NewReader(srv), bufio.NewWriter(srv), false)
	defer func() { _ = c.Close() }()
	stop := errors.New("stop")
	var got string
	c.SetPongHandler(func(p string) error { got = p; return stop })
	go func() { _, _ = peer.Write(clientFrame(true, PongMessage, []byte("seen"))) }()
	if _, _, err := c.ReadMessage(); !errors.Is(err, stop) || got != "seen" {
		t.Fatalf("err=%v payload=%q", err, got)
	}
}

// pair connects a server Conn and a client Conn over an in-memory pipe.
func pair(t *testing.T) (srv, cli *Conn) {
	t.Helper()
	a, b := net.Pipe()
	srv = newConn(a, bufio.NewReader(a), bufio.NewWriter(a), false)
	cli = newConn(b, bufio.NewReader(b), bufio.NewWriter(b), true)
	t.Cleanup(func() { _ = srv.Close(); _ = cli.Close() })
	return srv, cli
}

func TestRoundTripBothDirections(t *testing.T) {
	srv, cli := pair(t)
	for _, n := range []int{0, 125, 126, 65535, 65536} {
		msg := bytes.Repeat([]byte("m"), n)
		go func() { _ = cli.WriteMessage(BinaryMessage, msg) }()
		if op, got, err := srv.ReadMessage(); err != nil || op != BinaryMessage || !bytes.Equal(got, msg) {
			t.Fatalf("client->server %d: op=%d len=%d err=%v", n, op, len(got), err)
		}
		go func() { _ = srv.WriteMessage(TextMessage, msg) }()
		if op, got, err := cli.ReadMessage(); err != nil || op != TextMessage || !bytes.Equal(got, msg) {
			t.Fatalf("server->client %d: op=%d len=%d err=%v", n, op, len(got), err)
		}
	}
}

func TestClientMaskingLeavesCallerBufferIntact(t *testing.T) {
	srv, cli := pair(t)
	msg := []byte("keep me")
	go func() { _ = cli.WriteMessage(TextMessage, msg) }()
	if _, _, err := srv.ReadMessage(); err != nil {
		t.Fatal(err)
	}
	if string(msg) != "keep me" {
		t.Fatalf("caller buffer mutated: %q", msg)
	}
}

func TestClientRejectsMaskedServerFrame(t *testing.T) {
	a, b := net.Pipe()
	cli := newConn(b, bufio.NewReader(b), bufio.NewWriter(b), true)
	defer func() { _ = cli.Close() }()
	go func() {
		_, _ = a.Write(clientFrame(true, TextMessage, []byte("x")))
		_, _ = io.Copy(io.Discard, a)
	}()
	var pe *protocolError
	if _, _, err := cli.ReadMessage(); !errors.As(err, &pe) {
		t.Fatalf("err = %v", err)
	}
}

func TestWritesAfterClose(t *testing.T) {
	srv, cli := pair(t)
	go func() { _, _, _ = cli.ReadMessage() }()
	if err := srv.WriteControl(CloseMessage, FormatCloseMessage(CloseNormalClosure, "")); err != nil {
		t.Fatal(err)
	}
	if err := srv.WriteMessage(TextMessage, []byte("late")); err != ErrCloseSent {
		t.Fatalf("err = %v, want ErrCloseSent", err)
	}
}

func TestPingAfterOwnCloseIsIgnored(t *testing.T) {
	srv, peer := net.Pipe()
	defer func() { _ = peer.Close() }()
	c := newConn(srv, bufio.NewReader(srv), bufio.NewWriter(srv), false)
	defer func() { _ = c.Close() }()
	go func() {
		_, _ = peer.Write(clientFrame(true, PingMessage, nil))
		_, _ = peer.Write(clientFrame(true, CloseMessage, FormatCloseMessage(CloseNormalClosure, "")))
	}()
	go func() { _, _ = io.Copy(io.Discard, peer) }()
	if err := c.WriteControl(CloseMessage, nil); err != nil {
		t.Fatal(err)
	}
	var ce *CloseError
	if _, _, err := c.ReadMessage(); !errors.As(err, &ce) || ce.Code != CloseNormalClosure {
		t.Fatalf("err = %v", err)
	}
}

func TestWriteValidation(t *testing.T) {
	srv, _ := pair(t)
	if err := srv.WriteMessage(PingMessage, nil); err == nil {
		t.Fatal("WriteMessage accepted a control opcode")
	}
	if err := srv.WriteControl(TextMessage, nil); err == nil {
		t.Fatal("WriteControl accepted a data opcode")
	}
	if err := srv.WriteControl(PingMessage, make([]byte, 126)); err == nil {
		t.Fatal("WriteControl accepted 126 bytes")
	}
	if err := srv.SetReadDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err := srv.SetWriteDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
}

func TestWriteErrorsSurface(t *testing.T) {
	srv, cli := pair(t)
	_ = cli.Close()
	for _, n := range []int{0, 5000} { // header-only write and buffered-payload flush
		if err := srv.WriteMessage(TextMessage, make([]byte, n)); err == nil {
			t.Fatalf("write of %d bytes to closed peer succeeded", n)
		}
	}
}

// Concurrent pong replies from the reader must not interleave with pump writes.
func TestConcurrentControlAndDataWrites(t *testing.T) {
	srv, cli := pair(t)
	const n = 200
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range n {
			_ = srv.WriteMessage(TextMessage, bytes.Repeat([]byte("d"), 1000))
		}
	}()
	go func() {
		defer wg.Done()
		for range n {
			_ = cli.WriteControl(PingMessage, []byte("p"))
		}
	}()
	go func() { // server reader answers pings while the writer above runs
		for {
			if _, _, err := srv.ReadMessage(); err != nil {
				return
			}
		}
	}()
	for range n {
		if _, msg, err := cli.ReadMessage(); err != nil || len(msg) != 1000 {
			t.Fatalf("len=%d err=%v", len(msg), err)
		}
	}
	go func() { // drain trailing pongs so the ping writer can finish
		for {
			if _, _, err := cli.ReadMessage(); err != nil {
				return
			}
		}
	}()
	wg.Wait()
}

func TestUTF8Prefix(t *testing.T) {
	tests := []struct {
		in     string
		wantN  int
		wantOK bool
	}{
		{"", 0, true},
		{"abc", 3, true},
		{"aκ", 3, true},
		{"a\xce", 1, true},         // incomplete 2-byte rune
		{"a\xf0\x9f\x98", 1, true}, // incomplete 4-byte rune
		{"a\xff", 1, false},
		{"\xed\xa0\x80", 0, false}, // surrogate half
		{"\xf4\x90\x80\x80", 0, false},
	}
	for _, tt := range tests {
		if n, ok := utf8Prefix([]byte(tt.in)); n != tt.wantN || ok != tt.wantOK {
			t.Errorf("utf8Prefix(%q) = %d,%v want %d,%v", tt.in, n, ok, tt.wantN, tt.wantOK)
		}
	}
}

func TestValidCloseCode(t *testing.T) {
	for code, want := range map[int]bool{
		999: false, 1000: true, 1003: true, 1004: false, 1005: false, 1006: false,
		1007: true, 1011: true, 1012: false, 2999: false, 3000: true, 4999: true, 5000: false,
	} {
		if validCloseCode(code) != want {
			t.Errorf("validCloseCode(%d) = %v", code, !want)
		}
	}
}

// FuzzReadMessage feeds arbitrary bytes to a server Conn: it must never panic,
// never exceed the read limit, and never return invalid UTF-8 as text.
func FuzzReadMessage(f *testing.F) {
	f.Add(clientFrame(true, TextMessage, []byte("hello")))
	f.Add(append(clientFrame(false, TextMessage, []byte("a")), clientFrame(true, opContinuation, []byte("b"))...))
	f.Add(clientFrame(true, CloseMessage, FormatCloseMessage(1000, "x")))
	f.Add([]byte{0x82, 0xff, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})
	f.Add(append(clientFrame(false, BinaryMessage, []byte("a")), 0x80, 0xff, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff))
	f.Fuzz(func(t *testing.T, data []byte) {
		srv, peer := net.Pipe()
		c := newConn(srv, bufio.NewReader(srv), bufio.NewWriter(srv), false)
		c.SetReadLimit(1 << 16)
		go func() { _, _ = peer.Write(data); _ = peer.Close() }()
		go func() { _, _ = io.Copy(io.Discard, peer) }()
		defer func() { _ = c.Close() }()
		for {
			op, msg, err := c.ReadMessage()
			if err != nil {
				return
			}
			if len(msg) > 1<<16 {
				t.Fatalf("message of %d bytes exceeds limit", len(msg))
			}
			if op == TextMessage && !utf8.Valid(msg) {
				t.Fatalf("invalid UTF-8 returned as text: %q", msg)
			}
		}
	})
}
