package nativews_test

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dibyaranjan-pradhan/go-socket/internal/nativews"
)

func TestUpgradeRejectsUnsupportedVersion(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Sec-WebSocket-Key", nativews.GenerateKey())
	req.Header.Set("Sec-WebSocket-Version", "8")
	if _, err := nativews.Upgrade(rec, req, nil); err == nil {
		t.Fatal("expected error")
	}
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestUpgradeRejectsOrigin(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Sec-WebSocket-Key", nativews.GenerateKey())
	req.Header.Set("Sec-WebSocket-Version", "13")
	check := func(*http.Request) bool { return false }
	if _, err := nativews.Upgrade(rec, req, check); err == nil {
		t.Fatal("expected error")
	}
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestWriteControlTooLarge(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := nativews.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		big := make([]byte, 126)
		if err := c.WriteControl(0x9, big); err == nil {
			t.Error("expected control frame error")
		}
	}))
	defer ts.Close()
	conn, _ := net.Dial("tcp", strings.TrimPrefix(ts.URL, "http://"))
	defer conn.Close()
	writeClientHandshake(conn, nativews.GenerateKey())
}

func TestFragmentedTextMessage(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := nativews.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		op, msg, err := c.ReadMessage()
		if err != nil {
			t.Error(err)
			return
		}
		if op != 0x1 || string(msg) != "hello world" {
			t.Fatalf("op=%d msg=%q", op, msg)
		}
	}))
	defer ts.Close()

	conn, err := net.Dial("tcp", strings.TrimPrefix(ts.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	writeClientHandshake(conn, nativews.GenerateKey())
	writeClientFragment(conn, false, 0x1, []byte("hello "))
	writeClientFragment(conn, true, 0x0, []byte("world"))
}

func TestServerRespondsToClientPing(t *testing.T) {
	done := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := nativews.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		ponged := false
		c.SetPongHandler(func(string) error { ponged = true; return nil })
		// ReadMessage handles inbound ping and emits pong automatically.
		go func() {
			for {
				_, _, err := c.ReadMessage()
				if err != nil {
					close(done)
					return
				}
			}
		}()
		<-done
		_ = ponged
	}))
	defer ts.Close()

	conn, err := net.Dial("tcp", strings.TrimPrefix(ts.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	writeClientHandshake(conn, nativews.GenerateKey())
	writeClientPing(conn, []byte("x"))
	_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	br := bufio.NewReader(conn)
	b1, err := br.ReadByte()
	if err != nil {
		t.Fatal(err)
	}
	if b1&0x0F != 0xA {
		t.Fatalf("expected pong opcode, got %x", b1)
	}
}

func TestExtendedPayloadLength126(t *testing.T) {
	payload := strings.Repeat("a", 200)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, _ := nativews.Upgrade(w, r, nil)
		defer c.Close()
		_, msg, err := c.ReadMessage()
		if err != nil || len(msg) != len(payload) {
			t.Fatalf("len=%d err=%v", len(msg), err)
		}
	}))
	defer ts.Close()
	conn, _ := net.Dial("tcp", strings.TrimPrefix(ts.URL, "http://"))
	defer conn.Close()
	writeClientHandshake(conn, nativews.GenerateKey())
	writeClientTextFrameExtended(conn, payload)
}

func TestServerWriteExtendedLength126(t *testing.T) {
	payload := strings.Repeat("b", 500)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := nativews.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		if _, _, err := c.ReadMessage(); err != nil {
			t.Error(err)
			return
		}
		if err := c.WriteMessage(0x1, []byte(payload)); err != nil {
			t.Error(err)
		}
	}))
	defer ts.Close()
	conn, err := net.Dial("tcp", strings.TrimPrefix(ts.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	writeClientHandshake(conn, nativews.GenerateKey())
	if err := writeClientTextFrameBytes(conn, []byte("go")); err != nil {
		t.Fatal(err)
	}
	got, err := readServerTextFrame(conn)
	if err != nil || string(got) != payload {
		t.Fatalf("len=%d err=%v", len(got), err)
	}
}

func writeClientTextFrameBytes(conn net.Conn, payload []byte) error {
	header := []byte{0x81, 0x80 | byte(len(payload))}
	if _, err := conn.Write(header); err != nil {
		return err
	}
	mask := [4]byte{1, 2, 3, 4}
	masked := make([]byte, len(payload))
	for i := range payload {
		masked[i] = payload[i] ^ mask[i%4]
	}
	if _, err := conn.Write(mask[:]); err != nil {
		return err
	}
	_, err := conn.Write(masked)
	return err
}

func readServerTextFrame(conn net.Conn) ([]byte, error) {
	br := bufio.NewReader(conn)
	if _, err := br.ReadByte(); err != nil {
		return nil, err
	}
	b2, err := br.ReadByte()
	if err != nil {
		return nil, err
	}
	length := int(b2 & 0x7f)
	if length == 126 {
		var ext [2]byte
		if _, err := io.ReadFull(br, ext[:]); err != nil {
			return nil, err
		}
		length = int(ext[0])<<8 | int(ext[1])
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(br, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func TestCloseNilCloser(t *testing.T) {
	c := &nativews.Conn{}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

func writeClientFragment(conn net.Conn, fin bool, opcode byte, payload []byte) {
	b0 := opcode
	if fin {
		b0 |= 0x80
	}
	header := []byte{b0, 0x80 | byte(len(payload))}
	_, _ = conn.Write(header)
	mask := [4]byte{9, 8, 7, 6}
	masked := make([]byte, len(payload))
	for i := range payload {
		masked[i] = payload[i] ^ mask[i%4]
	}
	_, _ = conn.Write(mask[:])
	_, _ = conn.Write(masked)
}

func writeClientPing(conn net.Conn, payload []byte) {
	writeClientControl(conn, 0x9, payload)
}

func writeClientTextFrameExtended(conn net.Conn, payload string) {
	data := []byte(payload)
	n := len(data)
	header := []byte{0x81, 0x80 | 126, byte(n >> 8), byte(n)}
	_, _ = conn.Write(header)
	mask := [4]byte{1, 2, 3, 4}
	masked := make([]byte, n)
	for i := range data {
		masked[i] = data[i] ^ mask[i%4]
	}
	_, _ = conn.Write(mask[:])
	_, _ = conn.Write(masked)
}
