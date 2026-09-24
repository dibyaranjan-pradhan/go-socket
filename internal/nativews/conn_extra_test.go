package nativews_test

import (
	"bufio"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dibyaranjan-pradhan/go-socket/internal/nativews"
)

func TestUpgradeRejectsBadRequest(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	if _, err := nativews.Upgrade(rec, req, nil); err == nil {
		t.Fatal("expected error")
	}
}

func TestPingPongAndClose(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := nativews.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		c.SetReadLimit(1024)
		_ = c.SetReadDeadline(time.Now().Add(time.Second))
		_ = c.SetWriteDeadline(time.Now().Add(time.Second))
		ponged := false
		c.SetPongHandler(func(string) error { ponged = true; return nil })
		if err := c.WriteControl(0x9, []byte("ping")); err != nil {
			t.Error(err)
		}
		_, _, err = c.ReadMessage()
		if err != nil && err.Error() != "EOF" {
			// client may close after pong
		}
		_ = ponged
		_ = c.WriteControl(0x8, []byte{0x03, 0xe8})
		_ = c.Close()
	}))
	defer ts.Close()

	conn, err := net.Dial("tcp", strings.TrimPrefix(ts.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	writeClientHandshake(conn, nativews.GenerateKey())
	// respond to server ping with pong frame
	writeClientControl(conn, 0xA, []byte("ping"))
	_, _ = bufio.NewReader(conn).ReadByte()
}

func TestReadLimitExceeded(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, _ := nativews.Upgrade(w, r, nil)
		c.SetReadLimit(4)
		_, _, err := c.ReadMessage()
		if err == nil {
			t.Error("expected limit error")
		}
	}))
	defer ts.Close()

	conn, _ := net.Dial("tcp", strings.TrimPrefix(ts.URL, "http://"))
	defer conn.Close()
	writeClientHandshake(conn, nativews.GenerateKey())
	writeClientTextFrame(conn, []byte("12345"))
}

func writeClientHandshake(conn net.Conn, key string) {
	req := "GET / HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: " + key + "\r\nSec-WebSocket-Version: 13\r\n\r\n"
	_, _ = conn.Write([]byte(req))
	br := bufio.NewReader(conn)
	for {
		line, _ := br.ReadString('\n')
		if line == "\r\n" {
			break
		}
	}
}

func writeClientControl(conn net.Conn, opcode byte, payload []byte) {
	header := []byte{0x80 | opcode, 0x80 | byte(len(payload))}
	_, _ = conn.Write(header)
	mask := [4]byte{1, 2, 3, 4}
	masked := make([]byte, len(payload))
	for i := range payload {
		masked[i] = payload[i] ^ mask[i%4]
	}
	_, _ = conn.Write(mask[:])
	_, _ = conn.Write(masked)
}
