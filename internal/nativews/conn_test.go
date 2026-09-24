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

func TestUpgradeAndRoundTrip(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := nativews.Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		_, msg, err := c.ReadMessage()
		if err != nil {
			t.Error(err)
			return
		}
		if string(msg) != "hello" {
			t.Fatalf("msg = %q", msg)
		}
		if err := c.WriteMessage(0x1, []byte("world")); err != nil {
			t.Error(err)
		}
	}))
	defer ts.Close()

	conn, err := net.Dial("tcp", strings.TrimPrefix(ts.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	key := nativews.GenerateKey()
	req := "GET / HTTP/1.1\r\n" +
		"Host: " + strings.TrimPrefix(ts.URL, "http://") + "\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Key: " + key + "\r\n" +
		"Sec-WebSocket-Version: 13\r\n\r\n"
	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}

	br := bufio.NewReader(conn)
	status, err := br.ReadString('\n')
	if err != nil || !strings.Contains(status, "101") {
		t.Fatalf("status = %q err=%v", status, err)
	}
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if line == "\r\n" {
			break
		}
	}

	if err := writeClientTextFrame(conn, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	reply, err := readClientTextFrame(conn)
	if err != nil {
		t.Fatal(err)
	}
	if string(reply) != "world" {
		t.Fatalf("reply = %q", reply)
	}
}

func writeClientTextFrame(conn net.Conn, payload []byte) error {
	header := []byte{0x81, 0x80 | byte(len(payload))}
	if _, err := conn.Write(header); err != nil {
		return err
	}
	var mask [4]byte
	mask[0] = 0x12
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

func readClientTextFrame(conn net.Conn) ([]byte, error) {
	br := bufio.NewReader(conn)
	b1, err := br.ReadByte()
	if err != nil {
		return nil, err
	}
	b2, err := br.ReadByte()
	if err != nil {
		return nil, err
	}
	_ = b1
	length := int(b2 & 0x7f)
	buf := make([]byte, length)
	if _, err := br.Read(buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func TestSetDeadlines(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := nativews.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		_ = c.SetReadDeadline(time.Now().Add(time.Second))
		_ = c.SetWriteDeadline(time.Now().Add(time.Second))
	}))
	defer ts.Close()

	conn, err := net.Dial("tcp", strings.TrimPrefix(ts.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	key := nativews.GenerateKey()
	req := "GET / HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: " + key + "\r\nSec-WebSocket-Version: 13\r\n\r\n"
	_, _ = conn.Write([]byte(req))
}

func TestComputeAccept(t *testing.T) {
	// dGhlIHNhbXBsZSBub25jZQ== is the standard test vector key
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := nativews.Upgrade(w, r, nil); err != nil {
			t.Error(err)
		}
	}))
	defer ts.Close()

	conn, err := net.Dial("tcp", strings.TrimPrefix(ts.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	req := "GET / HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
	_, _ = conn.Write([]byte(req))
	br := bufio.NewReader(conn)
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(line, "Sec-WebSocket-Accept:") {
			if !strings.Contains(line, "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=") {
				t.Fatalf("bad accept: %q", line)
			}
			return
		}
	}
}
