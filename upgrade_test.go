package gosocket

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dibyaranjan-pradhan/go-socket/internal/engineio"
	"github.com/gorilla/websocket"
)

// TestEngineIOUpgradeEndToEnd verifies polling handshake, probe, upgrade, and message on WebSocket.
func TestEngineIOUpgradeEndToEnd(t *testing.T) {
	var mu sync.Mutex
	var got string
	s := New(Config{PingInterval: time.Hour, PongWait: time.Hour})
	s.On("echo", func(ctx *Context) {
		mu.Lock()
		got = ctx.Event()
		mu.Unlock()
		_ = ctx.Emit("reply", map[string]string{"ok": "1"})
	})

	mux := http.NewServeMux()
	mux.Handle("/engine.io/", s.EngineIOHandler())
	ts := httptest.NewServer(mux)
	defer ts.Close()

	sid, err := engineioHandshake(t, ts.URL)
	if err != nil {
		t.Fatal(err)
	}

	// probe
	probeWire, _ := engineio.Encode(engineio.Packet{Type: engineio.Ping, Data: []byte(engineio.ProbePayload)})
	postURL := ts.URL + "/engine.io/?EIO=4&transport=polling&sid=" + sid
	postProbe, err := http.Post(postURL, "text/plain", strings.NewReader(string(probeWire)))
	if err != nil {
		t.Fatal(err)
	}
	_ = postProbe.Body.Close()

	// upgrade packet
	upWire, _ := engineio.Encode(engineio.Packet{Type: engineio.Upgrade})
	postUp, err := http.Post(postURL, "text/plain", strings.NewReader(string(upWire)))
	if err != nil {
		t.Fatal(err)
	}
	_ = postUp.Body.Close()

	// websocket upgrade with same sid
	wsURL := strings.Replace(ts.URL, "http://", "ws://", 1) + "/engine.io/?EIO=4&transport=websocket&sid=" + sid
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	msgWire, _ := engineio.Encode(engineio.Packet{
		Type: engineio.Message,
		Data: []byte(`{"event":"echo","payload":{}}`),
	})
	if err := conn.WriteMessage(websocket.TextMessage, msgWire); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		g := got
		mu.Unlock()
		if g == "echo" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	mu.Lock()
	if got != "echo" {
		mu.Unlock()
		t.Fatal("handler not invoked after upgrade")
	}
	mu.Unlock()

	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	_, body, err := conn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "reply") {
		t.Fatalf("body = %q", body)
	}
}

func TestNativeWebSocketHandler(t *testing.T) {
	s := New(Config{NativeWebSocket: true, PingInterval: time.Hour, PongWait: time.Hour})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"event":"x","payload":{}}`)); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	_, _, _ = conn.ReadMessage()
}

func TestEngineIOUpgradePreservesSessionID(t *testing.T) {
	s := New(Config{PingInterval: time.Hour, PongWait: time.Hour})
	ts := httptest.NewServer(s.EngineIOHandler())
	defer ts.Close()

	sid, err := engineioHandshake(t, ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	postURL := ts.URL + "/?EIO=4&transport=polling&sid=" + sid
	probe, _ := engineio.Encode(engineio.Packet{Type: engineio.Ping, Data: []byte(engineio.ProbePayload)})
	_, _ = http.Post(postURL, "text/plain", strings.NewReader(string(probe)))
	up, _ := engineio.Encode(engineio.Packet{Type: engineio.Upgrade})
	_, _ = http.Post(postURL, "text/plain", strings.NewReader(string(up)))

	wsURL := strings.Replace(ts.URL, "http://", "ws://", 1) + "/?EIO=4&transport=websocket&sid=" + sid
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if s.GetServerStats().ActiveConnections < 1 {
		t.Fatal("expected active connection after upgrade")
	}
}
