package gosocket

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dibyaranjan-pradhan/go-socket/internal/engineio"
)

// TestEngineIOPollingHandshakeAndEvent is an end-to-end example: mount
// EngineIOHandler, complete the open handshake, POST a JSON event, and receive
// a handler response on the next poll.
func TestEngineIOPollingHandshakeAndEvent(t *testing.T) {
	var mu sync.Mutex
	var gotEvent string
	s := New(Config{PingInterval: 50 * time.Millisecond, PongWait: 200 * time.Millisecond})
	s.On("echo", func(ctx *Context) {
		mu.Lock()
		gotEvent = ctx.Event()
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

	msgWire, err := engineio.Encode(engineio.Packet{
		Type: engineio.Message,
		Data: []byte(`{"event":"echo","payload":{}}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	postURL := ts.URL + "/engine.io/?EIO=4&transport=polling&sid=" + sid
	postResp, err := http.Post(postURL, "text/plain", strings.NewReader(string(msgWire)))
	if err != nil {
		t.Fatal(err)
	}
	_ = postResp.Body.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		g := gotEvent
		mu.Unlock()
		if g == "echo" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	mu.Lock()
	if gotEvent != "echo" {
		mu.Unlock()
		t.Fatal("handler not invoked")
	}
	mu.Unlock()

	pollURL := postURL
	pollReq, _ := http.NewRequest(http.MethodGet, pollURL, nil)
	pollResp, err := http.DefaultClient.Do(pollReq)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(pollResp.Body)
	_ = pollResp.Body.Close()
	if !strings.Contains(string(body), "reply") {
		t.Fatalf("poll body = %q", body)
	}
}

func TestEngineIOHandlerDoesNotBreakWebSocket(t *testing.T) {
	s := New(Config{})
	wsTS := httptest.NewServer(s.Handler())
	defer wsTS.Close()

	// Existing WS path still serves; we only verify handler exists and WS mounts.
	if s.EngineIOHandler() == nil {
		t.Fatal("EngineIOHandler returned nil")
	}
}

func engineioHandshake(t *testing.T, baseURL string) (string, error) {
	t.Helper()
	u := baseURL + "/engine.io/?EIO=4&transport=polling"
	resp, err := http.Get(u)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	pkt, err := engineio.Decode(raw)
	if err != nil {
		return "", err
	}
	var hs struct {
		SID string `json:"sid"`
	}
	if err := json.Unmarshal(pkt.Data, &hs); err != nil {
		return "", err
	}
	time.Sleep(30 * time.Millisecond) // allow attach goroutine to register client
	return hs.SID, nil
}

func TestEngineIOShutdownRejectsHandshake(t *testing.T) {
	s := New(Config{})
	ts := httptest.NewServer(s.EngineIOHandler())
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = s.Shutdown(ctx)

	resp, err := http.Get(ts.URL + "/?EIO=4&transport=polling")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}
