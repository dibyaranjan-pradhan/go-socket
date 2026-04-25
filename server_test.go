package gosocket

import (
	"context"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func wsURL(httpURL string) string {
	return strings.Replace(httpURL, "http://", "ws://", 1)
}

func TestRoomSizeAfterOnConnectJoin(t *testing.T) {
	s := New(Config{})
	s.OnConnect(func(ctx *Context) {
		ctx.JoinRoom("room-a")
	})

	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	conn, _, err := websocket.DefaultDialer.Dial(wsURL(ts.URL), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	var n int
	for i := 0; i < 100; i++ {
		n = s.RoomSize("room-a")
		if n == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if n != 1 {
		t.Fatalf("RoomSize(room-a) = %d, want 1", n)
	}
	if s.RoomSize("missing") != 0 {
		t.Fatalf("RoomSize(missing) = %d, want 0", s.RoomSize("missing"))
	}
}

func TestWritePumpSendsCloseFrame(t *testing.T) {
	s := New(Config{})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	conn, _, err := websocket.DefaultDialer.Dial(wsURL(ts.URL), nil)
	if err != nil {
		t.Fatal(err)
	}

	// Initiate close from client; server ReadPump exits, WritePump should emit Close frame.
	_ = conn.WriteControl(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
		time.Now().Add(5*time.Second))

	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	mt, _, err := conn.ReadMessage()
	_ = conn.Close()
	if err != nil {
		// Some peers return error after close frame; still ok if we saw CloseMessage
		if ce, ok := err.(*websocket.CloseError); ok && ce.Code == websocket.CloseNormalClosure {
			return
		}
		t.Fatalf("read close: %v", err)
	}
	if mt != websocket.CloseMessage {
		t.Fatalf("message type = %d, want CloseMessage (%d)", mt, websocket.CloseMessage)
	}
}

func TestOnHandlerInvoked(t *testing.T) {
	var mu sync.Mutex
	var got string
	s := New(Config{})
	s.On("ping", func(ctx *Context) {
		mu.Lock()
		got = ctx.Event()
		mu.Unlock()
	})

	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	conn, _, err := websocket.DefaultDialer.Dial(wsURL(ts.URL), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_ = conn.SetWriteDeadline(time.Now().Add(1 * time.Second))
		if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"event":"ping","payload":{}}`)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
		mu.Lock()
		g := got
		mu.Unlock()
		if g == "ping" {
			return
		}
	}
	t.Fatal("handler not invoked")
}

func TestShutdownAllowsHubExit(t *testing.T) {
	s := New(Config{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestHandlerRejectsDialAfterShutdown(t *testing.T) {
	s := New(Config{})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	u := wsURL(ts.URL)

	shCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Shutdown(shCtx); err != nil {
		t.Fatal(err)
	}

	_, _, err := websocket.DefaultDialer.Dial(u, nil)
	if err == nil {
		t.Fatal("expected dial to fail after Shutdown")
	}
}

// Integration-style: two sockets in the same room; EmitToRoom from one handler reaches the peer.
func TestEmitToRoomDeliversToPeer(t *testing.T) {
	done := make(chan struct{}, 1)
	s := New(Config{})
	s.OnConnect(func(ctx *Context) {
		ctx.JoinRoom("g")
	})
	s.On("trigger", func(ctx *Context) {
		_ = ctx.EmitToRoom("g", "broadcast", map[string]bool{"ok": true})
	})

	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	u := wsURL(ts.URL)

	peer, _, err := websocket.DefaultDialer.Dial(u, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()

	go func() {
		for {
			_, data, err := peer.ReadMessage()
			if err != nil {
				return
			}
			if strings.Contains(string(data), `"event":"broadcast"`) {
				done <- struct{}{}
				return
			}
		}
	}()

	sender, _, err := websocket.DefaultDialer.Dial(u, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_ = sender.SetWriteDeadline(time.Now().Add(1 * time.Second))
		if err := sender.WriteMessage(websocket.TextMessage, []byte(`{"event":"trigger","payload":{}}`)); err != nil {
			t.Fatal(err)
		}
		select {
		case <-done:
			return
		case <-time.After(50 * time.Millisecond):
		}
	}
	t.Fatal("peer did not receive EmitToRoom payload in time")
}

func TestRecoverCatchesEventPanic(t *testing.T) {
	s := New(Config{})
	s.Use(Recover())
	s.On("die", func(ctx *Context) {
		panic("test panic")
	})

	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	conn, _, err := websocket.DefaultDialer.Dial(wsURL(ts.URL), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"event":"die","payload":{}}`)); err != nil {
		t.Fatal(err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	for i := 0; i < 50; i++ {
		_, _, err := conn.ReadMessage()
		if err != nil {
			// Expected: connection closed after panic + disconnect
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("expected connection to close after panicking handler")
}
