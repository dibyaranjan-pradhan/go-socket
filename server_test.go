package gosocket

import (
	"context"
	"net/http"
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

func TestGetConnectionStatsAfterConnect(t *testing.T) {
	s := New(Config{})
	defer func() { _ = s.Shutdown(newTestContext()) }()
	var clientID string
	s.OnConnect(func(ctx *Context) {
		clientID = ctx.ClientID()
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
		if clientID != "" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if clientID == "" {
		t.Fatal("expected client ID from OnConnect")
	}

	st := s.GetConnectionStats(clientID)
	if st == nil {
		t.Fatalf("stats should not be nil for connected client")
	}
	if st.ClientID != clientID {
		t.Fatalf("ClientID = %q, want %q", st.ClientID, clientID)
	}
	if st.MessagesReceived != 0 {
		t.Fatalf("MessagesReceived = %d before any inbound events", st.MessagesReceived)
	}

	_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"event":"ping","payload":{}}`)); err != nil {
		t.Fatal(err)
	}

	for time.Now().Before(deadline) {
		st = s.GetConnectionStats(clientID)
		if st != nil && st.MessagesReceived >= 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("expected MessagesReceived to increment after inbound message")
}

func TestHeartbeatTimeoutCallback(t *testing.T) {
	srv := New(Config{})
	defer func() { _ = srv.Shutdown(newTestContext()) }()

	called := false
	srv.OnHeartbeatTimeout(func(ctx *Context) {
		called = true
	})

	if called {
		t.Errorf("callback should not be called immediately")
	}
}

func TestHeartbeatTimeoutFires(t *testing.T) {
	var sawClient string
	s := New(Config{
		PingInterval: 20 * time.Millisecond,
		PongWait:     40 * time.Millisecond,
	})
	defer func() { _ = s.Shutdown(newTestContext()) }()
	s.OnHeartbeatTimeout(func(ctx *Context) {
		sawClient = ctx.ClientID()
	})

	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	conn, _, err := websocket.DefaultDialer.Dial(wsURL(ts.URL), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// Never read: peer does not complete ping/pong cycle toward our server's read deadlines.

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if sawClient != "" {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatal("expected OnHeartbeatTimeout to fire for unread peer")
}

func TestGetServerStatsTotalConnections(t *testing.T) {
	s := New(Config{})
	defer func() { _ = s.Shutdown(newTestContext()) }()
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	conn, _, err := websocket.DefaultDialer.Dial(wsURL(ts.URL), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		st := s.GetServerStats()
		if st != nil && st.TotalConnectionsEver >= 1 && st.ActiveConnections >= 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("expected TotalConnectionsEver and ActiveConnections after dial")
}

func TestRoomIdentitiesAndListAllRooms(t *testing.T) {
	s := New(Config{IdentityKey: "uid"})
	s.OnConnect(func(ctx *Context) {
		ctx.JoinRoom("r1")
		ctx.Set("uid", "user-1")
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
		if s.RoomSize("r1") == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	ids := s.RoomIdentities("r1")
	if len(ids) != 1 || ids[0] != "user-1" {
		t.Fatalf("identities = %v", ids)
	}
	rooms := s.ListAllRooms()
	if len(rooms) != 1 || rooms[0].MemberCount != 1 {
		t.Fatalf("rooms = %+v", rooms)
	}
}

func TestShutdownWithMessage(t *testing.T) {
	s := New(Config{})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	conn, _, err := websocket.DefaultDialer.Dial(wsURL(ts.URL), nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.ShutdownWithMessage(ctx, "bye", map[string]string{"reason": "test"}); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
}

func TestEngineIOOnConnectJoinsRoom(t *testing.T) {
	s := New(Config{})
	s.OnConnect(func(ctx *Context) { ctx.JoinRoom("poll-room") })

	mux := http.NewServeMux()
	mux.Handle("/engine.io/", s.EngineIOHandler())
	ts := httptest.NewServer(mux)
	defer ts.Close()

	sid, err := engineioHandshake(t, ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	_ = sid

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s.RoomSize("poll-room") == 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("polling client did not join room")
}

func TestConnectionLimitRejectsWebSocket(t *testing.T) {
	s := New(Config{MaxConnections: 1})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	c1, _, err := websocket.DefaultDialer.Dial(wsURL(ts.URL), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()

	time.Sleep(30 * time.Millisecond)
	req, _ := http.NewRequest(http.MethodGet, ts.URL, nil)
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Sec-WebSocket-Version", "13")
	req.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestLoggerMiddleware(t *testing.T) {
	s := New(Config{})
	s.Use(s.LoggerMiddleware())
	ctx := s.newContext(nil, httptest.NewRequest("GET", "/", nil), EventConnect, nil, "", "")
	if err := s.runMiddlewares(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestOnDisconnectHandlerRuns(t *testing.T) {
	called := false
	s := New(Config{})
	s.OnDisconnect(func(ctx *Context) {
		called = true
	})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	conn, _, err := websocket.DefaultDialer.Dial(wsURL(ts.URL), nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.WriteMessage(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	_ = conn.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if called {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("OnDisconnect not called")
}

func TestRecoverOnConnectMiddlewarePanic(t *testing.T) {
	s := New(Config{})
	s.Use(Recover())
	s.Use(MiddlewareFunc(func(ctx *Context) error {
		if ctx.Event() == EventConnect {
			panic("connect panic")
		}
		return nil
	}))
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	conn, _, err := websocket.DefaultDialer.Dial(wsURL(ts.URL), nil)
	if err != nil {
		return
	}
	_ = conn.Close()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s.GetServerStats().ActiveConnections == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("panicking connect middleware should not leave active connection")
}

func TestGetConnectionStatsUnknown(t *testing.T) {
	s := New(Config{})
	if s.GetConnectionStats("missing") != nil {
		t.Fatal("expected nil for unknown client")
	}
}

func TestConnectRejectedByMiddleware(t *testing.T) {
	s := New(Config{})
	s.Use(MiddlewareFunc(func(ctx *Context) error { return ErrRejected }))
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	conn, _, err := websocket.DefaultDialer.Dial(wsURL(ts.URL), nil)
	if err != nil {
		return
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	_, _, _ = conn.ReadMessage()
	if s.GetServerStats().ActiveConnections != 0 {
		t.Fatal("rejected connect should not leave active connection")
	}
}
