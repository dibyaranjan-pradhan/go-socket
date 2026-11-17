// Copyright (c) 2026 Dibyaranjan Pradhan. All rights reserved.

package gosocket

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/dibyaranjan-pradhan/go-socket/internal"
	"github.com/dibyaranjan-pradhan/go-socket/internal/engineio"
)

// sioClient is a minimal Socket.IO v5 client over a direct Engine.IO WebSocket. It
// speaks raw wire strings so tests double as protocol-conformance checks without Node.
type sioClient struct {
	t    *testing.T
	conn *websocket.Conn
	wmu  sync.Mutex
	in   chan string
}

func sioTestServer(t *testing.T, cfg Config) (*Server, *httptest.Server) {
	t.Helper()
	if cfg.PingInterval == 0 {
		cfg.PingInterval = time.Hour
		cfg.PongWait = 2 * time.Hour
	}
	s := New(cfg)
	mux := http.NewServeMux()
	mux.Handle("/socket.io/", s.SocketIOHandler())
	mux.Handle("/ws", s.Handler())
	ts := httptest.NewServer(mux)
	t.Cleanup(func() {
		ts.Close()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = s.Shutdown(ctx)
	})
	return s, ts
}

func dialSIO(t *testing.T, ts *httptest.Server) *sioClient {
	t.Helper()
	conn, _, err := websocket.DefaultDialer.Dial(wsURL(ts.URL)+"/socket.io/?EIO=4&transport=websocket", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	c := &sioClient{t: t, conn: conn, in: make(chan string, 256)}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, open, err := conn.ReadMessage()
	if err != nil || len(open) == 0 || open[0] != '0' {
		t.Fatalf("open packet = %q err=%v", open, err)
	}
	_ = conn.SetReadDeadline(time.Time{})
	// A gorilla read error is sticky, so a pump goroutine owns reads and tests poll a channel.
	go func() {
		defer close(c.in)
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			switch {
			case string(msg) == "2":
				_ = c.write("3")
			case len(msg) > 0 && msg[0] == '4':
				c.in <- string(msg[1:])
			}
		}
	}()
	return c
}

func (c *sioClient) write(raw string) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return c.conn.WriteMessage(websocket.TextMessage, []byte(raw))
}

// send writes a Socket.IO packet wrapped in an Engine.IO message ("4").
func (c *sioClient) send(packet string) {
	c.t.Helper()
	if err := c.write("4" + packet); err != nil {
		c.t.Fatalf("send %q: %v", packet, err)
	}
}

// recv returns the next Socket.IO packet, answering Engine.IO pings on the way.
func (c *sioClient) recv() string {
	c.t.Helper()
	got, err := c.tryRecv(2 * time.Second)
	if err != nil {
		c.t.Fatalf("recv: %v", err)
	}
	return got
}

func (c *sioClient) tryRecv(d time.Duration) (string, error) {
	select {
	case m, ok := <-c.in:
		if !ok {
			return "", io.EOF
		}
		return m, nil
	case <-time.After(d):
		return "", errors.New("timeout")
	}
}

func (c *sioClient) expect(want string) {
	c.t.Helper()
	if got := c.recv(); got != want {
		c.t.Fatalf("got %q, want %q", got, want)
	}
}

func (c *sioClient) expectPrefix(prefix string) string {
	c.t.Helper()
	got := c.recv()
	if !strings.HasPrefix(got, prefix) {
		c.t.Fatalf("got %q, want prefix %q", got, prefix)
	}
	return got
}

// connect joins a namespace ("" = default) and returns its socket id.
func (c *sioClient) connect(ns, auth string) string {
	c.t.Helper()
	pkt := "0"
	if ns != "" {
		pkt += ns + ","
	}
	c.send(pkt + auth)
	prefix := "0"
	if ns != "" {
		prefix += ns + ","
	}
	got := c.expectPrefix(prefix + `{"sid":"`)
	var body struct {
		SID string `json:"sid"`
	}
	if err := json.Unmarshal([]byte(got[len(prefix):]), &body); err != nil {
		c.t.Fatal(err)
	}
	return body.SID
}

func (c *sioClient) expectClosed() {
	c.t.Helper()
	got, err := c.tryRecv(2 * time.Second)
	if err == nil || err.Error() == "timeout" {
		c.t.Fatalf("expected close, got %q err=%v", got, err)
	}
}

func (c *sioClient) expectSilence() {
	c.t.Helper()
	if got, err := c.tryRecv(150 * time.Millisecond); err == nil {
		c.t.Fatalf("unexpected packet %q", got)
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

func TestSocketIOConnectEventAndAck(t *testing.T) {
	s, ts := sioTestServer(t, Config{})
	s.On("hello", func(ctx *Context) {
		var name string
		_ = ctx.Bind(&name)
		_ = ctx.Emit("greeted", name)
		if err := ctx.Ack("hi " + name); err != nil {
			t.Errorf("ack: %v", err)
		}
	})
	c := dialSIO(t, ts)
	c.connect("", "")
	c.send(`212["hello","bob"]`)
	c.expect(`2["greeted","bob"]`)
	c.expect(`312["hi bob"]`)
}

func TestSocketIONamespacesAuthAndErrors(t *testing.T) {
	s, ts := sioTestServer(t, Config{})
	admin := s.Of("/admin")
	admin.Use(MiddlewareFunc(func(ctx *Context) error {
		if ctx.Event() != EventConnect {
			return nil
		}
		var a struct {
			Token string `json:"token"`
		}
		_ = ctx.BindAuth(&a)
		switch a.Token {
		case "good":
			return nil
		case "boom":
			return errors.New("secret db password leaked")
		}
		return &ConnectError{Message: "forbidden", Data: map[string]int{"code": 7}}
	}))
	admin.On("ping", func(ctx *Context) { _ = ctx.Emit("pong", ctx.Namespace().Name()) })

	c := dialSIO(t, ts)
	c.send(`0/admin,{"token":"bad"}`)
	c.expect(`4/admin,{"message":"forbidden","data":{"code":7}}`)
	c.send(`0/admin,{"token":"boom"}`)
	c.expect(`4/admin,{"message":"rejected"}`)
	c.send(`0/nope,`)
	c.expect(`4/nope,{"message":"Invalid namespace"}`)

	c.connect("/admin", `{"token":"good"}`)
	c.send(`2/admin,["ping"]`)
	c.expect(`2/admin,["pong","/admin"]`)

	// Not connected to "/" yet: its events must be dropped, not routed to admin.
	s.On("secret", func(*Context) { t.Error("event ran without CONNECT") })
	c.send(`2["secret"]`)
	c.expectSilence()
}

func TestSocketIOGlobalMiddlewareAppliesToNamespaces(t *testing.T) {
	s, ts := sioTestServer(t, Config{})
	s.Use(MiddlewareFunc(func(ctx *Context) error {
		if ctx.Event() == EventConnect {
			return &ConnectError{Message: "global deny"}
		}
		return nil
	}))
	s.Of("/chat")
	c := dialSIO(t, ts)
	c.send(`0/chat,`)
	c.expect(`4/chat,{"message":"global deny"}`)
}

func TestSocketIOServerToClientAck(t *testing.T) {
	s, ts := sioTestServer(t, Config{})
	got := make(chan []json.RawMessage, 1)
	s.OnConnect(func(ctx *Context) {
		err := ctx.EmitWithAck("ask", 1, time.Second, func(args []json.RawMessage, err error) {
			if err != nil {
				t.Errorf("ack err: %v", err)
			}
			got <- args
		})
		if err != nil {
			t.Errorf("emit: %v", err)
		}
	})
	c := dialSIO(t, ts)
	c.connect("", "")
	c.expect(`21["ask",1]`)
	c.send(`31["yes",2]`)
	select {
	case args := <-got:
		if len(args) != 2 || string(args[0]) != `"yes"` || string(args[1]) != `2` {
			t.Fatalf("args = %s", args)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ack callback not called")
	}
	// A duplicate reply must be ignored, not panic or re-fire.
	c.send(`31["again"]`)
	c.expectSilence()
}

func TestSocketIOAckTimeoutAndLateReply(t *testing.T) {
	s, ts := sioTestServer(t, Config{})
	errs := make(chan error, 2)
	s.OnConnect(func(ctx *Context) {
		_ = ctx.EmitWithAck("ask", nil, 40*time.Millisecond, func(_ []json.RawMessage, err error) { errs <- err })
	})
	c := dialSIO(t, ts)
	c.connect("", "")
	c.expect(`21["ask"]`)
	if err := <-errs; !errors.Is(err, ErrAckTimeout) {
		t.Fatalf("err = %v", err)
	}
	c.send(`31["late"]`)
	select {
	case err := <-errs:
		t.Fatalf("late reply re-fired callback: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestSocketIOAckFailedOnDisconnect(t *testing.T) {
	s, ts := sioTestServer(t, Config{})
	errs := make(chan error, 1)
	s.OnConnect(func(ctx *Context) {
		_ = ctx.EmitWithAck("ask", nil, time.Minute, func(_ []json.RawMessage, err error) { errs <- err })
	})
	c := dialSIO(t, ts)
	c.connect("", "")
	c.expect(`21["ask"]`)
	_ = c.conn.Close()
	select {
	case err := <-errs:
		if !errors.Is(err, ErrAckDisconnected) {
			t.Fatalf("err = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("callback not failed on disconnect")
	}
}

func TestSocketIOPendingAckCap(t *testing.T) {
	s, ts := sioTestServer(t, Config{SocketIO: SocketIOConfig{MaxPendingAcks: 1}})
	res := make(chan [2]error, 1)
	s.OnConnect(func(ctx *Context) {
		cb := func([]json.RawMessage, error) {}
		res <- [2]error{
			ctx.EmitWithAck("a", nil, time.Minute, cb),
			ctx.EmitWithAck("b", nil, time.Minute, cb),
		}
		if err := ctx.EmitWithAck("c", nil, time.Minute, nil); !errors.Is(err, ErrNilAckCallback) {
			t.Errorf("nil cb err = %v", err)
		}
	})
	c := dialSIO(t, ts)
	c.connect("", "")
	r := <-res
	if r[0] != nil || !errors.Is(r[1], ErrTooManyPendingAcks) {
		t.Fatalf("errs = %v", r)
	}
}

func TestSocketIOAckMisuse(t *testing.T) {
	s, ts := sioTestServer(t, Config{})
	res := make(chan []error, 1)
	s.On("noack", func(ctx *Context) {
		res <- []error{ctx.Ack("x"), nil}
	})
	s.On("twice", func(ctx *Context) {
		res <- []error{ctx.Ack("a"), ctx.Ack("b")}
	})
	c := dialSIO(t, ts)
	c.connect("", "")
	c.send(`2["noack"]`)
	if r := <-res; !errors.Is(r[0], ErrNoAckRequested) {
		t.Fatalf("noack = %v", r)
	}
	c.send(`25["twice"]`)
	if r := <-res; r[0] != nil || !errors.Is(r[1], ErrAckAlreadySent) {
		t.Fatalf("twice = %v", r)
	}
	c.expect(`35["a"]`)

	legacy := &Context{server: s}
	if !errors.Is(legacy.Ack(), ErrNotSocketIO) || !errors.Is(legacy.EmitWithAck("x", nil, 0, func([]json.RawMessage, error) {}), ErrNotSocketIO) {
		t.Fatal("legacy context must reject Socket.IO-only calls")
	}
}

func TestSocketIOBroadcastAlgebra(t *testing.T) {
	s, ts := sioTestServer(t, Config{})
	s.On("join", func(ctx *Context) {
		var room string
		_ = ctx.Bind(&room)
		ctx.JoinRoom(room)
	})
	s.On("shout", func(ctx *Context) { _ = ctx.To("r1").Emit("shout", "x") })

	a, b, c := dialSIO(t, ts), dialSIO(t, ts), dialSIO(t, ts)
	sidA, sidB, sidC := a.connect("", ""), b.connect("", ""), c.connect("", "")
	a.send(`2["join","r1"]`)
	b.send(`2["join","r1"]`)
	c.send(`2["join","r2"]`)
	waitFor(t, "rooms", func() bool { return s.RoomSize("r1") == 2 && s.RoomSize("r2") == 1 })
	ns := s.Of("/")

	// Union of rooms.
	if err := ns.In("r1", "r2").Emit("u", 1); err != nil {
		t.Fatal(err)
	}
	for _, cl := range []*sioClient{a, b, c} {
		cl.expect(`2["u",1]`)
	}

	// Except a room and Except a socket id.
	_ = ns.To("r1", "r2").Except("r2").Emit("e1", 1)
	a.expect(`2["e1",1]`)
	b.expect(`2["e1",1]`)
	_ = ns.To("r1").Except(sidA).Emit("e2", 1)
	b.expect(`2["e2",1]`)

	// Whole namespace, minus one socket.
	_ = ns.Except(sidC).Emit("e3", 1)
	a.expect(`2["e3",1]`)
	b.expect(`2["e3",1]`)
	_ = ns.Emit("all", 1)
	for _, cl := range []*sioClient{a, b, c} {
		cl.expect(`2["all",1]`)
	}

	// Sender-relative: ctx.To skips the caller.
	a.send(`2["shout"]`)
	b.expect(`2["shout","x"]`)
	a.expectSilence()
	c.expectSilence()
	_ = sidB
}

func TestSocketIONamespaceRoomIsolation(t *testing.T) {
	s, ts := sioTestServer(t, Config{})
	for _, name := range []string{"/a", "/b"} {
		s.Of(name).On("join", func(ctx *Context) { ctx.JoinRoom("room") })
	}
	c := dialSIO(t, ts)
	c.connect("/a", "")
	c.connect("/b", "")
	c.send(`2/a,["join"]`)
	waitFor(t, "join", func() bool { return s.Of("/a").RoomSize("room") == 1 })

	_ = s.Of("/b").To("room").Emit("x", 1)
	c.expectSilence()
	_ = s.Of("/a").To("room").Emit("x", 1)
	c.expect(`2/a,["x",1]`)
	_ = s.Of("/b").Emit("y", 1)
	c.expect(`2/b,["y",1]`)
}

func TestSocketIOMixedLegacyAndSocketIOShareRooms(t *testing.T) {
	s, ts := sioTestServer(t, Config{})
	s.On("join", func(ctx *Context) { ctx.JoinRoom("lobby") })
	s.On("say", func(ctx *Context) { _ = ctx.BroadcastToRoom("lobby", "said", "hi") })

	leg, _, err := websocket.DefaultDialer.Dial(wsURL(ts.URL)+"/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = leg.Close() }()
	sio := dialSIO(t, ts)
	sio.connect("", "")
	_ = leg.WriteJSON(map[string]interface{}{"event": "join", "payload": nil})
	sio.send(`2["join"]`)
	waitFor(t, "both joined", func() bool { return s.RoomSize("lobby") == 2 })

	// Legacy sender reaches the Socket.IO peer in Socket.IO framing.
	_ = leg.WriteJSON(map[string]interface{}{"event": "say", "payload": nil})
	sio.expect(`2["said","hi"]`)

	// Socket.IO sender reaches the legacy peer in legacy framing.
	sio.send(`2["say"]`)
	_ = leg.SetReadDeadline(time.Now().Add(2 * time.Second))
	var msg ServerWireMessage
	if err := leg.ReadJSON(&msg); err != nil || msg.Event != "said" {
		t.Fatalf("legacy got %+v err=%v", msg, err)
	}

	// Whole-default-namespace broadcast includes legacy clients that never joined a room.
	_ = s.Of("/").Emit("all", 1)
	sio.expect(`2["all",1]`)
	if err := leg.ReadJSON(&msg); err != nil || msg.Event != "all" {
		t.Fatalf("legacy broadcast got %+v err=%v", msg, err)
	}
}

func TestSocketIOHiddenRoomsStayHidden(t *testing.T) {
	s, ts := sioTestServer(t, Config{})
	s.On("join", func(ctx *Context) {
		ctx.JoinRoom("visible")
		ctx.JoinRoom(internal.HiddenRoomPrefix + "s" + internal.HiddenRoomPrefix + "target")
	})
	c := dialSIO(t, ts)
	c.connect("", "")
	c.send(`2["join"]`)
	waitFor(t, "join", func() bool { return s.RoomSize("visible") == 1 })
	rooms := s.ListAllRooms()
	if len(rooms) != 1 || rooms[0].RoomID != "visible" {
		t.Fatalf("rooms = %+v", rooms)
	}
	if n := s.hub.ClientRoomCount(clientOf(t, s)); n != 1 {
		t.Fatalf("ClientRoomCount = %d", n)
	}
	// A client-influenced room name must not be able to address internal keys.
	if err := s.emitRoom(internal.HiddenRoomPrefix+"m"+internal.HiddenRoomPrefix+"/", "x", 1, "", nil, EmitOptions{}); err != nil {
		t.Fatal(err)
	}
	c.expectSilence()
}

func clientOf(t *testing.T, s *Server) *internal.Client {
	t.Helper()
	var out *internal.Client
	s.clientsByID.Range(func(_, v interface{}) bool {
		out = v.(*internal.Client)
		return false
	})
	if out == nil {
		t.Fatal("no client")
	}
	return out
}

func TestSocketIODisconnectLifecycle(t *testing.T) {
	s, ts := sioTestServer(t, Config{})
	var mu sync.Mutex
	var events []string
	rec := func(tag string) func(*Context) {
		return func(ctx *Context) {
			mu.Lock()
			events = append(events, tag+ctx.Namespace().Name())
			mu.Unlock()
		}
	}
	s.OnConnect(rec("c"))
	s.OnDisconnect(rec("d"))
	chat := s.Of("/chat")
	chat.OnConnect(rec("c"))
	chat.OnDisconnect(rec("d"))
	chat.On("kick", func(ctx *Context) { ctx.Disconnect() })

	c := dialSIO(t, ts)
	c.connect("", "")
	c.connect("/chat", "")
	c.send(`2/chat,["kick"]`)
	c.expect(`1/chat,`)
	c.expectSilence() // "/" still open: the connection survives

	_ = c.conn.Close()
	waitFor(t, "disconnect hooks", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(events) == 4
	})
	mu.Lock()
	defer mu.Unlock()
	got := strings.Join(events, ",")
	if got != "c/,c/chat,d/chat,d/" {
		t.Fatalf("events = %s", got)
	}
}

func TestSocketIOClientDisconnectPacket(t *testing.T) {
	s, ts := sioTestServer(t, Config{})
	done := make(chan struct{})
	s.Of("/chat").OnDisconnect(func(*Context) { close(done) })
	s.Of("/chat").On("join", func(ctx *Context) { ctx.JoinRoom("r") })
	c := dialSIO(t, ts)
	c.connect("/chat", "")
	c.send(`2/chat,["join"]`)
	waitFor(t, "join", func() bool { return s.Of("/chat").RoomSize("r") == 1 })
	c.send(`1/chat,`)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("disconnect hook not fired")
	}
	waitFor(t, "room left", func() bool { return s.Of("/chat").RoomSize("r") == 0 })
}

func TestSocketIOServerDisconnectLastNamespaceClosesConnection(t *testing.T) {
	s, ts := sioTestServer(t, Config{})
	s.On("bye", func(ctx *Context) { ctx.Disconnect() })
	c := dialSIO(t, ts)
	c.connect("", "")
	c.send(`2["bye"]`)
	c.expect(`1`)
	c.expectClosed()
}

func TestSocketIOConnectTimeout(t *testing.T) {
	_, ts := sioTestServer(t, Config{SocketIO: SocketIOConfig{ConnectTimeout: 60 * time.Millisecond}})
	c := dialSIO(t, ts)
	c.expectClosed()
}

func TestSocketIOMalformedPacketClosesConnection(t *testing.T) {
	_, ts := sioTestServer(t, Config{})
	c := dialSIO(t, ts)
	c.send(`9garbage`)
	c.expectClosed()
}

func TestSocketIOIgnoresReservedAndInvalidEvents(t *testing.T) {
	s, ts := sioTestServer(t, Config{})
	for _, ev := range []string{"connect", "disconnect", "connect_error"} {
		s.On(ev, func(*Context) { t.Error("reserved event dispatched") })
	}
	ran := make(chan struct{}, 1)
	s.On("ok", func(*Context) { ran <- struct{}{} })
	c := dialSIO(t, ts)
	c.connect("", "")
	for _, p := range []string{`2["connect"]`, `2["disconnect"]`, `2[]`, `2{}`, `2[1]`, `2[""]`, `3`} {
		c.send(p)
	}
	c.send(`2["ok"]`)
	select {
	case <-ran:
	case <-time.After(2 * time.Second):
		t.Fatal("valid event after garbage was not dispatched")
	}
}

func TestSocketIORecoverMiddleware(t *testing.T) {
	s, ts := sioTestServer(t, Config{})
	s.Use(Recover())
	s.On("panic", func(*Context) { panic("boom") })
	s.Of("/p").Use(MiddlewareFunc(func(ctx *Context) error {
		if ctx.Event() == EventConnect {
			panic("connect boom")
		}
		return nil
	}))
	c := dialSIO(t, ts)
	c.send(`0/p,`)
	c.expect(`4/p,{"message":"rejected"}`)
	c.connect("", "")
	c.send(`2["panic"]`)
	c.expect(`1`)
	c.expectClosed()
}

func TestSocketIOShutdownWithMessage(t *testing.T) {
	s, ts := sioTestServer(t, Config{})
	c := dialSIO(t, ts)
	c.connect("", "")
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = s.ShutdownWithMessage(ctx, "bye", map[string]int{"n": 1})
	}()
	c.expect(`2["bye",{"n":1}]`)
}

func TestSocketIOStatsAndHooksSeeSocketIOClients(t *testing.T) {
	s, ts := sioTestServer(t, Config{})
	c := dialSIO(t, ts)
	c.connect("", "")
	waitFor(t, "stats", func() bool { return s.GetServerStats().ActiveConnections == 1 })
	_ = c.conn.Close()
	waitFor(t, "stats drop", func() bool { return s.GetServerStats().ActiveConnections == 0 })
}

func TestSocketIOEncodeErrors(t *testing.T) {
	s, ts := sioTestServer(t, Config{})
	bad := make(chan int)
	if err := s.Of("/").Emit("x", bad); err == nil {
		t.Fatal("expected marshal error")
	}
	if err := s.Of("/x").Emit("x", bad); err == nil {
		t.Fatal("expected marshal error")
	}
	s.OnConnect(func(ctx *Context) {
		if err := ctx.Emit("x", bad); err == nil {
			t.Error("expected marshal error from ctx.Emit")
		}
		if err := ctx.EmitWithAck("x", bad, 0, func([]json.RawMessage, error) {}); err == nil {
			t.Error("expected marshal error from EmitWithAck")
		}
		if err := ctx.Ack(); !errors.Is(err, ErrNoAckRequested) {
			t.Errorf("ack err = %v", err)
		}
	})
	c := dialSIO(t, ts)
	c.connect("", "")
	waitFor(t, "connected", func() bool { return s.GetServerStats().ActiveConnections == 1 })
}

func TestOfNamespaceNaming(t *testing.T) {
	s := New(Config{})
	if s.Of("") != s.Of("/") || s.Of("/") != s.defaultNS {
		t.Fatal("empty and slash must map to the default namespace")
	}
	if s.Of("chat") != s.Of("/chat") || s.Of("chat").Name() != "/chat" {
		t.Fatal("leading slash must be normalized")
	}
	for _, bad := range []string{"/a,b", "/a\x00", "/" + strings.Repeat("x", maxNamespaceLen)} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("Of(%q) must panic", bad)
				}
			}()
			s.Of(bad)
		}()
	}
}

func TestDefaultNamespaceSharesServerRegistrations(t *testing.T) {
	s := New(Config{})
	d := s.Of("/")
	d.On("e", func(*Context) {})
	d.Use(MiddlewareFunc(func(*Context) error { return nil }))
	d.Use(Recover())
	d.OnConnect(func(*Context) {})
	d.OnDisconnect(func(*Context) {})
	if s.defaultNS.handler("e") == nil || len(s.middlewares) != 2 || !s.hasRecover ||
		len(s.onConnect) != 1 || len(s.onDisconnect) != 1 {
		t.Fatal("default namespace must delegate to the server registries")
	}
	n := s.Of("/n")
	n.Use(Recover())
	if !n.recovers() || s.Of("/other").recovers() && !s.hasRecover {
		t.Fatal("recover flag scoping wrong")
	}
	if n.RoomSize("") != 0 {
		t.Fatal("empty room size must be 0")
	}
}

func TestSocketIOOverPolling(t *testing.T) {
	s, ts := sioTestServer(t, Config{})
	s.On("hello", func(ctx *Context) { _ = ctx.Ack("pong") })
	base := ts.URL + "/socket.io/?EIO=4&transport=polling"

	resp, err := http.Get(base)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	pkt, err := engineio.Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	var hs struct {
		SID string `json:"sid"`
	}
	_ = json.Unmarshal(pkt.Data, &hs)
	url := base + "&sid=" + hs.SID
	time.Sleep(30 * time.Millisecond)

	post := func(body string) {
		r, err := http.Post(url, "text/plain", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		_ = r.Body.Close()
	}
	poll := func() []string {
		r, err := http.Get(url)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = r.Body.Close() }()
		b, _ := io.ReadAll(r.Body)
		pkts, err := engineio.DecodeBatch(b)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, p := range pkts {
			if p.Type == engineio.Message {
				out = append(out, string(p.Data))
			}
		}
		return out
	}

	post("40")
	got := poll()
	if len(got) != 1 || !strings.HasPrefix(got[0], `0{"sid":"`) {
		t.Fatalf("connect reply = %v", got)
	}
	post(`42` + "7" + `["hello"]`)
	got = poll()
	if len(got) != 1 || got[0] != `37["pong"]` {
		t.Fatalf("ack reply = %v", got)
	}
}

func TestSocketIOHandlerIsSingletonAndFlagsMounted(t *testing.T) {
	s := New(Config{})
	if s.sioMounted.Load() {
		t.Fatal("must not be mounted before SocketIOHandler")
	}
	if s.SocketIOHandler() != s.SocketIOHandler() || !s.sioMounted.Load() {
		t.Fatal("SocketIOHandler must be idempotent and set the mounted flag")
	}
}

func TestSocketIOServerClosedAndConnLimit(t *testing.T) {
	s, ts := sioTestServer(t, Config{MaxConnections: 1})
	a := dialSIO(t, ts)
	a.connect("", "")
	waitFor(t, "slot taken", func() bool { return s.conns.Load() == 1 })

	// The handshake succeeds (Engine.IO open), then the server drops the extra connection.
	b := dialSIO(t, ts)
	b.expectClosed()
}

func TestEngineIODirectWebSocketPathOnLegacyMount(t *testing.T) {
	s := New(Config{PingInterval: time.Hour, PongWait: 2 * time.Hour})
	ts := httptest.NewServer(s.EngineIOHandler())
	defer ts.Close()
	conn, _, err := websocket.DefaultDialer.Dial(wsURL(ts.URL)+"/?EIO=4&transport=websocket", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, open, err := conn.ReadMessage()
	if err != nil || !strings.HasPrefix(string(open), `0{"sid":"`) || !strings.Contains(string(open), `"upgrades":[]`) {
		t.Fatalf("open = %q err=%v", open, err)
	}
}

func TestSocketIOManyClientsBroadcast(t *testing.T) {
	s, ts := sioTestServer(t, Config{})
	const n = 25
	clients := make([]*sioClient, n)
	for i := range clients {
		clients[i] = dialSIO(t, ts)
		clients[i].connect("", "")
	}
	_ = s.Of("/").Emit("tick", fmt.Sprint(n))
	for _, c := range clients {
		c.expect(`2["tick","25"]`)
	}
}
