// Copyright (c) 2026 Dibyaranjan Pradhan. All rights reserved.

package gosocket

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/dibyaranjan-pradhan/go-socket/internal"
)

type testLogger func(format string)

func (f testLogger) Printf(format string, _ ...interface{}) { f(format) }

// stubTransport is a controllable internal.Transport: reads are fed by the test and
// writes block until the transport is closed, which models a stalled peer.
type stubTransport struct {
	in           chan []byte
	done         chan struct{}
	once         sync.Once
	timeoutReads bool
}

var _ internal.Transport = (*stubTransport)(nil)

type stubTimeout struct{}

func (stubTimeout) Error() string   { return "stub read timeout" }
func (stubTimeout) Timeout() bool   { return true }
func (stubTimeout) Temporary() bool { return true }

func newStubTransport() *stubTransport {
	return &stubTransport{in: make(chan []byte, 8), done: make(chan struct{})}
}

func (s *stubTransport) feed(msg string) { s.in <- []byte(msg) }
func (s *stubTransport) closed() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

func (s *stubTransport) Name() string                     { return "stub" }
func (s *stubTransport) SetReadLimit(int64)               {}
func (s *stubTransport) SetReadDeadline(time.Time) error  { return nil }
func (s *stubTransport) SetWriteDeadline(time.Time) error { return nil }
func (s *stubTransport) OnPong(func())                    {}
func (s *stubTransport) Ping() error                      { return nil }
func (s *stubTransport) SendClose() error                 { return nil }
func (s *stubTransport) Close() error                     { s.once.Do(func() { close(s.done) }); return nil }

func (s *stubTransport) Read() ([]byte, error) {
	select {
	case m := <-s.in:
		return m, nil
	case <-s.done:
		return nil, io.EOF
	default:
	}
	if s.timeoutReads {
		time.Sleep(10 * time.Millisecond)
		return nil, stubTimeout{}
	}
	select {
	case m := <-s.in:
		return m, nil
	case <-s.done:
		return nil, io.EOF
	}
}

func (s *stubTransport) Write([]byte) error {
	<-s.done
	return io.EOF
}

// TestSocketIOContextSurface drives every Context method on a Socket.IO connection
// so the dialect-aware branches stay covered.
func TestSocketIOContextSurface(t *testing.T) {
	s, ts := sioTestServer(t, Config{})
	s.Use(s.LoggerMiddleware())
	type snap struct {
		wantsAck            bool
		socketID, sessionID string
		ns                  string
		args                int
		authLen             int
		bindErr             error
		getOK               bool
	}
	got := make(chan snap, 1)

	s.On("probe", func(ctx *Context) {
		ctx.Set("k", "v")
		_, ok := ctx.Get("k")
		var bad int
		got <- snap{
			wantsAck: ctx.WantsAck(), socketID: ctx.SocketID(), sessionID: ctx.SessionID(),
			ns: ctx.Namespace().Name(), args: len(ctx.Args()), authLen: len(ctx.Auth()),
			bindErr: ctx.BindAuth(&bad), getOK: ok,
		}
	})
	s.On("rooms", func(ctx *Context) {
		ctx.JoinRoom("r")
		ctx.JoinRoom("")           // invalid: ignored
		ctx.JoinRoom("\x00hidden") // reserved: ignored
		ctx.LeaveRoom("\x00hidden")
		_ = ctx.EmitWith("w", 1, EmitOptions{Policy: BackpressureDrop})
	})
	s.On("fanout", func(ctx *Context) {
		_ = ctx.EmitToRoom("r", "er", 1)
		_ = ctx.EmitToRoomWith("r", "erw", 1, EmitOptions{})
		_ = ctx.BroadcastToRoom("r", "br", 1)
		_ = ctx.BroadcastToRoomWith("r", "brw", 1, EmitOptions{})
		_ = ctx.In("r").Emit("in", 1)
		_ = ctx.Broadcast().Emit("bc", 1)
	})
	s.On("leave", func(ctx *Context) { ctx.LeaveRoom("r") })

	a, b := dialSIO(t, ts), dialSIO(t, ts)
	sid := a.connect("", `{"token":"abc"}`)
	b.connect("", "")

	a.send(`27["probe",{"p":1},2]`)
	r := <-got
	if !r.wantsAck || r.socketID != sid || r.sessionID != "" || r.ns != "/" || r.args != 2 || r.authLen == 0 || !r.getOK {
		t.Fatalf("snapshot = %+v", r)
	}
	if r.bindErr == nil {
		t.Fatal("BindAuth into an incompatible type must error")
	}

	a.send(`2["rooms"]`)
	a.expect(`2["w",1]`)
	b.send(`2["rooms"]`)
	b.expect(`2["w",1]`)
	waitFor(t, "both joined", func() bool { return s.RoomSize("r") == 2 })

	a.send(`2["fanout"]`)
	for _, ev := range []string{"er", "erw"} {
		a.expect(`2["` + ev + `",1]`)
		b.expect(`2["` + ev + `",1]`)
	}
	for _, ev := range []string{"br", "brw", "in", "bc"} {
		b.expect(`2["` + ev + `",1]`) // sender is excluded, peer is not
	}
	a.expectSilence()

	a.send(`2["leave"]`)
	waitFor(t, "left", func() bool { return s.RoomSize("r") == 1 })
}

func TestContextBindAuthWithoutAuthIsNoop(t *testing.T) {
	s := New(Config{})
	ctx := &Context{server: s}
	var v struct{}
	if ctx.Auth() != nil || ctx.BindAuth(&v) != nil {
		t.Fatal("legacy context has no auth")
	}
	if ctx.SocketID() != "" || ctx.Namespace() != s.defaultNS {
		t.Fatal("legacy context defaults wrong")
	}
	sk := &ioSocket{}
	ctx.sock = sk
	if err := ctx.BindAuth(&v); err != nil {
		t.Fatalf("empty auth must be a no-op: %v", err)
	}
}

func TestConnectErrorErrorString(t *testing.T) {
	var err error = &ConnectError{Message: "nope"}
	if err.Error() != "nope" || errorMessage(errors.New("x")) != "rejected" || errorData(errors.New("x")) != nil {
		t.Fatal("ConnectError helpers wrong")
	}
	if errorMessage(&ConnectError{}) != "rejected" {
		t.Fatal("empty ConnectError message must fall back to the generic text")
	}
}

func TestRecoverMarkerIsInertMiddleware(t *testing.T) {
	if err := Recover().RunMiddleware(nil); err != nil {
		t.Fatal(err)
	}
	if isRecoverMiddleware(nil) {
		t.Fatal("nil is not a recover marker")
	}
}

func TestNamespaceBroadcastBuilderIsImmutable(t *testing.T) {
	s := New(Config{})
	ns := s.Of("/x")
	base := ns.To("a")
	derived := base.In("b").Except("c", "\x00bad", "")
	if len(base.rooms) != 1 || len(derived.rooms) != 2 || len(derived.except) != 1 {
		t.Fatalf("base=%v derived=%v/%v", base.rooms, derived.rooms, derived.except)
	}
	if err := ns.Broadcast().Emit("e", 1); err != nil {
		t.Fatal(err)
	}
	// Default namespace with no Socket.IO mount: legacy frame only; encoding errors surface.
	if err := s.Of("/").Emit("e", make(chan int)); err == nil {
		t.Fatal("expected encode error")
	}
	// Non-default namespace with nothing mounted has no recipients and no frames.
	if err := ns.Emit("e", 1); err != nil {
		t.Fatal(err)
	}
}

func TestLegacyEmitRoomRejectsReservedNames(t *testing.T) {
	s := New(Config{})
	if err := s.emitRoom("\x00m\x00/", "e", 1, "", nil, EmitOptions{}); err != nil {
		t.Fatal(err)
	}
}

func TestAttachRejectsWhenClosedOrFull(t *testing.T) {
	s := New(Config{MaxConnections: 1})
	s.conns.Store(1)
	for name, attach := range map[string]func(*stubTransport){
		"socketio": func(tr *stubTransport) { s.attachSocketIO(tr, nil) },
		"poll":     func(tr *stubTransport) { s.attachPollTransport(tr, nil) },
	} {
		tr := newStubTransport()
		attach(tr)
		if !tr.closed() {
			t.Fatalf("%s: transport not closed at connection limit", name)
		}
	}
	s.conns.Store(0)
	s.closed.Store(true)
	for name, attach := range map[string]func(*stubTransport){
		"socketio": func(tr *stubTransport) { s.attachSocketIO(tr, nil) },
		"poll":     func(tr *stubTransport) { s.attachPollTransport(tr, nil) },
	} {
		tr := newStubTransport()
		attach(tr)
		if !tr.closed() {
			t.Fatalf("%s: transport not closed on a closed server", name)
		}
	}
}

// TestSocketIOSlowConsumerIsDropped fills a tiny send queue behind a blocked writer.
func TestSocketIOSlowConsumerIsDropped(t *testing.T) {
	var logs sync.Map
	s := New(Config{
		WriteBufferSize: 1, PingInterval: time.Hour, PongWait: 2 * time.Hour,
		Logger: testLogger(func(format string) { logs.Store(format, true) }),
	})
	t.Cleanup(func() { _ = s.Shutdown(t.Context()) })
	tr := newStubTransport()
	s.OnConnect(func(ctx *Context) {
		// Paced so the writer has taken frame 1 (and blocks), frame 2 fills the queue and
		// frame 3 trips the drop; the loop stops once the peer is closed so no send races the close.
		for i := 0; i < 10 && !tr.closed(); i++ {
			_ = ctx.Emit("flood", i)
			time.Sleep(30 * time.Millisecond)
		}
	})
	tr.feed("0")
	s.attachSocketIO(tr, httptest.NewRequest(http.MethodGet, "/", nil))
	waitFor(t, "slow consumer dropped", tr.closed)
	if _, ok := logs.Load("gosocket: [WARN] emit queue full: clientID=%s"); !ok {
		t.Fatal("queue-full warning not logged")
	}
}

func TestSocketIOHeartbeatTimeoutHook(t *testing.T) {
	s := New(Config{PingInterval: 20 * time.Millisecond, PongWait: 60 * time.Millisecond})
	t.Cleanup(func() { _ = s.Shutdown(t.Context()) })
	fired := make(chan struct{}, 1)
	s.OnHeartbeatTimeout(func(*Context) {
		select {
		case fired <- struct{}{}:
		default:
		}
	})
	tr := newStubTransport()
	tr.timeoutReads = true
	s.attachSocketIO(tr, httptest.NewRequest(http.MethodGet, "/", nil))
	select {
	case <-fired:
	case <-time.After(3 * time.Second):
		t.Fatal("heartbeat timeout hook did not fire for a silent Socket.IO peer")
	}
}

func TestSocketIORejectsInvalidAckPayload(t *testing.T) {
	s, ts := sioTestServer(t, Config{})
	called := make(chan error, 1)
	s.OnConnect(func(ctx *Context) {
		_ = ctx.EmitWithAck("q", nil, time.Minute, func(_ []json.RawMessage, err error) { called <- err })
	})
	c := dialSIO(t, ts)
	c.connect("", "")
	c.expect(`21["q"]`)
	c.send(`31{not json`)
	c.send(`3`) // no ack id
	select {
	case err := <-called:
		t.Fatalf("callback must not fire on a malformed ack: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	c.send(`31[]`) // empty args are a valid ack
	if err := <-called; err != nil {
		t.Fatalf("err = %v", err)
	}
}

func TestSocketIOAckOnPlainCallbackPanicIsContained(t *testing.T) {
	s, ts := sioTestServer(t, Config{})
	s.OnConnect(func(ctx *Context) {
		_ = ctx.EmitWithAck("q", nil, time.Minute, func([]json.RawMessage, error) { panic("cb boom") })
	})
	ok := make(chan struct{}, 1)
	s.On("alive", func(ctx *Context) { ok <- struct{}{} })
	c := dialSIO(t, ts)
	c.connect("", "")
	c.expect(`21["q"]`)
	c.send(`31["x"]`)
	c.send(`2["alive"]`)
	select {
	case <-ok:
	case <-time.After(2 * time.Second):
		t.Fatal("connection died after callback panic")
	}
}

func TestSocketIODuplicateConnectIsIgnored(t *testing.T) {
	_, ts := sioTestServer(t, Config{})
	c := dialSIO(t, ts)
	c.connect("", "")
	c.send(`0`)
	c.expectSilence()
}

func TestSocketIOEmitAfterTeardownFailsAck(t *testing.T) {
	s, ts := sioTestServer(t, Config{})
	held := make(chan *Context, 1)
	s.OnConnect(func(ctx *Context) { held <- ctx })
	c := dialSIO(t, ts)
	c.connect("", "")
	ctx := <-held
	c.send(`1`)
	waitFor(t, "torn down", func() bool {
		return errors.Is(ctx.EmitWithAck("x", nil, time.Second, func([]json.RawMessage, error) {}), ErrAckDisconnected)
	})
	ctx.JoinRoom("late") // must not resurrect membership
	if s.RoomSize("late") != 0 {
		t.Fatal("a closed socket joined a room")
	}
}
