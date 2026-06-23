package gosocket

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dibyaranjan-pradhan/go-socket/internal"
)

func TestContextBindGetSetRoom(t *testing.T) {
	s := New(Config{})
	ft := &fakeCtxTransport{}
	c := internal.NewClient(s.hub, ft, internal.ClientOptions{ID: "ctx-1", SendBuffer: 2})
	ctx := s.newContext(c, httptest.NewRequest("GET", "/", nil), "evt", []byte(`{"n":1}`), "room-z", "ack-1")

	var v struct{ N int }
	if err := ctx.Bind(&v); err != nil || v.N != 1 {
		t.Fatalf("Bind: %+v err=%v", v, err)
	}
	ctx.Set("user", "alice")
	val, ok := ctx.Get("user")
	if !ok || val != "alice" {
		t.Fatalf("Get: %v %v", val, ok)
	}
	if ctx.Room() != "room-z" || ctx.ClientID() != "ctx-1" || ctx.Event() != "evt" {
		t.Fatalf("accessors mismatch")
	}
	if ctx.Request() == nil {
		t.Fatal("Request nil")
	}
}

func TestContextBindEmptyPayload(t *testing.T) {
	s := New(Config{})
	c := internal.NewClient(s.hub, &fakeCtxTransport{}, internal.ClientOptions{ID: "c", SendBuffer: 2})
	ctx := s.newContext(c, httptest.NewRequest("GET", "/", nil), "e", nil, "", "")
	var v map[string]int
	if err := ctx.Bind(&v); err != nil {
		t.Fatal(err)
	}
}

func TestContextDisconnectNilSafe(t *testing.T) {
	ctx := &Context{}
	ctx.Disconnect() // must not panic
}

func TestContextJoinLeaveBroadcast(t *testing.T) {
	s := New(Config{})
	ft := &fakeCtxTransport{}
	c := internal.NewClient(s.hub, ft, internal.ClientOptions{ID: "c", SendBuffer: 4})
	ctx := s.newContext(c, httptest.NewRequest("GET", "/", nil), "e", nil, "r", "")

	ctx.JoinRoom("r")
	ctx.JoinRoom("") // no-op
	ctx.LeaveRoom("r")
	ctx.LeaveRoom("")

	_ = ctx.EmitToRoom("r", "x", nil)
	_ = ctx.BroadcastToRoom("r", "y", nil)
}

type fakeCtxTransport struct {
	writes chan []byte
}

func (f *fakeCtxTransport) init() {
	if f.writes == nil {
		f.writes = make(chan []byte, 8)
	}
}

func (f *fakeCtxTransport) Name() string                     { return "fake" }
func (f *fakeCtxTransport) SetReadLimit(int64)               {}
func (f *fakeCtxTransport) SetReadDeadline(time.Time) error  { return nil }
func (f *fakeCtxTransport) SetWriteDeadline(time.Time) error { return nil }
func (f *fakeCtxTransport) OnPong(func())                    {}
func (f *fakeCtxTransport) Read() ([]byte, error)            { return nil, nil }
func (f *fakeCtxTransport) Ping() error                      { return nil }
func (f *fakeCtxTransport) SendClose() error                 { return nil }
func (f *fakeCtxTransport) Close() error                     { return nil }

func (f *fakeCtxTransport) Write(data []byte) error {
	f.init()
	cp := make([]byte, len(data))
	copy(cp, data)
	f.writes <- cp
	return nil
}
