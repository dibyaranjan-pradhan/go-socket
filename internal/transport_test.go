package internal

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Compile-time checks: both the real and the fake transport satisfy Transport.
var (
	_ Transport = (*wsTransport)(nil)
	_ Transport = (*fakeTransport)(nil)
)

// fakeTransport is an in-memory Transport used to drive the client pumps in
// tests without a real socket. It doubles as a minimal example of what a
// custom transport implementation looks like.
type fakeTransport struct {
	reads  chan []byte
	writes chan []byte
	pings  atomic.Int32
	closed atomic.Bool
	onPong func()
	mu     sync.Mutex
}

func newFakeTransport() *fakeTransport {
	return &fakeTransport{
		reads:  make(chan []byte, 8),
		writes: make(chan []byte, 8),
	}
}

func (f *fakeTransport) Name() string                     { return "fake" }
func (f *fakeTransport) SetReadLimit(int64)               {}
func (f *fakeTransport) SetReadDeadline(time.Time) error  { return nil }
func (f *fakeTransport) SetWriteDeadline(time.Time) error { return nil }
func (f *fakeTransport) OnPong(handler func())            { f.mu.Lock(); f.onPong = handler; f.mu.Unlock() }

func (f *fakeTransport) Read() ([]byte, error) {
	msg, ok := <-f.reads
	if !ok {
		return nil, io.EOF
	}
	return msg, nil
}

func (f *fakeTransport) Write(data []byte) error {
	cp := make([]byte, len(data))
	copy(cp, data)
	f.writes <- cp
	return nil
}

func (f *fakeTransport) Ping() error      { f.pings.Add(1); return nil }
func (f *fakeTransport) SendClose() error { return nil }
func (f *fakeTransport) Close() error     { f.closed.Store(true); return nil }

// TestNewWSUpgraderDefaultsToOpenOrigin shows that a nil CheckOrigin accepts
// every origin, while a custom one is honored.
func TestNewWSUpgraderDefaultsToOpenOrigin(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)

	open := NewWSUpgrader(0, 0, nil)
	if !open.upgrader.CheckOrigin(req) {
		t.Fatal("nil CheckOrigin should allow any origin")
	}

	denied := NewWSUpgrader(0, 0, func(*http.Request) bool { return false })
	if denied.upgrader.CheckOrigin(req) {
		t.Fatal("custom CheckOrigin should be honored")
	}
}

// TestClientWritePumpWritesThroughTransport shows the outbound path: a queued
// frame travels through WritePump to the transport, and send stats update.
func TestClientWritePumpWritesThroughTransport(t *testing.T) {
	ft := newFakeTransport()
	c := NewClient(NewHub(nil, 0), ft, ClientOptions{
		ID:         "c1",
		WriteWait:  time.Second,
		PingPeriod: time.Hour, // far enough away that no ping fires during the test
	})
	go c.WritePump()
	defer c.Shutdown()

	if !c.TrySend([]byte("hello")) {
		t.Fatal("TrySend returned false on a fresh client")
	}

	select {
	case got := <-ft.writes:
		if string(got) != "hello" {
			t.Fatalf("transport received %q, want %q", got, "hello")
		}
	case <-time.After(time.Second):
		t.Fatal("WritePump did not write to the transport")
	}

	waitFor(t, func() bool { return c.GetStats().MessagesSent == 1 })
	if got := c.GetStats().BytesSent; got != 5 {
		t.Fatalf("BytesSent = %d, want 5", got)
	}
}

// TestClientReadPumpDeliversThroughTransport shows the inbound path: a frame
// from the transport reaches OnMessage and bumps receive stats.
func TestClientReadPumpDeliversThroughTransport(t *testing.T) {
	hub := NewHub(nil, 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	ft := newFakeTransport()
	delivered := make(chan []byte, 1)
	c := NewClient(hub, ft, ClientOptions{
		ID:        "c2",
		PongWait:  time.Second,
		OnMessage: func(data []byte) { delivered <- data },
	})
	hub.Register(c)

	ft.reads <- []byte(`{"event":"ping"}`)
	close(ft.reads) // next read returns EOF, ending ReadPump

	go c.ReadPump()

	select {
	case got := <-delivered:
		if string(got) != `{"event":"ping"}` {
			t.Fatalf("OnMessage got %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("ReadPump did not deliver the message")
	}

	if got := c.GetStats().MessagesReceived; got != 1 {
		t.Fatalf("MessagesReceived = %d, want 1", got)
	}
	if got := c.GetStats().BytesReceived; got != int64(len(`{"event":"ping"}`)) {
		t.Fatalf("BytesReceived = %d, want %d", got, len(`{"event":"ping"}`))
	}
}

// waitFor polls cond for up to a second so stat assertions are not racy.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}
