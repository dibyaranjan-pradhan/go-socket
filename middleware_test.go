package gosocket

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dibyaranjan-pradhan/go-socket/internal"
)

func TestRateLimiterRejectsFastEvents(t *testing.T) {
	s := New(Config{})
	mw := RateLimiter(200 * time.Millisecond)
	c := internal.NewClient(s.hub, &rateFakeTransport{}, internal.ClientOptions{ID: "x", SendBuffer: 2})
	ctx := s.newContext(c, httptest.NewRequest("GET", "/", nil), "fast", nil, "", "")
	if err := mw.RunMiddleware(ctx); err != nil {
		t.Fatal(err)
	}
	if err := mw.RunMiddleware(ctx); err == nil {
		t.Fatal("expected rate limit rejection")
	}
}

func TestRateLimiterSkipsConnectDisconnect(t *testing.T) {
	mw := RateLimiter(time.Second)
	if err := mw.RunMiddleware(&Context{event: EventConnect}); err != nil {
		t.Fatal(err)
	}
	if err := mw.RunMiddleware(&Context{event: EventDisconnect}); err != nil {
		t.Fatal(err)
	}
}

type rateFakeTransport struct{}

func (rateFakeTransport) Name() string                     { return "f" }
func (rateFakeTransport) SetReadLimit(int64)               {}
func (rateFakeTransport) SetReadDeadline(time.Time) error  { return nil }
func (rateFakeTransport) SetWriteDeadline(time.Time) error { return nil }
func (rateFakeTransport) OnPong(func())                    {}
func (rateFakeTransport) Read() ([]byte, error)            { return nil, nil }
func (rateFakeTransport) Write([]byte) error               { return nil }
func (rateFakeTransport) Ping() error                      { return nil }
func (rateFakeTransport) SendClose() error                 { return nil }
func (rateFakeTransport) Close() error                     { return nil }
