// Copyright (c) 2026 Dibyaranjan Pradhan. All rights reserved.

package internal

import (
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/dibyaranjan-pradhan/go-socket/internal/engineio"
)

func testSession() *engineio.Session {
	return &engineio.Session{
		ID:           "sid-test",
		PingInterval: 25 * time.Millisecond,
		PingTimeout:  100 * time.Millisecond,
		MaxPayload:   1 << 20,
	}
}

// TestPollingTransportWriteRead shows message payloads round-trip through the
// Engine.IO framing layer: Write wraps JSON, handleInbound unwraps into Read.
func TestPollingTransportWriteRead(t *testing.T) {
	pt := newPollingTransport(testSession())
	defer pt.Close()

	payload := []byte(`{"event":"ping","payload":{}}`)
	if err := pt.Write(payload); err != nil {
		t.Fatal(err)
	}
	body, err := pt.poll(50 * time.Millisecond)
	if err != nil || len(body) == 0 {
		t.Fatalf("poll: body=%q err=%v", body, err)
	}
	packets, err := engineio.DecodeBatch(body)
	if err != nil {
		t.Fatal(err)
	}
	if err := pt.handleInbound(packets); err != nil {
		t.Fatal(err)
	}

	_ = pt.SetReadDeadline(time.Now().Add(time.Second))
	got, err := pt.Read()
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Fatalf("got %q want %q", got, payload)
	}
}

func TestPollingTransportPingPong(t *testing.T) {
	pt := newPollingTransport(testSession())
	defer pt.Close()

	ponged := false
	pt.OnPong(func() { ponged = true })

	pingWire, _ := engineio.Encode(engineio.Packet{Type: engineio.Ping})
	if err := pt.handleInbound([]engineio.Packet{{Type: engineio.Ping}}); err != nil {
		t.Fatal(err)
	}
	_ = pingWire

	body, err := pt.poll(10 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	pkts, _ := engineio.DecodeBatch(body)
	if len(pkts) == 0 || pkts[0].Type != engineio.Pong {
		t.Fatalf("expected pong response, got %+v", pkts)
	}

	pongWire, _ := engineio.Encode(engineio.Packet{Type: engineio.Pong})
	_ = pt.handleInbound([]engineio.Packet{{Type: engineio.Pong}})
	_ = pongWire
	if !ponged {
		t.Fatal("OnPong not called")
	}
}

func TestPollingTransportClose(t *testing.T) {
	pt := newPollingTransport(testSession())
	closed := false
	pt.onClose = func() { closed = true }
	if err := pt.handleInbound([]engineio.Packet{{Type: engineio.Close}}); err != io.EOF {
		t.Fatalf("err = %v", err)
	}
	if !closed {
		t.Fatal("onClose not called")
	}
}

func TestPollingTransportReadTimeout(t *testing.T) {
	pt := newPollingTransport(testSession())
	defer pt.Close()
	_ = pt.SetReadDeadline(time.Now().Add(5 * time.Millisecond))
	_, err := pt.Read()
	if err == nil {
		t.Fatal("expected timeout")
	}
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("expected timeout error, got %v", err)
	}
}

func TestPollingTransportOutboxFull(t *testing.T) {
	pt := newPollingTransport(testSession())
	pt.outbox = make(chan []byte, 1)
	pt.outbox <- []byte("blocked")
	if err := pt.enqueueOutbound([]byte("x")); err == nil {
		t.Fatal("expected outbox full error")
	}
}

func TestPollingTransportSendCloseClosed(t *testing.T) {
	pt := newPollingTransport(testSession())
	_ = pt.Close()
	if err := pt.SendClose(); err != nil {
		t.Fatalf("SendClose on closed: %v", err)
	}
}

func TestPollManagerRemove(t *testing.T) {
	m := newPollManager(time.Second, time.Second, 1000)
	_, pt := m.create()
	sid := pt.sess.ID
	m.remove(sid)
	if m.count() != 0 {
		t.Fatal("expected 0 sessions")
	}
}

func TestPollingTransportPingOutbound(t *testing.T) {
	pt := newPollingTransport(testSession())
	defer pt.Close()
	if err := pt.Ping(); err != nil {
		t.Fatal(err)
	}
	body, err := pt.poll(10 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	pkt, err := engineio.Decode(body)
	if err != nil || pkt.Type != engineio.Ping {
		t.Fatalf("pkt=%+v err=%v", pkt, err)
	}
}

func TestPollTimeoutErrorMethods(t *testing.T) {
	var err error = pollTimeoutError{}
	if err.Error() == "" {
		t.Fatal("expected message")
	}
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatal("expected timeout net.Error")
	}
}

func TestPollingTransportPollSendsPingOnIdle(t *testing.T) {
	pt := newPollingTransport(testSession())
	defer pt.Close()
	body, err := pt.poll(5 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	pkt, err := engineio.Decode(body)
	if err != nil {
		t.Fatal(err)
	}
	if pkt.Type != engineio.Ping {
		t.Fatalf("type = %v", pkt.Type)
	}
}

func TestPollingTransportInboxFull(t *testing.T) {
	pt := newPollingTransport(testSession())
	pt.inbox = make(chan []byte, 1)
	pt.inbox <- []byte("blocked")
	if err := pt.deliverInbound([]byte("x")); err == nil {
		t.Fatal("expected inbox full error")
	}
}
