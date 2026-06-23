// Copyright (c) 2026 Dibyaranjan Pradhan. All rights reserved.

package internal

import (
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dibyaranjan-pradhan/go-socket/internal/engineio"
)

// pollingTransport implements Transport for Engine.IO HTTP long-polling. Inbound
// Engine.IO message packets are stripped to their payload before Read returns;
// outbound writes are wrapped in message packets and batched for the next GET.
type pollingTransport struct {
	sess *engineio.Session

	inbox  chan []byte
	outbox chan []byte
	wake   chan struct{}

	onClose func()
	onPong  func()

	mu           sync.Mutex
	readDeadline time.Time
	closed       atomic.Bool
}

func newPollingTransport(sess *engineio.Session) *pollingTransport {
	return &pollingTransport{
		sess:   sess,
		inbox:  make(chan []byte, 64),
		outbox: make(chan []byte, 64),
		wake:   make(chan struct{}, 1),
	}
}

func (t *pollingTransport) Name() string { return engineio.TransportPolling }

func (t *pollingTransport) SetReadLimit(int64) {}

func (t *pollingTransport) SetReadDeadline(tm time.Time) error {
	t.mu.Lock()
	t.readDeadline = tm
	t.mu.Unlock()
	return nil
}

func (t *pollingTransport) SetWriteDeadline(time.Time) error { return nil }

func (t *pollingTransport) OnPong(handler func()) {
	t.mu.Lock()
	t.onPong = handler
	t.mu.Unlock()
}

func (t *pollingTransport) Read() ([]byte, error) {
	if t.closed.Load() {
		return nil, io.EOF
	}
	deadline := t.readDeadline
	if deadline.IsZero() {
		deadline = time.Now().Add(t.sess.PingTimeout)
	}
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()

	select {
	case msg, ok := <-t.inbox:
		if !ok {
			return nil, io.EOF
		}
		return msg, nil
	case <-timer.C:
		return nil, &pollTimeoutError{}
	}
}

func (t *pollingTransport) Write(data []byte) error {
	if t.closed.Load() {
		return io.EOF
	}
	wire, err := engineio.Encode(engineio.Packet{Type: engineio.Message, Data: data})
	if err != nil {
		return err
	}
	return t.enqueueOutbound(wire)
}

func (t *pollingTransport) Ping() error {
	if t.closed.Load() {
		return io.EOF
	}
	wire, err := engineio.Encode(engineio.Packet{Type: engineio.Ping})
	if err != nil {
		return err
	}
	return t.enqueueOutbound(wire)
}

func (t *pollingTransport) SendClose() error {
	if t.closed.Load() {
		return nil
	}
	wire, err := engineio.Encode(engineio.Packet{Type: engineio.Close})
	if err != nil {
		return err
	}
	return t.enqueueOutbound(wire)
}

func (t *pollingTransport) Close() error {
	if t.closed.Swap(true) {
		return nil
	}
	close(t.inbox)
	if t.onClose != nil {
		t.onClose()
	}
	return nil
}

// handleInbound decodes POSTed Engine.IO packets and routes them to pumps or
// heartbeat handlers.
func (t *pollingTransport) handleInbound(packets []engineio.Packet) error {
	for _, p := range packets {
		if err := t.handleOne(p); err != nil {
			return err
		}
	}
	return nil
}

func (t *pollingTransport) handleOne(p engineio.Packet) error {
	switch p.Type {
	case engineio.Ping:
		wire, err := engineio.Encode(engineio.Packet{Type: engineio.Pong, Data: p.Data})
		if err != nil {
			return err
		}
		return t.enqueueOutbound(wire)
	case engineio.Pong:
		t.firePong()
		return nil
	case engineio.Message:
		return t.deliverInbound(p.Data)
	case engineio.Close:
		_ = t.Close()
		return io.EOF
	default:
		return nil
	}
}

func (t *pollingTransport) deliverInbound(data []byte) error {
	if t.closed.Load() {
		return io.EOF
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	select {
	case t.inbox <- cp:
		return nil
	default:
		return errors.New("gosocket: polling inbox full")
	}
}

func (t *pollingTransport) enqueueOutbound(wire []byte) error {
	if t.closed.Load() {
		return io.EOF
	}
	select {
	case t.outbox <- wire:
		t.signalPoll()
		return nil
	default:
		return errors.New("gosocket: polling outbox full")
	}
}

// poll waits up to waitFor for outbound data and returns a batched body. When
// nothing arrives before the deadline, an Engine.IO ping packet is sent so the
// client stays alive — matching Engine.IO long-poll heartbeat behaviour.
func (t *pollingTransport) poll(waitFor time.Duration) ([]byte, error) {
	if t.closed.Load() {
		return nil, io.EOF
	}
	timer := time.NewTimer(waitFor)
	defer timer.Stop()

	select {
	case wire := <-t.outbox:
		return t.collectBatch(wire)
	case <-timer.C:
		ping, err := engineio.Encode(engineio.Packet{Type: engineio.Ping})
		if err != nil {
			return nil, err
		}
		return ping, nil
	case <-t.wake:
		select {
		case wire := <-t.outbox:
			return t.collectBatch(wire)
		default:
			return nil, nil
		}
	}
}

func (t *pollingTransport) collectBatch(first []byte) ([]byte, error) {
	var buf strings.Builder
	buf.Write(first)
	for {
		select {
		case wire := <-t.outbox:
			buf.WriteString(engineio.RecordSeparator)
			buf.Write(wire)
		default:
			return []byte(buf.String()), nil
		}
	}
}

func (t *pollingTransport) signalPoll() {
	select {
	case t.wake <- struct{}{}:
	default:
	}
}

func (t *pollingTransport) firePong() {
	t.mu.Lock()
	fn := t.onPong
	t.mu.Unlock()
	if fn != nil {
		fn()
	}
}

// pollTimeoutError implements net.Error for Read deadline expiry.
type pollTimeoutError struct{}

func (pollTimeoutError) Error() string   { return "gosocket: polling read timeout" }
func (pollTimeoutError) Timeout() bool   { return true }
func (pollTimeoutError) Temporary() bool { return true }
