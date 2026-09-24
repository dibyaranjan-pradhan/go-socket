// Copyright (c) 2026 Dibyaranjan Pradhan. All rights reserved.

package internal

import (
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dibyaranjan-pradhan/go-socket/internal/engineio"
)

// eioWebSocketTransport carries Engine.IO packets over a WebSocket connection after
// a polling→WebSocket upgrade. Message payloads are stripped on read and wrapped on write.
type eioWebSocketTransport struct {
	raw Transport

	mu           sync.Mutex
	readDeadline time.Time
	onPong       func()
	closed       atomic.Bool
}

func newEIOWebSocketTransport(raw Transport) *eioWebSocketTransport {
	return &eioWebSocketTransport{raw: raw}
}

func (t *eioWebSocketTransport) Name() string { return engineio.TransportWebSocket }

func (t *eioWebSocketTransport) SetReadLimit(limit int64) { t.raw.SetReadLimit(limit) }

func (t *eioWebSocketTransport) SetReadDeadline(tm time.Time) error {
	t.mu.Lock()
	t.readDeadline = tm
	t.mu.Unlock()
	return t.raw.SetReadDeadline(tm)
}

func (t *eioWebSocketTransport) SetWriteDeadline(tm time.Time) error {
	return t.raw.SetWriteDeadline(tm)
}

func (t *eioWebSocketTransport) OnPong(handler func()) {
	t.mu.Lock()
	t.onPong = handler
	t.mu.Unlock()
	t.raw.OnPong(handler)
}

func (t *eioWebSocketTransport) Read() ([]byte, error) {
	if t.closed.Load() {
		return nil, io.EOF
	}
	for {
		frame, err := t.raw.Read()
		if err != nil {
			return nil, err
		}
		packets, err := engineio.DecodeBatch(frame)
		if err != nil {
			return nil, err
		}
		for _, p := range packets {
			switch p.Type {
			case engineio.Message:
				return p.Data, nil
			case engineio.Ping:
				if err := t.writePacket(engineio.Packet{Type: engineio.Pong, Data: p.Data}); err != nil {
					return nil, err
				}
			case engineio.Pong:
				t.firePong()
			case engineio.Close:
				_ = t.Close()
				return nil, io.EOF
			}
		}
	}
}

func (t *eioWebSocketTransport) Write(data []byte) error {
	if t.closed.Load() {
		return io.EOF
	}
	return t.writePacket(engineio.Packet{Type: engineio.Message, Data: data})
}

func (t *eioWebSocketTransport) Ping() error {
	if t.closed.Load() {
		return io.EOF
	}
	return t.writePacket(engineio.Packet{Type: engineio.Ping})
}

func (t *eioWebSocketTransport) SendClose() error {
	if t.closed.Load() {
		return nil
	}
	return t.writePacket(engineio.Packet{Type: engineio.Close})
}

func (t *eioWebSocketTransport) Close() error {
	if t.closed.Swap(true) {
		return nil
	}
	return t.raw.Close()
}

func (t *eioWebSocketTransport) writePacket(p engineio.Packet) error {
	wire, err := engineio.Encode(p)
	if err != nil {
		return err
	}
	return t.raw.Write(wire)
}

func (t *eioWebSocketTransport) firePong() {
	t.mu.Lock()
	fn := t.onPong
	t.mu.Unlock()
	if fn != nil {
		fn()
	}
}

// writeEIOBatch joins multiple Engine.IO packets for one WebSocket text frame.
func writeEIOBatch(packets []engineio.Packet) ([]byte, error) {
	if len(packets) == 0 {
		return nil, nil
	}
	var buf strings.Builder
	for i, p := range packets {
		wire, err := engineio.Encode(p)
		if err != nil {
			return nil, err
		}
		if i > 0 {
			buf.WriteString(engineio.RecordSeparator)
		}
		buf.Write(wire)
	}
	return []byte(buf.String()), nil
}
