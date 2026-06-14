// Package engineio implements Engine.IO v4 packet types and their text codec.
//
// Engine.IO is the framing layer that Socket.IO rides on. Every packet starts
// with a single digit naming its type, followed by an opaque payload. For
// example the wire bytes "4hello" are a Message packet carrying "hello".
//
// This package only deals with one packet at a time and only with text
// payloads. Multi-packet payloads (used by the HTTP long-polling transport)
// and binary attachments arrive in later milestones.
package engineio

import (
	"errors"
	"fmt"
)

// PacketType is the Engine.IO packet kind, encoded on the wire as one digit.
type PacketType int

const (
	// Open starts a session and carries the handshake JSON from the server.
	Open PacketType = iota
	// Close asks the transport to shut down.
	Close
	// Ping is a heartbeat probe.
	Ping
	// Pong answers a Ping.
	Pong
	// Message carries an application payload (for example a Socket.IO packet).
	Message
	// Upgrade confirms a transport switch, such as polling to WebSocket.
	Upgrade
	// Noop wakes a blocked transport without carrying data.
	Noop
)

// minType and maxType bound the valid PacketType range.
const (
	minType = Open
	maxType = Noop
)

// ErrEmptyPacket is returned when decoding is given no bytes.
var ErrEmptyPacket = errors.New("engineio: empty packet")

// String returns a human-readable name, handy for logs and test output.
func (t PacketType) String() string {
	switch t {
	case Open:
		return "open"
	case Close:
		return "close"
	case Ping:
		return "ping"
	case Pong:
		return "pong"
	case Message:
		return "message"
	case Upgrade:
		return "upgrade"
	case Noop:
		return "noop"
	default:
		return fmt.Sprintf("unknown(%d)", int(t))
	}
}

// valid reports whether the type is one of the known Engine.IO kinds.
func (t PacketType) valid() bool {
	return t >= minType && t <= maxType
}

// Packet is a single Engine.IO frame: a type and its raw payload.
type Packet struct {
	Type PacketType
	Data []byte
}

// Encode renders a packet to its wire bytes: one type digit then the payload.
func Encode(p Packet) ([]byte, error) {
	if !p.Type.valid() {
		return nil, fmt.Errorf("engineio: invalid packet type %d", int(p.Type))
	}
	out := make([]byte, 0, len(p.Data)+1)
	out = append(out, byte('0'+int(p.Type)))
	out = append(out, p.Data...)
	return out, nil
}

// Decode parses wire bytes back into a packet. It is the inverse of Encode:
// for any valid packet, Decode(Encode(p)) returns an equivalent packet.
func Decode(raw []byte) (Packet, error) {
	if len(raw) == 0 {
		return Packet{}, ErrEmptyPacket
	}
	digit := raw[0]
	if digit < '0' || digit > '6' {
		return Packet{}, fmt.Errorf("engineio: invalid packet type %q", digit)
	}
	data := make([]byte, len(raw)-1)
	copy(data, raw[1:])
	return Packet{Type: PacketType(digit - '0'), Data: data}, nil
}
