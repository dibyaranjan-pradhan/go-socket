// Package socketio implements Socket.IO v5 packet types and their text codec.
//
// A Socket.IO packet rides inside an Engine.IO Message packet. Its text form is:
//
//	<type>[<#attachments>-][<namespace>,][<ackId>]<json-payload>
//
// Examples:
//
//	2["chat","hi"]            an EVENT on the default namespace "/"
//	2/admin,7["kick","bob"]   an EVENT on "/admin" expecting ack id 7
//	0{"token":"x"}            a CONNECT carrying auth data
//	51-/admin,9["file"]       a BINARY_EVENT with 1 attachment, ns "/admin", ack 9
//
// The default namespace "/" is left off the wire. Binary attachment bytes are
// not reconstructed here yet (that arrives with binary support later); only the
// attachment count in the header is parsed.
package socketio

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// PacketType is the Socket.IO packet kind, encoded on the wire as one digit.
type PacketType int

const (
	// Connect joins a namespace; its payload may carry auth data.
	Connect PacketType = iota
	// Disconnect leaves a namespace.
	Disconnect
	// Event delivers a named message as a JSON array ["name", args...].
	Event
	// Ack replies to an Event that asked for one, matched by ack id.
	Ack
	// ConnectError reports a refused Connect.
	ConnectError
	// BinaryEvent is an Event that carries binary attachments.
	BinaryEvent
	// BinaryAck is an Ack that carries binary attachments.
	BinaryAck
)

const (
	minType = Connect
	maxType = BinaryAck
	// defaultNamespace is the namespace assumed when none is on the wire.
	defaultNamespace = "/"
)

// ErrEmptyPacket is returned when decoding is given no bytes.
var ErrEmptyPacket = errors.New("socketio: empty packet")

// String returns a human-readable name, handy for logs and test output.
func (t PacketType) String() string {
	switch t {
	case Connect:
		return "connect"
	case Disconnect:
		return "disconnect"
	case Event:
		return "event"
	case Ack:
		return "ack"
	case ConnectError:
		return "connect_error"
	case BinaryEvent:
		return "binary_event"
	case BinaryAck:
		return "binary_ack"
	default:
		return fmt.Sprintf("unknown(%d)", int(t))
	}
}

func (t PacketType) valid() bool {
	return t >= minType && t <= maxType
}

func (t PacketType) isBinary() bool {
	return t == BinaryEvent || t == BinaryAck
}

// Packet is a decoded Socket.IO frame.
type Packet struct {
	// Type is the packet kind.
	Type PacketType
	// Namespace is the target namespace; "/" when not specified on the wire.
	Namespace string
	// AckID is the acknowledgement id, or nil when the packet has none.
	AckID *int
	// Data is the raw JSON payload (event array, connect auth, error body), or
	// nil when the packet carries no payload.
	Data json.RawMessage
	// Attachments is the number of binary parts for binary packet types.
	Attachments int
}

// Encode renders a packet to its wire bytes. It is the inverse of Decode for
// any packet Decode produces.
func Encode(p Packet) ([]byte, error) {
	if !p.Type.valid() {
		return nil, fmt.Errorf("socketio: invalid packet type %d", int(p.Type))
	}
	if !p.Type.isBinary() && p.Attachments != 0 {
		return nil, errors.New("socketio: attachments set on a non-binary packet")
	}
	if p.Type.isBinary() && p.Attachments < 0 {
		return nil, errors.New("socketio: negative attachment count")
	}

	var b strings.Builder
	b.WriteByte(byte('0' + int(p.Type)))

	if p.Type.isBinary() {
		b.WriteString(strconv.Itoa(p.Attachments))
		b.WriteByte('-')
	}

	if ns := p.Namespace; ns != "" && ns != defaultNamespace {
		if !strings.HasPrefix(ns, "/") {
			return nil, fmt.Errorf("socketio: namespace %q must start with '/'", ns)
		}
		b.WriteString(ns)
		b.WriteByte(',')
	}

	if p.AckID != nil {
		b.WriteString(strconv.Itoa(*p.AckID))
	}

	if len(p.Data) > 0 {
		b.Write(p.Data)
	}

	return []byte(b.String()), nil
}

// Decode parses wire bytes into a Packet. The default namespace "/" is filled
// in when the wire omits it.
func Decode(raw []byte) (Packet, error) {
	if len(raw) == 0 {
		return Packet{}, ErrEmptyPacket
	}
	digit := raw[0]
	if digit < '0' || digit > '6' {
		return Packet{}, fmt.Errorf("socketio: invalid packet type %q", digit)
	}

	p := Packet{Type: PacketType(digit - '0'), Namespace: defaultNamespace}
	i := 1

	if p.Type.isBinary() {
		j := i
		for j < len(raw) && isDigit(raw[j]) {
			j++
		}
		if j == i || j >= len(raw) || raw[j] != '-' {
			return Packet{}, errors.New("socketio: malformed binary attachment header")
		}
		n, err := strconv.Atoi(string(raw[i:j]))
		if err != nil {
			return Packet{}, fmt.Errorf("socketio: bad attachment count: %w", err)
		}
		p.Attachments = n
		i = j + 1
	}

	// A leading '/' marks a namespace that runs until the next comma (or the
	// end of input when the packet has no ack id and no payload).
	if i < len(raw) && raw[i] == '/' {
		j := i
		for j < len(raw) && raw[j] != ',' {
			j++
		}
		if j < len(raw) {
			p.Namespace = string(raw[i:j])
			i = j + 1
		} else {
			p.Namespace = string(raw[i:])
			i = len(raw)
		}
	}

	// A run of digits here is the ack id. A JSON payload never starts with a
	// digit (it is '[' or '{'), so this is unambiguous.
	j := i
	for j < len(raw) && isDigit(raw[j]) {
		j++
	}
	if j > i {
		n, err := strconv.Atoi(string(raw[i:j]))
		if err != nil {
			return Packet{}, fmt.Errorf("socketio: bad ack id: %w", err)
		}
		p.AckID = &n
		i = j
	}

	if i < len(raw) {
		data := make([]byte, len(raw)-i)
		copy(data, raw[i:])
		p.Data = json.RawMessage(data)
	}

	return p, nil
}

func isDigit(b byte) bool {
	return b >= '0' && b <= '9'
}
