package engineio

import (
	"encoding/json"
	"fmt"
	"time"
)

// Handshake is the JSON payload inside an Engine.IO open packet (type 0).
type Handshake struct {
	SID          string   `json:"sid"`
	Upgrades     []string `json:"upgrades"`
	PingInterval int      `json:"pingInterval"` // milliseconds
	PingTimeout  int      `json:"pingTimeout"`  // milliseconds
	MaxPayload   int64    `json:"maxPayload"`
}

// NewHandshake builds the open-packet body for a new session. Intervals are
// converted to milliseconds because that is what Engine.IO clients expect.
func NewHandshake(sid string, pingInterval, pingTimeout time.Duration, maxPayload int64) Handshake {
	return Handshake{
		SID:          sid,
		Upgrades:     []string{TransportWebSocket},
		PingInterval: int(pingInterval / time.Millisecond),
		PingTimeout:  int(pingTimeout / time.Millisecond),
		MaxPayload:   maxPayload,
	}
}

// MarshalOpenPacket returns the full wire bytes for an open packet.
func MarshalOpenPacket(h Handshake) ([]byte, error) {
	body, err := json.Marshal(h)
	if err != nil {
		return nil, fmt.Errorf("engineio: marshal handshake: %w", err)
	}
	return Encode(Packet{Type: Open, Data: body})
}

// ParseHandshake decodes the JSON body of an open packet.
func ParseHandshake(data []byte) (Handshake, error) {
	var h Handshake
	if err := json.Unmarshal(data, &h); err != nil {
		return Handshake{}, fmt.Errorf("engineio: parse handshake: %w", err)
	}
	return h, nil
}
