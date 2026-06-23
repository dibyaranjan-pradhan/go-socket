package engineio

import (
	"encoding/json"
	"testing"
	"time"
)

func TestNewHandshakeMilliseconds(t *testing.T) {
	h := NewHandshake("abc", 25*time.Second, 20*time.Second, 1<<20)
	if h.SID != "abc" || h.PingInterval != 25000 || h.PingTimeout != 20000 || h.MaxPayload != 1<<20 {
		t.Fatalf("unexpected handshake: %+v", h)
	}
	if len(h.Upgrades) != 1 || h.Upgrades[0] != TransportWebSocket {
		t.Fatalf("upgrades = %v", h.Upgrades)
	}
}

func TestMarshalOpenPacketRoundTrip(t *testing.T) {
	h := NewHandshake("sid-1", time.Second, 2*time.Second, 1000)
	wire, err := MarshalOpenPacket(h)
	if err != nil {
		t.Fatal(err)
	}
	pkt, err := Decode(wire)
	if err != nil {
		t.Fatal(err)
	}
	if pkt.Type != Open {
		t.Fatalf("type = %v", pkt.Type)
	}
	parsed, err := ParseHandshake(pkt.Data)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.SID != "sid-1" {
		t.Fatalf("sid = %q", parsed.SID)
	}
}

func TestParseHandshakeInvalidJSON(t *testing.T) {
	if _, err := ParseHandshake([]byte("{")); err == nil {
		t.Fatal("expected error for bad json")
	}
}

func TestEncodeBatchDecodeBatch(t *testing.T) {
	packets := []Packet{
		{Type: Ping},
		{Type: Message, Data: []byte("hi")},
	}
	body, err := EncodeBatch(packets)
	if err != nil {
		t.Fatal(err)
	}
	out, err := DecodeBatch(body)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 2 {
		t.Fatalf("len = %d", len(out))
	}
}

func TestDecodeBatchEmpty(t *testing.T) {
	out, err := DecodeBatch(nil)
	if err != nil || len(out) != 0 {
		t.Fatalf("empty batch: out=%v err=%v", out, err)
	}
}

func TestDecodeBatchBadSegment(t *testing.T) {
	if _, err := DecodeBatch([]byte("x")); err == nil {
		t.Fatal("expected error")
	}
}

func TestHandshakeJSONShape(t *testing.T) {
	h := NewHandshake("x", time.Second, time.Second, 500)
	b, _ := json.Marshal(h)
	if !json.Valid(b) {
		t.Fatal("invalid json")
	}
}

func TestPacketTypeStringUnknown(t *testing.T) {
	if PacketType(9).String() == "" {
		t.Fatal("expected non-empty unknown label")
	}
}
