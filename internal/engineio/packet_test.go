package engineio

import (
	"bytes"
	"testing"
)

// TestEncodeDecodeRoundTrip shows the normal use of the codec: build a packet,
// encode it to wire bytes, decode it back, and get the same packet. Each row is
// a small worked example of what a given packet looks like on the wire.
func TestEncodeDecodeRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		pkt  Packet
		wire string
	}{
		{"open with handshake json", Packet{Type: Open, Data: []byte(`{"sid":"abc"}`)}, `0{"sid":"abc"}`},
		{"close no payload", Packet{Type: Close}, "1"},
		{"ping probe", Packet{Type: Ping, Data: []byte("probe")}, "2probe"},
		{"pong probe", Packet{Type: Pong, Data: []byte("probe")}, "3probe"},
		{"message payload", Packet{Type: Message, Data: []byte("hello")}, "4hello"},
		{"upgrade no payload", Packet{Type: Upgrade}, "5"},
		{"noop no payload", Packet{Type: Noop}, "6"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := Encode(tc.pkt)
			if err != nil {
				t.Fatalf("Encode(%+v) error: %v", tc.pkt, err)
			}
			if string(encoded) != tc.wire {
				t.Fatalf("Encode = %q, want %q", encoded, tc.wire)
			}

			decoded, err := Decode(encoded)
			if err != nil {
				t.Fatalf("Decode(%q) error: %v", encoded, err)
			}
			if decoded.Type != tc.pkt.Type {
				t.Fatalf("Type = %v, want %v", decoded.Type, tc.pkt.Type)
			}
			if !bytes.Equal(decoded.Data, tc.pkt.Data) && !(len(decoded.Data) == 0 && len(tc.pkt.Data) == 0) {
				t.Fatalf("Data = %q, want %q", decoded.Data, tc.pkt.Data)
			}
		})
	}
}

// TestEncodeRejectsInvalidType is the negative path for Encode: a type outside
// the known 0..6 range is refused instead of producing garbage bytes.
func TestEncodeRejectsInvalidType(t *testing.T) {
	if _, err := Encode(Packet{Type: PacketType(99)}); err == nil {
		t.Fatal("expected error for out-of-range packet type, got nil")
	}
	if _, err := Encode(Packet{Type: PacketType(-1)}); err == nil {
		t.Fatal("expected error for negative packet type, got nil")
	}
}

// TestDecodeErrors is the negative path for Decode: empty input and a leading
// byte that is not a valid type digit are both rejected.
func TestDecodeErrors(t *testing.T) {
	if _, err := Decode(nil); err == nil {
		t.Fatal("expected error decoding empty input, got nil")
	}
	for _, bad := range []string{"x", "7hello", "9", " 4"} {
		if _, err := Decode([]byte(bad)); err == nil {
			t.Fatalf("expected error decoding %q, got nil", bad)
		}
	}
}

// TestPacketTypeString checks the readable names used in logs and errors.
func TestPacketTypeString(t *testing.T) {
	if Message.String() != "message" {
		t.Fatalf("Message.String() = %q, want %q", Message.String(), "message")
	}
	if PacketType(42).String() == "" {
		t.Fatal("unknown type should still produce a non-empty label")
	}
}

// FuzzDecode feeds arbitrary bytes to Decode. The contract under test: Decode
// never panics, and any input it accepts must re-encode to the exact same bytes
// (decode and encode stay perfect inverses).
func FuzzDecode(f *testing.F) {
	for _, seed := range []string{"", "0", "4hello", "2probe", "x", "7bad"} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		pkt, err := Decode(raw)
		if err != nil {
			return // rejected input is fine; we only care that it did not panic
		}
		reencoded, err := Encode(pkt)
		if err != nil {
			t.Fatalf("Encode after successful Decode failed: %v", err)
		}
		if !bytes.Equal(reencoded, raw) {
			t.Fatalf("round-trip mismatch: Decode(%q) re-encoded to %q", raw, reencoded)
		}
	})
}
