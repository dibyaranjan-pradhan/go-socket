package socketio

import (
	"testing"
)

func ackPtr(n int) *int { return &n }

// TestEncodeDecodeRoundTrip is the main worked-example set: each row is a real
// Socket.IO wire string paired with the packet it represents. Encoding the
// packet must produce the wire string, and decoding it must reproduce the
// packet.
func TestEncodeDecodeRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		pkt  Packet
		wire string
	}{
		{"connect default namespace", Packet{Type: Connect, Namespace: "/"}, "0"},
		{"connect with auth payload", Packet{Type: Connect, Namespace: "/", Data: []byte(`{"token":"x"}`)}, `0{"token":"x"}`},
		{"connect to a namespace", Packet{Type: Connect, Namespace: "/admin"}, "0/admin,"},
		{"disconnect default", Packet{Type: Disconnect, Namespace: "/"}, "1"},
		{"event no ack", Packet{Type: Event, Namespace: "/", Data: []byte(`["msg","hi"]`)}, `2["msg","hi"]`},
		{"event with ack id", Packet{Type: Event, Namespace: "/", AckID: ackPtr(1), Data: []byte(`["msg"]`)}, `21["msg"]`},
		{"event with namespace and ack", Packet{Type: Event, Namespace: "/admin", AckID: ackPtr(7), Data: []byte(`["kick","bob"]`)}, `2/admin,7["kick","bob"]`},
		{"ack with id zero", Packet{Type: Ack, Namespace: "/", AckID: ackPtr(0), Data: []byte(`["ok"]`)}, `30["ok"]`},
		{"connect error", Packet{Type: ConnectError, Namespace: "/", Data: []byte(`{"message":"nope"}`)}, `4{"message":"nope"}`},
		{"binary event with attachments", Packet{Type: BinaryEvent, Namespace: "/admin", AckID: ackPtr(9), Attachments: 1, Data: []byte(`["file",{"_placeholder":true,"num":0}]`)}, `51-/admin,9["file",{"_placeholder":true,"num":0}]`},
		{"binary ack", Packet{Type: BinaryAck, Namespace: "/", AckID: ackPtr(5), Attachments: 1, Data: []byte(`[{"_placeholder":true,"num":0}]`)}, `61-5[{"_placeholder":true,"num":0}]`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := Encode(tc.pkt)
			if err != nil {
				t.Fatalf("Encode error: %v", err)
			}
			if string(encoded) != tc.wire {
				t.Fatalf("Encode = %q, want %q", encoded, tc.wire)
			}

			decoded, err := Decode([]byte(tc.wire))
			if err != nil {
				t.Fatalf("Decode error: %v", err)
			}
			if !equalPacket(decoded, tc.pkt) {
				t.Fatalf("Decode = %+v, want %+v", decoded, tc.pkt)
			}
		})
	}
}

// TestDecodeFillsDefaultNamespace shows that an omitted namespace decodes to
// "/", which is what callers should branch on.
func TestDecodeFillsDefaultNamespace(t *testing.T) {
	p, err := Decode([]byte(`2["x"]`))
	if err != nil {
		t.Fatal(err)
	}
	if p.Namespace != "/" {
		t.Fatalf("Namespace = %q, want %q", p.Namespace, "/")
	}
	if p.AckID != nil {
		t.Fatalf("AckID = %v, want nil", *p.AckID)
	}
}

// TestEncodeEmptyNamespaceTreatedAsDefault documents that "" and "/" mean the
// same thing to the encoder.
func TestEncodeEmptyNamespaceTreatedAsDefault(t *testing.T) {
	withEmpty, err := Encode(Packet{Type: Event, Namespace: "", Data: []byte(`["x"]`)})
	if err != nil {
		t.Fatal(err)
	}
	if string(withEmpty) != `2["x"]` {
		t.Fatalf("Encode with empty namespace = %q, want %q", withEmpty, `2["x"]`)
	}
}

// TestEncodeErrors covers the inputs Encode must reject.
func TestEncodeErrors(t *testing.T) {
	if _, err := Encode(Packet{Type: PacketType(99)}); err == nil {
		t.Fatal("expected error for invalid type")
	}
	if _, err := Encode(Packet{Type: Event, Attachments: 2}); err == nil {
		t.Fatal("expected error: attachments on a non-binary packet")
	}
	if _, err := Encode(Packet{Type: BinaryEvent, Attachments: -1}); err == nil {
		t.Fatal("expected error for negative attachment count")
	}
	if _, err := Encode(Packet{Type: Event, Namespace: "admin"}); err == nil {
		t.Fatal("expected error: namespace without leading slash")
	}
}

// TestDecodeErrors covers malformed wire input.
func TestDecodeErrors(t *testing.T) {
	bad := []string{
		"",                        // empty
		"x",                       // not a digit
		"7[]",                     // type out of range
		"5abc",                    // binary type without attachment dash
		"5-[]",                    // binary type with empty attachment count
		"399999999999999999999[]", // ack id overflows int
	}
	for _, in := range bad {
		if _, err := Decode([]byte(in)); err == nil {
			t.Fatalf("expected error decoding %q, got nil", in)
		}
	}
}

// FuzzDecode asserts Decode never panics and reaches a stable canonical form:
// decoding, re-encoding, and decoding again yields an identical packet.
func FuzzDecode(f *testing.F) {
	seeds := []string{"", "0", "1", `2["e"]`, `21["e"]`, `2/admin,7["e"]`, "51-/x,9[]", "x", "5-", "7"}
	for _, s := range seeds {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		p1, err := Decode(raw)
		if err != nil {
			return
		}
		encoded, err := Encode(p1)
		if err != nil {
			t.Fatalf("Encode after Decode failed for %q: %v", raw, err)
		}
		p2, err := Decode(encoded)
		if err != nil {
			t.Fatalf("Decode of re-encoded bytes %q failed: %v", encoded, err)
		}
		if !equalPacket(p1, p2) {
			t.Fatalf("canonical form unstable: %+v != %+v (from %q)", p1, p2, raw)
		}
	})
}

// equalPacket compares two packets field by field, treating the ack id pointer
// and the raw JSON payload sensibly.
func equalPacket(a, b Packet) bool {
	if a.Type != b.Type || a.Namespace != b.Namespace || a.Attachments != b.Attachments {
		return false
	}
	if (a.AckID == nil) != (b.AckID == nil) {
		return false
	}
	if a.AckID != nil && *a.AckID != *b.AckID {
		return false
	}
	return string(a.Data) == string(b.Data)
}
