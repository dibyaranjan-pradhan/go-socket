package engineio

import (
	"bytes"
	"fmt"
)

// EncodeBatch joins multiple packets into one HTTP body using the Engine.IO
// record separator.
func EncodeBatch(packets []Packet) ([]byte, error) {
	if len(packets) == 0 {
		return nil, nil
	}
	var buf bytes.Buffer
	for i, p := range packets {
		wire, err := Encode(p)
		if err != nil {
			return nil, err
		}
		if i > 0 {
			buf.WriteString(RecordSeparator)
		}
		buf.Write(wire)
	}
	return buf.Bytes(), nil
}

// DecodeBatch splits a batched HTTP body into individual packets.
func DecodeBatch(body []byte) ([]Packet, error) {
	if len(body) == 0 {
		return nil, nil
	}
	parts := bytes.Split(body, []byte(RecordSeparator))
	out := make([]Packet, 0, len(parts))
	for _, part := range parts {
		if len(part) == 0 {
			continue
		}
		p, err := Decode(part)
		if err != nil {
			return nil, fmt.Errorf("engineio: decode batch segment: %w", err)
		}
		out = append(out, p)
	}
	return out, nil
}
