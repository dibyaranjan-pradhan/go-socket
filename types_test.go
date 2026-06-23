package gosocket

import (
	"encoding/json"
	"testing"
)

func TestClientWireMessageJSON(t *testing.T) {
	raw := []byte(`{"event":"hello","room":"r1","payload":{"k":1},"id":"ack-1"}`)
	var m ClientWireMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if m.Event != "hello" || m.Room != "r1" || m.ID != "ack-1" {
		t.Fatalf("%+v", m)
	}
	if len(m.Payload) == 0 {
		t.Fatal("expected payload")
	}
}

func TestServerWireMessageJSON(t *testing.T) {
	msg := ServerWireMessage{Event: "x", Payload: map[string]int{"n": 2}, AckID: "a"}
	b, err := json.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	var out ServerWireMessage
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if out.Event != "x" || out.AckID != "a" {
		t.Fatalf("%+v", out)
	}
}

func TestPresetConstantsStable(t *testing.T) {
	if PresetChatFetchHistory != "fetch_history" ||
		PresetChatMessage != "message" ||
		PresetChatTyping != "typing" ||
		PresetChatHistory != "history" ||
		PresetChatConnected != "connected" ||
		PresetError != "error" {
		t.Fatal("preset drift — update docs/EVENTS.md")
	}
}

func TestMiddlewareFunc(t *testing.T) {
	var ran bool
	var f MiddlewareFunc = func(ctx *Context) error {
		ran = true
		return nil
	}
	if err := f.RunMiddleware(&Context{}); err != nil {
		t.Fatal(err)
	}
	if !ran {
		t.Fatal("expected run")
	}
}
