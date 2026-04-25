package internal

import "testing"

func TestWireEventOrDash(t *testing.T) {
	if got := (WireEvent("hello")).OrDash(); got != "hello" {
		t.Fatalf("got %q", got)
	}
	if got := (WireEvent("")).OrDash(); got != "-" {
		t.Fatalf("empty: got %q want -", got)
	}
}

func TestWireRoomOrDash(t *testing.T) {
	if got := (WireRoom("room-1")).OrDash(); got != "room-1" {
		t.Fatalf("got %q", got)
	}
	if got := (WireRoom("")).OrDash(); got != "-" {
		t.Fatalf("empty: got %q want -", got)
	}
}
