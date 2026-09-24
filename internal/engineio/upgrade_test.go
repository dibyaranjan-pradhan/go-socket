package engineio

import "testing"

func TestIsProbe(t *testing.T) {
	if !IsProbe([]byte(ProbePayload)) {
		t.Fatal("expected probe")
	}
	if IsProbe([]byte("other")) {
		t.Fatal("expected not probe")
	}
}
