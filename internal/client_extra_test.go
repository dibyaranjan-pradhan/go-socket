// Copyright (c) 2026 Dibyaranjan Pradhan. All rights reserved.

package internal

import (
	"testing"
	"time"
)

func TestClientAllowEventRateLimit(t *testing.T) {
	c := NewClient(nil, newFakeTransport(), ClientOptions{ID: "c", SendBuffer: 2})
	if err := c.AllowEvent(100 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := c.AllowEvent(100 * time.Millisecond); err != ErrRateLimited {
		t.Fatalf("err = %v", err)
	}
}

func TestClientMetaAndID(t *testing.T) {
	c := NewClient(nil, newFakeTransport(), ClientOptions{ID: "id-9", SendBuffer: 2})
	if c.ID() != "id-9" {
		t.Fatal("id mismatch")
	}
	c.MetaSet("k", 1)
	v, ok := c.MetaGet("k")
	if !ok || v.(int) != 1 {
		t.Fatalf("meta %v %v", v, ok)
	}
}

func TestClientIncrementMissedPongs(t *testing.T) {
	c := NewClient(nil, newFakeTransport(), ClientOptions{ID: "c", SendBuffer: 2})
	if c.IncrementMissedPongs() {
		t.Fatal("should not trip on first miss")
	}
	_ = c.IncrementMissedPongs()
	if !c.IncrementMissedPongs() {
		t.Fatal("should trip on third miss")
	}
}

func TestPollingTransportNameAndLimits(t *testing.T) {
	pt := newPollingTransport(testSession())
	defer pt.Close()
	if pt.Name() != "polling" {
		t.Fatalf("name = %q", pt.Name())
	}
	pt.SetReadLimit(100)
	_ = pt.SetWriteDeadline(time.Now())
}
