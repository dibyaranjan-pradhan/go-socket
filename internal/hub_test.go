// Copyright (c) 2026 Dibyaranjan Pradhan. All rights reserved.

package internal

import (
	"context"
	"testing"
	"time"
)

func TestHubRegisterJoinEmit(t *testing.T) {
	h := NewHub(nil, 16)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)

	ft1 := newFakeTransport()
	ft2 := newFakeTransport()
	c1 := NewClient(h, ft1, ClientOptions{ID: "c1", SendBuffer: 4})
	c2 := NewClient(h, ft2, ClientOptions{ID: "c2", SendBuffer: 4})
	h.Register(c1)
	h.Register(c2)
	h.Join(c1, "room-a")
	h.Join(c2, "room-a")

	waitUntil(t, func() bool { return h.RoomSize("room-a") == 2 })

	h.Emit(EmitJob{Room: "room-a", Data: []byte(`{"event":"x"}`)})

	waitUntil(t, func() bool {
		return len(c1.send) > 0 && len(c2.send) > 0
	})
}

func TestHubRoomSizeEmpty(t *testing.T) {
	h := NewHub(nil, 8)
	if h.RoomSize("nope") != 0 {
		t.Fatal("expected 0")
	}
}

func TestHubAllRooms(t *testing.T) {
	h := NewHub(nil, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)

	c := NewClient(h, newFakeTransport(), ClientOptions{ID: "c", SendBuffer: 2})
	h.Register(c)
	h.Join(c, "r1")

	waitUntil(t, func() bool { return len(h.AllRooms()) == 1 })
}

func TestHubClientRoomCount(t *testing.T) {
	h := NewHub(nil, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)

	c := NewClient(h, newFakeTransport(), ClientOptions{ID: "c", SendBuffer: 2})
	h.Register(c)
	h.Join(c, "a")
	h.Join(c, "b")

	waitUntil(t, func() bool { return h.ClientRoomCount(c) == 2 })
}

func TestHubEmitSaturated(t *testing.T) {
	h := NewHub(nil, 1) // tiny buffer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)

	// fill emit channel
	h.Emit(EmitJob{Room: "x", Data: []byte("1")})
	h.Emit(EmitJob{Room: "x", Data: []byte("2")}) // should hit default branch
}

func TestHubLeaveRoomAndRoomClients(t *testing.T) {
	h := NewHub(nil, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)

	c := NewClient(h, newFakeTransport(), ClientOptions{ID: "c", SendBuffer: 2})
	h.Register(c)
	h.Join(c, "r1")
	waitUntil(t, func() bool { return h.RoomSize("r1") == 1 })

	h.Leave(c, "r1")
	waitUntil(t, func() bool { return h.RoomSize("r1") == 0 })

	h.Join(c, "r2")
	waitUntil(t, func() bool { return len(h.RoomClients("r2")) == 1 })
	if _, ok := h.RoomCreatedAt("r2"); !ok {
		t.Fatal("expected room created time")
	}
}

func TestHubEmitExcludeAndOnly(t *testing.T) {
	h := NewHub(nil, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)

	c1 := NewClient(h, newFakeTransport(), ClientOptions{ID: "1", SendBuffer: 4})
	c2 := NewClient(h, newFakeTransport(), ClientOptions{ID: "2", SendBuffer: 4})
	h.Register(c1)
	h.Register(c2)
	h.Join(c1, "r")
	h.Join(c2, "r")
	waitUntil(t, func() bool { return h.RoomSize("r") == 2 })

	h.Emit(EmitJob{Room: "r", Data: []byte("all"), Exclude: c1})
	waitUntil(t, func() bool { return len(c2.send) == 1 && len(c1.send) == 0 })

	h.Emit(EmitJob{Only: c1, Data: []byte("one")})
	waitUntil(t, func() bool { return len(c1.send) == 1 })
}

func TestHubEmitAllClients(t *testing.T) {
	h := NewHub(nil, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)

	c1 := NewClient(h, newFakeTransport(), ClientOptions{ID: "1", SendBuffer: 4})
	c2 := NewClient(h, newFakeTransport(), ClientOptions{ID: "2", SendBuffer: 4})
	h.Register(c1)
	h.Register(c2)
	waitUntil(t, func() bool { return true })

	h.Emit(EmitJob{AllClients: true, Data: []byte("all")})
	waitUntil(t, func() bool { return len(c1.send) > 0 && len(c2.send) > 0 })
}

func TestHubUnregisterRemovesFromRooms(t *testing.T) {
	h := NewHub(nil, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)

	c := NewClient(h, newFakeTransport(), ClientOptions{ID: "c", SendBuffer: 2})
	h.Register(c)
	h.Join(c, "r")
	waitUntil(t, func() bool { return h.RoomSize("r") == 1 })
	h.Unregister(c)
	waitUntil(t, func() bool { return h.RoomSize("r") == 0 })
}

func TestHubLogEmitQueueFull(t *testing.T) {
	var logged bool
	h := NewHub(PrintfLoggerFunc(func(string, ...interface{}) { logged = true }), 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)

	ft := newFakeTransport()
	c := NewClient(h, ft, ClientOptions{ID: "c", SendBuffer: 1})
	h.Register(c)
	h.Join(c, "r")
	waitUntil(t, func() bool { return h.RoomSize("r") == 1 })

	// fill send buffer
	c.TrySend([]byte("fill"))
	h.Emit(EmitJob{Room: "r", Data: []byte("overflow")})
	time.Sleep(30 * time.Millisecond)
	if !logged {
		t.Fatal("expected log on full queue")
	}
}

// PrintfLoggerFunc adapts a function to PrintfLogger for tests.
type PrintfLoggerFunc func(string, ...interface{})

func (f PrintfLoggerFunc) Printf(format string, v ...interface{}) { f(format, v...) }

func TestHubLogEmitSaturated(t *testing.T) {
	var logged bool
	h := NewHub(PrintfLoggerFunc(func(string, ...interface{}) { logged = true }), 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)

	h.Emit(EmitJob{Room: "x", Data: []byte("1")})
	h.Emit(EmitJob{Room: "x", Data: []byte("2")})
	time.Sleep(20 * time.Millisecond)
	if !logged {
		t.Fatal("expected saturated log")
	}
}

func TestHubLeaveEmptyRoomNoOp(t *testing.T) {
	h := NewHub(nil, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)
	c := NewClient(h, newFakeTransport(), ClientOptions{ID: "c", SendBuffer: 2})
	h.Leave(c, "")
	h.Join(c, "")
}

func TestHubWaitAfterCancel(t *testing.T) {
	h := NewHub(nil, 4)
	ctx, cancel := context.WithCancel(context.Background())
	go h.Run(ctx)
	cancel()
	done := make(chan struct{})
	go func() {
		h.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("hub did not exit")
	}
}

func TestHubDrainClosesClients(t *testing.T) {
	h := NewHub(nil, 8)
	ctx, cancel := context.WithCancel(context.Background())
	go h.Run(ctx)

	ft := newFakeTransport()
	c := NewClient(h, ft, ClientOptions{ID: "c", SendBuffer: 2})
	h.Register(c)
	waitUntil(t, func() bool { return true })
	cancel()
	h.Wait()
}

func TestHubRegisterAfterClose(t *testing.T) {
	h := NewHub(nil, 4)
	ctx, cancel := context.WithCancel(context.Background())
	go h.Run(ctx)
	cancel()
	h.Wait()
	c := NewClient(h, newFakeTransport(), ClientOptions{ID: "c", SendBuffer: 2})
	h.Register(c)
}

func waitUntil(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met")
}
