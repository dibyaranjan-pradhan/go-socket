// Copyright (c) 2026 Dibyaranjan Pradhan. All rights reserved.

package internal

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func newDialectHub(t *testing.T) *Hub {
	t.Helper()
	h := NewHub(nil, 16)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go h.Run(ctx)
	return h
}

func dialectClient(h *Hub, id string, d Dialect) *Client {
	c := NewClient(h, newFakeTransport(), ClientOptions{ID: id, SendBuffer: 8})
	c.SetDialect(d)
	h.Register(c)
	return c
}

func TestClientDialectAndResumedFlags(t *testing.T) {
	c := NewClient(NewHub(nil, 1), newFakeTransport(), ClientOptions{ID: "x"})
	if c.Dialect() != DialectLegacy {
		t.Fatal("default dialect must be legacy")
	}
	c.SetDialect(DialectSocketIO)
	if c.Dialect() != DialectSocketIO {
		t.Fatal("SetDialect not applied")
	}
	c.SetResumed(true)
	if !c.Resumed() {
		t.Fatal("SetResumed not applied")
	}
}

func TestHubUnionExceptAndPerDialectFrames(t *testing.T) {
	h := newDialectHub(t)
	leg := dialectClient(h, "leg", DialectLegacy)
	sio1 := dialectClient(h, "s1", DialectSocketIO)
	sio2 := dialectClient(h, "s2", DialectSocketIO)
	idle := dialectClient(h, "idle", DialectSocketIO)
	h.Join(leg, "a")
	h.Join(sio1, "a")
	h.Join(sio2, "b")
	h.Join(sio2, "banned")
	waitUntil(t, func() bool { return h.RoomSize("a") == 2 && h.RoomSize("b") == 1 && h.RoomSize("banned") == 1 })

	// Union of a and b minus banned: legacy and sio1 get their own frame; sio2 is excluded.
	h.Emit(EmitJob{
		Rooms: []string{"a", "b"}, ExceptRooms: []string{"banned"},
		Data: []byte("legacy"), SIOData: []byte("sio"),
	})
	waitUntil(t, func() bool { return len(leg.send) == 1 && len(sio1.send) == 1 })
	if got := string(<-leg.send); got != "legacy" {
		t.Fatalf("legacy frame = %q", got)
	}
	if got := string(<-sio1.send); got != "sio" {
		t.Fatalf("sio frame = %q", got)
	}
	if len(sio2.send) != 0 || len(idle.send) != 0 {
		t.Fatal("excluded or non-member client received the frame")
	}

	// A Socket.IO target must never receive legacy JSON: nil SIOData skips it.
	h.Emit(EmitJob{Rooms: []string{"a"}, Data: []byte("legacy-only")})
	waitUntil(t, func() bool { return len(leg.send) == 1 })
	time.Sleep(20 * time.Millisecond)
	if len(sio1.send) != 0 {
		t.Fatal("legacy frame leaked to a Socket.IO client")
	}
	<-leg.send

	// IncludeLegacy reaches legacy clients that joined no listed room.
	h.Emit(EmitJob{Rooms: []string{"b"}, IncludeLegacy: true, Data: []byte("L"), SIOData: []byte("S")})
	waitUntil(t, func() bool { return len(leg.send) == 1 && len(sio2.send) == 1 })
	if len(sio1.send) != 0 {
		t.Fatal("sio1 is not in room b")
	}

	// AllClients honours ExceptRooms and Exclude.
	drainSends(leg, sio1, sio2, idle)
	h.Emit(EmitJob{AllClients: true, ExceptRooms: []string{"banned"}, Exclude: idle, Data: []byte("L"), SIOData: []byte("S")})
	waitUntil(t, func() bool { return len(leg.send) == 1 && len(sio1.send) == 1 })
	if len(sio2.send) != 0 || len(idle.send) != 0 {
		t.Fatal("exclusions not applied to AllClients")
	}
}

func drainSends(cs ...*Client) {
	for _, c := range cs {
		for len(c.send) > 0 {
			<-c.send
		}
	}
}

func TestHubHiddenRoomsNotListed(t *testing.T) {
	h := newDialectHub(t)
	c := dialectClient(h, "c", DialectSocketIO)
	h.Join(c, "visible")
	h.Join(c, HiddenRoomPrefix+"m"+HiddenRoomPrefix+"/")
	waitUntil(t, func() bool { return h.RoomSize("visible") == 1 })
	waitUntil(t, func() bool { return h.RoomSize(HiddenRoomPrefix+"m"+HiddenRoomPrefix+"/") == 1 })
	if rooms := h.AllRooms(); len(rooms) != 1 || rooms[0] != "visible" {
		t.Fatalf("AllRooms = %v", rooms)
	}
	if n := h.ClientRoomCount(c); n != 1 {
		t.Fatalf("ClientRoomCount = %d", n)
	}
}

func TestShutdownAfterFlushDeliversQueuedFramesThenCloses(t *testing.T) {
	h := NewHub(nil, 4)
	ft := newFakeTransport()
	c := NewClient(h, ft, ClientOptions{ID: "c", SendBuffer: 4, WriteWait: time.Second, PingPeriod: time.Hour})
	if !c.TrySend([]byte("bye")) {
		t.Fatal("enqueue failed")
	}
	c.ShutdownAfterFlush()
	c.ShutdownAfterFlush() // idempotent
	go c.WritePump()
	select {
	case got := <-ft.writes:
		if string(got) != "bye" {
			t.Fatalf("flushed frame = %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("queued frame was not flushed")
	}
	deadline := time.Now().Add(3 * time.Second)
	for !ft.closed.Load() {
		if time.Now().After(deadline) {
			t.Fatal("transport not force-closed after linger")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if c.TrySend([]byte("late")) {
		t.Fatal("send after shutdown must fail")
	}
}

func newDirectWSServer(t *testing.T, attach PollAttachFunc) *httptest.Server {
	t.Helper()
	up := NewTransportUpgrader(WebSocketBackendGorilla, 1024, 1024, nil)
	h := NewEngineIOHandler(time.Hour, time.Hour, 1<<20, up, attach, func() bool { return false })
	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)
	return ts
}

func TestDirectWebSocketWithoutAttachClosesAfterOpen(t *testing.T) {
	ts := newDirectWSServer(t, nil)
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(ts.URL, "http")+"/?EIO=4&transport=websocket", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, msg, err := conn.ReadMessage(); err != nil || msg[0] != '0' {
		t.Fatalf("open = %q err=%v", msg, err)
	}
	if _, _, err := conn.ReadMessage(); err == nil {
		t.Fatal("connection must close when nothing handles it")
	}
}

func TestDirectWebSocketRejectsPlainHTTP(t *testing.T) {
	ts := newDirectWSServer(t, func(Transport, *http.Request) {})
	resp, err := http.Get(ts.URL + "/?EIO=4&transport=websocket")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode < 400 {
		t.Fatalf("status = %d, want an upgrade error", resp.StatusCode)
	}
}
