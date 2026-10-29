// Copyright (c) 2026 Dibyaranjan Pradhan. All rights reserved.

package gosocket

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dibyaranjan-pradhan/go-socket/internal/engineio"
	"github.com/gorilla/websocket"
)

func recoveryServer(t *testing.T, rc SessionRecoveryConfig) (*Server, *httptest.Server) {
	t.Helper()
	rc.Enabled = true
	s := New(Config{SessionRecovery: rc, PingInterval: time.Hour, PongWait: time.Hour, HubEmitBufferSize: 4096, WriteBufferSize: 2048})
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	return s, ts
}

func dialRecovery(t *testing.T, ts *httptest.Server, sid string, seq uint64, hdr http.Header) *websocket.Conn {
	t.Helper()
	u := wsURL(ts.URL)
	if sid != "" {
		u += "/?" + QuerySession + "=" + url.QueryEscape(sid) + "&" + QuerySeq + "=" + strconv.FormatUint(seq, 10)
	}
	conn, _, err := websocket.DefaultDialer.Dial(u, hdr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func readFrame(t *testing.T, conn *websocket.Conn) map[string]interface{} {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, b, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var m map[string]interface{}
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("decode %q: %v", b, err)
	}
	return m
}

func sessionPayload(t *testing.T, f map[string]interface{}) map[string]interface{} {
	t.Helper()
	if f["event"] != EventSession {
		t.Fatalf("expected %s frame, got %v", EventSession, f)
	}
	return f["payload"].(map[string]interface{})
}

func recoveryWait(t *testing.T, cond func() bool) {
	t.Helper()
	for i := 0; i < 400; i++ {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met")
}

func dropTCP(conn *websocket.Conn) { _ = conn.UnderlyingConn().Close() }

func TestRecoveryDisabledLeavesWireUntouched(t *testing.T) {
	s := New(Config{PingInterval: time.Hour, PongWait: time.Hour})
	s.OnConnect(func(ctx *Context) { _ = ctx.Emit("hello", map[string]int{"n": 1}) })
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	conn := dialRecovery(t, ts, "ignored-token", 3, nil)
	f := readFrame(t, conn)
	if f["event"] != "hello" {
		t.Fatalf("first frame must be the app's own: %v", f)
	}
	if _, ok := f["seq"]; ok {
		t.Fatal("seq must not appear when recovery is off")
	}
	if st := s.GetServerStats(); st.Sessions != 0 {
		t.Fatalf("no sessions expected: %+v", st)
	}
}

func TestRecoveryResumeReplaysMissedFramesInOrder(t *testing.T) {
	s, ts := recoveryServer(t, SessionRecoveryConfig{})
	var welcomes, resumedConnects atomic.Int32
	var sessionSeen atomic.Value
	s.OnConnect(func(ctx *Context) {
		ctx.JoinRoom("r")
		sessionSeen.Store(ctx.SessionID())
		if ctx.Resumed() {
			resumedConnects.Add(1)
			return
		}
		welcomes.Add(1)
		_ = ctx.Emit("welcome", nil)
	})
	tick := func(i int) { _ = s.emitRoom("r", "tick", i, "", nil, EmitOptions{}) }

	c1 := dialRecovery(t, ts, "", 0, nil)
	sp := sessionPayload(t, readFrame(t, c1))
	sid := sp["sessionId"].(string)
	if sp["resumed"] != false || sid == "" {
		t.Fatalf("%v", sp)
	}
	if sessionSeen.Load() != sid {
		t.Fatal("ctx.SessionID mismatch")
	}
	if f := readFrame(t, c1); f["event"] != "welcome" || f["seq"] != float64(1) {
		t.Fatalf("%v", f)
	}
	for i := 1; i <= 3; i++ {
		tick(i)
	}
	var last float64
	for i := 1; i <= 3; i++ {
		f := readFrame(t, c1)
		if f["payload"] != float64(i) {
			t.Fatalf("%v", f)
		}
		last = f["seq"].(float64)
	}
	if last != 4 {
		t.Fatalf("last seq = %v", last)
	}

	// The connection dies mid-stream; the server keeps producing.
	dropTCP(c1)
	recoveryWait(t, func() bool { return s.GetServerStats().SessionsParked == 1 })
	if s.RoomSize("r") != 0 {
		t.Fatal("parked session must not count as present")
	}
	for i := 4; i <= 6; i++ {
		tick(i)
	}

	c2 := dialRecovery(t, ts, sid, uint64(last), nil)
	sp = sessionPayload(t, readFrame(t, c2))
	if sp["resumed"] != true || sp["sessionId"] != sid || sp["replayed"] != float64(3) || sp["lastSeq"] != float64(7) {
		t.Fatalf("%v", sp)
	}
	for i := 4; i <= 6; i++ {
		f := readFrame(t, c2)
		if f["payload"] != float64(i) || f["seq"] != float64(i+1) {
			t.Fatalf("lossless in-order replay violated: %v", f)
		}
	}

	// Live traffic continues on the resumed session, rooms intact, seq contiguous.
	recoveryWait(t, func() bool { return s.RoomSize("r") == 1 })
	tick(7)
	if f := readFrame(t, c2); f["payload"] != float64(7) || f["seq"] != float64(8) {
		t.Fatalf("%v", f)
	}
	if welcomes.Load() != 1 || resumedConnects.Load() != 1 {
		t.Fatalf("welcomes=%d resumed=%d", welcomes.Load(), resumedConnects.Load())
	}
	st := s.GetServerStats()
	if st.SessionsResumed != 1 || st.MessagesReplayed != 3 || st.SessionsParked != 0 {
		t.Fatalf("%+v", st)
	}
}

func TestRecoveryGapFallsBackToFreshSession(t *testing.T) {
	s, ts := recoveryServer(t, SessionRecoveryConfig{ReplayBufferMessages: 2})
	s.OnConnect(func(ctx *Context) { ctx.JoinRoom("r") })

	c1 := dialRecovery(t, ts, "", 0, nil)
	sid := sessionPayload(t, readFrame(t, c1))["sessionId"].(string)
	dropTCP(c1)
	recoveryWait(t, func() bool { return s.GetServerStats().SessionsParked == 1 })
	for i := 0; i < 5; i++ { // overflows the 2-frame buffer
		_ = s.emitRoom("r", "tick", i, "", nil, EmitOptions{})
	}
	recoveryWait(t, func() bool { return s.GetServerStats().Sessions == 1 })

	c2 := dialRecovery(t, ts, sid, 0, nil)
	sp := sessionPayload(t, readFrame(t, c2))
	if sp["resumed"] != false || sp["reason"] != "gap_exceeded" || sp["sessionId"] == sid {
		t.Fatalf("client must be told to resync: %v", sp)
	}
}

func TestRecoveryOwnerBinding(t *testing.T) {
	s, ts := recoveryServer(t, SessionRecoveryConfig{
		OwnerFunc: func(r *http.Request) string { return r.Header.Get("X-User") },
	})
	as := func(u string) http.Header { return http.Header{"X-User": []string{u}} }

	c1 := dialRecovery(t, ts, "", 0, as("alice"))
	sid := sessionPayload(t, readFrame(t, c1))["sessionId"].(string)
	dropTCP(c1)
	recoveryWait(t, func() bool { return s.GetServerStats().SessionsParked == 1 })

	c2 := dialRecovery(t, ts, sid, 0, as("mallory"))
	if sp := sessionPayload(t, readFrame(t, c2)); sp["resumed"] != false || sp["reason"] != "owner_mismatch" {
		t.Fatalf("%v", sp)
	}
	c3 := dialRecovery(t, ts, sid, 0, as("alice"))
	if sp := sessionPayload(t, readFrame(t, c3)); sp["resumed"] != true {
		t.Fatalf("owner must still be able to resume: %v", sp)
	}
}

func TestRecoveryHeaderTokenAndKickedPeer(t *testing.T) {
	s, ts := recoveryServer(t, SessionRecoveryConfig{})
	s.On("kick", func(ctx *Context) { ctx.Disconnect() })

	c1 := dialRecovery(t, ts, "", 0, nil)
	sid := sessionPayload(t, readFrame(t, c1))["sessionId"].(string)
	if err := c1.WriteMessage(websocket.TextMessage, []byte(`{"event":"kick","payload":{}}`)); err != nil {
		t.Fatal(err)
	}
	recoveryWait(t, func() bool { return s.GetServerStats().Sessions == 0 })

	h := http.Header{HeaderSession: []string{sid}, HeaderSeq: []string{"0"}}
	c2 := dialRecovery(t, ts, "", 0, h)
	if sp := sessionPayload(t, readFrame(t, c2)); sp["resumed"] != false || sp["reason"] != "unknown_session" {
		t.Fatalf("a server-kicked peer must not resume: %v", sp)
	}
}

func TestRecoveryIdempotencyAndAck(t *testing.T) {
	s, ts := recoveryServer(t, SessionRecoveryConfig{IdempotencyWindow: 8})
	var runs, acks atomic.Int32
	s.On("pay", func(ctx *Context) { runs.Add(1) })
	s.On(EventAck, func(ctx *Context) { acks.Add(1) })

	c := dialRecovery(t, ts, "", 0, nil)
	readFrame(t, c)
	for _, m := range []string{
		`{"event":"pay","payload":{},"idempotencyKey":"k1"}`,
		`{"event":"pay","payload":{},"idempotencyKey":"k1"}`,
		`{"event":"pay","payload":{},"idempotencyKey":"k2"}`,
		`{"event":"pay","payload":{}}`,
		`{"event":"pay","payload":{}}`,
		`{"event":"$ack","payload":{"seq":1}}`,
		`{"event":"$ack","payload":"not-an-object"}`,
	} {
		if err := c.WriteMessage(websocket.TextMessage, []byte(m)); err != nil {
			t.Fatal(err)
		}
	}
	recoveryWait(t, func() bool { return s.GetServerStats().TotalMessagesProcessed == 5 })
	if runs.Load() != 4 || acks.Load() != 0 {
		t.Fatalf("runs=%d acks=%d (keyed dup must run once, unkeyed always, $ack is control)", runs.Load(), acks.Load())
	}
	if s.GetServerStats().DuplicatesSuppressed != 1 {
		t.Fatalf("%+v", s.GetServerStats())
	}
}

func TestRecoveryIdempotencyKeyNotBurnedByMiddlewareReject(t *testing.T) {
	s, ts := recoveryServer(t, SessionRecoveryConfig{})
	var runs atomic.Int32
	var reject atomic.Bool
	reject.Store(true)
	s.Use(MiddlewareFunc(func(ctx *Context) error {
		if ctx.Event() == "pay" && reject.Load() {
			return ErrRejected
		}
		return nil
	}))
	s.On("pay", func(ctx *Context) { runs.Add(1) })

	c := dialRecovery(t, ts, "", 0, nil)
	readFrame(t, c)
	msg := []byte(`{"event":"pay","payload":{},"idempotencyKey":"k"}`)
	_ = c.WriteMessage(websocket.TextMessage, msg)
	recoveryWait(t, func() bool { return s.GetServerStats().TotalMessagesProcessed == 1 })
	reject.Store(false)
	_ = c.WriteMessage(websocket.TextMessage, msg)
	recoveryWait(t, func() bool { return runs.Load() == 1 })
}

func TestRecoveryExpiryCallbackAndStats(t *testing.T) {
	s, ts := recoveryServer(t, SessionRecoveryConfig{
		SessionTTL: 40 * time.Millisecond,
		OwnerFunc:  func(*http.Request) string { return "bob" },
	})
	var cid atomic.Value
	s.OnConnect(func(ctx *Context) { cid.Store(ctx.ClientID()) })
	got := make(chan SessionInfo, 1)
	s.OnSessionExpire(func(i SessionInfo) { got <- i })
	s.OnSessionExpire(func(SessionInfo) { panic("must be contained") })

	c := dialRecovery(t, ts, "", 0, nil)
	sid := sessionPayload(t, readFrame(t, c))["sessionId"].(string)
	if st := s.GetConnectionStats(cid.Load().(string)); st == nil || st.SessionID != sid {
		t.Fatalf("%+v", st)
	}
	dropTCP(c)
	select {
	case info := <-got:
		if info.SessionID != sid || info.Owner != "bob" || info.ClientID != cid.Load().(string) {
			t.Fatalf("%+v", info)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("expiry callback not fired")
	}
	if st := s.GetServerStats(); st.Sessions != 0 {
		t.Fatalf("%+v", st)
	}
}

func TestEmitWithPoliciesAndKeys(t *testing.T) {
	s, ts := recoveryServer(t, SessionRecoveryConfig{})
	s.On("go", func(ctx *Context) {
		_ = ctx.EmitWith("rel", 1, EmitOptions{IdempotencyKey: "job-1", Policy: BackpressureDrop})
		_ = ctx.EmitWith("vol", 2, EmitOptions{Policy: BackpressureVolatile})
		_ = ctx.EmitToRoomWith("r", "room", 3, EmitOptions{IdempotencyKey: "r-1"})
		_ = ctx.BroadcastToRoomWith("r", "bcast", 4, EmitOptions{})
	})
	s.OnConnect(func(ctx *Context) { ctx.JoinRoom("r") })

	c := dialRecovery(t, ts, "", 0, nil)
	readFrame(t, c)
	_ = c.WriteMessage(websocket.TextMessage, []byte(`{"event":"go","payload":{}}`))

	rel := readFrame(t, c)
	if rel["event"] != "rel" || rel["idempotencyKey"] != "job-1" || rel["seq"] != float64(1) {
		t.Fatalf("%v", rel)
	}
	vol := readFrame(t, c)
	if vol["event"] != "vol" || vol["seq"] != nil {
		t.Fatalf("volatile frames are never sequenced: %v", vol)
	}
	room := readFrame(t, c)
	if room["event"] != "room" || room["idempotencyKey"] != "r-1" || room["seq"] != float64(2) {
		t.Fatalf("%v", room)
	}
}

func TestBackpressureDefaultResolution(t *testing.T) {
	s := New(Config{DefaultBackpressure: BackpressureDrop})
	if s.resolvePolicy(BackpressureDefault) != BackpressureDrop.internal() {
		t.Fatal("server default not applied")
	}
	if s.resolvePolicy(BackpressureDisconnect) != BackpressureDisconnect.internal() {
		t.Fatal("explicit per-emit policy must win over the default")
	}
	if New(Config{}).resolvePolicy(BackpressureDefault) != BackpressureDisconnect.internal() {
		t.Fatal("zero config must keep the historical disconnect behaviour")
	}
	if BackpressureVolatile.internal() == BackpressureDrop.internal() {
		t.Fatal("policies must map to distinct internal values")
	}
}

func TestSessionRecoveryConfigDefaults(t *testing.T) {
	off := (&Config{}).normalized()
	if off.SessionRecovery.Enabled || off.SessionRecovery.ReplayBufferMessages != 0 {
		t.Fatalf("disabled config must stay untouched: %+v", off.SessionRecovery)
	}
	on := (&Config{SessionRecovery: SessionRecoveryConfig{Enabled: true}}).normalized().SessionRecovery
	if on.ReplayBufferMessages != DefaultReplayBufferMessages || on.ReplayBufferBytes != DefaultReplayBufferBytes ||
		on.SessionTTL != DefaultSessionTTL || on.MaxParkedSessions != DefaultMaxParkedSessions ||
		on.IdempotencyWindow != DefaultIdempotencyWindow {
		t.Fatalf("%+v", on)
	}
}

func TestResumeRequestParsing(t *testing.T) {
	s := New(Config{SessionRecovery: SessionRecoveryConfig{
		Enabled:   true,
		OwnerFunc: func(r *http.Request) string { return r.Header.Get("X-User") },
	}})
	mk := func(rawURL string, h map[string]string) *http.Request {
		r := httptest.NewRequest(http.MethodGet, rawURL, nil)
		for k, v := range h {
			r.Header.Set(k, v)
		}
		return r
	}
	if req := s.resumeRequest(mk("/", nil)); req.Requested {
		t.Fatalf("%+v", req)
	}
	req := s.resumeRequest(mk("/?gs_session=abc&gs_seq=12", map[string]string{"X-User": "u"}))
	if !req.Requested || req.SessionID != "abc" || req.LastSeq != 12 || req.Owner != "u" || req.Invalid {
		t.Fatalf("%+v", req)
	}
	req = s.resumeRequest(mk("/?gs_session=abc&gs_seq=1", map[string]string{HeaderSession: "hdr", HeaderSeq: "9"}))
	if req.SessionID != "hdr" || req.LastSeq != 9 {
		t.Fatalf("headers must win: %+v", req)
	}
	if req := s.resumeRequest(mk("/?gs_session=abc", nil)); !req.Requested || req.LastSeq != 0 || req.Invalid {
		t.Fatalf("%+v", req)
	}
	if req := s.resumeRequest(mk("/?gs_session=abc&gs_seq=-4", nil)); !req.Invalid {
		t.Fatalf("%+v", req)
	}
	if req := s.resumeRequest(nil); req.Requested {
		t.Fatal("nil request")
	}
}

func TestRecoveryOverEngineIOHandshake(t *testing.T) {
	s := New(Config{
		SessionRecovery: SessionRecoveryConfig{Enabled: true},
		PingInterval:    time.Hour, PongWait: time.Hour,
	})
	ts := httptest.NewServer(s.EngineIOHandler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/?EIO=4&transport=polling&gs_session=ghost&gs_seq=0")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	pkt, err := engineio.Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	var hs struct {
		SID string `json:"sid"`
	}
	_ = json.Unmarshal(pkt.Data, &hs)

	var body string
	recoveryWait(t, func() bool {
		r, err := http.Get(ts.URL + "/?EIO=4&transport=polling&sid=" + hs.SID)
		if err != nil {
			return false
		}
		b, _ := io.ReadAll(r.Body)
		_ = r.Body.Close()
		body = string(b)
		return strings.Contains(body, EventSession)
	})
	if !strings.Contains(body, `"reason":"unknown_session"`) || !strings.Contains(body, `"resumed":false`) {
		t.Fatalf("handshake query must reach the recovery layer: %s", body)
	}
}

func TestRecoveryConcurrentEmittersKeepSeqContiguous(t *testing.T) {
	s, ts := recoveryServer(t, SessionRecoveryConfig{ReplayBufferMessages: 4096})
	s.OnConnect(func(ctx *Context) { ctx.JoinRoom("r") })
	c := dialRecovery(t, ts, "", 0, nil)
	readFrame(t, c)
	recoveryWait(t, func() bool { return s.RoomSize("r") == 1 })

	const n, workers = 100, 8
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < n; i++ {
				_ = s.emitRoom("r", "t", i, "", nil, EmitOptions{})
			}
		}()
	}
	wg.Wait()
	for want := 1; want <= n*workers; want++ {
		f := readFrame(t, c)
		if f["seq"] != float64(want) {
			t.Fatalf("wire order broke at %d: %v", want, f["seq"])
		}
	}
}
