// Copyright (c) 2026 Dibyaranjan Pradhan. All rights reserved.

package internal

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

type releaseLog struct {
	mu  sync.Mutex
	ids []string
	why []ReleaseReason
}

func (r *releaseLog) release(s *Session, why ReleaseReason) {
	r.mu.Lock()
	r.ids = append(r.ids, s.ID())
	r.why = append(r.why, why)
	r.mu.Unlock()
}

func (r *releaseLog) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.ids)
}

func testStore(clk *fakeClock, rl *releaseLog, maxParked int) *SessionStore {
	return NewSessionStore(SessionOptions{
		ReplayMessages: 4,
		ReplayBytes:    1024,
		TTL:            time.Minute,
		MaxParked:      maxParked,
		KeyWindow:      3,
		Release:        rl.release,
		Now:            clk.now,
	})
}

func testClient(h *Hub, buf int) *Client {
	return NewClient(h, newFakeTransport(), ClientOptions{ID: "c", SendBuffer: buf})
}

func nextSeq(s *Session) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.next
}

func frame(n int) []byte {
	return []byte(`{"event":"e","payload":` + string(rune('0'+n)) + `}`)
}

func drain(c *Client) []string {
	var out []string
	for {
		select {
		case b := <-c.send:
			out = append(out, string(b))
		default:
			return out
		}
	}
}

func TestStamp(t *testing.T) {
	if got := string(stamp([]byte(`{"a":1}`), 7)); got != `{"seq":7,"a":1}` {
		t.Fatalf("got %s", got)
	}
	if got := string(stamp([]byte(`{}`), 7)); got != `{"seq":7}` {
		t.Fatalf("got %s", got)
	}
	if got := string(stamp([]byte(`[1]`), 7)); got != `[1]` {
		t.Fatalf("non-object must pass through, got %s", got)
	}
	if got := string(stamp([]byte(`x`), 7)); got != `x` {
		t.Fatalf("short must pass through, got %s", got)
	}
}

func TestReplayBufferEvictsByCount(t *testing.T) {
	b := replayBuffer{maxMsgs: 3, maxBytes: 1 << 20}
	for i := uint64(1); i <= 5; i++ {
		b.push(i, frame(int(i)))
	}
	if b.len() != 3 || b.entries[b.head].seq != 3 {
		t.Fatalf("len=%d first=%d", b.len(), b.entries[b.head].seq)
	}
	if !b.covers(2) || b.covers(1) {
		t.Fatal("covers boundary wrong")
	}
}

func TestReplayBufferEvictsByBytes(t *testing.T) {
	b := replayBuffer{maxMsgs: 100, maxBytes: 100}
	for i := uint64(1); i <= 10; i++ {
		b.push(i, make([]byte, 30))
	}
	if b.bytes > 100 || b.len() != 3 {
		t.Fatalf("bytes=%d len=%d", b.bytes, b.len())
	}
}

func TestReplayBufferOversizedFrameResets(t *testing.T) {
	b := replayBuffer{maxMsgs: 10, maxBytes: 50}
	b.push(1, make([]byte, 10))
	b.push(2, make([]byte, 100))
	if b.len() != 0 || b.bytes != 0 {
		t.Fatalf("oversized frame must clear the contiguous run, len=%d", b.len())
	}
	if b.covers(0) {
		t.Fatal("empty buffer cannot cover a gap")
	}
}

func TestReplayBufferAckAndCompaction(t *testing.T) {
	b := replayBuffer{maxMsgs: 1000, maxBytes: 1 << 20}
	for i := uint64(1); i <= 300; i++ {
		b.push(i, []byte("x"))
	}
	b.ackUpTo(200)
	if b.len() != 100 || b.entries[b.head].seq != 201 {
		t.Fatalf("len=%d", b.len())
	}
	b.ackUpTo(1000)
	if b.len() != 0 {
		t.Fatal("expected empty")
	}
	if got := b.after(0); len(got) != 0 {
		t.Fatal("after on empty")
	}
}

func TestReplayBufferMemoryBoundedUnderLoad(t *testing.T) {
	b := replayBuffer{maxMsgs: 256, maxBytes: 64 << 10}
	payload := make([]byte, 512)
	for i := uint64(1); i <= 200000; i++ {
		b.push(i, payload)
	}
	if b.len() > 256 || b.bytes > 64<<10 {
		t.Fatalf("buffer exceeded caps: len=%d bytes=%d", b.len(), b.bytes)
	}
	if cap(b.entries) > 1024 {
		t.Fatalf("entry slice grew unbounded: cap=%d", cap(b.entries))
	}
}

func TestKeyWindow(t *testing.T) {
	var k keyWindow
	first, second := k.claim("a"), k.claim("a")
	if !first || !second {
		t.Fatal("max<=0 must never dedupe")
	}
	k = keyWindow{max: 2}
	if !k.claim("a") || k.claim("a") {
		t.Fatal("dup not detected")
	}
	k.claim("b")
	k.claim("c") // evicts a
	if !k.claim("a") {
		t.Fatal("evicted key should be claimable again")
	}
	for i := 0; i < 500; i++ {
		k.claim(strings.Repeat("k", i%7) + string(rune('A'+i%26)) + string(rune(i)))
	}
	if len(k.set) > 2 {
		t.Fatalf("window exceeded: %d", len(k.set))
	}
}

func TestSessionClaimKeyHashesLongKeys(t *testing.T) {
	s := &Session{keys: keyWindow{max: 4}}
	long := strings.Repeat("z", 10_000)
	if !s.ClaimKey(long) || s.ClaimKey(long) {
		t.Fatal("long key dedupe failed")
	}
	for k := range s.keys.set {
		if len(k) > maxKeyLen {
			t.Fatalf("stored key too long: %d", len(k))
		}
	}
}

func openFresh(t *testing.T, st *SessionStore, c *Client, owner string) *Session {
	t.Helper()
	res := st.Open(c, ResumeRequest{Owner: owner})
	if res.Session == nil || res.Resumed {
		t.Fatalf("expected fresh session: %+v", res)
	}
	return res.Session
}

func TestDeliverAttachedStampsAndBuffers(t *testing.T) {
	st := testStore(&fakeClock{t: time.Unix(0, 0)}, &releaseLog{}, 5)
	c := testClient(nil, 8)
	openFresh(t, st, c, "")

	for i := 1; i <= 3; i++ {
		if !c.TrySend(frame(i)) {
			t.Fatal("send failed")
		}
	}
	got := drain(c)
	if len(got) != 3 || !strings.HasPrefix(got[0], `{"seq":1,`) || !strings.HasPrefix(got[2], `{"seq":3,`) {
		t.Fatalf("wire order/seq wrong: %v", got)
	}
	if c.Session().buf.len() != 3 {
		t.Fatal("frames not buffered")
	}
}

func TestDeliverParkedBuffersOnly(t *testing.T) {
	st := testStore(&fakeClock{t: time.Unix(0, 0)}, &releaseLog{}, 5)
	c := testClient(nil, 8)
	s := openFresh(t, st, c, "")
	if st.Leave(s, c, true) != LeaveParked {
		t.Fatal("expected parked")
	}
	if !c.TrySend(frame(1)) || !c.TrySendPolicy(frame(2), PolicyDrop) {
		t.Fatal("parked reliable send must succeed")
	}
	if !c.TrySendPolicy(frame(3), PolicyVolatile) {
		t.Fatal("volatile on parked must not signal disconnect")
	}
	if s.buf.len() != 2 || s.Dropped() != 1 {
		t.Fatalf("buffered=%d dropped=%d", s.buf.len(), s.Dropped())
	}
	if len(c.send) != 0 {
		t.Fatal("parked session must not enqueue")
	}
}

func TestDeliverFullQueuePolicies(t *testing.T) {
	st := testStore(&fakeClock{t: time.Unix(0, 0)}, &releaseLog{}, 5)

	// Drop: connection survives, no seq consumed, nothing buffered for the dropped frame.
	c := testClient(nil, 1)
	s := openFresh(t, st, c, "")
	c.TrySend(frame(1))
	if !c.TrySendPolicy(frame(2), PolicyDrop) {
		t.Fatal("drop must keep the connection")
	}
	if s.next != 2 || s.buf.len() != 1 || c.dropped.Load() != 1 {
		t.Fatalf("next=%d buf=%d dropped=%d", s.next, s.buf.len(), c.dropped.Load())
	}

	// Volatile: dropped, never sequenced.
	if !c.TrySendPolicy(frame(3), PolicyVolatile) || s.next != 2 || c.dropped.Load() != 2 {
		t.Fatal("volatile must be dropped without a seq")
	}
	drain(c)
	if !c.TrySendPolicy(frame(4), PolicyVolatile) {
		t.Fatal("volatile with room must succeed")
	}
	if got := drain(c); len(got) != 1 || strings.Contains(got[0], `"seq"`) {
		t.Fatalf("volatile frame must carry no seq: %v", got)
	}

	// Disconnect: frame persisted for replay, caller told to drop the peer, later frames buffer-only.
	c.TrySend(frame(5)) // fills the queue again (cap 1)
	if c.TrySend(frame(6)) {
		t.Fatal("full queue under disconnect policy must return false")
	}
	if !s.stalled || s.buf.len() != 3 {
		t.Fatalf("stalled=%v buf=%d", s.stalled, s.buf.len())
	}
	drain(c)
	if !c.TrySend(frame(7)) || len(c.send) != 0 {
		t.Fatal("stalled session must buffer without enqueueing (avoids wire gaps)")
	}
	if s.buf.len() != 4 {
		t.Fatalf("buf=%d", s.buf.len())
	}
}

func TestDeliverClosedChannelKeepsFrame(t *testing.T) {
	st := testStore(&fakeClock{t: time.Unix(0, 0)}, &releaseLog{}, 5)
	c := testClient(nil, 2)
	s := openFresh(t, st, c, "")
	c.ShutdownResumable()
	if !c.TrySend(frame(1)) {
		t.Fatal("closed transport with a session must not trigger another disconnect")
	}
	if s.buf.len() != 1 || !s.stalled {
		t.Fatal("frame must be retained for resume")
	}
	if !c.TrySendPolicy(frame(2), PolicyVolatile) {
		t.Fatal("volatile after stall")
	}
}

func TestTrySendPolicyWithoutSession(t *testing.T) {
	c := testClient(nil, 1)
	if !c.TrySend(frame(1)) {
		t.Fatal("first send")
	}
	if c.TrySend(frame(2)) {
		t.Fatal("full queue + disconnect policy must return false (legacy behaviour)")
	}
	if !c.TrySendPolicy(frame(2), PolicyDrop) || !c.TrySendPolicy(frame(3), PolicyVolatile) {
		t.Fatal("drop/volatile keep the connection")
	}
	if c.GetStats().MessagesDropped != 2 {
		t.Fatalf("dropped=%d", c.GetStats().MessagesDropped)
	}
	c.Shutdown()
	if c.TrySendPolicy(frame(4), PolicyDrop) {
		t.Fatal("closed client must report failure")
	}
}

func TestOpenFreshSendsSessionFrame(t *testing.T) {
	st := testStore(&fakeClock{t: time.Unix(0, 0)}, &releaseLog{}, 5)
	c := testClient(nil, 4)
	res := st.Open(c, ResumeRequest{Requested: true, SessionID: "nope", Owner: "u1"})
	if res.Reason != ReasonUnknownSession || res.Resumed {
		t.Fatalf("%+v", res)
	}
	if len(c.replay) != 1 {
		t.Fatalf("replay=%d", len(c.replay))
	}
	var f struct {
		Event   string
		Payload sessionPayload
	}
	if err := json.Unmarshal(c.replay[0], &f); err != nil {
		t.Fatal(err)
	}
	if f.Event != "$session" || f.Payload.SessionID != res.Session.ID() || f.Payload.Resumed || f.Payload.Reason != ReasonUnknownSession {
		t.Fatalf("%+v", f)
	}
	if res.Session.Owner() != "u1" || res.Session.Member() != c {
		t.Fatal("owner/member not recorded")
	}
}

func TestResumeReplaysGapAndTransfers(t *testing.T) {
	rl := &releaseLog{}
	st := testStore(&fakeClock{t: time.Unix(0, 0)}, rl, 5)
	c1 := testClient(nil, 8)
	s := openFresh(t, st, c1, "u")
	for i := 1; i <= 4; i++ {
		c1.TrySend(frame(i))
	}
	drain(c1)
	c1.MetaSet("user", "alice")
	st.Leave(s, c1, true)
	c1.TrySend(frame(5)) // arrives while parked
	c1.TrySend(frame(6))

	c2 := testClient(nil, 8)
	res := st.Open(c2, ResumeRequest{Requested: true, SessionID: s.ID(), LastSeq: 4, Owner: "u"})
	if !res.Resumed || res.Session != s || res.Replayed != 2 || res.Prev != c1 || res.Displaced != nil {
		t.Fatalf("%+v", res)
	}
	// $session first, then exactly seq 5 and 6, in order.
	if len(c2.replay) != 3 || !strings.Contains(string(c2.replay[0]), `"resumed":true`) ||
		!strings.HasPrefix(string(c2.replay[1]), `{"seq":5,`) || !strings.HasPrefix(string(c2.replay[2]), `{"seq":6,`) {
		t.Fatalf("replay = %q", c2.replay)
	}
	// Acked frames were trimmed.
	if s.buf.len() != 2 {
		t.Fatalf("buf=%d", s.buf.len())
	}
	// New traffic lands on the new connection, after the replay.
	c1.TrySend(frame(7))
	if got := drain(c2); len(got) != 1 || !strings.HasPrefix(got[0], `{"seq":7,`) {
		t.Fatalf("%v", got)
	}
	c2.CopyMetaFrom(res.Prev)
	if v, _ := c2.MetaGet("user"); v != "alice" {
		t.Fatal("meta not carried over")
	}
	if st.Stats().Resumed != 1 || st.Stats().Replayed != 2 || st.Stats().Parked != 0 {
		t.Fatalf("%+v", st.Stats())
	}
}

func TestResumeDisplacesLiveConnection(t *testing.T) {
	st := testStore(&fakeClock{t: time.Unix(0, 0)}, &releaseLog{}, 5)
	c1 := testClient(nil, 4)
	s := openFresh(t, st, c1, "")
	c2 := testClient(nil, 4)
	res := st.Open(c2, ResumeRequest{Requested: true, SessionID: s.ID()})
	if !res.Resumed || res.Displaced != c1 {
		t.Fatalf("%+v", res)
	}
	if st.Leave(s, c1, true) != LeaveDisplaced {
		t.Fatal("old connection must not park or close the session")
	}
}

func TestResumeRefusals(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	rl := &releaseLog{}
	st := testStore(clk, rl, 5)

	mk := func(owner string) (*Session, *Client) {
		c := testClient(nil, 8)
		s := openFresh(t, st, c, owner)
		for i := 1; i <= 6; i++ { // buffer holds 4, so seq 1-2 are evicted
			c.TrySend(frame(i))
		}
		return s, c
	}

	t.Run("owner mismatch keeps the session", func(t *testing.T) {
		s, c := mk("alice")
		st.Leave(s, c, true)
		res := st.Open(testClient(nil, 2), ResumeRequest{Requested: true, SessionID: s.ID(), LastSeq: 6, Owner: "mallory"})
		if res.Resumed || res.Reason != ReasonOwnerMismatch {
			t.Fatalf("%+v", res)
		}
		if st.Open(testClient(nil, 2), ResumeRequest{Requested: true, SessionID: s.ID(), LastSeq: 6, Owner: "alice"}).Resumed != true {
			t.Fatal("legitimate owner must still resume")
		}
	})

	t.Run("gap beyond buffer", func(t *testing.T) {
		before := rl.count()
		s, c := mk("")
		st.Leave(s, c, true)
		res := st.Open(testClient(nil, 2), ResumeRequest{Requested: true, SessionID: s.ID(), LastSeq: 1})
		if res.Resumed || res.Reason != ReasonGap || res.Session == s {
			t.Fatalf("%+v", res)
		}
		if rl.count() != before+1 || rl.why[len(rl.why)-1] != ReleaseReplaced {
			t.Fatal("old session must be released as replaced")
		}
	})

	t.Run("future seq", func(t *testing.T) {
		s, c := mk("")
		st.Leave(s, c, true)
		res := st.Open(testClient(nil, 2), ResumeRequest{Requested: true, SessionID: s.ID(), LastSeq: 99})
		if res.Resumed || res.Reason != ReasonBadSeq {
			t.Fatalf("%+v", res)
		}
	})

	t.Run("expired before sweep", func(t *testing.T) {
		s, c := mk("")
		st.Leave(s, c, true)
		clk.advance(2 * time.Minute)
		res := st.Open(testClient(nil, 2), ResumeRequest{Requested: true, SessionID: s.ID(), LastSeq: 6})
		if res.Resumed || res.Reason != ReasonExpired {
			t.Fatalf("%+v", res)
		}
	})

	t.Run("malformed request", func(t *testing.T) {
		res := st.Open(testClient(nil, 2), ResumeRequest{Requested: true, Invalid: true, SessionID: "x"})
		if res.Reason != ReasonBadRequest || res.Session == nil {
			t.Fatalf("%+v", res)
		}
	})

	t.Run("nothing missed resumes with empty buffer", func(t *testing.T) {
		c := testClient(nil, 2)
		s := openFresh(t, st, c, "")
		st.Leave(s, c, true)
		res := st.Open(testClient(nil, 2), ResumeRequest{Requested: true, SessionID: s.ID(), LastSeq: 0})
		if !res.Resumed || res.Replayed != 0 {
			t.Fatalf("%+v", res)
		}
	})
}

func TestLeaveTerminalAndClosed(t *testing.T) {
	st := testStore(&fakeClock{t: time.Unix(0, 0)}, &releaseLog{}, 5)
	c := testClient(nil, 4)
	s := openFresh(t, st, c, "")
	if st.Leave(s, c, false) != LeaveGone {
		t.Fatal("non-resumable leave must close the session")
	}
	if st.Stats().Sessions != 0 {
		t.Fatal("session still stored")
	}
	if st.Leave(s, c, true) != LeaveGone {
		t.Fatal("leaving a closed session is gone")
	}
	if !c.TrySend(frame(1)) {
		t.Fatal("delivery to a closed session must be a quiet no-op")
	}
	res := st.Open(testClient(nil, 2), ResumeRequest{Requested: true, SessionID: s.ID()})
	if res.Reason != ReasonUnknownSession {
		t.Fatalf("kicked peer must not resume: %+v", res)
	}
}

func TestParkedCapEvictsOldest(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	rl := &releaseLog{}
	st := testStore(clk, rl, 2)
	var ids []string
	for i := 0; i < 3; i++ {
		c := testClient(nil, 2)
		s := openFresh(t, st, c, "")
		ids = append(ids, s.ID())
		st.Leave(s, c, true)
		clk.advance(time.Second)
	}
	if st.Stats().Parked != 2 || rl.count() != 1 || rl.ids[0] != ids[0] || rl.why[0] != ReleaseEvicted {
		t.Fatalf("parked=%d released=%v", st.Stats().Parked, rl.ids)
	}
}

func TestSweepExpiresParked(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	rl := &releaseLog{}
	st := testStore(clk, rl, 10)
	c1, c2 := testClient(nil, 2), testClient(nil, 2)
	s1 := openFresh(t, st, c1, "")
	st.Leave(s1, c1, true)
	clk.advance(40 * time.Second)
	s2 := openFresh(t, st, c2, "")
	st.Leave(s2, c2, true)
	clk.advance(30 * time.Second) // s1 is 70s old, s2 is 30s

	st.Sweep()
	if rl.count() != 1 || rl.ids[0] != s1.ID() || rl.why[0] != ReleaseExpired || st.Stats().Parked != 1 {
		t.Fatalf("released=%v parked=%d", rl.ids, st.Stats().Parked)
	}
}

func TestStoreRunSweepsAndStops(t *testing.T) {
	rl := &releaseLog{}
	st := NewSessionStore(SessionOptions{TTL: 20 * time.Millisecond, Release: rl.release})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { st.Run(ctx); close(done) }()

	c := testClient(nil, 2)
	s := openFresh(t, st, c, "")
	st.Leave(s, c, true)
	waitUntil(t, func() bool { return rl.count() == 1 })
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not stop")
	}
}

func TestStoreDefaultsAndDuplicates(t *testing.T) {
	st := NewSessionStore(SessionOptions{})
	if st.opt.ReplayMessages != 256 || st.opt.ReplayBytes != 256<<10 || st.opt.TTL != 2*time.Minute ||
		st.opt.MaxParked != 10000 || st.opt.KeyWindow != 1024 || st.opt.Now == nil {
		t.Fatalf("%+v", st.opt)
	}
	st.NoteDuplicate()
	if st.Stats().Duplicates != 1 {
		t.Fatal("duplicate not counted")
	}
	// Release is optional.
	st.releaseOne(&Session{}, ReleaseExpired)
}

func TestSessionAckIgnoresFutureSeq(t *testing.T) {
	st := testStore(&fakeClock{t: time.Unix(0, 0)}, &releaseLog{}, 5)
	c := testClient(nil, 4)
	s := openFresh(t, st, c, "")
	c.TrySend(frame(1))
	c.TrySend(frame(2))
	s.Ack(100) // beyond head: must not wipe unseen frames
	if s.buf.len() != 2 {
		t.Fatal("bogus ack trimmed the buffer")
	}
	s.Ack(1)
	if s.buf.len() != 1 {
		t.Fatal("ack did not trim")
	}
}

func TestReadPumpParksRecoverableClient(t *testing.T) {
	h := NewHub(nil, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)

	st := testStore(&fakeClock{t: time.Unix(0, 0)}, &releaseLog{}, 5)
	ft := newFakeTransport()
	done := make(chan struct{})
	c := NewClient(h, ft, ClientOptions{ID: "c", SendBuffer: 4, OnDone: func() { close(done) }})
	h.Register(c)
	h.Join(c, "r")
	s := openFresh(t, st, c, "")
	waitUntil(t, func() bool { return h.RoomSize("r") == 1 })

	go c.ReadPump()
	close(ft.reads) // network drop
	<-done

	if !c.Parked() || st.Stats().Parked != 1 {
		t.Fatal("client should be parked")
	}
	if h.RoomSize("r") != 0 {
		t.Fatal("parked members must not count as present")
	}
	if len(h.RoomClients("r")) != 0 {
		t.Fatal("parked members must be hidden from room views")
	}
	// Room broadcast still reaches the parked session's buffer.
	h.Emit(EmitJob{Room: "r", Data: frame(1)})
	waitUntil(t, func() bool { return nextSeq(s) == 2 })

	// Resume: memberships move to the new connection and the buffered frame replays.
	c2 := testClient(h, 4)
	h.Register(c2)
	res := st.Open(c2, ResumeRequest{Requested: true, SessionID: s.ID()})
	h.Transfer(res.Prev, c2)
	waitUntil(t, func() bool { return h.RoomSize("r") == 1 })
	if !res.Resumed || res.Replayed != 1 {
		t.Fatalf("%+v", res)
	}
	h.Emit(EmitJob{Room: "r", Data: frame(2)})
	waitUntil(t, func() bool { return len(c2.send) == 1 })
}

func TestReadPumpTerminalShutdownClosesSession(t *testing.T) {
	h := NewHub(nil, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)

	st := testStore(&fakeClock{t: time.Unix(0, 0)}, &releaseLog{}, 5)
	ft := newFakeTransport()
	done := make(chan struct{})
	c := NewClient(h, ft, ClientOptions{ID: "c", SendBuffer: 4, OnDone: func() { close(done) }})
	h.Register(c)
	s := openFresh(t, st, c, "")
	go c.ReadPump()
	c.Shutdown() // ctx.Disconnect()
	close(ft.reads)
	<-done
	if st.Stats().Sessions != 0 || c.Parked() {
		t.Fatal("server-initiated disconnect must terminate the session")
	}
	_ = s
}

func TestHubTransferNoops(t *testing.T) {
	h := NewHub(nil, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)
	c := testClient(h, 2)
	h.Transfer(nil, c)
	h.Transfer(c, nil)
	h.Transfer(c, c)
	cancel()
	h.Wait()
	h.Transfer(c, testClient(h, 2)) // closed hub must not block
}

func TestHubEmitsDroppedCounter(t *testing.T) {
	h := NewHub(nil, 1)
	h.Emit(EmitJob{Room: "x", Data: []byte("1")})
	h.Emit(EmitJob{Room: "x", Data: []byte("2")})
	if h.EmitsDropped() != 1 {
		t.Fatalf("dropped=%d", h.EmitsDropped())
	}
}

func TestHubPolicyDropKeepsSlowClient(t *testing.T) {
	h := NewHub(nil, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)
	ft := newFakeTransport()
	c := NewClient(h, ft, ClientOptions{ID: "c", SendBuffer: 1})
	h.Register(c)
	h.Join(c, "r")
	waitUntil(t, func() bool { return h.RoomSize("r") == 1 })

	c.TrySend([]byte("fill"))
	h.Emit(EmitJob{Room: "r", Data: []byte("x"), Policy: PolicyDrop})
	waitUntil(t, func() bool { return c.dropped.Load() == 1 })
	if ft.closed.Load() {
		t.Fatal("drop policy must not disconnect")
	}
}
