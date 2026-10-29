// Copyright (c) 2026 Dibyaranjan Pradhan. All rights reserved.

package internal

import (
	"container/list"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Session is the durable identity behind a logical connection. A Client (one
// transport) is disposable; the Session outlives it so that a reconnecting peer
// can pick up where it left off. All fields below mu are guarded by it.
//
// Lock order: SessionStore.mu -> Session.mu. deliver never takes the store lock.
type Session struct {
	id    string
	owner string
	store *SessionStore

	mu      sync.Mutex
	state   sessionState
	next    uint64 // next seq to assign; seqs start at 1
	buf     replayBuffer
	live    *Client // transport currently bound, nil while parked
	member  *Client // client registered in the hub on this session's behalf
	stalled bool    // live queue overflowed; buffer-only until the client detaches
	parked  time.Time
	keys    keyWindow

	elem    *list.Element // parked-list position; guarded by SessionStore.mu
	dropped atomic.Int64
}

type sessionState uint8

const (
	stateAttached sessionState = iota
	stateParked
	stateClosed
)

// SendPolicy selects what happens to an outbound frame when the peer cannot take it.
type SendPolicy uint8

const (
	// PolicyDisconnect keeps the frame reliable: a full queue disconnects the slow
	// peer (and, with recovery enabled, the frame is replayed after it reconnects).
	// It is the zero value and matches the pre-recovery behaviour.
	PolicyDisconnect SendPolicy = iota
	// PolicyDrop keeps the connection and discards the frame when the queue is full.
	// Frames that do get queued are sequenced and replayable.
	PolicyDrop
	// PolicyVolatile is best-effort: never sequenced, never buffered, never replayed.
	PolicyVolatile
)

// Resume refusal reasons, surfaced to the client in the $session frame.
const (
	ReasonUnknownSession = "unknown_session"
	ReasonExpired        = "expired"
	ReasonOwnerMismatch  = "owner_mismatch"
	ReasonGap            = "gap_exceeded"
	ReasonBadSeq         = "bad_seq"
	ReasonBadRequest     = "bad_request"
)

// ReleaseReason says why a parked session left the store.
type ReleaseReason uint8

const (
	// ReleaseExpired: parked longer than the TTL.
	ReleaseExpired ReleaseReason = iota
	// ReleaseEvicted: dropped to honour MaxParked.
	ReleaseEvicted
	// ReleaseReplaced: the peer came back but could not resume, so a fresh session took over.
	ReleaseReplaced
)

// maxKeyLen bounds memory per idempotency key; longer keys are hashed.
const maxKeyLen = 128

// ID returns the session token. It is a bearer secret; do not log it.
func (s *Session) ID() string { return s.id }

// Owner returns the principal the session was bound to at creation ("" if unbound).
func (s *Session) Owner() string { return s.owner }

// Member returns the client currently registered in the hub for this session.
func (s *Session) Member() *Client {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.member
}

// Dropped returns frames discarded by PolicyDrop/PolicyVolatile while parked or stalled.
func (s *Session) Dropped() int64 { return s.dropped.Load() }

// Ack releases buffered frames the peer confirmed (seq and below).
func (s *Session) Ack(seq uint64) {
	s.mu.Lock()
	if seq < s.next {
		s.buf.ackUpTo(seq)
	}
	s.mu.Unlock()
}

// ClaimKey records an idempotency key. It reports false when the key was already
// seen inside the dedupe window.
func (s *Session) ClaimKey(key string) bool {
	if len(key) > maxKeyLen {
		sum := sha256.Sum256([]byte(key))
		key = hex.EncodeToString(sum[:])
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.keys.claim(key)
}

// deliver is the single outbound path for a session-bound client. Sequence
// assignment and queueing happen under one lock so seq order equals wire order.
// It returns false only when the caller should disconnect the slow peer.
func (s *Session) deliver(data []byte, pol SendPolicy) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.state == stateClosed {
		return true
	}
	live := s.live
	offline := live == nil || s.stalled

	if pol == PolicyVolatile {
		if offline {
			s.dropped.Add(1)
			return true
		}
		if live.enqueue(data) != enqOK {
			live.dropped.Add(1)
		}
		return true
	}

	seq := s.next
	if offline {
		s.commit(seq, data)
		return true
	}

	switch live.enqueue(stamp(data, seq)) {
	case enqOK:
		s.commit(seq, data)
		return true
	case enqClosed:
		// Transport is already going away; keep the frame for the resume.
		s.commit(seq, data)
		s.stalled = true
		return true
	}

	if pol == PolicyDrop {
		live.dropped.Add(1)
		return true
	}
	// Disconnect: persist first so the frame survives the disconnect we are about to cause.
	s.commit(seq, data)
	s.stalled = true
	return false
}

func (s *Session) commit(seq uint64, data []byte) {
	s.next = seq + 1
	s.buf.push(seq, data)
}

// verdict decides whether a resume request can be honoured. Callers hold s.mu.
// kill reports that the session is unusable for this peer and should be discarded.
func (s *Session) verdict(req ResumeRequest, now time.Time, ttl time.Duration) (reason string, kill bool) {
	switch {
	case s.state == stateClosed:
		return ReasonUnknownSession, false
	case s.owner != req.Owner:
		// Keep the session: the legitimate owner may still come back.
		return ReasonOwnerMismatch, false
	case s.state == stateParked && now.Sub(s.parked) > ttl:
		return ReasonExpired, true
	case req.LastSeq >= s.next:
		return ReasonBadSeq, true
	case req.LastSeq < s.next-1 && !s.buf.covers(req.LastSeq):
		return ReasonGap, true
	}
	return "", false
}

// stamp splices "seq" into a JSON object frame without re-marshalling the payload.
// Broadcast frames are marshalled once and shared, so the copy here is per recipient.
func stamp(data []byte, seq uint64) []byte {
	if len(data) < 2 || data[0] != '{' {
		return data
	}
	out := make([]byte, 0, len(data)+24)
	out = append(out, `{"seq":`...)
	out = strconv.AppendUint(out, seq, 10)
	if data[1] != '}' {
		out = append(out, ',')
	}
	return append(out, data[1:]...)
}

// replayEntry shares the broadcast backing array; only the 32-byte header is per session.
type replayEntry struct {
	seq  uint64
	data []byte
}

// replayBuffer holds a contiguous run of sequenced frames, oldest first. Both
// limits are hard caps: the oldest frames are evicted to stay under them.
type replayBuffer struct {
	entries  []replayEntry
	head     int
	bytes    int64
	maxMsgs  int
	maxBytes int64
}

func (b *replayBuffer) len() int { return len(b.entries) - b.head }

func (b *replayBuffer) push(seq uint64, data []byte) {
	size := int64(len(data))
	if size > b.maxBytes {
		// A hole in the sequence makes older frames unreplayable across it.
		b.reset()
		return
	}
	b.entries = append(b.entries, replayEntry{seq: seq, data: data})
	b.bytes += size
	for b.len() > b.maxMsgs || b.bytes > b.maxBytes {
		b.dropOldest()
	}
}

func (b *replayBuffer) reset() {
	clear(b.entries)
	b.entries = b.entries[:0]
	b.head = 0
	b.bytes = 0
}

func (b *replayBuffer) dropOldest() {
	e := b.entries[b.head]
	b.bytes -= int64(len(e.data))
	b.entries[b.head] = replayEntry{}
	b.head++
	if b.head == len(b.entries) {
		b.entries = b.entries[:0]
		b.head = 0
		return
	}
	if b.head >= 64 && b.head*2 >= len(b.entries) {
		n := copy(b.entries, b.entries[b.head:])
		clear(b.entries[n:])
		b.entries = b.entries[:n]
		b.head = 0
	}
}

func (b *replayBuffer) ackUpTo(seq uint64) {
	for b.len() > 0 && b.entries[b.head].seq <= seq {
		b.dropOldest()
	}
}

// covers reports whether every frame after lastSeq is still buffered.
func (b *replayBuffer) covers(lastSeq uint64) bool {
	return b.len() > 0 && b.entries[b.head].seq <= lastSeq+1
}

func (b *replayBuffer) after(seq uint64) []replayEntry {
	var out []replayEntry
	for _, e := range b.entries[b.head:] {
		if e.seq > seq {
			out = append(out, e)
		}
	}
	return out
}

// keyWindow is a FIFO-bounded set for idempotency keys.
type keyWindow struct {
	max   int
	set   map[string]struct{}
	order []string
	head  int
}

func (k *keyWindow) claim(key string) bool {
	if k.max <= 0 {
		return true
	}
	if _, dup := k.set[key]; dup {
		return false
	}
	if k.set == nil {
		k.set = make(map[string]struct{})
	}
	k.set[key] = struct{}{}
	k.order = append(k.order, key)
	if len(k.order)-k.head > k.max {
		delete(k.set, k.order[k.head])
		k.order[k.head] = ""
		k.head++
		if k.head >= 64 && k.head*2 >= len(k.order) {
			n := copy(k.order, k.order[k.head:])
			clear(k.order[n:])
			k.order = k.order[:n]
			k.head = 0
		}
	}
	return true
}

// SessionOptions configures a SessionStore. Zero numeric fields fall back to safe defaults.
type SessionOptions struct {
	ReplayMessages int
	ReplayBytes    int64
	TTL            time.Duration
	MaxParked      int
	KeyWindow      int
	// Release runs outside all locks when a parked session is discarded.
	Release func(s *Session, why ReleaseReason)
	// Now is injectable for tests.
	Now func() time.Time
}

// SessionStats is a point-in-time view of the store.
type SessionStats struct {
	Sessions   int
	Parked     int
	Resumed    int64
	Replayed   int64
	Duplicates int64
}

// SessionStore is the process-local registry of resumable sessions. Memory is
// bounded by (live sessions + MaxParked) x ReplayBytes; multi-node replay needs an
// external store and is out of scope here.
type SessionStore struct {
	opt SessionOptions

	mu       sync.Mutex
	sessions map[string]*Session
	parked   *list.List // oldest park time at the front

	resumed  atomic.Int64
	replayed atomic.Int64
	dupes    atomic.Int64
}

// NewSessionStore builds a store; start Run in a goroutine to enable TTL sweeps.
func NewSessionStore(opt SessionOptions) *SessionStore {
	if opt.ReplayMessages <= 0 {
		opt.ReplayMessages = 256
	}
	if opt.ReplayBytes <= 0 {
		opt.ReplayBytes = 256 << 10
	}
	if opt.TTL <= 0 {
		opt.TTL = 2 * time.Minute
	}
	if opt.MaxParked <= 0 {
		opt.MaxParked = 10000
	}
	if opt.KeyWindow <= 0 {
		opt.KeyWindow = 1024
	}
	if opt.Now == nil {
		opt.Now = time.Now
	}
	return &SessionStore{
		opt:      opt,
		sessions: make(map[string]*Session),
		parked:   list.New(),
	}
}

// ResumeRequest is what the peer asked for during the handshake.
type ResumeRequest struct {
	// Requested is true when the peer presented a session token.
	Requested bool
	// Invalid marks a malformed resume request (bad seq); treated as a refusal.
	Invalid   bool
	SessionID string
	LastSeq   uint64
	// Owner is the principal derived from the handshake request ("" when unbound).
	Owner string
}

// OpenResult describes how a client was bound to a session.
type OpenResult struct {
	Session *Session
	Resumed bool
	// Reason is set when a resume was requested but refused.
	Reason string
	// Replayed is the number of buffered frames queued for the client.
	Replayed int
	// Prev is the hub member that held the session's rooms; the caller transfers them.
	Prev *Client
	// Displaced is a still-live older connection that must be closed.
	Displaced *Client
}

// Open binds c to a resumed or brand-new session and primes c's pre-queue with the
// $session frame followed by any replay. Call it only after the connection has
// passed authentication so a rejected handshake cannot displace a live peer.
func (st *SessionStore) Open(c *Client, req ResumeRequest) OpenResult {
	now := st.opt.Now()
	var res OpenResult
	var victim *Session

	st.mu.Lock()
	switch {
	case req.Invalid:
		res.Reason = ReasonBadRequest
	case req.Requested:
		s := st.sessions[req.SessionID]
		if s == nil {
			res.Reason = ReasonUnknownSession
			break
		}
		s.mu.Lock()
		reason, kill := s.verdict(req, now, st.opt.TTL)
		if reason == "" {
			st.attachLocked(s, c, req.LastSeq, &res)
			s.mu.Unlock()
			st.mu.Unlock()
			st.resumed.Add(1)
			st.replayed.Add(int64(res.Replayed))
			return res
		}
		if kill {
			s.state = stateClosed
			st.removeLocked(s)
			victim = s
		}
		s.mu.Unlock()
		res.Reason = reason
	}

	id := newSessionID()
	if id == "" {
		// No entropy: serve the peer without recovery rather than with a guessable token.
		st.mu.Unlock()
		st.releaseOne(victim, ReleaseReplaced)
		return res
	}
	s := &Session{
		id:     id,
		owner:  req.Owner,
		store:  st,
		next:   1,
		live:   c,
		member: c,
		buf:    replayBuffer{maxMsgs: st.opt.ReplayMessages, maxBytes: st.opt.ReplayBytes},
		keys:   keyWindow{max: st.opt.KeyWindow},
	}
	st.sessions[id] = s
	res.Session = s
	c.session.Store(s)
	c.replay = [][]byte{sessionFrame(id, false, 0, 0, res.Reason)}
	st.mu.Unlock()

	st.releaseOne(victim, ReleaseReplaced)
	return res
}

// attachLocked rebinds s to c. Callers hold st.mu and s.mu.
func (st *SessionStore) attachLocked(s *Session, c *Client, lastSeq uint64, res *OpenResult) {
	s.buf.ackUpTo(lastSeq)
	entries := s.buf.after(lastSeq)

	frames := make([][]byte, 0, len(entries)+1)
	frames = append(frames, sessionFrame(s.id, true, len(entries), s.next-1, ""))
	for _, e := range entries {
		frames = append(frames, stamp(e.data, e.seq))
	}

	res.Session = s
	res.Resumed = true
	res.Replayed = len(entries)
	res.Prev = s.member
	if s.live != nil && s.live != c {
		res.Displaced = s.live
	}

	s.live, s.member = c, c
	s.state = stateAttached
	s.stalled = false
	if s.elem != nil {
		st.parked.Remove(s.elem)
		s.elem = nil
	}
	c.session.Store(s)
	c.replay = frames
}

// LeaveResult tells the departing client what to do with its hub membership.
type LeaveResult uint8

const (
	// LeaveGone: the session is closed; unregister the client from the hub.
	LeaveGone LeaveResult = iota
	// LeaveParked: keep the client registered so rooms keep buffering for the resume.
	LeaveParked
	// LeaveDisplaced: a newer connection owns the session; do nothing.
	LeaveDisplaced
)

// Leave is called when c's transport ends. A non-resumable leave (server-initiated
// disconnect) terminates the session so a kicked peer cannot resume.
func (st *SessionStore) Leave(s *Session, c *Client, resumable bool) LeaveResult {
	now := st.opt.Now()
	st.mu.Lock()
	s.mu.Lock()
	switch {
	case s.state == stateClosed:
		s.mu.Unlock()
		st.mu.Unlock()
		return LeaveGone
	case s.live != c:
		s.mu.Unlock()
		st.mu.Unlock()
		return LeaveDisplaced
	case !resumable:
		s.state = stateClosed
		s.live = nil
		st.removeLocked(s)
		s.mu.Unlock()
		st.mu.Unlock()
		return LeaveGone
	}

	s.live = nil
	s.stalled = false
	s.state = stateParked
	s.parked = now
	c.parked.Store(true)
	s.elem = st.parked.PushBack(s)
	s.mu.Unlock()

	var evicted []*Session
	for st.parked.Len() > st.opt.MaxParked {
		v := st.parked.Front().Value.(*Session)
		v.mu.Lock()
		v.state = stateClosed
		v.mu.Unlock()
		st.removeLocked(v)
		evicted = append(evicted, v)
	}
	st.mu.Unlock()

	for _, v := range evicted {
		st.releaseOne(v, ReleaseEvicted)
	}
	return LeaveParked
}

// Sweep discards parked sessions older than the TTL.
func (st *SessionStore) Sweep() {
	now := st.opt.Now()
	var expired []*Session

	st.mu.Lock()
	for e := st.parked.Front(); e != nil; e = st.parked.Front() {
		s := e.Value.(*Session)
		s.mu.Lock()
		old := now.Sub(s.parked) > st.opt.TTL
		if old {
			s.state = stateClosed
		}
		s.mu.Unlock()
		if !old {
			break
		}
		st.removeLocked(s)
		expired = append(expired, s)
	}
	st.mu.Unlock()

	for _, s := range expired {
		st.releaseOne(s, ReleaseExpired)
	}
}

// Run sweeps expired sessions until ctx is cancelled.
func (st *SessionStore) Run(ctx context.Context) {
	every := st.opt.TTL / 4
	if every < 10*time.Millisecond {
		every = 10 * time.Millisecond
	}
	if every > 5*time.Second {
		every = 5 * time.Second
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			st.Sweep()
		}
	}
}

// Stats returns a snapshot of store counters.
func (st *SessionStore) Stats() SessionStats {
	st.mu.Lock()
	n, p := len(st.sessions), st.parked.Len()
	st.mu.Unlock()
	return SessionStats{
		Sessions:   n,
		Parked:     p,
		Resumed:    st.resumed.Load(),
		Replayed:   st.replayed.Load(),
		Duplicates: st.dupes.Load(),
	}
}

// NoteDuplicate counts a suppressed duplicate inbound message.
func (st *SessionStore) NoteDuplicate() { st.dupes.Add(1) }

func (st *SessionStore) removeLocked(s *Session) {
	delete(st.sessions, s.id)
	if s.elem != nil {
		st.parked.Remove(s.elem)
		s.elem = nil
	}
}

func (st *SessionStore) releaseOne(s *Session, why ReleaseReason) {
	if s != nil && st.opt.Release != nil {
		st.opt.Release(s, why)
	}
}

func newSessionID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return ""
	}
	return hex.EncodeToString(b[:])
}

type sessionPayload struct {
	SessionID string `json:"sessionId"`
	Resumed   bool   `json:"resumed"`
	Replayed  int    `json:"replayed"`
	LastSeq   uint64 `json:"lastSeq"`
	Reason    string `json:"reason,omitempty"`
}

// sessionFrame is the control frame sent first on every recovery-enabled connection.
// It carries no seq: control frames are never replayed.
func sessionFrame(id string, resumed bool, replayed int, lastSeq uint64, reason string) []byte {
	b, _ := json.Marshal(struct {
		Event   string         `json:"event"`
		Payload sessionPayload `json:"payload"`
	}{"$session", sessionPayload{id, resumed, replayed, lastSeq, reason}})
	return b
}
