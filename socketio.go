// Copyright (c) 2026 Dibyaranjan Pradhan. All rights reserved.

package gosocket

import (
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/dibyaranjan-pradhan/go-socket/internal"
	"github.com/dibyaranjan-pradhan/go-socket/internal/socketio"
)

// Socket.IO is served from a second Engine.IO mount rather than by sniffing the
// existing handlers: legacy clients expect OnConnect at open and plain JSON frames,
// while Socket.IO clients must CONNECT first. Both mounts feed the same hub,
// middleware and handlers, so one codebase serves both dialects.

// reservedEvents are Socket.IO lifecycle names a peer must not trigger as events.
var reservedEvents = map[string]struct{}{
	"connect": {}, "connect_error": {}, "disconnect": {}, "disconnecting": {},
	"newListener": {}, "removeListener": {},
}

// SocketIOHandler returns an http.Handler speaking Socket.IO v5 over Engine.IO v4
// (polling and WebSocket, including the upgrade). Mount it at "/socket.io/", the
// official client's default path. Handler and EngineIOHandler are unaffected and
// legacy clients keep working; handlers registered with On serve both dialects.
// Session recovery (Config.SessionRecovery) applies to legacy connections only.
func (s *Server) SocketIOHandler() http.Handler {
	s.sioOnce.Do(func() {
		s.sioHandler = internal.NewEngineIOHandler(
			s.cfg.PingInterval,
			s.cfg.PongWait,
			s.cfg.MaxMessageSize,
			s.wsUpgrader,
			s.attachSocketIO,
			func() bool { return s.closed.Load() },
		)
		s.sioMounted.Store(true)
	})
	return s.sioHandler
}

func (s *Server) attachSocketIO(transport internal.Transport, r *http.Request) {
	if s.closed.Load() || int(s.conns.Load()) >= s.cfg.MaxConnections {
		_ = transport.Close()
		return
	}
	s.conns.Add(1)
	id := randomClientID()
	conn := &sioConn{s: s, r: r, sockets: make(map[string]*ioSocket)}
	opt := internal.ClientOptions{
		ID:             id,
		MaxMessageSize: s.cfg.MaxMessageSize,
		WriteWait:      s.cfg.WriteWait,
		PingPeriod:     s.cfg.PingInterval,
		PongWait:       s.cfg.PongWait,
		SendBuffer:     s.cfg.WriteBufferSize,
	}
	opt.OnMessage = conn.handlePacket
	opt.OnDone = func() {
		s.conns.Add(-1)
		s.clientsByID.Delete(id)
		s.connStats.Delete(id)
		conn.closeAll()
	}
	opt.OnHeartbeatTimeout = func() { s.fireHeartbeatTimeout(conn.c, r) }
	ic := internal.NewClient(s.hub, transport, opt)
	ic.SetDialect(internal.DialectSocketIO)
	conn.c = ic

	s.hub.Register(ic)
	s.clientsByID.Store(id, ic)
	s.totalConnectionsEver.Add(1)
	conn.armConnectTimer()

	go ic.WritePump()
	go ic.ReadPump()
}

// sioConn is one Engine.IO connection multiplexing any number of namespace sockets.
type sioConn struct {
	s *Server
	c *internal.Client
	r *http.Request

	mu      sync.Mutex
	sockets map[string]*ioSocket
	closed  bool
	timer   *time.Timer
}

// armConnectTimer drops connections that open Engine.IO but never CONNECT, which
// would otherwise hold a slot against MaxConnections indefinitely.
func (cn *sioConn) armConnectTimer() {
	cn.mu.Lock()
	defer cn.mu.Unlock()
	cn.timer = time.AfterFunc(cn.s.cfg.SocketIO.ConnectTimeout, func() {
		cn.mu.Lock()
		idle := len(cn.sockets) == 0 && !cn.closed
		cn.mu.Unlock()
		if idle {
			cn.c.Shutdown()
		}
	})
}

func (cn *sioConn) socket(ns string) *ioSocket {
	cn.mu.Lock()
	defer cn.mu.Unlock()
	return cn.sockets[ns]
}

// send queues a frame under the server's backpressure policy; a client that cannot
// keep up is dropped rather than allowed to stall the hub.
func (cn *sioConn) send(frame []byte, policy internal.SendPolicy) {
	if !cn.c.TrySendPolicy(frame, policy) {
		cn.s.diag.Printf("gosocket: [WARN] emit queue full: clientID=%s", cn.c.ID())
		go cn.c.Shutdown()
	}
}

// handlePacket runs on the connection's read pump, so packets for one connection
// are processed strictly in order.
func (cn *sioConn) handlePacket(raw []byte) {
	p, err := socketio.Decode(raw)
	if err != nil {
		// socket.io closes on a malformed packet; so do we.
		cn.c.Shutdown()
		return
	}
	switch p.Type {
	case socketio.Connect:
		cn.onConnectPacket(p)
	case socketio.Disconnect:
		if sk := cn.socket(p.Namespace); sk != nil {
			sk.teardown(true)
		}
	case socketio.Event:
		cn.onEvent(p)
	case socketio.Ack:
		cn.onAck(p)
	case socketio.BinaryEvent, socketio.BinaryAck:
		// Binary attachments arrive with M6; refuse cleanly instead of desyncing the stream.
		cn.s.diag.Printf("gosocket: binary Socket.IO packets are not supported yet: clientID=%s", cn.c.ID())
	default:
		cn.c.Shutdown()
	}
}

func (cn *sioConn) onConnectPacket(p socketio.Packet) {
	s := cn.s
	ns := s.lookupNamespace(p.Namespace)
	if ns == nil {
		cn.sendConnectError(p.Namespace, "Invalid namespace", nil)
		return
	}
	if cn.socket(ns.name) != nil {
		return
	}

	sk := &ioSocket{conn: cn, ns: ns, sid: randomClientID(), pending: make(map[int]*pendingAck)}
	if len(p.Data) > 0 {
		sk.auth = append(json.RawMessage(nil), p.Data...)
	}
	ctx := s.newContext(cn.c, cn.r, EventConnect, nil, "", "")
	ctx.sock = sk
	err := s.runGuarded(ns, func() error { return runMiddlewareChain(ctx, ns.chain()) })
	if err != nil {
		cn.sendConnectError(ns.name, errorMessage(err), errorData(err))
		return
	}

	cn.mu.Lock()
	if cn.closed {
		cn.mu.Unlock()
		return
	}
	cn.sockets[ns.name] = sk
	if cn.timer != nil {
		cn.timer.Stop()
	}
	cn.mu.Unlock()

	s.hub.Join(cn.c, ns.memberKey())
	s.hub.Join(cn.c, sidKey(sk.sid))

	body, _ := json.Marshal(map[string]string{"sid": sk.sid})
	if frame, err := socketio.Encode(socketio.Packet{Type: socketio.Connect, Namespace: ns.name, Data: body}); err == nil {
		cn.send(frame, internal.PolicyDisconnect)
	}
	for _, fn := range ns.connectCallbacks() {
		if fn != nil {
			fn(ctx)
		}
	}
}

func (cn *sioConn) sendConnectError(nsName, msg string, data interface{}) {
	body := struct {
		Message string      `json:"message"`
		Data    interface{} `json:"data,omitempty"`
	}{msg, data}
	b, err := json.Marshal(body)
	if err != nil {
		b = []byte(`{"message":"rejected"}`)
	}
	if frame, err := socketio.Encode(socketio.Packet{Type: socketio.ConnectError, Namespace: nsName, Data: b}); err == nil {
		cn.send(frame, internal.PolicyDisconnect)
	}
}

// errorMessage exposes only what the middleware author chose to expose.
func errorMessage(err error) string {
	var ce *ConnectError
	if errors.As(err, &ce) && ce.Message != "" {
		return ce.Message
	}
	return "rejected"
}

func errorData(err error) interface{} {
	var ce *ConnectError
	if errors.As(err, &ce) {
		return ce.Data
	}
	return nil
}

// runGuarded wraps fn in panic recovery when Recover() is registered globally or on ns.
func (s *Server) runGuarded(ns *Namespace, fn func() error) (err error) {
	if !ns.recovers() {
		return fn()
	}
	defer func() {
		if rec := recover(); rec != nil {
			s.diag.Printf("gosocket: panic during connect: %v", rec)
			err = ErrRejected
		}
	}()
	return fn()
}

func (cn *sioConn) onEvent(p socketio.Packet) {
	sk := cn.socket(p.Namespace)
	if sk == nil {
		return
	}
	var args []json.RawMessage
	if err := json.Unmarshal(p.Data, &args); err != nil || len(args) == 0 {
		return
	}
	var name string
	if err := json.Unmarshal(args[0], &name); err != nil || name == "" {
		return
	}
	if _, reserved := reservedEvents[name]; reserved {
		return
	}
	s := cn.s
	s.totalMessages.Add(1)

	var payload []byte
	if len(args) > 1 {
		payload = args[1]
	}
	ctx := s.newContext(cn.c, cn.r, name, payload, "", "")
	ctx.sock = sk
	ctx.args = args[1:]
	ctx.ioAckID = p.AckID

	if sk.ns.recovers() {
		defer func() {
			if rec := recover(); rec != nil {
				s.diag.Printf("gosocket: panic in event %q: %v", name, rec)
				ctx.Disconnect()
			}
		}()
	}
	if err := runMiddlewareChain(ctx, sk.ns.chain()); err != nil {
		return
	}
	if h := sk.ns.handler(name); h != nil {
		h(ctx)
	}
}

func (cn *sioConn) onAck(p socketio.Packet) {
	sk := cn.socket(p.Namespace)
	if sk == nil || p.AckID == nil {
		return
	}
	var args []json.RawMessage
	if len(p.Data) > 0 {
		if err := json.Unmarshal(p.Data, &args); err != nil {
			return
		}
	}
	sk.resolveAck(*p.AckID, args, nil)
}

// closeAll runs when the Engine.IO connection ends: every namespace socket is torn
// down and its disconnect hooks fire, mirroring the legacy OnDone path.
func (cn *sioConn) closeAll() {
	cn.mu.Lock()
	cn.closed = true
	if cn.timer != nil {
		cn.timer.Stop()
	}
	socks := make([]*ioSocket, 0, len(cn.sockets))
	for _, sk := range cn.sockets {
		socks = append(socks, sk)
	}
	cn.mu.Unlock()
	for _, sk := range socks {
		sk.teardown(false)
	}
}

// pendingAck is one outstanding server-to-client ack.
type pendingAck struct {
	cb    func(args []json.RawMessage, err error)
	timer *time.Timer
}

// ioSocket is the per-namespace view of a Socket.IO connection. It owns the socket
// id, the rooms it joined and the acks it is waiting for.
type ioSocket struct {
	conn *sioConn
	ns   *Namespace
	sid  string
	auth json.RawMessage

	mu      sync.Mutex
	rooms   map[string]struct{}
	pending map[int]*pendingAck
	nextAck int
	gone    bool
}

func (sk *ioSocket) emit(event string, data interface{}, opts EmitOptions) error {
	frame, err := encodeSIOEvent(sk.ns.name, nil, event, data)
	if err != nil {
		return err
	}
	sk.conn.send(frame, sk.conn.s.resolvePolicy(opts.Policy))
	return nil
}

func (sk *ioSocket) emitWithAck(event string, data interface{}, timeout time.Duration, cb func([]json.RawMessage, error)) error {
	if cb == nil {
		return ErrNilAckCallback
	}
	cfg := sk.conn.s.cfg.SocketIO
	if timeout <= 0 {
		timeout = cfg.AckTimeout
	}

	sk.mu.Lock()
	if sk.gone {
		sk.mu.Unlock()
		return ErrAckDisconnected
	}
	if len(sk.pending) >= cfg.MaxPendingAcks {
		sk.mu.Unlock()
		return ErrTooManyPendingAcks
	}
	sk.nextAck++
	id := sk.nextAck
	pa := &pendingAck{cb: cb}
	sk.pending[id] = pa
	pa.timer = time.AfterFunc(timeout, func() { sk.resolveAck(id, nil, ErrAckTimeout) })
	sk.mu.Unlock()

	frame, err := encodeSIOEvent(sk.ns.name, &id, event, data)
	if err != nil {
		sk.dropAck(id)
		return err
	}
	sk.conn.send(frame, sk.conn.s.resolvePolicy(BackpressureDefault))
	return nil
}

func (sk *ioSocket) dropAck(id int) {
	sk.mu.Lock()
	pa := sk.pending[id]
	delete(sk.pending, id)
	sk.mu.Unlock()
	if pa != nil {
		pa.timer.Stop()
	}
}

// resolveAck completes at most once per id: a late client reply after a timeout, or a
// duplicate reply, finds nothing pending and is dropped.
func (sk *ioSocket) resolveAck(id int, args []json.RawMessage, err error) {
	sk.mu.Lock()
	pa := sk.pending[id]
	delete(sk.pending, id)
	sk.mu.Unlock()
	if pa == nil {
		return
	}
	pa.timer.Stop()
	defer func() {
		if rec := recover(); rec != nil {
			sk.conn.s.diag.Printf("gosocket: ack callback panic: %v", rec)
		}
	}()
	pa.cb(args, err)
}

func (sk *ioSocket) join(room string) {
	if !validRoom(room) {
		return
	}
	key := sk.ns.roomKey(room)
	sk.mu.Lock()
	if sk.gone {
		sk.mu.Unlock()
		return
	}
	if sk.rooms == nil {
		sk.rooms = make(map[string]struct{})
	}
	sk.rooms[key] = struct{}{}
	sk.mu.Unlock()
	sk.conn.s.hub.Join(sk.conn.c, key)
}

func (sk *ioSocket) leave(room string) {
	if !validRoom(room) {
		return
	}
	key := sk.ns.roomKey(room)
	sk.mu.Lock()
	delete(sk.rooms, key)
	sk.mu.Unlock()
	sk.conn.s.hub.Leave(sk.conn.c, key)
}

// disconnect is a server-initiated leave of this namespace. Closing the last
// namespace closes the connection, as socket.io does.
func (sk *ioSocket) disconnect() {
	sk.teardown(true)
	if frame, err := socketio.Encode(socketio.Packet{Type: socketio.Disconnect, Namespace: sk.ns.name}); err == nil {
		sk.conn.send(frame, internal.PolicyDisconnect)
	}
	sk.conn.mu.Lock()
	left := len(sk.conn.sockets)
	sk.conn.mu.Unlock()
	if left == 0 {
		sk.conn.c.ShutdownAfterFlush()
	}
}

// teardown is idempotent. leaveHub is false when the whole connection is already
// gone and the hub has dropped the client.
func (sk *ioSocket) teardown(leaveHub bool) {
	sk.mu.Lock()
	if sk.gone {
		sk.mu.Unlock()
		return
	}
	sk.gone = true
	keys := make([]string, 0, len(sk.rooms))
	for k := range sk.rooms {
		keys = append(keys, k)
	}
	pend := sk.pending
	sk.pending = map[int]*pendingAck{}
	sk.mu.Unlock()

	sk.conn.mu.Lock()
	if sk.conn.sockets[sk.ns.name] == sk {
		delete(sk.conn.sockets, sk.ns.name)
	}
	sk.conn.mu.Unlock()

	if leaveHub {
		h := sk.conn.s.hub
		for _, k := range keys {
			h.Leave(sk.conn.c, k)
		}
		h.Leave(sk.conn.c, sk.ns.memberKey())
		h.Leave(sk.conn.c, sidKey(sk.sid))
	}
	for _, pa := range pend {
		pa.timer.Stop()
		func() {
			defer func() { _ = recover() }()
			pa.cb(nil, ErrAckDisconnected)
		}()
	}
	// A client-sent DISCONNECT and transport loss both end here; fire hooks once.
	ctx := sk.conn.s.newContext(sk.conn.c, sk.conn.r, EventDisconnect, nil, "", "")
	ctx.sock = sk
	sk.conn.s.runDisconnect(ctx, sk.ns.chain(), sk.ns.disconnectCallbacks())
}

// encodeSIOEvent builds an EVENT packet: ["event", data?]. A nil data omits the argument.
func encodeSIOEvent(nsName string, ackID *int, event string, data interface{}) ([]byte, error) {
	args := []interface{}{event}
	if data != nil {
		args = append(args, data)
	}
	return encodeSIOArgs(socketio.Event, nsName, ackID, args)
}

func encodeSIOArgs(t socketio.PacketType, nsName string, ackID *int, args []interface{}) ([]byte, error) {
	body, err := json.Marshal(args)
	if err != nil {
		return nil, err
	}
	return socketio.Encode(socketio.Packet{Type: t, Namespace: nsName, AckID: ackID, Data: body})
}
