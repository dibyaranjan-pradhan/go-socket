// Copyright (c) 2026 Dibyaranjan Pradhan. All rights reserved.

package gosocket

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/dibyaranjan-pradhan/go-socket/internal"
	"github.com/dibyaranjan-pradhan/go-socket/internal/socketio"
)

// Context carries per-message or per-connection data for handlers and middleware.
type Context struct {
	server *Server
	client *internal.Client
	req    *http.Request

	event   string
	room    string
	payload []byte

	// pendingAck is copied into the first outbound frame from Emit/EmitToRoom/BroadcastToRoom.
	pendingAck string
	ackUsed    bool

	// Socket.IO fields: sock is nil for legacy connections.
	sock    *ioSocket
	args    []json.RawMessage
	ioAckID *int
	ioAcked bool
}

func (s *Server) newContext(ic *internal.Client, r *http.Request, event string, payload []byte, room, ack string) *Context {
	return &Context{
		server:     s,
		client:     ic,
		req:        r,
		event:      event,
		room:       room,
		payload:    payload,
		pendingAck: ack,
	}
}

// Bind unmarshal the inbound JSON payload into v.
func (ctx *Context) Bind(v interface{}) error {
	if len(ctx.payload) == 0 {
		return nil
	}
	return json.Unmarshal(ctx.payload, v)
}

// Emit sends an event to this connection. The first emit after an inbound message with id includes ackId.
func (ctx *Context) Emit(event string, data interface{}) error {
	if ctx.sock != nil {
		return ctx.sock.emit(event, data, EmitOptions{})
	}
	return ctx.server.emitOne(ctx.client, event, data, ctx.consumeAck(), EmitOptions{})
}

// EmitWith is Emit with a per-call backpressure policy and optional idempotency key.
func (ctx *Context) EmitWith(event string, data interface{}, opts EmitOptions) error {
	if ctx.sock != nil {
		return ctx.sock.emit(event, data, opts)
	}
	return ctx.server.emitOne(ctx.client, event, data, ctx.consumeAck(), opts)
}

// EmitToRoom sends an event to every client in room, including the caller when joined.
func (ctx *Context) EmitToRoom(room, event string, data interface{}) error {
	if ctx.sock != nil {
		return ctx.sock.ns.To(room).Emit(event, data)
	}
	return ctx.server.emitRoom(room, event, data, ctx.consumeAck(), nil, EmitOptions{})
}

// EmitToRoomWith is EmitToRoom with per-call options.
func (ctx *Context) EmitToRoomWith(room, event string, data interface{}, opts EmitOptions) error {
	if ctx.sock != nil {
		return ctx.sock.ns.To(room).EmitWith(event, data, opts)
	}
	return ctx.server.emitRoom(room, event, data, ctx.consumeAck(), nil, opts)
}

// BroadcastToRoom sends an event to every client in room except this connection.
func (ctx *Context) BroadcastToRoom(room, event string, data interface{}) error {
	if ctx.sock != nil {
		return ctx.To(room).Emit(event, data)
	}
	return ctx.server.emitRoom(room, event, data, ctx.consumeAck(), ctx.client, EmitOptions{})
}

// BroadcastToRoomWith is BroadcastToRoom with per-call options.
func (ctx *Context) BroadcastToRoomWith(room, event string, data interface{}, opts EmitOptions) error {
	if ctx.sock != nil {
		return ctx.To(room).EmitWith(event, data, opts)
	}
	return ctx.server.emitRoom(room, event, data, ctx.consumeAck(), ctx.client, opts)
}

// JoinRoom registers this client with a room for targeted delivery.
func (ctx *Context) JoinRoom(room string) {
	if ctx.client == nil || !validRoom(room) {
		return
	}
	if ctx.sock != nil {
		ctx.sock.join(room)
		return
	}
	ctx.server.hub.Join(ctx.client, room)
}

// LeaveRoom removes this client from a room.
func (ctx *Context) LeaveRoom(room string) {
	if ctx.client == nil || !validRoom(room) {
		return
	}
	if ctx.sock != nil {
		ctx.sock.leave(room)
		return
	}
	ctx.server.hub.Leave(ctx.client, room)
}

// Get returns connection metadata stored under key.
func (ctx *Context) Get(key string) (interface{}, bool) {
	if ctx.client == nil {
		return nil, false
	}
	return ctx.client.MetaGet(key)
}

// Set stores connection metadata under key.
func (ctx *Context) Set(key string, value interface{}) {
	if ctx.client == nil {
		return
	}
	ctx.client.MetaSet(key, value)
}

// Request returns the HTTP request that initiated the WebSocket.
func (ctx *Context) Request() *http.Request {
	return ctx.req
}

// ClientID returns the unique connection identifier.
func (ctx *Context) ClientID() string {
	if ctx.client == nil {
		return ""
	}
	return ctx.client.ID()
}

// SessionID returns the recovery session token ("" when recovery is off, or inside
// connect middleware, which runs before the session is bound). It is a bearer secret.
func (ctx *Context) SessionID() string {
	if ctx.client == nil {
		return ""
	}
	if sess := ctx.client.Session(); sess != nil {
		return sess.ID()
	}
	return ""
}

// Resumed reports whether this connection continued a previous session. Use it in
// OnConnect to skip work already done (welcome message, initial snapshot).
func (ctx *Context) Resumed() bool {
	return ctx.client != nil && ctx.client.Resumed()
}

// Disconnect closes the underlying WebSocket. With session recovery the session is
// terminated as well, so a kicked peer cannot resume.
func (ctx *Context) Disconnect() {
	if ctx.client == nil {
		return
	}
	if ctx.sock != nil {
		ctx.sock.disconnect()
		return
	}
	ctx.client.Shutdown()
}

// Event returns the current event name, including synthetic connect and disconnect markers.
func (ctx *Context) Event() string {
	return ctx.event
}

// Room returns the room field from the inbound wire message, if any.
func (ctx *Context) Room() string {
	return ctx.room
}

func (ctx *Context) consumeAck() string {
	if ctx.pendingAck == "" || ctx.ackUsed {
		return ""
	}
	ctx.ackUsed = true
	return ctx.pendingAck
}

// Namespace returns the namespace this context runs in ("/" for legacy connections).
func (ctx *Context) Namespace() *Namespace {
	if ctx.sock != nil {
		return ctx.sock.ns
	}
	return ctx.server.defaultNS
}

// SocketID returns the Socket.IO socket id of this namespace connection, or the
// connection id for legacy clients.
func (ctx *Context) SocketID() string {
	if ctx.sock != nil {
		return ctx.sock.sid
	}
	return ctx.ClientID()
}

// To selects rooms to broadcast to in this context's namespace, excluding the
// caller itself (socket.io's socket.to(room) semantics).
func (ctx *Context) To(rooms ...string) *Broadcast {
	b := ctx.Namespace().To(rooms...)
	b.self = ctx.client
	return b
}

// In is an alias of To.
func (ctx *Context) In(rooms ...string) *Broadcast { return ctx.To(rooms...) }

// Broadcast selects everyone in this context's namespace except the caller.
func (ctx *Context) Broadcast() *Broadcast {
	b := ctx.Namespace().Broadcast()
	b.self = ctx.client
	return b
}

// Auth returns the raw JSON auth payload the client sent in its CONNECT packet, or
// nil when there was none or the connection is not Socket.IO. Validate it in
// middleware and treat it as untrusted input.
func (ctx *Context) Auth() json.RawMessage {
	if ctx.sock == nil {
		return nil
	}
	return ctx.sock.auth
}

// BindAuth unmarshals the CONNECT auth payload into v. It is a no-op without auth.
func (ctx *Context) BindAuth(v interface{}) error {
	if a := ctx.Auth(); len(a) > 0 {
		return json.Unmarshal(a, v)
	}
	return nil
}

// Args returns the raw JSON arguments of a Socket.IO event after the event name.
// Bind covers the common single-argument case.
func (ctx *Context) Args() []json.RawMessage { return ctx.args }

// WantsAck reports whether the client asked for an acknowledgement of this event.
func (ctx *Context) WantsAck() bool { return ctx.ioAckID != nil }

// Ack answers the client's ack request for this event with args. Socket.IO acks
// are explicit and one-shot; calling Ack for an event that did not request one is
// an error rather than a silent no-op so lost replies are visible.
func (ctx *Context) Ack(args ...interface{}) error {
	if ctx.sock == nil {
		return ErrNotSocketIO
	}
	if ctx.ioAckID == nil {
		return ErrNoAckRequested
	}
	if ctx.ioAcked {
		return ErrAckAlreadySent
	}
	frame, err := encodeSIOArgs(socketio.Ack, ctx.sock.ns.name, ctx.ioAckID, args)
	if err != nil {
		return err
	}
	ctx.ioAcked = true
	ctx.sock.conn.send(frame, ctx.server.resolvePolicy(BackpressureDefault))
	return nil
}

// EmitWithAck emits to this socket and calls cb with the client's reply or an
// error (ErrAckTimeout, ErrAckDisconnected). cb runs on the read pump or a timer
// goroutine, so it may overlap with event handlers and other callbacks.
// timeout <= 0 uses Config.SocketIO.AckTimeout.
func (ctx *Context) EmitWithAck(event string, data interface{}, timeout time.Duration, cb func(args []json.RawMessage, err error)) error {
	if ctx.sock == nil {
		return ErrNotSocketIO
	}
	return ctx.sock.emitWithAck(event, data, timeout, cb)
}
