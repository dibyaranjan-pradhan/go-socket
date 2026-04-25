package gosocket

import (
	"encoding/json"
	"net/http"

	"github.com/dibyaranjan-pradhan/go-socket/internal"
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
	return ctx.server.emitOne(ctx.client, event, data, ctx.consumeAck())
}

// EmitToRoom sends an event to every client in room, including the caller when joined.
func (ctx *Context) EmitToRoom(room, event string, data interface{}) error {
	return ctx.server.emitRoom(room, event, data, ctx.consumeAck(), nil)
}

// BroadcastToRoom sends an event to every client in room except this connection.
func (ctx *Context) BroadcastToRoom(room, event string, data interface{}) error {
	return ctx.server.emitRoom(room, event, data, ctx.consumeAck(), ctx.client)
}

// JoinRoom registers this client with a room for targeted delivery.
func (ctx *Context) JoinRoom(room string) {
	if ctx.client == nil || room == "" {
		return
	}
	ctx.server.hub.Join(ctx.client, room)
}

// LeaveRoom removes this client from a room.
func (ctx *Context) LeaveRoom(room string) {
	if ctx.client == nil || room == "" {
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

// Disconnect closes the underlying WebSocket.
func (ctx *Context) Disconnect() {
	if ctx.client == nil {
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
