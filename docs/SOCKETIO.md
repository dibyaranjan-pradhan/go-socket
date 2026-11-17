# Socket.IO semantics (M5 — v0.6.0)

Official `socket.io-client` (v4) can connect to go-socket: namespaces, auth,
events, acks in both directions, and the room/broadcast API. Opt-in; nothing
changes for existing clients.

## Mount

```go
srv := gosocket.New(gosocket.Config{})
mux.Handle("/socket.io/", srv.SocketIOHandler()) // default path of socket.io-client
mux.Handle("/ws", srv.Handler())                 // legacy JSON clients, unchanged
```

`SocketIOHandler()` is a **separate Engine.IO mount**, not a mode switch on the
existing ones. Why: legacy clients expect `OnConnect` at open and plain JSON
frames; Socket.IO clients must send `CONNECT` first and expect `2[...]` frames.
One mount cannot be both without breaking one side. Both mounts share one hub,
one middleware chain and one set of handlers.

Transports: polling, WebSocket-only (`transports:["websocket"]`), and
polling→WebSocket upgrade all work.

## Handlers serve both dialects

```go
srv.Use(authMiddleware)                       // runs for every namespace
srv.On("chat", func(ctx *gosocket.Context) {  // default namespace "/"
    var m Msg
    _ = ctx.Bind(&m)                          // first argument after the event name
    _ = ctx.To("lobby").Emit("chat", m)       // everyone in lobby except the sender
    _ = ctx.Ack("ok")                         // answer the client's ack callback
})

admin := srv.Of("/admin")                     // own handlers, middleware, lifecycle
admin.Use(adminOnly)
admin.On("kick", kick)
```

Server-wide `Use` always runs first, so a global auth check cannot be bypassed
by connecting to another namespace.

## Auth and CONNECT_ERROR

The client's `auth` object arrives in `CONNECT`. Read it in middleware:

```go
srv.Of("/admin").Use(gosocket.MiddlewareFunc(func(ctx *gosocket.Context) error {
    if ctx.Event() != gosocket.EventConnect { return nil }
    var a struct{ Token string `json:"token"` }
    if err := ctx.BindAuth(&a); err != nil || !valid(a.Token) {
        return &gosocket.ConnectError{Message: "forbidden", Data: map[string]int{"code": 401}}
    }
    return nil
}))
```

The client sees `connect_error` with `err.message` / `err.data`. Any error other
than `*ConnectError` is reported as the generic `"rejected"` so internal error
text never reaches clients. Unknown namespaces get `"Invalid namespace"`.

## Acks

| Direction | API | Notes |
|---|---|---|
| client → server | `ctx.Ack(args...)` | explicit, one-shot; errors if the client asked for no ack or you ack twice. `ctx.WantsAck()` tells you. |
| server → client | `ctx.EmitWithAck(event, data, timeout, cb)` | `cb(args, err)`; `err` is `ErrAckTimeout` or `ErrAckDisconnected`. `timeout <= 0` uses `SocketIOConfig.AckTimeout`. |

Pending server→client acks are capped per socket (`MaxPendingAcks`, default
1024; excess returns `ErrTooManyPendingAcks`), so a silent client cannot grow
memory. A late or duplicate reply is dropped.

> Legacy `ackId` (`{"id":"…"}` → first `Emit`) is unchanged and unrelated.

## Broadcast algebra

```go
ns := srv.Of("/chat")
ns.To("r1", "r2").Emit("e", x)            // union of rooms
ns.In("r1").Except("r2").Emit("e", x)     // minus a room
ns.Except(socketID).Emit("e", x)          // whole namespace minus one socket
ns.Emit("e", x)                           // whole namespace
ctx.To("r1").Emit("e", x)                 // from a handler: caller excluded
ctx.Broadcast().Emit("e", x)              // everyone else in the namespace
```

`Except` matches each name as a room **and** as a socket id. Builders are
immutable, so a shared base is safe across goroutines. Rooms are namespaced:
`"r1"` in `/chat` is unrelated to `"r1"` in `/admin`.

## Configuration

```go
gosocket.Config{SocketIO: gosocket.SocketIOConfig{
    ConnectTimeout: 45 * time.Second, // close connections that never CONNECT
    AckTimeout:     30 * time.Second,
    MaxPendingAcks: 1024,
}}
```

## Behaviour notes

- Packets for one connection are handled in order on its read pump; a slow
  handler delays that connection only.
- Closing the last namespace (`ctx.Disconnect()`) closes the connection after
  flushing the `DISCONNECT` packet (1 s linger).
- `ShutdownWithMessage` reaches Socket.IO clients as an event on `/`.
- Malformed packets close the connection (same as `socket.io`).
- **Not in M5:** binary events/acks (M6), session recovery for Socket.IO
  clients (legacy only), `socket.io-client` `volatile`/`timeout()` helpers
  beyond what the wire carries.

## Testing

Conformance is checked with a Go test client built on the packet codecs, using
the Socket.IO v5 spec's wire examples (`socketio_test.go`). It runs in plain
`go test`, no Node needed. v0.6.0 was also smoke-tested against an independent
client, `python-socketio` (ack both ways, broadcast, namespace auth, over
WebSocket-only, polling-only and polling→WebSocket upgrade). The official JS
`socket.io-client` has not been run against it yet.

See also: [COMPATIBILITY.md](COMPATIBILITY.md), [ENGINEIO.md](ENGINEIO.md),
[ARCHITECTURE.md](ARCHITECTURE.md).
