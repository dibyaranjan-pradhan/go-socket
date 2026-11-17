# Compatibility: legacy JSON ↔ Socket.IO

go-socket speaks two client dialects from one set of handlers. This page maps
them and states the support policy.

## Status

| Dialect | Mount | Status |
|---|---|---|
| Legacy JSON | `Handler()`, `EngineIOHandler()` | **Supported.** New client work should use Socket.IO. Not removed before v1.0 (M12); a deprecation notice with a stated window precedes any removal. |
| Socket.IO v5 / Engine.IO v4 | `SocketIOHandler()` | **Supported** since v0.6.0. |

## Wire mapping

| Concept | Legacy | Socket.IO |
|---|---|---|
| Client → server event | `{"event":"chat","room":"r","payload":{…},"id":"1"}` | `42["chat",{…}]` (ack: `421["chat",{…}]`) |
| Server → client event | `{"event":"chat","payload":{…}}` | `2["chat",{…}]` |
| Connect | implicit at socket open; `OnConnect` runs at once | client sends `0` (+ auth); server replies `0{"sid":"…"}` |
| Namespaces | none (one implicit) | `0/admin,` … `2/admin,["e"]` |
| Auth | HTTP headers/query on upgrade | `auth` object in `CONNECT`; rejection = `CONNECT_ERROR` |
| Ack | `id` in request → `ackId` on the first server emit | separate ACK packet `3<id>[…]`, both directions |
| Room in message | `room` field (informational) | none; rooms are server-side only |
| Disconnect | close frame | `1` packet per namespace, then close |
| Session recovery | `$session`, `seq`, `gs_session` | not supported |
| Idempotency key | `idempotencyKey` field | not supported |
| Binary | not supported | M6 |

## Handler API is shared

| Server code | Legacy client | Socket.IO client |
|---|---|---|
| `srv.On("e", h)` | event `e` | event `e` on `/` |
| `srv.Use(mw)` | every event + connect | every event + `CONNECT` of every namespace |
| `ctx.Emit` | JSON frame | `2[…]` frame |
| `ctx.JoinRoom("r")` | joins `r` | joins `r` in the same namespace |
| `ctx.BroadcastToRoom` / `ctx.To("r")` | reaches both | reaches both |

**Mixed rooms.** Legacy clients live in the default namespace `/`. A legacy
client and a Socket.IO client in room `lobby` of `/` see each other's
broadcasts, each in its own framing. This is what lets you migrate clients one
at a time while backend services already speak Socket.IO. Rooms in other
namespaces are Socket.IO-only.

## Semantic differences to know

- **Ack timing.** Legacy: the *first* `Emit` after a request carries the ack.
  Socket.IO: `ctx.Ack(...)` is explicit. In a handler serving both, call both
  `ctx.Emit` and `ctx.Ack`. `Ack` on a legacy context returns `ErrNotSocketIO`
  (ignore it); `Emit` is safe on both.
- **Connect ordering.** Legacy `OnConnect` runs before the client sends
  anything; for Socket.IO it runs after the client's `CONNECT` and auth.
- **Reserved events.** `connect`, `connect_error`, `disconnect`,
  `disconnecting`, `newListener`, `removeListener` sent by a Socket.IO peer are
  ignored.
- **Lifecycle hooks.** `OnDisconnect` fires once per namespace socket.

## Migration order

1. Upgrade the backend to v0.6.0 and mount `SocketIOHandler()` next to the
   legacy mounts (no client change).
2. Move clients one by one to `socket.io-client` pointing at `/socket.io/`.
3. When no legacy clients remain (check `ServerStats`/access logs), unmount
   the legacy handlers.
