# Wire protocol

See also **[EVENTS.md](./EVENTS.md)** for named events (library synthetic events and optional `Preset*` constants).

All application payloads are JSON text frames (UTF-8).

## Client → server

```json
{
  "event": "message_name",
  "room": "optional-room-id",
  "payload": { },
  "id": "optional-correlation-id"
}
```

- **event** — routes to the handler registered with `Server.On("message_name", ...)`.
- **room** — optional; exposed on the handler context for your app (e.g. join/emit routing).
- **payload** — arbitrary JSON object/array; use `ctx.Bind(&v)` in handlers.
- **id** — optional; if set, the next outbound message from this connection may include `ackId` with the same value.

Example:

```json
{
  "event": "typing",
  "room": "chat-uuid",
  "payload": { "isTyping": true }
}
```

## Server → client

```json
{
  "event": "logical_name",
  "payload": { },
  "ackId": "matches-client-id-if-any"
}
```

- **event** — same string you passed to `Emit` / `EmitToRoom` / `BroadcastToRoom`.
- **payload** — JSON-encoded value you passed (object, array, string, etc.).
- **ackId** — present only when replying to a client frame that had `id`.

## Ack behaviour

1. Client sends an inbound message with `"id": "req-1"`.
2. The first `Emit` (or ack-bearing emit) from the server for that turn may include `"ackId": "req-1"`.

If you do not use `id`, omit `ackId` on the client.

## Rooms

- `ctx.JoinRoom(room)` / `ctx.LeaveRoom(room)` register the connection with the hub.
- `ctx.EmitToRoom(room, event, data)` sends to all members of `room` (including the sender).
- `ctx.BroadcastToRoom(room, event, data)` sends to all members **except** the sender.

Room membership is server-side only; the wire `room` field on client messages does not auto-join — join in `OnConnect` or in an explicit handler.

## Control frames

Ping/pong and WebSocket close frames are handled by the library. On shutdown, the server attempts to send a **normal close** control frame so clients can observe graceful teardown.

## Shutdown notice (v0.1.0)

`Server.ShutdownWithMessage` delivers one **Server → client** JSON text frame (same JSON shape as above) to **every** connected client before the hub begins draining, then waits a short interval so slow peers can process it. The `event` name and `payload` are application-defined (for example a `server_restart` event with a small metadata object).
