# Engine.IO HTTP long-polling (v0.3.0)

This page shows how to mount **Engine.IO v4** HTTP long-polling alongside the
existing WebSocket handler. Your application handlers (`On`, `Emit`, rooms,
middleware) stay the same — only the transport path changes.

## When to use
- Clients that cannot keep a WebSocket open (some proxies, mobile background).
- You want a **Socket.IO-compatible handshake** before upgrading to WebSocket in
  a later milestone.
- You already use go-socket for JSON events and want polling without a second
  server.

## Mount both handlers
Keep your WebSocket route and add Engine.IO on a separate path:

```go
s := gosocket.New(gosocket.Config{
    PingInterval: 25 * time.Second,
    PongWait:     60 * time.Second,
})

mux := http.NewServeMux()
mux.Handle("/ws", s.Handler())              // existing WebSocket clients
mux.Handle("/engine.io", s.EngineIOHandler()) // Engine.IO v4 polling
http.ListenAndServe(":8080", mux)
```

Nothing changes for current WebSocket users. Polling is opt-in via the new
handler.

## Client flow (polling)
Engine.IO clients use query parameters `EIO=4` and `transport=polling`.

1. **Handshake** — `GET /engine.io?EIO=4&transport=polling`  
   Response is an Engine.IO `open` packet (`0{...}`) with a session id (`sid`),
   ping intervals, and `upgrades: ["websocket"]`.

2. **Send** — `POST /engine.io?EIO=4&transport=polling&sid=<sid>`  
   Body is one or more Engine.IO packets. A JSON app event is wrapped as a
   `message` packet (`4...`) whose payload is the usual go-socket wire JSON:

   ```json
   {"event":"hello","payload":{"name":"bob"}}
   ```

3. **Receive** — `GET /engine.io?EIO=4&transport=polling&sid=<sid>`  
   Long-poll until the server has outbound data. The body may contain several
   packets separated by `\x1e`. Message packets carry your handler replies.

4. **Heartbeat** — If a long-poll times out with no data, the server may respond
   with an Engine.IO `ping` (`2`). Clients answer with `pong` (`3`) on the next
   POST, matching Engine.IO behaviour.

## End-to-end test as reference
See `TestEngineIOPollingHandshakeAndEvent` in `engineio_test.go` for a full
handshake → POST event → poll reply flow using `httptest`.

## Limitations (M2)
- **Polling only** — WebSocket upgrade within Engine.IO is not wired yet.
- **Same JSON wire** — Socket.IO packet types are not on the wire yet; payloads
  use the existing `{event, payload, ack}` JSON inside Engine.IO message packets.
- **Separate mount** — `EngineIOHandler()` does not replace `Handler()`; mount
  both if you need both transports.

## Related docs
- [Architecture](ARCHITECTURE.md) — how Engine.IO fits under your handlers.
- [Wire protocol](WIRE_PROTOCOL.md) — JSON event shape (unchanged).
