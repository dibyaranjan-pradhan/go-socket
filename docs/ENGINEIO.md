# Engine.IO HTTP long-polling & upgrade (v0.3.0+)

This page shows how to mount **Engine.IO v4** HTTP long-polling alongside the
existing WebSocket handler. Your application handlers (`On`, `Emit`, rooms,
middleware) stay the same — only the transport path changes.

## When to use
- Clients that cannot keep a WebSocket open (some proxies, mobile background).
- You want a **Socket.IO-compatible handshake** and optional upgrade to WebSocket.
- You already use go-socket for JSON events and want Engine.IO without a second
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
mux.Handle("/engine.io", s.EngineIOHandler()) // Engine.IO v4 polling + upgrade
http.ListenAndServe(":8080", mux)
```

Nothing changes for current WebSocket users. Polling and upgrade are opt-in via
the new handler.

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

## Client flow (upgrade to WebSocket, v0.4.0)

After polling is established, standard Engine.IO clients upgrade:

1. POST `2probe` on the polling URL.
2. POST upgrade packet `5`.
3. GET `transport=websocket&sid=<same sid>` with a WebSocket handshake.

The server keeps the same session and client pumps; see **[UPGRADE.md](UPGRADE.md)** for details and tests.

## End-to-end test as reference
See `TestEngineIOPollingHandshakeAndEvent` in `engineio_test.go` (polling) and
`TestEngineIOUpgradeEndToEnd` in `upgrade_test.go` (upgrade).

## Limitations
- **Socket.IO packet types** are not on the wire yet; payloads use the existing
  `{event, payload, ack}` JSON inside Engine.IO message packets.
- **Separate mount** — `EngineIOHandler()` does not replace `Handler()`; mount
  both if you need both transports.
- **Native WebSocket** (`Config.NativeWebSocket`) is alpha; gorilla is default.

## Related docs
- [Upgrade guide](UPGRADE.md) — probe/upgrade sequence, native codec, Autobahn.
- [Architecture](ARCHITECTURE.md) — how Engine.IO fits under your handlers.
- [Wire protocol](WIRE_PROTOCOL.md) — JSON event shape (unchanged).
