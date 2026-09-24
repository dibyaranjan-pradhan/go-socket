# Engine.IO transport upgrade & native WebSocket (v0.4.0)

This page covers **M3**: polling→WebSocket upgrade within Engine.IO v4, and the
optional **native RFC 6455** WebSocket backend behind `Config.NativeWebSocket`.

## When to use

- **Engine.IO upgrade** — Socket.IO / Engine.IO clients that start on polling and
  upgrade to WebSocket on the same session (`sid`). Your handlers, rooms, and
  middleware stay attached; only the byte pipe changes.
- **Native WebSocket** — experiments toward dropping gorilla/websocket. Gorilla
  remains the default; enable native only when you want to run Autobahn or test
  the alpha codec.

## Engine.IO upgrade flow

After the usual polling handshake (see [ENGINEIO.md](./ENGINEIO.md)):

1. **Probe** — client POSTs `2probe` (Engine.IO ping with probe payload).
2. **Upgrade packet** — client POSTs `5` (upgrade packet type).
3. **WebSocket GET** — client opens
   `GET /engine.io/?EIO=4&transport=websocket&sid=<sid>` with a normal
   WebSocket upgrade handshake.

The server swaps the session's underlying `Transport` from polling to WebSocket
**without** restarting read/write pumps or re-running `$connect`. The same
`internal.Client` and hub registration remain.

```go
mux.Handle("/engine.io/", s.EngineIOHandler())
```

Reference tests: `TestEngineIOUpgradeEndToEnd` and
`TestEngineIOUpgradePreservesSessionID` in `upgrade_test.go`.

### Upgrade sequence (client-side sketch)

```text
GET  /engine.io/?EIO=4&transport=polling          → open packet + sid
POST /engine.io/?EIO=4&transport=polling&sid=…     → body: 2probe
POST /engine.io/?EIO=4&transport=polling&sid=…     → body: 5
GET  /engine.io/?EIO=4&transport=websocket&sid=…  → 101 Switching Protocols
```

After step 4, Engine.IO message packets (`4…`) travel inside WebSocket text
frames instead of HTTP POST/GET bodies.

## Native WebSocket backend (alpha)

Gorilla is still the default for `Handler()` and Engine.IO upgrades. Opt in with:

```go
s := gosocket.New(gosocket.Config{
    NativeWebSocket: true, // alpha: internal/nativews instead of gorilla
})
mux.Handle("/ws", s.Handler())
mux.Handle("/engine.io/", s.EngineIOHandler())
```

| Path | Default (gorilla) | `NativeWebSocket: true` |
|------|-------------------|-------------------------|
| `Handler()` | Plain JSON wire | Plain JSON wire |
| `EngineIOHandler()` WS upgrade | gorilla + EIO framing | native + EIO framing |

Direct WebSocket via `Handler()` still uses the app JSON wire (no Engine.IO
packet prefix). Only the Engine.IO mount wraps payloads in EIO packets.

## Autobahn Testsuite

Run the native codec against the [Autobahn Testsuite](https://github.com/crossbario/autobahn-testsuite):

```bash
make autobahn
```

This starts the echo server in `tools/autobahn/echo` (native RFC 6455) and runs
`wstest` in Docker. Reports land in `tools/autobahn/reports/`. Requires Docker.

Manual steps:

```bash
go run ./tools/autobahn/echo -addr :9001
docker run --rm -v "$(pwd)/tools/autobahn:/config" -v "$(pwd)/tools/autobahn/reports:/reports" \
  crossbario/autobahn-testsuite wstest -m fuzzingclient -s /config/fuzzingclient.json
```

## Limitations (M3)

- **Native codec is alpha** — not production-ready; gorilla remains default.
- **Socket.IO packet framing** on the wire is still future work; payloads remain
  go-socket JSON inside Engine.IO message packets.
- **Sticky sessions** still required for multi-instance deployments (same `sid`
  must reach the pod that created it).

## Related

- [ENGINEIO.md](./ENGINEIO.md) — polling handshake and POST/GET
- [ARCHITECTURE.md](./ARCHITECTURE.md) — `sessionTransport`, `TransportUpgrader`
- [ARCHITECTURE-VISUAL.html](./ARCHITECTURE-VISUAL.html) — visual upgrade walkthrough
