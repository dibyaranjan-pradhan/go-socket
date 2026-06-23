# go-socket — documentation

The **library overview and integration steps** live in the repo root: [**../README.md**](../README.md).

This folder holds deeper references and planning:

| Document | Contents |
|----------|----------|
| [WIRE_PROTOCOL.md](./WIRE_PROTOCOL.md) | Client ↔ server JSON, acks, rooms |
| [MIDDLEWARE.md](./MIDDLEWARE.md) | Recover, rate limit, custom `MiddlewareFunc` |
| [EVENTS.md](./EVENTS.md) | Synthetic lifecycle events and optional `Preset*` chat constants |
| [TESTING.md](./TESTING.md) | Unit-test plan and coverage target (~85%) |
| [ARCHITECTURE.md](./ARCHITECTURE.md) | Internals: layers, packet codecs, the `Transport` seam (for contributors) |
| [ENGINEIO.md](./ENGINEIO.md) | Engine.IO v4 HTTP long-polling: mount `EngineIOHandler()`, client flow |

**v0.3.0 (library):** **`Server.EngineIOHandler()`** for Engine.IO v4 HTTP long-polling — session handshake, ping/pong heartbeat, and JSON event round-trip over polling. WebSocket `Handler()` and the JSON wire are unchanged. See [ENGINEIO.md](./ENGINEIO.md).

**v0.1.0 (library):** tunable **`HubEmitBufferSize`** on `Config`; **`ConnectionStats` / `ServerStats` / `RoomStats`** and server query helpers; **`OnHeartbeatTimeout`**; **`ShutdownWithMessage`**. Details and examples are in the root [**README.md**](../README.md).

Design notes: optional wire-event constants (`PresetChat*`, etc.) live in `events.go` so clients and servers can share strings without duplicating literals; the protocol itself remains string-based JSON `event` fields.
