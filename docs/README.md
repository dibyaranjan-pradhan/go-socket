# go-socket — documentation

The **library overview and integration steps** live in the repo root: [**../README.md**](../README.md).

This folder holds deeper references and planning:

| Document | Contents |
|----------|----------|
| [WIRE_PROTOCOL.md](./WIRE_PROTOCOL.md) | Client ↔ server JSON, acks, rooms |
| [MIDDLEWARE.md](./MIDDLEWARE.md) | Recover, rate limit, custom `MiddlewareFunc` |
| [EVENTS.md](./EVENTS.md) | All synthetic + STAG chat events; optional `Preset*` constants |
| [TESTING.md](./TESTING.md) | Unit-test plan and coverage target (~80%) |

**v0.1.0 (library):** tunable **`HubEmitBufferSize`** on `Config`; **`ConnectionStats` / `ServerStats` / `RoomStats`** and server query helpers; **`OnHeartbeatTimeout`**; **`ShutdownWithMessage`**. Details and examples are in the root [**README.md**](../README.md).

Design notes: optional wire-event constants (`PresetChat*`, etc.) live in `events.go` so clients and servers can share strings without reading STAG handler code; the protocol itself remains string-based JSON `event` fields.
