# Changelog

All notable changes to **go-socket** are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
While the version is `0.x`, minor releases may include documented breaking changes.

## [Unreleased]

## [0.3.0](https://github.com/dibyaranjan-pradhan/go-socket/releases/tag/v0.3.0) - 2026-06-24

M2 roadmap milestone merged to `master` — Engine.IO core and HTTP long-polling.

### Added

- `Server.EngineIOHandler()` — Engine.IO v4 HTTP long-polling endpoint (opt-in mount).
- Engine.IO session lifecycle: `sid` registry, open handshake packet, ping/pong heartbeat.
- `internal/polling_transport` — `Transport` implementation that frames JSON in Engine.IO message packets.
- `internal/poll_manager` and `internal/polling_http` — handshake GET, long-poll GET, POST send.
- Engine.IO helpers under `internal/engineio`: handshake JSON, batch encode/decode (`\x1e` separator).
- End-to-end integration test: polling handshake → POST event → poll reply.
- `docs/ENGINEIO.md` usage guide; architecture and README updates.

### Changed

- Documentation and comments no longer reference any specific downstream product;
examples use generic paths and chat event names.

### Fixed

- No user-facing bug fixes in this release; this milestone adds the polling transport path.

### Notes

- **No breaking changes.** Existing WebSocket `Handler()` and JSON wire format are unchanged.
- Polling clients must mount `/engine.io/` (or your chosen prefix) separately from WebSocket.
- WebSocket upgrade within Engine.IO is not wired yet (planned for a later milestone).

## [0.2.0](https://github.com/dibyaranjan-pradhan/go-socket/releases/tag/v0.2.0) - 2026-06-14

M1 roadmap milestone merged to `master`.

### Added

- Engine.IO packet codec foundation under `internal/engineio`:
packet types (`open`, `close`, `ping`, `pong`, `message`, `upgrade`, `noop`)
and encode/decode helpers.
- Socket.IO packet codec foundation under `internal/socketio`:
packet types (`connect`, `disconnect`, `event`, `ack`, `connect_error`,
`binary_event`, `binary_ack`) with namespace/ack/attachment header parsing.
- Transport abstraction seam in `internal/transport.go`:
`Transport` interface, `wsTransport`, and `WSUpgrader`.
- Rewired client/server internals to use the transport abstraction.
- New tests for codecs and transport seam:
positive/negative table tests, fuzz targets, and example-style tests.
- `docs/ARCHITECTURE.md` and docs index update for contributor-facing internals.

### Notes

- Public app-level behavior and existing wire usage remain backward compatible.
- This release is foundational for Engine.IO/Socket.IO protocol work in M2+.

## [0.1.0](https://github.com/dibyaranjan-pradhan/go-socket/releases/tag/0.1.0) - 2026-06-11

Earlier milestone release already pushed to `master`.

### Added

- Connection, server, and room statistics:
`ConnectionStats`, `ServerStats`, `RoomStats`, plus server query methods.
- Tunable hub outbound queue via `Config.HubEmitBufferSize`
(`DefaultHubEmitBufferSize = 256`).
- Heartbeat-timeout hook with `Server.OnHeartbeatTimeout`.
- `Server.ShutdownWithMessage` for graceful pre-shutdown notifications.
- Project `Makefile` with build, vet, test, race, coverage, vulncheck,
errcheck, and staticcheck targets.

### Notes

- Changes are additive and backward compatible with existing usage.

