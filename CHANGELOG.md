# Changelog

All notable changes to **go-socket** are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
While the version is `0.x`, minor releases may include documented breaking changes.

## [Unreleased](https://github.com/dibyaranjan-pradhan/go-socket/compare/v0.6.0...HEAD)

M6 (in progress) — public beta, native codec default, gorilla removal.

### Fixed

- Native WebSocket codec (`Config.NativeWebSocket`) now passes the Autobahn fuzzingclient suite (301 cases, 0 failures; previously 88 failures). It enforces RFC 6455 framing rules (reserved bits/opcodes, control-frame limits, masking), validates UTF-8 in text messages and close reasons, validates and echoes close codes, and answers violations with 1002/1007/1009.
- Native codec: a forged frame length can no longer exhaust memory or crash the process. It is checked against the read limit without integer overflow, and payload memory grows only as bytes arrive. Read errors are now permanent, like gorilla's, so a retry after a timeout cannot misread a half-read frame. Pong/close replies no longer interleave with concurrent writes; frames are written with one buffered flush instead of two syscalls.
- Native codec: non-WebSocket requests get an HTTP error (400/403/405, and `Sec-WebSocket-Version: 13` on a version mismatch) instead of an empty 200, matching gorilla. `Config.ReadBufferSize` now sizes its read buffer.
- Flaky data races in the test suite; `go test -race ./...` is green.

### Changed

- `make autobahn` fails on any failing Autobahn case and runs on Linux Docker hosts.

## [0.6.0](https://github.com/dibyaranjan-pradhan/go-socket/releases/tag/v0.6.0) - 2026-11-17

M5 — Socket.IO semantics: namespaces, bidirectional acks, auth, broadcast algebra.

### Added

- **`Server.SocketIOHandler()`**: Socket.IO v5 over Engine.IO v4 (polling, WebSocket-only, and upgrade) for the official `socket.io-client`. Mount at `/socket.io/`. See [docs/SOCKETIO.md](docs/SOCKETIO.md).
- **Namespaces**: `Server.Of(name)` returns a `Namespace` with its own `On`, `Use`, `OnConnect`, `OnDisconnect`. The default namespace `/` shares the server's own registrations, so existing handlers serve Socket.IO clients unchanged; server-wide `Use` middleware also guards every namespace.
- **Auth + `CONNECT_ERROR`**: `Context.Auth()` / `BindAuth()` expose the CONNECT auth payload; `ConnectError{Message, Data}` controls what the client sees, any other error becomes a generic `rejected`.
- **Acks both ways**: `Context.Ack(...)` (client → server) and `Context.EmitWithAck(event, data, timeout, cb)` (server → client) with `ErrAckTimeout` / `ErrAckDisconnected`, a per-socket cap (`SocketIOConfig.MaxPendingAcks`) and `WantsAck()`.
- **Broadcast algebra**: `Namespace` and `Context` gain `To`, `In`, `Except`, `Broadcast`, `Emit`; chainable immutable `Broadcast` with `Emit`/`EmitWith`.
- `Config.SocketIO` (`ConnectTimeout`, `AckTimeout`, `MaxPendingAcks`); `Context.Namespace()`, `SocketID()`, `Args()`.
- `EngineIOHandler` accepts direct WebSocket connections (`transport=websocket` without `sid`), as `socket.io-client` with `transports:["websocket"]` sends.
- [docs/COMPATIBILITY.md](docs/COMPATIBILITY.md): legacy JSON ↔ Socket.IO mapping, support policy, migration order.

### Changed

- `Context.JoinRoom` / `LeaveRoom` and `EmitToRoom`-style calls ignore room names starting with NUL (`\x00`), which the library reserves for internal bookkeeping.
- `ShutdownWithMessage` also reaches Socket.IO clients (as an event on `/`).

### Notes

- **No breaking changes.** Until `SocketIOHandler()` is called, behaviour and wire output are identical to 0.5.0; `Handler()` and `EngineIOHandler()` are untouched. Legacy clients stay supported (not removed before 1.0); legacy and Socket.IO clients share rooms of the default namespace.
- Not in this release: binary events/acks (M6) and session recovery for Socket.IO clients (legacy only).

## [0.5.0](https://github.com/dibyaranjan-pradhan/go-socket/releases/tag/v0.5.0) - 2026-10-29

M4 — session resume and connection-state recovery, per-emit backpressure policy, idempotency keys.

### Added

- **Session recovery** (opt-in via `Config.SessionRecovery`): per-session monotonic `seq` on outbound events, bounded replay buffer, `$session` handshake frame, resume with `?gs_session=&gs_seq=` (or `X-GoSocket-*` headers) on the WebSocket URL and the Engine.IO handshake. See [docs/RECOVERY.md](docs/RECOVERY.md).
- Parked sessions stay room members and keep buffering broadcasts until resumed, expired (`SessionTTL`), or evicted (`MaxParkedSessions`); `OnSessionExpire` callback.
- `SessionRecoveryConfig.OwnerFunc` binds a session to a verified principal so a leaked token cannot be resumed by someone else.
- **Backpressure policy**: `BackpressureDisconnect` / `BackpressureDrop` / `BackpressureVolatile`, per emit via `ctx.EmitWith`, `EmitToRoomWith`, `BroadcastToRoomWith`, and server-wide via `Config.DefaultBackpressure`.
- **Idempotency keys**: inbound `idempotencyKey` is deduplicated per session window; `EmitOptions.IdempotencyKey` is echoed on outbound events.
- Client `$ack` control event to release buffered events early.
- `Context.SessionID()` and `Context.Resumed()`.
- Stats: `ServerStats` (`Sessions`, `SessionsParked`, `SessionsResumed`, `MessagesReplayed`, `DuplicatesSuppressed`, `HubEmitsDropped`) and `ConnectionStats` (`MessagesDropped`, `SessionID`).

### Changed

- Hub `RoomSize`/room views exclude parked (disconnected) sessions; with recovery disabled no session is ever parked, so results are unchanged.
- A slow-consumer disconnect no longer ends the session when recovery is enabled; `ctx.Disconnect()` still does.

### Fixed

- `TestHubLogEmitSaturated` was timing-dependent; it now fills the queue deterministically.

### Notes

- **No breaking changes.** With `SessionRecovery.Enabled == false` (default) the wire format and behaviour are identical to 0.4.0; new JSON fields are `omitempty`.
- Recovery state is per process; cross-node resume is out of scope until the Redis/NATS adapter milestone.

## [0.4.0](https://github.com/dibyaranjan-pradhan/go-socket/releases/tag/v0.4.0) - 2026-09-24

M3 — Engine.IO polling→WebSocket upgrade and alpha native WebSocket codec (`internal/nativews`).

### Added

- Engine.IO **transport upgrade**: probe (`2probe`) → upgrade packet (`5`) → WebSocket GET on the same `sid`; session-preserving swap via `sessionTransport` (read/write pumps keep running).
- `EngineIOHandler()` routes `transport=websocket` for upgrade as well as `transport=polling`.
- `Config.NativeWebSocket` — opt-in alpha backend using `internal/nativews` instead of gorilla (gorilla remains default).
- `internal/nativews` — server-side RFC 6455 WebSocket codec (handshake, frames, ping/pong, close).
- `TransportUpgrader` (`NewTransportUpgrader`, `NewNativeUpgrader`) shared by `Handler()` and Engine.IO WebSocket upgrade.
- [Autobahn Testsuite](https://github.com/crossbario/autobahn-testsuite) harness: `make autobahn`, `tools/autobahn/echo`, [docs/UPGRADE.md](docs/UPGRADE.md).
- E2E tests: upgrade message round-trip, session ID preserved after upgrade, native `Handler()` smoke test.

### Changed

- Engine.IO HTTP handling consolidated in `internal/engineio_handler.go` (replaces `internal/polling_http.go`).
- `PollHandler` remains a type alias to `EngineIOHandler` for backward-compatible tests.

### Fixed

- **Upgrade disconnect** — closing the polling inbox after upgrade made `ReadPump` see EOF and drop the client; polling now signals `errTransportUpgraded` and swaps transport without tearing down pumps.

### Notes

- **No breaking changes.** Existing `Handler()`, JSON wire, and polling-only Engine.IO clients behave as before.
- **Native WebSocket is alpha.** Keep gorilla for production until Autobahn reports are clean.
- **0.3.0 docs** said Engine.IO WebSocket upgrade was “not wired yet”; that limitation is lifted in this release.

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
