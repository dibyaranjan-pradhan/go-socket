# go-socket — Roadmap

A 12-month plan to evolve **go-socket** from a JSON-over-WebSocket server into a
**std-lib-only, Socket.IO/Engine.IO-compatible real-time library** for Go — usable
for web, mobile, services, and IoT gateways.

This document is a public promise of direction, not a contract on exact dates.
Each milestone ships only when its **exit criteria** are green.

## Current status

| Milestone | Version | GitHub issue | Status |
|-----------|---------|--------------|--------|
| M1 — Packets & transport seam | [v0.2.0](https://github.com/dibyaranjan-pradhan/go-socket/releases/tag/v0.2.0) | #2 | **Shipped** |
| M2 — Engine.IO core + polling | [v0.3.0](https://github.com/dibyaranjan-pradhan/go-socket/releases/tag/v0.3.0) | #3 | **Shipped** |
| M3 — Upgrade + native WS (alpha) | [v0.4.0](https://github.com/dibyaranjan-pradhan/go-socket/releases/tag/v0.4.0) | #4 | **Shipped** |
| M4 — Session resume / recovery | v0.5.0 | [#5](https://github.com/dibyaranjan-pradhan/go-socket/issues/5) | **Implemented** (pending release) |
| M5–M12 | v0.6.0 … v1.0.0 | [#6–#13](https://github.com/dibyaranjan-pradhan/go-socket/issues) | Planned |

Details per release: [CHANGELOG.md](../CHANGELOG.md).

## Guiding principles

- **Zero third-party dependencies in the core.** The core module uses only the Go
  standard library. Anything needing a dependency (Redis/NATS adapters, msgpack,
  Prometheus) ships as a **separate, opt-in module** so `go get` of the core pulls
  in no third-party code.
- **Protocol fidelity = ecosystem.** We target the real Socket.IO/Engine.IO wire
  grammar so existing clients (e.g. `socket.io-client`) work, while keeping the
  legacy `{event,room,payload,id}` wire behind a compatibility mode during a
  documented deprecation window.
- **Reliability over an unreliable network.** Sessions survive transport changes;
  sequence numbers + a replay buffer give connection-state recovery.
- **Every primitive has a cost budget.** O(1) room fan-out, bounded per-connection
  memory, explicit backpressure policy, benchmarked against those budgets.

## Versioning & cadence

- **Pre-1.0 (`v0.x`):** one minor release per month; documented breaking changes
  allowed. Features land behind flags/interfaces so an unfinished item never blocks
  a release.
- **`v1.0.0`:** locked public API + SemVer stability guarantees (Month 12).
- **Adapters/exporters** are versioned independently as separate modules.

## Public launch gate (Month 6)

A loud public beta happens only when **all** of the following are true:

- Native WebSocket codec **passes the Autobahn Testsuite** (text/binary/fragmentation/close/UTF-8).
- **Interop CI** connects the official `socket.io-client` and passes events + acks + namespaces.
- Codec/parser **fuzzers** run clean; `-race` soak test green.
- README + runnable examples (chat, presence, IoT gateway) + first benchmarks.
- Core module proves **zero third-party deps** (`go mod graph`).

## 12-month plan

### Q1 — Build the engine (quiet)

| Month | Tag | Theme | Highlights |
|------:|-----|-------|------------|
| 1 | `v0.2.0` | Packets & transport seam | Engine.IO (0–6) + Socket.IO (0–6) `Packet` types; codecs with round-trip + fuzz tests; `Transport` interface; gorilla moved behind it |
| 2 | `v0.3.0` | Engine.IO core + polling | `Session{sid}`, `open` handshake, engine.io ping/pong, HTTP long-polling transport |
| 3 | `v0.4.0` | Transport upgrade + own WS codec (alpha) | polling→WebSocket upgrade; native RFC 6455 codec (`internal/nativews`) behind a flag; Autobahn harness |

### Q2 — Make it reliable, then launch

| Month | Tag | Theme | Highlights |
|------:|-----|-------|------------|
| 4 | `v0.5.0` | Reliability: session resume | Sequence numbers + replay buffer; connection-state recovery; backpressure policy; idempotency keys |
| 5 | `v0.6.0` | Socket.IO semantics | Namespaces; bidirectional acks (timeout + callback); auth in CONNECT; broadcast algebra |
| 6 | `v0.7.0-beta` | **Public beta** + own codec default | Autobahn passing; gorilla removed; interop vs `socket.io-client`; docs site, examples, benchmarks |

### Q3 — Performance & scale

| Month | Tag | Theme | Highlights |
|------:|-----|-------|------------|
| 7 | `v0.8.0` | Binary & performance | Binary attachments; optional msgpack module; permessage-deflate; O(1) room index; benchmarks |
| 8 | `v0.9.0` | Scale-out: Redis adapter | `Adapter` interface + `adapters/redis`; sticky-session guide for polling |
| 9 | `v0.10.0` | NATS adapter + presence | `adapters/nats`; presence/online tracking; room metadata |

### Q4 — Polish to 1.0

| Month | Tag | Theme | Highlights |
|------:|-----|-------|------------|
| 10 | `v0.11.0` | Observability & ops | OpenTelemetry spans; `exporters/prometheus`; admin/inspector endpoint |
| 11 | `v0.12.0-rc` | Go client SDK + hardening | Native Go client (reconnect/acks/resume); interop CI matrix; fuzz/soak; security review; API freeze |
| 12 | `v1.0.0` | Stability promise | Locked API + SemVer; migration guide off legacy wire; IoT reference (MQTT→go-socket gateway); full docs |

## IoT positioning

go-socket is the **real-time fabric from the gateway up to humans and services** —
not the last-hop protocol to a coin-cell sensor. Run it on edge gateways / cloud,
and bridge constrained devices (MQTT-SN/CoAP) at the edge. Session resume, binary
frames, and tunable heartbeats are what make it a strong fit for flaky device links.

## Tracking

Work is tracked as 12 monthly GitHub issues (one per milestone above) plus
[CHANGELOG.md](../CHANGELOG.md) updated on every release. Compatibility and
deprecation details will live in `docs/COMPATIBILITY.md` (added in Month 5–6).
