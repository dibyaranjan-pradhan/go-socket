# Architecture (internals)

This page explains how go-socket is put together inside. It is for people who
work **on** the library. If you only want to **use** it, the root
[README](../README.md) is enough.

> Status: **M3 (v0.4.0)** adds Engine.IO polling→WebSocket upgrade (same `sid`,
> transport swap without restarting pumps) and an alpha native RFC 6455 codec
> behind `Config.NativeWebSocket`. Gorilla remains the default WebSocket backend.

## The layers

Think of a real-time connection as a few stacked layers. Each one has a single
job and does not care how the layers below it work.

```
Your handlers (On / Emit / rooms)        ← what your app sees
        │
Socket.IO packets (events, acks, ...)    ← "what" is being said
        │
Engine.IO packets (open, ping, msg, ...) ← framing + heartbeat
        │
Transport (WebSocket or polling)         ← the actual byte pipe
```

## What lives where

| Package | Job |
|---------|-----|
| `internal/engineio` | The Engine.IO packet: a type (open, close, ping, pong, message, upgrade, noop) plus a payload, and the code to turn it into bytes and back. |
| `internal/socketio` | The Socket.IO packet: a type (connect, event, ack, ...), a namespace, an optional ack id, and a JSON payload, plus its bytes-and-back code. |
| `internal/transport.go`, `internal/upgrader.go` | The `Transport` interface and `TransportUpgrader` — gorilla (`wsTransport`) or native (`nativeTransport` + `internal/nativews`). |
| `internal/nativews` | Alpha server-side WebSocket codec (RFC 6455) for Autobahn and future gorilla removal. |
| `internal/engineio_handler.go`, `internal/poll_manager.go` | Engine.IO HTTP: polling handshake, long-poll GET, POST send, and WebSocket upgrade on existing sessions. |
| `internal/session_transport.go`, `internal/eio_ws_transport.go` | Swappable transport wrapper and Engine.IO framing over WebSocket after upgrade. |
| `internal/hub.go`, `internal/client.go` | The connection hub (rooms, fan-out) and the per-connection read/write loops. The client talks to the socket **only** through `Transport`. |

## Why the Transport seam matters

`Client` used to hold a WebSocket connection directly. Now it holds a
`Transport`. Two payoffs:

1. **One dependency, one place.** Only `wsTransport` imports gorilla/websocket
   by default. `NativeWebSocket` switches to `internal/nativews` for experiments.
2. **Room to grow.** Polling and WebSocket implement the same interface.
   `sessionTransport` lets an Engine.IO session move from polling to WebSocket
   without restarting pumps. See [UPGRADE.md](UPGRADE.md).

## Packet codecs in one line each

- **Engine.IO:** `4hello` means "message packet carrying `hello`". The first
  character is the type digit; the rest is the payload.
- **Socket.IO:** `2/admin,7["kick","bob"]` means "event on namespace `/admin`,
  expecting ack `7`, with arguments `["kick","bob"]`". The default namespace
  `/` is left off the wire.

Encoding and decoding are exact inverses, and both decoders are fuzz-tested so
junk input is rejected instead of crashing. The test files
(`*_test.go`, including the `Example` functions) are written to read as usage
samples.
