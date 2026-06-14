# Architecture (internals)

This page explains how go-socket is put together inside. It is for people who
work **on** the library. If you only want to **use** it, the root
[README](../README.md) is enough.

> Status: the pieces below are **foundation work** (roadmap Month 1). They are
> wired in gradually. Today the public behaviour and the wire format are
> unchanged — these internals do not yet alter how clients talk to the server.

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
Transport (WebSocket today)              ← the actual byte pipe
```

## What lives where

| Package | Job |
|---------|-----|
| `internal/engineio` | The Engine.IO packet: a type (open, close, ping, pong, message, upgrade, noop) plus a payload, and the code to turn it into bytes and back. |
| `internal/socketio` | The Socket.IO packet: a type (connect, event, ack, ...), a namespace, an optional ack id, and a JSON payload, plus its bytes-and-back code. |
| `internal/transport.go` | The `Transport` interface — a plain contract for "read a message, write a message, ping, close". `wsTransport` is the one place that uses the third-party WebSocket package. |
| `internal/hub.go`, `internal/client.go` | The connection hub (rooms, fan-out) and the per-connection read/write loops. The client talks to the socket **only** through `Transport`. |

## Why the Transport seam matters

`Client` used to hold a WebSocket connection directly. Now it holds a
`Transport`. Two payoffs:

1. **One dependency, one place.** Only `wsTransport` imports the third-party
   WebSocket package. Everything else is standard library. This is what lets us
   later ship our own WebSocket code and drop the dependency.
2. **Room to grow.** A second transport (HTTP long-polling) can implement the
   same interface, so a session can switch transports without the rest of the
   code noticing.

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
