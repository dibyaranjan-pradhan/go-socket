# go-socket

JSON-over-WebSocket server for Go, built on [gorilla/websocket](https://github.com/gorilla/websocket). Use it in any service that wants named events, rooms, and middleware on a single upgrade path.

**v0.1.0** adds generic observability hooks (per-connection and server stats), a tunable hub emit queue (`Config.HubEmitBufferSize`), heartbeat-timeout callbacks for zombie connections, and optional `ShutdownWithMessage` for graceful deploys. See below.

## When to use

- You control the **Go server** and want **WebSocket** with **event names** (not a raw stream).
- You need **rooms**, **per-connection metadata**, optional **ack ids**, and **middleware** (logging, recover, rate limit).

## Integration (quick)

1. **Module** — in the consuming repo:
  ```bash
   go get github.com/dibyaranjan-pradhan/go-socket
  ```
2. **Construct** — one `Server` per HTTP mux path (or one shared server with one `Handler()`):
  ```go
   s := gosocket.New(gosocket.Config{
       Logger:              myLogger, // anything with Printf(format string, v ...interface{})
       MaxMessageSize:      512 * 1024,
       PingInterval:        30 * time.Second,
       PongWait:            60 * time.Second,
       WriteWait:           10 * time.Second,
       HubEmitBufferSize:   2048, // optional: hub fan-out queue; default 256, raise for heavy broadcast
   })
   s.Use(s.LoggerMiddleware())
   s.Use(gosocket.Recover())
  ```
3. **HTTP** — mount the upgrader on a route (often behind your existing auth middleware so `*http.Request` carries user context):
  ```go
   http.Handle("/chat/{id}/ws", yourAuth(s.Handler()))
  ```
4. **Handlers** — register events and lifecycle hooks:
  ```go
   s.OnConnect(func(ctx *gosocket.Context) { ctx.JoinRoom(roomID) })
   s.On("hello", func(ctx *gosocket.Context) { _ = ctx.Emit("world", payload) })
  ```
5. **Shutdown** — on process stop:
  ```go
   _ = s.Shutdown(ctx)
  ```
   For zero-downtime style rollouts you can warn clients before the hub drains:
6. **Observability (v0.1.0)** — metrics without wiring every app:
  ```go
   agg := s.GetServerStats() // aggregate: active sockets, lifetime connects, messages processed, uptime
   cs := s.GetConnectionStats(clientID) // per socket: bytes, message counts, buffer fill, rooms, last activity
   rs := s.GetRoomStats(roomID)         // members + room creation time
   all := s.ListAllRooms()             // snapshot of every room
  ```
7. **Heartbeat timeouts** — after several read deadlines without a responding pong (slow or zombie peer), registered handlers run:
  ```go
   s.OnHeartbeatTimeout(func(ctx *gosocket.Context) {
       // log, metrics, or ctx.Disconnect()
   })
  ```
8. **Engine.IO polling (v0.3.0)** — optional HTTP long-polling for Engine.IO v4 clients. Mount alongside WebSocket; existing WS clients are unchanged:
  ```go
   mux.Handle("/engine.io/", s.EngineIOHandler())
  ```
   See **[docs/ENGINEIO.md](./docs/ENGINEIO.md)** for handshake, POST/GET flow, and limitations.

## Repository layout


| Path             | Role                                                                                      |
| ---------------- | ----------------------------------------------------------------------------------------- |
| Root `.go` files | Public API: `Server`, `Context`, `Config`, middleware, optional `Preset*` event constants |
| `internal/`      | Hub, client pumps — **do not import** from apps                                           |
| `docs/`          | Wire format, middleware, **events reference**, testing/coverage plan                      |
| `Makefile`       | **fmt, vet, test, race, coverage %, govulncheck, errcheck, staticcheck** (`make help`)    |


## Documentation index

- **[docs/WIRE_PROTOCOL.md](./docs/WIRE_PROTOCOL.md)** — JSON shapes, rooms, acks  
- **[docs/MIDDLEWARE.md](./docs/MIDDLEWARE.md)** — `Recover`, `RateLimiter`, `MiddlewareFunc`  
- **[docs/EVENTS.md](./docs/EVENTS.md)** — synthetic and optional preset event names  
- **[docs/ENGINEIO.md](./docs/ENGINEIO.md)** — Engine.IO v4 HTTP long-polling (`EngineIOHandler`)  
- **[docs/ARCHITECTURE.md](./docs/ARCHITECTURE.md)** — internals for contributors  
- **[docs/TESTING.md](./docs/TESTING.md)** — coverage goals and test plan

## Requirements

- Go **1.21+** (module declares 1.25.x in this repo; align with your toolchain).  
- Logger: implement `**Printf(format string, v ...interface{})`** only.

## License / stability

Treat public types under semantic versioning once published; internal packages may change. Prefer importing only `github.com/dibyaranjan-pradhan/go-socket` (not `internal`).