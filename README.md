# go-socket

JSON-over-WebSocket server for Go, built on [gorilla/websocket](https://github.com/gorilla/websocket). Used by **STAG** for real-time chat; usable by any service that wants named events, rooms, and middleware on a single upgrade path.

## When to use

- You control the **Go server** and want **WebSocket** with **event names** (not a raw stream).
- You need **rooms**, **per-connection metadata**, optional **ack ids**, and **middleware** (logging, recover, rate limit).

## Integration (quick)

1. **Module** — in the consuming repo:

   ```bash
   go get github.com/dibyaranjan-pradhan/go-socket
   ```

   Or a `replace` directive to a local copy (as STAG does with `./lib/go-socket`).

2. **Construct** — one `Server` per HTTP mux path (or one shared server with one `Handler()`):

   ```go
   s := gosocket.New(gosocket.Config{
       Logger:         myLogger, // anything with Printf(format string, v ...interface{})
       MaxMessageSize: 512 * 1024,
       PingInterval:   30 * time.Second,
       PongWait:       60 * time.Second,
       WriteWait:      10 * time.Second,
   })
   s.Use(s.LoggerMiddleware())
   s.Use(gosocket.Recover())
   ```

3. **HTTP** — mount the upgrader on a route (often behind your existing auth middleware so `*http.Request` carries user context):

   ```go
   http.Handle("/stag/v1/chat/{id}/ws", yourAuth(s.Handler()))
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

## Repository layout

| Path | Role |
|------|------|
| Root `.go` files | Public API: `Server`, `Context`, `Config`, middleware, optional `Preset*` event constants |
| `internal/` | Hub, client pumps — **do not import** from apps |
| `docs/` | Wire format, middleware, **events reference**, testing/coverage plan |

## Documentation index

- **[docs/WIRE_PROTOCOL.md](./docs/WIRE_PROTOCOL.md)** — JSON shapes, rooms, acks  
- **[docs/MIDDLEWARE.md](./docs/MIDDLEWARE.md)** — `Recover`, `RateLimiter`, `MiddlewareFunc`  
- **[docs/EVENTS.md](./docs/EVENTS.md)** — synthetic + STAG chat event names and payload pointers  
- **[docs/TESTING.md](./docs/TESTING.md)** — coverage goals and test plan  

## Requirements

- Go **1.21+** (module declares 1.25.x in this repo; align with your toolchain).  
- Logger: implement **`Printf(format string, v ...interface{})`** only.

## License / stability

Treat public types under semantic versioning once published; internal packages may change. Prefer importing only `github.com/dibyaranjan-pradhan/go-socket` (not `internal`).
