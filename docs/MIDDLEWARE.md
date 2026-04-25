# Middleware

Event names for the synthetic connect/disconnect passes are listed in **[EVENTS.md](./EVENTS.md)** (`$connect` / `$disconnect`).

Middleware runs **before** your `On` handler for each inbound event, and for the synthetic **`$connect`** pass (after the WebSocket upgrade, before `OnConnect` callbacks).

It does **not** run the same stack as HTTP middleware — only the types registered with `Server.Use`.

## Registration order

`Use` appends middleware. For each inbound message, middleware runs in registration order. If any middleware returns a non-nil error, later middleware and the event handler are skipped (connect may be rejected).

## Recover

```go
s.Use(gosocket.Recover())
```

Enables panic recovery for:

- The synthetic connect middleware pass
- Each inbound event dispatch (`On` handlers)

Panics are logged via the server `Logger`; the connection is disconnected. **Disconnect** callbacks and disconnect-time middleware are each wrapped in their own recover as well, so a panicking `OnDisconnect` handler does not take down the process.

Register `Recover()` once (typically after `LoggerMiddleware`).

## RateLimiter

```go
s.Use(gosocket.RateLimiter(200 * time.Millisecond))
```

Enforces a minimum spacing between **handled** messages **per connection**. Events named `$connect` and `$disconnect` are not rate-limited.

If the client sends faster than the interval, the middleware returns `ErrRejected` and the event handler does not run.

## Custom middleware

Implement the `Middleware` interface:

```go
type Middleware interface {
    RunMiddleware(ctx *gosocket.Context) error
}
```

For a plain function, use the adapter:

```go
s.Use(gosocket.MiddlewareFunc(func(ctx *gosocket.Context) error {
    if ctx.Event() == gosocket.EventConnect {
        return nil
    }
    // ...
    return nil
}))
```

## When middleware runs

| Moment | Runs? |
|--------|--------|
| After upgrade, before `OnConnect` | Yes — synthetic event `$connect` |
| Each inbound JSON event | Yes |
| `OnDisconnect` callbacks | No — those are separate; disconnect **middleware** entries still run with event `$disconnect` |

The built-in **LoggerMiddleware** logs connect, disconnect, and each event name.

## Hub / emit warnings

If a client’s outbound queue is full, the hub logs a warning (`emit queue full`) including `clientID`, `roomID`, and `event`, then shuts down that client. If the hub’s internal emit job queue is saturated, a separate warning is logged (`hub emit queue saturated`).
