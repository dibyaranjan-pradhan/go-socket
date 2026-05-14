# Testing and coverage plan

## Current state

- `server_test.go`: `RoomSize` after join, close frame on teardown, `On` dispatch, `Recover` on panicking handler, **`EmitToRoom`**, **Shutdown / dial after close**, connection **stats** (`GetConnectionStats`, `GetServerStats`), **heartbeat timeout** (`OnHeartbeatTimeout`), and related integration cases.
- `stats_test.go`: zero defaults for `ConnectionStats`, `GetServerStats`, `GetRoomStats` / `ListAllRooms` empty cases (uses shared `newTestContext()` helper).
- `config_test.go`: **`HubEmitBufferSize`** normalization (default, custom, negative → default).
- `types_test.go`: wire message JSON, preset constants, `MiddlewareFunc`.
- `internal/`: hub/client covered mostly **indirectly** via `Server` tests; add focused `internal` tests when the toolchain allows (see coverage below).

**Approximate package coverage (root module only):** run `go test . -coverprofile=cover.out && go tool cover -func=cover.out` — expect **~50–55%** until hub/client get direct tests; `internal` is a large uncovered surface.

## Target: ~80% statement coverage

Achieving ~80% in a small library is realistic once the following are added:

| Area | Tests to add |
|------|----------------|
| **Hub** | Register/unregister/join/leave/emit in isolation with a fake `PrintfLogger`; saturated `Emit` default branch; `RoomSize` / `RoomClients` empty vs non-empty |
| **Client** | `TrySend` success vs full buffer; `Shutdown` idempotency; `AllowEvent` rate limit |
| **Server** | `dispatch` unknown event; handler registered; `emitOne` queue full path (small `WriteBufferSize` + flood) |
| **Middleware** | `RateLimiter` rejection; `Recover` + panicking handler (integration-style) |
| **Context** | `Bind` empty payload; `MetaSet` / `MetaGet` surfaced via public API if any |

## How to measure

From the repo root, **`make print-coverage`** runs `go test` with `-coverprofile=coverage.out` and prints the total statement coverage line (see the **`Makefile`** for `make coverage`, `make coverage-html`, `make ci`, `make vulncheck`, `make errcheck`, and `make staticcheck`).

```bash
make print-coverage
```

Equivalent manual steps (from the repository root):

```bash
go test ./... -coverprofile=cover.out
go tool cover -func=cover.out | tail -1
```

Optional HTML:

```bash
go tool cover -html=cover.out -o cover.html
```

## CI suggestion

Run `go test ./... -race -coverprofile=cover.out` on each PR; fail if total coverage drops below a threshold (e.g. 75% until the suite catches up, then 80%).

## Integration tests (this module)

The root package tests include **integration-style** cases (real `httptest` + `websocket.Dial`): room join + `RoomSize`, close frames, `On` dispatch, `Recover` on panic, **`EmitToRoom` to a second peer**, **Shutdown** draining, and **dial rejected after Shutdown**. Run:

```bash
go test ./... -count=1
```

For extra confidence before a release, also run with the race detector (slower):

```bash
go test ./... -race -count=1
```

## Manual smoke (STAG)

After library changes: connect chat WS, `fetch_history`, `message`, disconnect; confirm close frame and no hub panic on disconnect handler failure.
