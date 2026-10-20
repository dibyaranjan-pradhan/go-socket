# Session recovery (v0.5.0)

Make a logical session survive a dropped connection: the server numbers every
outbound event, keeps a bounded replay buffer, and a reconnecting client resumes
from the last event it saw. Opt-in; with `SessionRecovery.Enabled == false` nothing
about the wire or behaviour changes.

## Why

Mobile networks, load balancers and IoT links drop sockets constantly. Without
recovery every drop loses whatever was in flight and forces the app to re-sync
state. With recovery a 5-second outage costs the client one reconnect and a replay
of the few events it missed.

**Not covered:** server restarts and multi-node setups. Sessions live in process
memory; resume only works against the node that holds the session (cross-node
replay arrives with the Redis/NATS adapters in a later milestone). When a resume
is impossible the client is told and falls back to a fresh session — it never gets
silent partial state.

## Enable

```go
s := gosocket.New(gosocket.Config{
    SessionRecovery: gosocket.SessionRecoveryConfig{
        Enabled:              true,
        ReplayBufferMessages: 512,              // per session, default 256
        ReplayBufferBytes:    512 << 10,        // per session, default 256 KiB
        SessionTTL:           2 * time.Minute,  // how long a dropped session stays resumable
        MaxParkedSessions:    10000,            // dropped sessions kept in memory
        IdempotencyWindow:    1024,             // inbound keys remembered per session
        // Bind sessions to a verified identity so a leaked token is useless to others.
        OwnerFunc: func(r *http.Request) string { return userIDFrom(r) },
    },
})

s.OnConnect(func(ctx *gosocket.Context) {
    ctx.JoinRoom("lobby") // safe to repeat on resume
    if ctx.Resumed() {
        return // welcome/snapshot already delivered
    }
    _ = ctx.Emit("welcome", nil)
})

// "Truly offline" presence: OnDisconnect fires on every transport drop,
// including brief ones. This fires only if the session was never resumed.
s.OnSessionExpire(func(i gosocket.SessionInfo) { markOffline(i.Owner) })
```

## Wire protocol

All additions are optional JSON fields; clients that ignore unknown fields keep working.

| Direction | Item | Meaning |
|-----------|------|---------|
| S→C | first frame `{"event":"$session","payload":{...}}` | `sessionId`, `resumed`, `replayed`, `lastSeq`, `reason?` (not sequenced, never replayed) |
| S→C | `"seq": N` on every reliable event | per-session, starts at 1, contiguous on the wire |
| S→C | `"idempotencyKey"` | echoed from `EmitOptions.IdempotencyKey` |
| C→S | `?gs_session=<id>&gs_seq=<last seen>` on the connect URL | resume request. Headers `X-GoSocket-Session` / `X-GoSocket-Seq` also work and win; prefer them for non-browser clients (keeps the token out of access logs). Works on the plain WebSocket URL and on the Engine.IO handshake. |
| C→S | `{"event":"$ack","payload":{"seq":N}}` | optional; frees buffered events ≤ N early. Not counted, bypasses middleware. |
| C→S | `"idempotencyKey": "..."` on any event | handler runs once per key per session window |

`$session.reason` when a resume was refused: `unknown_session`, `expired`,
`owner_mismatch`, `gap_exceeded`, `bad_seq`, `bad_request`. In every refusal the
client gets a **fresh** session (`resumed:false`) and must resync its state.

### Client algorithm

```text
state: sessionId, lastSeq = 0
connect(url + "?gs_session=" + sessionId + "&gs_seq=" + lastSeq)   // omit on first connect
on frame:
  if event == "$session":
      if !resumed and sessionId was set: resyncFromScratch()       // gap / expired / foreign
      sessionId = payload.sessionId
      if !resumed: lastSeq = 0
      return
  if frame.seq:
      if frame.seq <= lastSeq: return          // duplicate from replay overlap
      lastSeq = frame.seq
  handle(frame)
every ~N events or T seconds: send {"event":"$ack","payload":{"seq":lastSeq}}
on retry of a request: reuse the same idempotencyKey
```

## Delivery guarantees

- **At-least-once** for reliable events within the replay window. After a resume the
  client may see an event twice (if its `gs_seq` lagged); dedupe on `seq`.
- **Ordered**: seq order equals wire order, replay precedes new traffic.
- **Exactly-once effect** for client→server retries when the client reuses the
  `idempotencyKey`: replay delivers the first attempt's response, the duplicate is
  suppressed. Keys are claimed after middleware passes, so a rate-limited attempt
  can be retried with the same key. A handler that panics still burns its key; use
  a new key for a deliberate re-run.
- Room broadcasts sent **while the client is disconnected** are buffered too: a
  dropped session stays a (hidden) room member until it resumes or expires.

## Backpressure policy

Applies to every emit, with or without recovery. Default is unchanged: disconnect a
peer whose queue is full.

| Policy | Queue full | Sequenced / replayed |
|--------|-----------|----------------------|
| `BackpressureDisconnect` (default) | disconnect the slow peer; with recovery the event is buffered first, so the peer reconnects and replays it | yes |
| `BackpressureDrop` | discard this event, keep the connection | yes (when it was queued) |
| `BackpressureVolatile` | discard if the peer is not ready, never buffered | no — stale-is-worthless data (cursors, telemetry) |

```go
cfg.DefaultBackpressure = gosocket.BackpressureDrop            // server-wide default
_ = ctx.EmitWith("cursor", pos, gosocket.EmitOptions{Policy: gosocket.BackpressureVolatile})
_ = ctx.EmitToRoomWith("room", "job_done", res, gosocket.EmitOptions{IdempotencyKey: jobID})
```

## Memory bounds

Per session the buffer is capped by both `ReplayBufferMessages` and
`ReplayBufferBytes` (oldest evicted first). Total worst case:

```text
(live sessions + MaxParkedSessions) x ReplayBufferBytes
```

Broadcast events share one backing array across sessions, so real usage is far
lower. Dropped sessions are removed after `SessionTTL` and the oldest are evicted
beyond `MaxParkedSessions`. Size the buffer as `peak events/sec x longest outage
you want to cover`.

## Behaviour to know

- `ClientID` is per **connection**; `SessionID` is stable across resumes.
  `OnDisconnect` / `OnConnect` fire on every drop / reconnect — use `ctx.Resumed()`
  and `OnSessionExpire`.
- Connect middleware runs again on resume (auth is re-checked) before the old
  connection is displaced. `ctx.SessionID()` is empty inside connect middleware.
- `ctx.Disconnect()` terminates the session: a kicked peer cannot resume.
- A newer resume of a still-"live" session (zombie socket) closes the old socket.
- `SessionID` is a bearer secret. Use `OwnerFunc` unless the token alone is an
  acceptable credential, and avoid logging it.
- Broadcasts lost to a saturated hub queue (`HubEmitBufferSize`) happen before
  sequencing and leave no gap; watch `ServerStats.HubEmitsDropped`.
- Metadata set with `ctx.Set` is carried onto the resumed connection.

## Observability

`ServerStats`: `Sessions`, `SessionsParked`, `SessionsResumed`, `MessagesReplayed`,
`DuplicatesSuppressed`, `HubEmitsDropped`. `ConnectionStats`: `MessagesDropped`,
`SessionID`.
