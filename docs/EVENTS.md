# Events reference

Wire format is always JSON with an **`event`** string. Values below are the **exact** names used today unless noted.

## Library (gosocket) — synthetic / internal

These are reserved by the server for lifecycle and logging. Clients do not send them as normal app events.

| Event name | Constant in code | Direction | Purpose |
|------------|------------------|-----------|---------|
| `$connect` | `gosocket.EventConnect` | Internal | Runs middleware after upgrade, before `OnConnect` callbacks |
| `$disconnect` | `gosocket.EventDisconnect` | Internal | Runs disconnect middleware and `OnDisconnect` handlers |

## Optional preset strings (`events.go`)

For convenience (no protocol change), the library exports constants for common chat-style event names. You may still use raw string literals.

| Constant | String value | Typical direction |
|----------|--------------|-------------------|
| `PresetChatFetchHistory` | `fetch_history` | Client → server |
| `PresetChatMessage` | `message` | Client → server and server → room |
| `PresetChatTyping` | `typing` | Client → server; server broadcasts |
| `PresetChatHistory` | `history` | Server → client |
| `PresetChatConnected` | `connected` | Server → client |
| `PresetError` | `error` | Server → client |

Adding more presets later is backward compatible: existing apps keep using literals.

## Example chat application events

A typical chat service might register handlers and emit the following on its WebSocket path. Payload shapes are defined by your application; this table names events only.

### Client → server (register with `Server.On`)

| `event` | Role |
|---------|------|
| `fetch_history` | Load older messages (cursor / `beforeMessageId` in payload) |
| `message` | Send chat message body |
| `typing` | Typing indicator |

### Server → client (`Emit` / `EmitToRoom` / `BroadcastToRoom`)

| `event` | Role |
|---------|------|
| `history` | Array of messages (initial or paged) |
| `connected` | Short handshake message after connect |
| `message` | Persisted message fan-out to room |
| `typing` | Typing fan-out (excludes sender on broadcast) |
| `error` | `{ "message": "..." }` style error envelope |

Any other `event` name is valid if you register it with `On` in your app.

## Why optional constants in the library?

**Pros:** One import for shared strings between Go services, generated clients, and docs; fewer typos; grep-friendly.  
**Cons:** Library releases should document if a preset string value changes (apps can always keep literals).

Recommended: use **`Preset*`** when you want shared names across teams; literals remain fully supported.
