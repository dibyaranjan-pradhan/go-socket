# Events reference

Wire format is always JSON with an **`event`** string. Values below are the **exact** names used today unless noted.

## Library (gosocket) — synthetic / internal

These are reserved by the server for lifecycle and logging. Clients do not send them as normal app events.

| Event name | Constant in code | Direction | Purpose |
|------------|------------------|-----------|---------|
| `$connect` | `gosocket.EventConnect` | Internal | Runs middleware after upgrade, before `OnConnect` callbacks |
| `$disconnect` | `gosocket.EventDisconnect` | Internal | Runs disconnect middleware and `OnDisconnect` handlers |

## Optional preset strings (`events.go`)

For convenience (no protocol change), the library exports constants matching common STAG chat usage. You may still use raw string literals.

| Constant | String value | Typical direction |
|----------|--------------|-------------------|
| `PresetChatFetchHistory` | `fetch_history` | Client → server |
| `PresetChatMessage` | `message` | Client → server and server → room |
| `PresetChatTyping` | `typing` | Client → server; server broadcasts |
| `PresetChatHistory` | `history` | Server → client |
| `PresetChatConnected` | `connected` | Server → client |
| `PresetError` | `error` | Server → client |

Adding more presets later is backward compatible: existing apps keep using literals.

## STAG backend (chat) — application events

STAG registers handlers and emits the following on the chat WebSocket (path `/stag/v1/chat/{chatId}/ws`). Payload shapes are defined in the STAG `models` package and REST/OpenAPI if present; this table names events only.

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

Any other `event` name is valid if you register it with `On` in STAG or another app; the table above is what the **current** STAG chat module uses.

## Why optional constants in the library?

**Pros:** One import for shared strings between Go services, generated mobile clients, and docs; fewer typos; grep-friendly.  
**Cons:** Library releases should bump or document if STAG renames an event (or STAG keeps literals and ignores presets).

Recommended: use **`Preset*`** for new cross-team clients; STAG may migrate gradually.
