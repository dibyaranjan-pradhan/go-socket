package gosocket

// Optional wire event names for apps that want shared constants instead of string literals.
// The server still routes purely on the JSON "event" string — these are documentation + convenience only.
//
// Library-reserved synthetic events (not in this list) are EventConnect and EventDisconnect ("$connect", "$disconnect").
//
// PresetChat* are optional wire event names for chat-style apps that want shared
// constants instead of string literals. Other backends may define their own events.
const (
	// PresetChatFetchHistory is a common client→server name for paginated history.
	PresetChatFetchHistory = "fetch_history"
	// PresetChatMessage is a common bidirectional name for chat payloads.
	PresetChatMessage = "message"
	// PresetChatTyping is a common client→server typing indicator.
	PresetChatTyping = "typing"
	// PresetChatHistory is a common server→client batch of messages.
	PresetChatHistory = "history"
	// PresetChatConnected is a common server→client handshake ack.
	PresetChatConnected = "connected"
	// PresetError is a common server→client error envelope event.
	PresetError = "error"
)
