package engineio

// Engine.IO v4 constants shared by the polling transport and HTTP handler.
const (
	// Version is the Engine.IO protocol revision this package implements.
	Version = "4"
	// RecordSeparator splits multiple packets in one HTTP body.
	RecordSeparator = "\x1e"
	// TransportPolling is the query-string value for HTTP long-polling.
	TransportPolling = "polling"
	// TransportWebSocket is advertised in the open handshake upgrades list.
	TransportWebSocket = "websocket"
)
