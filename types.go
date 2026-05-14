// Package gosocket provides a small JSON-over-WebSocket server built on gorilla/websocket.
package gosocket

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"
)

const (
	// EventConnect is the synthetic event name used when running middleware for a new connection.
	EventConnect = "$connect"
	// EventDisconnect is the synthetic event name passed to OnDisconnect handlers.
	EventDisconnect = "$disconnect"
	// DefaultMaxConnections is the default upper bound on concurrent sockets.
	DefaultMaxConnections = 10000
	// DefaultWriteBufferSize is the default per-client outbound queue depth.
	DefaultWriteBufferSize = 256
	// DefaultReadBufferSize is the default size passed to websocket.Upgrader.ReadBufferSize.
	DefaultReadBufferSize = 4096
	// DefaultMaxMessageSize is the default maximum inbound frame size (1 MiB).
	DefaultMaxMessageSize = 1 << 20
	// DefaultPingInterval is the default interval between server ping frames.
	DefaultPingInterval = 30 * time.Second
	// DefaultPongWait is the default read deadline extension window.
	DefaultPongWait = 60 * time.Second
	// DefaultWriteWait is the default write deadline for websocket writes.
	DefaultWriteWait = 10 * time.Second
	// DefaultHubEmitBufferSize is the default capacity of the hub outbound emit job queue.
	DefaultHubEmitBufferSize = 256
)

var (
	// ErrRejected indicates middleware rejected a connection or event.
	ErrRejected = errors.New("gosocket: rejected by middleware")
	// ErrServerClosed is returned when the server has shut down.
	ErrServerClosed = errors.New("gosocket: server closed")
	// ErrConnectionLimit is returned when MaxConnections is reached.
	ErrConnectionLimit = errors.New("gosocket: connection limit reached")
)

// Config holds tunables for the WebSocket server and pumps.
type Config struct {
	// IdentityKey is the metadata key used by RoomIdentities to identify clients.
	// Set via ctx.Set(IdentityKey, value) in OnConnect handlers.
	// Defaults to returning clientIDs if empty.
	IdentityKey string
	// MaxConnections caps concurrent WebSocket clients. Zero uses DefaultMaxConnections.
	MaxConnections int
	// WriteBufferSize is the outbound message channel depth per client. Zero uses DefaultWriteBufferSize.
	WriteBufferSize int
	// ReadBufferSize is passed to websocket.Upgrader. Zero uses DefaultReadBufferSize.
	ReadBufferSize int
	// MaxMessageSize is the maximum read frame size in bytes. Zero uses DefaultMaxMessageSize.
	MaxMessageSize int64
	// PingInterval is how often the server sends ping frames. Zero uses DefaultPingInterval.
	PingInterval time.Duration
	// PongWait is the maximum time between successful pongs before the read deadline fails. Zero uses DefaultPongWait.
	PongWait time.Duration
	// WriteWait is the write deadline for control and data frames. Zero uses DefaultWriteWait.
	WriteWait time.Duration
	// HubEmitBufferSize is the capacity of the hub's outbound emit job queue.
	// Increase this for broadcast-heavy workloads to prevent message drops.
	// Default is 256; recommended minimum for high-throughput apps is 2048+.
	HubEmitBufferSize int
	// CheckOrigin is passed to websocket.Upgrader. If nil, all origins are allowed.
	CheckOrigin func(r *http.Request) bool
	// Logger receives diagnostic lines. If nil, the standard library log package is used.
	Logger Logger
}

func (c *Config) normalized() Config {
	out := *c
	if out.MaxConnections <= 0 {
		out.MaxConnections = DefaultMaxConnections
	}
	if out.WriteBufferSize <= 0 {
		out.WriteBufferSize = DefaultWriteBufferSize
	}
	if out.ReadBufferSize <= 0 {
		out.ReadBufferSize = DefaultReadBufferSize
	}
	if out.MaxMessageSize <= 0 {
		out.MaxMessageSize = DefaultMaxMessageSize
	}
	if out.PingInterval <= 0 {
		out.PingInterval = DefaultPingInterval
	}
	if out.PongWait <= 0 {
		out.PongWait = DefaultPongWait
	}
	if out.WriteWait <= 0 {
		out.WriteWait = DefaultWriteWait
	}
	if out.HubEmitBufferSize <= 0 {
		out.HubEmitBufferSize = DefaultHubEmitBufferSize
	}
	return out
}

// EventHandler handles a named client event.
type EventHandler func(ctx *Context)

// Middleware runs before an event handler (or the synthetic connect pass). Returning a non-nil error rejects the connection or skips the event.
// Use MiddlewareFunc to wrap plain functions. Recover() returns a distinct type so the server can enable panic handling without reflect.
type Middleware interface {
	RunMiddleware(ctx *Context) error
}

// MiddlewareFunc adapts a function to Middleware (like http.HandlerFunc).
type MiddlewareFunc func(ctx *Context) error

// RunMiddleware implements Middleware.
func (f MiddlewareFunc) RunMiddleware(ctx *Context) error {
	return f(ctx)
}

// ClientWireMessage is the JSON shape clients send to the server.
type ClientWireMessage struct {
	// Event names the handler route on the server.
	Event string `json:"event"`
	// Room optionally scopes join/emit routing for the message.
	Room string `json:"room,omitempty"`
	// Payload carries the JSON body for Bind and handlers.
	Payload json.RawMessage `json:"payload"`
	// ID optionally correlates a single ack-bearing server response (ackId).
	ID string `json:"id,omitempty"`
}

// ServerWireMessage is the JSON shape the server sends to clients.
type ServerWireMessage struct {
	// Event mirrors the logical event name delivered to the client.
	Event string `json:"event"`
	// Payload is JSON-marshaled into the wire frame.
	Payload interface{} `json:"payload"`
	// AckID is set when replying to a client-provided id.
	AckID string `json:"ackId,omitempty"`
}
