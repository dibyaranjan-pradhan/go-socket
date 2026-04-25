package internal

import (
	"errors"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// ErrRateLimited is returned when a message arrives faster than the configured limit.
var ErrRateLimited = errors.New("gosocket: rate limited")

// Client represents one WebSocket connection and its outbound queue.
type Client struct {
	hub *Hub

	id   string
	conn *websocket.Conn
	send chan []byte

	maxMessageSize int64
	writeWait      time.Duration
	pingPeriod     time.Duration
	pongWait       time.Duration

	onMessage func([]byte)
	onDone    func()

	shutdownOnce sync.Once
	meta         sync.Map

	rateMu    sync.Mutex
	lastEvent time.Time
}

// ClientOptions configures a new Client.
type ClientOptions struct {
	// ID is the public connection identifier surfaced to handlers.
	ID string
	// MaxMessageSize caps inbound frame sizes.
	MaxMessageSize int64
	// WriteWait bounds websocket write deadlines.
	WriteWait time.Duration
	// PingPeriod controls how often ping frames are sent.
	PingPeriod time.Duration
	// PongWait defines the read deadline window refreshed by reads and pongs.
	PongWait time.Duration
	// SendBuffer sets the outbound message channel capacity.
	SendBuffer int
	// OnMessage receives raw JSON payloads from the peer.
	OnMessage func([]byte)
	// OnDone runs once after pumps stop (after unregister).
	OnDone func()
}

// NewClient wires pumps configuration for a peer connection.
func NewClient(hub *Hub, conn *websocket.Conn, opt ClientOptions) *Client {
	if opt.SendBuffer <= 0 {
		opt.SendBuffer = 256
	}
	return &Client{
		hub:            hub,
		id:             opt.ID,
		conn:           conn,
		send:           make(chan []byte, opt.SendBuffer),
		maxMessageSize: opt.MaxMessageSize,
		writeWait:      opt.WriteWait,
		pingPeriod:     opt.PingPeriod,
		pongWait:       opt.PongWait,
		onMessage:      opt.OnMessage,
		onDone:         opt.OnDone,
	}
}

// ID returns the stable connection identifier.
func (c *Client) ID() string {
	return c.id
}

// MetaGet returns metadata associated with this connection.
func (c *Client) MetaGet(key string) (interface{}, bool) {
	return c.meta.Load(key)
}

// MetaSet stores metadata on this connection.
func (c *Client) MetaSet(key string, value interface{}) {
	c.meta.Store(key, value)
}

// AllowEvent enforces a minimum spacing between successful checks for the same client.
func (c *Client) AllowEvent(interval time.Duration) error {
	if interval <= 0 {
		return nil
	}
	now := time.Now()
	c.rateMu.Lock()
	defer c.rateMu.Unlock()
	if !c.lastEvent.IsZero() && now.Sub(c.lastEvent) < interval {
		return ErrRateLimited
	}
	c.lastEvent = now
	return nil
}

// TrySend enqueues a wire frame without blocking. It returns false if the buffer is full or the client has shut down.
func (c *Client) TrySend(data []byte) (ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	select {
	case c.send <- data:
		return true
	default:
		return false
	}
}

// Shutdown closes the socket and outbound channel exactly once.
func (c *Client) Shutdown() {
	c.shutdownOnce.Do(func() {
		_ = c.conn.Close()
		close(c.send)
	})
}

// ReadPump reads JSON frames until the connection ends or fails.
func (c *Client) ReadPump() {
	defer func() {
		c.hub.Unregister(c)
		c.Shutdown()
		if c.onDone != nil {
			c.onDone()
		}
	}()

	c.conn.SetReadLimit(c.maxMessageSize)
	_ = c.conn.SetReadDeadline(time.Now().Add(c.pongWait))
	c.conn.SetPongHandler(func(string) error {
		_ = c.conn.SetReadDeadline(time.Now().Add(c.pongWait))
		return nil
	})

	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
		_ = c.conn.SetReadDeadline(time.Now().Add(c.pongWait))
		if c.onMessage != nil {
			c.onMessage(message)
		}
	}
}

// WritePump sends outbound frames and periodic pings until the send channel closes.
// On exit it attempts a WebSocket close frame so peers can observe graceful shutdown.
func (c *Client) WritePump() {
	ticker := time.NewTicker(c.pingPeriod)
	defer ticker.Stop()
	defer c.writeCloseFrame()

	for {
		select {
		case msg, ok := <-c.send:
			if !ok {
				return
			}
			_ = c.conn.SetWriteDeadline(time.Now().Add(c.writeWait))
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(c.writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// writeCloseFrame sends a normal close control frame; errors are ignored (conn may already be closed).
func (c *Client) writeCloseFrame() {
	_ = c.conn.SetWriteDeadline(time.Now().Add(c.writeWait))
	_ = c.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
}
