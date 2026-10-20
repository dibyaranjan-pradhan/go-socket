// Copyright (c) 2026 Dibyaranjan Pradhan. All rights reserved.

package internal

import (
	"errors"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// ErrRateLimited is returned when a message arrives faster than the configured limit.
var ErrRateLimited = errors.New("gosocket: rate limited")

// Client represents one peer connection and its outbound queue. The underlying
// socket is reached only through the Transport interface.
type Client struct {
	hub *Hub

	id        string
	transport Transport
	send      chan []byte

	maxMessageSize int64
	writeWait      time.Duration
	pingPeriod     time.Duration
	pongWait       time.Duration

	onMessage          func([]byte)
	onDone             func()
	onHeartbeatTimeout func()

	shutdownOnce sync.Once
	meta         sync.Map

	// Recovery state. session is atomic because the hub goroutine reads it while
	// the connect path binds it. replay is written before WritePump starts.
	session  atomic.Pointer[Session]
	replay   [][]byte
	parked   atomic.Bool
	resumed  atomic.Bool
	noResume atomic.Bool
	dropped  atomic.Int64

	rateMu    sync.Mutex
	lastEvent time.Time

	// Stats tracking
	bytesSent     atomic.Int64
	bytesReceived atomic.Int64
	messagesSent  atomic.Int64
	messagesRcvd  atomic.Int64
	connectedAt   time.Time
	lastActivity  atomic.Value // time.Time
	missedPongs   atomic.Int32
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
	// OnHeartbeatTimeout is invoked after repeated read deadlines without a pong (zombie path).
	OnHeartbeatTimeout func()
}

// NewClient wires pumps configuration for a peer connection over the transport.
func NewClient(hub *Hub, transport Transport, opt ClientOptions) *Client {
	if opt.SendBuffer <= 0 {
		opt.SendBuffer = 256
	}
	now := time.Now()
	c := &Client{
		hub:                hub,
		id:                 opt.ID,
		transport:          transport,
		send:               make(chan []byte, opt.SendBuffer),
		maxMessageSize:     opt.MaxMessageSize,
		writeWait:          opt.WriteWait,
		pingPeriod:         opt.PingPeriod,
		pongWait:           opt.PongWait,
		onMessage:          opt.OnMessage,
		onDone:             opt.OnDone,
		onHeartbeatTimeout: opt.OnHeartbeatTimeout,
		connectedAt:        now,
	}
	c.lastActivity.Store(now)
	return c
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

type enqueueResult uint8

const (
	enqOK enqueueResult = iota
	enqFull
	enqClosed
)

// enqueue is a non-blocking send that tolerates a concurrently closed channel.
func (c *Client) enqueue(frame []byte) (res enqueueResult) {
	defer func() {
		if recover() != nil {
			res = enqClosed
		}
	}()
	select {
	case c.send <- frame:
		return enqOK
	default:
		return enqFull
	}
}

// TrySend enqueues a wire frame without blocking. It returns false if the buffer is full or the client has shut down.
func (c *Client) TrySend(data []byte) bool {
	return c.TrySendPolicy(data, PolicyDisconnect)
}

// TrySendPolicy is TrySend with a per-frame backpressure policy. A false return
// means the caller should disconnect the peer; Drop and Volatile never ask for that
// on a full queue. With a bound Session the frame is sequenced and buffered first.
func (c *Client) TrySendPolicy(data []byte, pol SendPolicy) bool {
	if s := c.session.Load(); s != nil {
		return s.deliver(data, pol)
	}
	switch c.enqueue(data) {
	case enqOK:
		return true
	case enqFull:
		if pol == PolicyDisconnect {
			return false
		}
		c.dropped.Add(1)
		return true
	}
	return false
}

// Shutdown closes the socket and outbound channel exactly once. It is a terminal
// close: the session (if any) will not be resumable.
func (c *Client) Shutdown() {
	c.noResume.Store(true)
	c.closeRaw()
}

// ShutdownResumable closes the transport but leaves the session resumable. Used
// for slow-consumer disconnects, where the point is that the peer reconnects and replays.
func (c *Client) ShutdownResumable() {
	c.closeRaw()
}

func (c *Client) closeRaw() {
	c.shutdownOnce.Do(func() {
		_ = c.transport.Close()
		close(c.send)
	})
}

// Session returns the bound recovery session, or nil when recovery is off.
func (c *Client) Session() *Session { return c.session.Load() }

// Resumed reports whether this connection continued an earlier session.
func (c *Client) Resumed() bool { return c.resumed.Load() }

// SetResumed records the handshake outcome for handlers.
func (c *Client) SetResumed(v bool) { c.resumed.Store(v) }

// Parked reports whether the client is a detached session member awaiting resume.
func (c *Client) Parked() bool { return c.parked.Load() }

// CopyMetaFrom carries metadata over from the connection this one replaces.
func (c *Client) CopyMetaFrom(prev *Client) {
	if prev == nil || prev == c {
		return
	}
	prev.meta.Range(func(k, v any) bool {
		c.meta.LoadOrStore(k, v)
		return true
	})
}

// retire runs when the read side ends. Recovery-bound clients stay registered in
// the hub (as parked members) so room broadcasts keep landing in the replay buffer.
func (c *Client) retire() {
	s := c.session.Load()
	if s == nil {
		c.hub.Unregister(c)
		return
	}
	switch s.store.Leave(s, c, !c.noResume.Load()) {
	case LeaveParked, LeaveDisplaced:
		return
	}
	c.hub.Unregister(c)
}

// GetStats returns a snapshot of this client's metrics.
func (c *Client) GetStats() *ClientStats {
	bufferLen := len(c.send)
	bufferCap := cap(c.send)
	utilization := float32(0)
	if bufferCap > 0 {
		utilization = float32(bufferLen) / float32(bufferCap)
	}

	dropped := c.dropped.Load()
	if sess := c.session.Load(); sess != nil {
		dropped += sess.Dropped()
	}

	lastActivity := c.connectedAt
	if v := c.lastActivity.Load(); v != nil {
		lastActivity = v.(time.Time)
	}

	return &ClientStats{
		BytesSent:         c.bytesSent.Load(),
		BytesReceived:     c.bytesReceived.Load(),
		MessagesSent:      c.messagesSent.Load(),
		MessagesReceived:  c.messagesRcvd.Load(),
		MessagesDropped:   dropped,
		BufferUtilization: utilization,
		ConnectedAt:       c.connectedAt,
		LastActivity:      lastActivity,
	}
}

func (c *Client) recordBytesSent(n int64) {
	c.bytesSent.Add(n)
	c.lastActivity.Store(time.Now())
}

func (c *Client) recordBytesReceived(n int64) {
	c.bytesReceived.Add(n)
	c.lastActivity.Store(time.Now())
}

func (c *Client) recordMessageSent() {
	c.messagesSent.Add(1)
}

func (c *Client) recordMessageReceived() {
	c.messagesRcvd.Add(1)
}

// IncrementMissedPongs increments the pong miss counter and returns true if threshold reached.
func (c *Client) IncrementMissedPongs() bool {
	count := c.missedPongs.Add(1)
	return count > 2
}

// ReadPump reads JSON frames until the connection ends or fails.
func (c *Client) ReadPump() {
	defer func() {
		c.retire()
		c.closeRaw()
		if c.onDone != nil {
			c.onDone()
		}
	}()

	c.transport.SetReadLimit(c.maxMessageSize)
	_ = c.transport.SetReadDeadline(time.Now().Add(c.pongWait))
	c.transport.OnPong(func() {
		_ = c.transport.SetReadDeadline(time.Now().Add(c.pongWait))
		c.missedPongs.Store(0)
	})

	for {
		message, err := c.transport.Read()
		if err != nil {
			var netErr net.Error
			if (errors.As(err, &netErr) && netErr.Timeout()) || errors.Is(err, os.ErrDeadlineExceeded) {
				if c.IncrementMissedPongs() {
					if c.onHeartbeatTimeout != nil {
						c.onHeartbeatTimeout()
					}
					return
				}
				_ = c.transport.SetReadDeadline(time.Now().Add(c.pongWait))
				continue
			}
			return
		}
		_ = c.transport.SetReadDeadline(time.Now().Add(c.pongWait))
		c.recordBytesReceived(int64(len(message)))
		c.recordMessageReceived()
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

	// Handshake control frame and replay go first so they precede any live frame.
	for _, msg := range c.replay {
		_ = c.transport.SetWriteDeadline(time.Now().Add(c.writeWait))
		if err := c.transport.Write(msg); err != nil {
			return
		}
		c.recordBytesSent(int64(len(msg)))
		c.recordMessageSent()
	}
	c.replay = nil

	for {
		select {
		case msg, ok := <-c.send:
			if !ok {
				return
			}
			_ = c.transport.SetWriteDeadline(time.Now().Add(c.writeWait))
			if err := c.transport.Write(msg); err != nil {
				return
			}
			c.recordBytesSent(int64(len(msg)))
			c.recordMessageSent()
		case <-ticker.C:
			_ = c.transport.SetWriteDeadline(time.Now().Add(c.writeWait))
			if err := c.transport.Ping(); err != nil {
				return
			}
		}
	}
}

// writeCloseFrame sends a normal close control frame; errors are ignored (conn may already be closed).
func (c *Client) writeCloseFrame() {
	_ = c.transport.SetWriteDeadline(time.Now().Add(c.writeWait))
	_ = c.transport.SendClose()
}
