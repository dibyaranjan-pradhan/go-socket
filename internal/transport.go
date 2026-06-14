package internal

import (
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

// Transport is the connection contract the client pumps depend on. It hides the
// concrete WebSocket implementation so the rest of the library never imports a
// third-party package directly. Future transports (HTTP long-polling) implement
// the same interface, which lets a session move between transports unchanged.
type Transport interface {
	// Name reports the transport kind, e.g. "websocket".
	Name() string
	// SetReadLimit caps the size of a single inbound message in bytes.
	SetReadLimit(limit int64)
	// SetReadDeadline bounds how long the next Read may block.
	SetReadDeadline(t time.Time) error
	// SetWriteDeadline bounds how long the next write may block.
	SetWriteDeadline(t time.Time) error
	// OnPong registers a handler run whenever the peer answers a ping.
	OnPong(handler func())
	// Read returns the next application message from the peer.
	Read() ([]byte, error)
	// Write sends one application (text) message to the peer.
	Write(data []byte) error
	// Ping sends a heartbeat ping frame to the peer.
	Ping() error
	// SendClose sends a normal-closure handshake frame.
	SendClose() error
	// Close releases the underlying connection.
	Close() error
}

// wsTransport adapts a gorilla *websocket.Conn to Transport. This is the only
// type in the library that touches the gorilla/websocket package.
type wsTransport struct {
	conn *websocket.Conn
}

func (t *wsTransport) Name() string { return "websocket" }

func (t *wsTransport) SetReadLimit(limit int64) { t.conn.SetReadLimit(limit) }

func (t *wsTransport) SetReadDeadline(tm time.Time) error { return t.conn.SetReadDeadline(tm) }

func (t *wsTransport) SetWriteDeadline(tm time.Time) error { return t.conn.SetWriteDeadline(tm) }

func (t *wsTransport) OnPong(handler func()) {
	t.conn.SetPongHandler(func(string) error {
		handler()
		return nil
	})
}

func (t *wsTransport) Read() ([]byte, error) {
	_, data, err := t.conn.ReadMessage()
	return data, err
}

func (t *wsTransport) Write(data []byte) error {
	return t.conn.WriteMessage(websocket.TextMessage, data)
}

func (t *wsTransport) Ping() error {
	return t.conn.WriteMessage(websocket.PingMessage, nil)
}

func (t *wsTransport) SendClose() error {
	return t.conn.WriteMessage(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
}

func (t *wsTransport) Close() error { return t.conn.Close() }

// WSUpgrader turns an HTTP request into a WebSocket Transport. It owns the
// gorilla upgrader so the server package stays free of the dependency.
type WSUpgrader struct {
	upgrader websocket.Upgrader
}

// NewWSUpgrader builds an upgrader with the given buffer sizes. A nil
// checkOrigin allows connections from any origin.
func NewWSUpgrader(readBufferSize, writeBufferSize int, checkOrigin func(*http.Request) bool) *WSUpgrader {
	if checkOrigin == nil {
		checkOrigin = func(*http.Request) bool { return true }
	}
	return &WSUpgrader{
		upgrader: websocket.Upgrader{
			ReadBufferSize:  readBufferSize,
			WriteBufferSize: writeBufferSize,
			CheckOrigin:     checkOrigin,
		},
	}
}

// Upgrade switches an HTTP request to a WebSocket Transport. On failure the
// gorilla upgrader has already written an error response.
func (u *WSUpgrader) Upgrade(w http.ResponseWriter, r *http.Request) (Transport, error) {
	conn, err := u.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return nil, err
	}
	return &wsTransport{conn: conn}, nil
}
