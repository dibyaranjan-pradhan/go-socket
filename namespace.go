// Copyright (c) 2026 Dibyaranjan Pradhan. All rights reserved.

package gosocket

import (
	"errors"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/dibyaranjan-pradhan/go-socket/internal"
)

// Socket.IO tunables. Zero values in SocketIOConfig fall back to these.
const (
	// DefaultSocketIOConnectTimeout is how long a Socket.IO connection may sit
	// without CONNECTing to any namespace before it is closed (matches socket.io).
	DefaultSocketIOConnectTimeout = 45 * time.Second
	// DefaultAckTimeout bounds server-to-client acks when the caller passes no timeout.
	DefaultAckTimeout = 30 * time.Second
	// DefaultMaxPendingAcks caps unanswered server-to-client acks per socket.
	DefaultMaxPendingAcks = 1024

	maxNamespaceLen = 128
)

// SocketIOConfig tunes the Socket.IO dialect served by Server.SocketIOHandler.
type SocketIOConfig struct {
	// ConnectTimeout closes a connection that never CONNECTs to a namespace.
	// Zero uses DefaultSocketIOConnectTimeout.
	ConnectTimeout time.Duration
	// AckTimeout is the default for EmitWithAck when timeout <= 0.
	// Zero uses DefaultAckTimeout.
	AckTimeout time.Duration
	// MaxPendingAcks caps unanswered EmitWithAck calls per socket so a silent
	// client cannot grow memory without bound. Zero uses DefaultMaxPendingAcks.
	MaxPendingAcks int
}

var (
	// ErrAckTimeout is passed to an ack callback when the peer did not answer in time.
	ErrAckTimeout = errors.New("gosocket: ack timeout")
	// ErrAckDisconnected is passed to an ack callback when the socket closed first.
	ErrAckDisconnected = errors.New("gosocket: socket disconnected before ack")
	// ErrTooManyPendingAcks is returned by EmitWithAck when MaxPendingAcks is reached.
	ErrTooManyPendingAcks = errors.New("gosocket: too many pending acks")
	// ErrNotSocketIO is returned by Socket.IO-only calls on a legacy connection.
	ErrNotSocketIO = errors.New("gosocket: operation requires a Socket.IO connection")
	// ErrNoAckRequested is returned by Ack when the inbound event carried no ack id.
	ErrNoAckRequested = errors.New("gosocket: inbound event did not request an ack")
	// ErrAckAlreadySent is returned when Ack is called twice for one event.
	ErrAckAlreadySent = errors.New("gosocket: ack already sent")
	// ErrNilAckCallback is returned by EmitWithAck when cb is nil.
	ErrNilAckCallback = errors.New("gosocket: nil ack callback")
)

// ConnectError lets connect middleware control the CONNECT_ERROR a Socket.IO
// client receives. Any other error is reported as a generic "rejected" so internal
// error text never leaks to clients.
type ConnectError struct {
	// Message is shown to the client as err.message.
	Message string
	// Data is optional JSON-serializable detail, exposed as err.data.
	Data interface{}
}

func (e *ConnectError) Error() string { return e.Message }

// Namespace is a Socket.IO namespace: its own handlers, middleware and lifecycle
// callbacks on a shared connection. The default namespace "/" shares the Server's
// own On/Use/OnConnect/OnDisconnect, so existing handlers serve Socket.IO clients
// unchanged and legacy and Socket.IO clients can share rooms.
type Namespace struct {
	s    *Server
	name string
	def  bool

	mu           sync.RWMutex
	handlers     map[string]EventHandler
	middlewares  []Middleware
	onConnect    []func(*Context)
	onDisconnect []func(*Context)
	hasRecover   bool
}

// Of returns the namespace called name, creating it on first use. A missing
// leading "/" is added. It panics on an invalid name (empty, over 128 bytes, or
// containing ',' or NUL), like http.Handle does for a bad pattern: it is a
// programmer error at setup time.
func (s *Server) Of(name string) *Namespace {
	if name == "" || name == "/" {
		return s.defaultNS
	}
	if !strings.HasPrefix(name, "/") {
		name = "/" + name
	}
	if len(name) > maxNamespaceLen || strings.ContainsAny(name, ",\x00") {
		panic("gosocket: invalid namespace " + name)
	}
	s.nsMu.Lock()
	defer s.nsMu.Unlock()
	if ns, ok := s.namespaces[name]; ok {
		return ns
	}
	ns := &Namespace{s: s, name: name, handlers: make(map[string]EventHandler)}
	s.namespaces[name] = ns
	return ns
}

func (s *Server) lookupNamespace(name string) *Namespace {
	if name == "" || name == "/" {
		return s.defaultNS
	}
	s.nsMu.RLock()
	defer s.nsMu.RUnlock()
	return s.namespaces[name]
}

// Name returns the namespace path, "/" for the default one.
func (n *Namespace) Name() string { return n.name }

// On registers a handler for a named event on this namespace.
func (n *Namespace) On(event string, h EventHandler) {
	if n.def {
		n.s.On(event, h)
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[event] = h
}

// Use appends middleware for this namespace. Server-wide Use middleware always
// runs first, so a global auth check cannot be bypassed by connecting to another namespace.
func (n *Namespace) Use(mw Middleware) {
	if n.def {
		n.s.Use(mw)
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if isRecoverMiddleware(mw) {
		n.hasRecover = true
	}
	n.middlewares = append(n.middlewares, mw)
}

// OnConnect registers a callback run after a socket joins this namespace. Connect
// middleware (and auth via ctx.Auth) has already passed.
func (n *Namespace) OnConnect(fn func(*Context)) {
	if n.def {
		n.s.OnConnect(fn)
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.onConnect = append(n.onConnect, fn)
}

// OnDisconnect registers a callback run when a socket leaves this namespace.
func (n *Namespace) OnDisconnect(fn func(*Context)) {
	if n.def {
		n.s.OnDisconnect(fn)
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.onDisconnect = append(n.onDisconnect, fn)
}

func (n *Namespace) handler(event string) EventHandler {
	if n.def {
		n.s.mu.RLock()
		defer n.s.mu.RUnlock()
		return n.s.handlers[event]
	}
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.handlers[event]
}

func (n *Namespace) recovers() bool {
	if n.s.hasRecover {
		return true
	}
	if n.def {
		return false
	}
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.hasRecover
}

// chain returns server-wide middleware followed by this namespace's own.
func (n *Namespace) chain() []Middleware {
	n.s.mu.RLock()
	out := append([]Middleware(nil), n.s.middlewares...)
	n.s.mu.RUnlock()
	if n.def {
		return out
	}
	n.mu.RLock()
	defer n.mu.RUnlock()
	return append(out, n.middlewares...)
}

func (n *Namespace) connectCallbacks() []func(*Context) {
	if n.def {
		n.s.mu.RLock()
		defer n.s.mu.RUnlock()
		return slices.Clone(n.s.onConnect)
	}
	n.mu.RLock()
	defer n.mu.RUnlock()
	return slices.Clone(n.onConnect)
}

func (n *Namespace) disconnectCallbacks() []func(*Context) {
	if n.def {
		n.s.mu.RLock()
		defer n.s.mu.RUnlock()
		return slices.Clone(n.s.onDisconnect)
	}
	n.mu.RLock()
	defer n.mu.RUnlock()
	return slices.Clone(n.onDisconnect)
}

// Hub room keys. User rooms of the default namespace use their plain name so
// legacy and Socket.IO clients share them; everything else is NUL-prefixed, which
// keeps it out of room listings and unreachable from legacy JoinRoom.
func (n *Namespace) roomKey(room string) string {
	if n.def {
		return room
	}
	return internal.HiddenRoomPrefix + n.name + internal.HiddenRoomPrefix + room
}

func (n *Namespace) memberKey() string {
	return internal.HiddenRoomPrefix + "m" + internal.HiddenRoomPrefix + n.name
}

func sidKey(sid string) string {
	return internal.HiddenRoomPrefix + "s" + internal.HiddenRoomPrefix + sid
}

func validRoom(room string) bool {
	return room != "" && !strings.HasPrefix(room, internal.HiddenRoomPrefix)
}

// RoomSize returns the live members of a room in this namespace.
func (n *Namespace) RoomSize(room string) int {
	if !validRoom(room) {
		return 0
	}
	return n.s.hub.RoomSize(n.roomKey(room))
}
