package gosocket

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/gorilla/websocket"

	"github.com/dibyaranjan-pradhan/go-socket/internal"
)

// Server wires HTTP upgrades to hub-managed clients and event handlers.
type Server struct {
	cfg Config

	hub       *internal.Hub
	hubCtx    context.Context
	hubCancel context.CancelFunc

	mu           sync.RWMutex
	handlers     map[string]EventHandler
	middlewares  []Middleware
	onConnect    []func(*Context)
	onDisconnect []func(*Context)
	hasRecover   bool
	closed       atomic.Bool
	conns        atomic.Int64
	upgrader     websocket.Upgrader
	diag         Logger
}

// New constructs a Server with defaults applied from cfg.
func New(cfg Config) *Server {
	cfg = cfg.normalized()
	diag := resolveLogger(cfg.Logger)
	hub := internal.NewHub(diag)
	hCtx, cancel := context.WithCancel(context.Background())
	s := &Server{
		cfg:       cfg,
		hub:       hub,
		hubCtx:    hCtx,
		hubCancel: cancel,
		handlers:  make(map[string]EventHandler),
		diag:      diag,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  cfg.ReadBufferSize,
			WriteBufferSize: cfg.WriteBufferSize,
			CheckOrigin:     cfg.CheckOrigin,
		},
	}
	if s.upgrader.CheckOrigin == nil {
		s.upgrader.CheckOrigin = func(r *http.Request) bool { return true }
	}
	go hub.Run(hCtx)
	return s
}

// LoggerMiddleware logs connect, disconnect, and inbound events using the server-configured Logger.
func (s *Server) LoggerMiddleware() Middleware {
	return MiddlewareFunc(func(ctx *Context) error {
		switch ctx.event {
		case EventConnect:
			s.diag.Printf("gosocket: connect client=%s remote=%s", ctx.ClientID(), ctx.Request().RemoteAddr)
		case EventDisconnect:
			s.diag.Printf("gosocket: disconnect client=%s remote=%s", ctx.ClientID(), ctx.Request().RemoteAddr)
		default:
			if ctx.event != "" {
				s.diag.Printf("gosocket: event=%s client=%s room=%s", ctx.event, ctx.ClientID(), ctx.room)
			}
		}
		return nil
	})
}

// Use appends middleware executed before each synthetic connect pass and each inbound event.
func (s *Server) Use(mw Middleware) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if isRecoverMiddleware(mw) {
		s.hasRecover = true
	}
	s.middlewares = append(s.middlewares, mw)
}

// On registers a handler for a named client event.
func (s *Server) On(event string, h EventHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[event] = h
}

// OnConnect registers callbacks invoked after a connection passes middleware and before reads begin.
func (s *Server) OnConnect(fn func(*Context)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onConnect = append(s.onConnect, fn)
}

// OnDisconnect registers callbacks invoked after a client fully tears down.
func (s *Server) OnDisconnect(fn func(*Context)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onDisconnect = append(s.onDisconnect, fn)
}

// Handler returns an http.Handler that upgrades connections to WebSocket.
func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(s.serveWS)
}

// RoomSize returns the number of active connections in a room.
// Returns 0 if the room does not exist or is empty.
func (s *Server) RoomSize(roomID string) int {
	return s.hub.RoomSize(roomID)
}

// RoomIdentities returns identity values for all clients in a room.
// Identity is determined by Config.IdentityKey — the metadata key set via ctx.Set() in OnConnect.
// If IdentityKey is empty, returns clientIDs instead.
// Clients that have not set the identity key are excluded from the result.
func (s *Server) RoomIdentities(roomID string) []string {
	clients := s.hub.RoomClients(roomID)
	if len(clients) == 0 {
		return nil
	}
	out := make([]string, 0, len(clients))
	for _, c := range clients {
		if s.cfg.IdentityKey == "" {
			out = append(out, c.ID())
			continue
		}
		val, ok := c.MetaGet(s.cfg.IdentityKey)
		if !ok {
			continue
		}
		if id, ok := val.(string); ok && id != "" {
			out = append(out, id)
		}
	}
	return out
}

// Shutdown stops accepting new clients and waits for the hub to drain existing connections.
func (s *Server) Shutdown(ctx context.Context) error {
	s.closed.Store(true)
	s.hubCancel()
	done := make(chan struct{})
	go func() {
		s.hub.Wait()
		close(done)
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

func (s *Server) serveWS(w http.ResponseWriter, r *http.Request) {
	if s.closed.Load() {
		http.Error(w, ErrServerClosed.Error(), http.StatusServiceUnavailable)
		return
	}
	if int(s.conns.Load()) >= s.cfg.MaxConnections {
		http.Error(w, ErrConnectionLimit.Error(), http.StatusServiceUnavailable)
		return
	}

	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	s.conns.Add(1)
	id := randomClientID()
	var ic *internal.Client
	opt := internal.ClientOptions{
		ID:             id,
		MaxMessageSize: s.cfg.MaxMessageSize,
		WriteWait:      s.cfg.WriteWait,
		PingPeriod:     s.cfg.PingInterval,
		PongWait:       s.cfg.PongWait,
		SendBuffer:     s.cfg.WriteBufferSize,
	}
	opt.OnMessage = func(data []byte) {
		s.dispatch(ic, r, data)
	}
	opt.OnDone = func() {
		s.conns.Add(-1)
		s.fireDisconnect(ic, r)
	}
	ic = internal.NewClient(s.hub, conn, opt)

	s.hub.Register(ic)

	connectCtx := s.newContext(ic, r, EventConnect, nil, "", "")
	if err := s.withRecover(func() error { return s.runMiddlewares(connectCtx) }); err != nil {
		s.conns.Add(-1)
		s.hub.Unregister(ic)
		ic.Shutdown()
		return
	}

	go ic.WritePump()
	s.mu.RLock()
	cbs := make([]func(*Context), len(s.onConnect))
	copy(cbs, s.onConnect)
	s.mu.RUnlock()
	for _, fn := range cbs {
		if fn != nil {
			fn(connectCtx)
		}
	}
	go ic.ReadPump()
}

func (s *Server) fireDisconnect(ic *internal.Client, r *http.Request) {
	ctx := s.newContext(ic, r, EventDisconnect, nil, "", "")
	s.mu.RLock()
	mws := make([]Middleware, len(s.middlewares))
	copy(mws, s.middlewares)
	disc := make([]func(*Context), len(s.onDisconnect))
	copy(disc, s.onDisconnect)
	s.mu.RUnlock()

	for _, mw := range mws {
		if isRecoverMiddleware(mw) {
			continue
		}
		func() {
			defer func() {
				if rec := recover(); rec != nil {
					s.diag.Printf("gosocket: disconnect middleware panic: %v", rec)
				}
			}()
			_ = mw.RunMiddleware(ctx)
		}()
	}
	for _, fn := range disc {
		if fn == nil {
			continue
		}
		func() {
			defer func() {
				if rec := recover(); rec != nil {
					s.diag.Printf("gosocket: disconnect handler panic: %v", rec)
				}
			}()
			fn(ctx)
		}()
	}
}

func (s *Server) dispatch(ic *internal.Client, r *http.Request, raw []byte) {
	var msg ClientWireMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		return
	}
	ctx := s.newContext(ic, r, msg.Event, msg.Payload, msg.Room, msg.ID)

	exec := func() {
		if err := s.runMiddlewares(ctx); err != nil {
			return
		}
		if msg.Event == "" {
			return
		}
		s.mu.RLock()
		h := s.handlers[msg.Event]
		s.mu.RUnlock()
		if h != nil {
			h(ctx)
		}
	}

	if s.hasRecover {
		defer func() {
			if rec := recover(); rec != nil {
				s.diag.Printf("gosocket: panic in event %q: %v", msg.Event, rec)
				ctx.Disconnect()
			}
		}()
	}
	exec()
}

func (s *Server) runMiddlewares(ctx *Context) error {
	s.mu.RLock()
	mws := append([]Middleware(nil), s.middlewares...)
	s.mu.RUnlock()
	for _, mw := range mws {
		if isRecoverMiddleware(mw) {
			continue
		}
		if err := mw.RunMiddleware(ctx); err != nil {
			return err
		}
	}
	return nil
}

// withRecover runs fn with a deferred recover when Use(Recover()) was registered.
// Connect middleware runs before *Client and *http.Request are wired into every path; only fn needs to run here.
func (s *Server) withRecover(fn func() error) (err error) {
	if !s.hasRecover {
		return fn()
	}
	defer func() {
		if rec := recover(); rec != nil {
			s.diag.Printf("gosocket: panic during connect: %v", rec)
			err = ErrRejected
		}
	}()
	return fn()
}

func (s *Server) emitOne(ic *internal.Client, event string, data interface{}, ack string) error {
	if ic == nil {
		return nil
	}
	msg := ServerWireMessage{Event: event, Payload: data, AckID: ack}
	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	if !ic.TrySend(b) {
		s.diag.Printf("gosocket: [WARN] emit queue full: clientID=%s roomID=- event=%s", ic.ID(), event)
		go ic.Shutdown()
	}
	return nil
}

func (s *Server) emitRoom(room, event string, data interface{}, ack string, exclude *internal.Client) error {
	if room == "" {
		return nil
	}
	msg := ServerWireMessage{Event: event, Payload: data, AckID: ack}
	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	s.hub.Emit(internal.EmitJob{
		Room:    internal.WireRoom(room),
		Event:   internal.WireEvent(event),
		Exclude: exclude,
		Data:    b,
	})
	return nil
}

func randomClientID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "client-unknown"
	}
	return hex.EncodeToString(b[:])
}
