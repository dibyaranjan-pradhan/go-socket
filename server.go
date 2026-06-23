package gosocket

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

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
	// Tracking
	connStats            sync.Map // clientID -> *ConnectionStats (last snapshot from GetConnectionStats)
	clientsByID          sync.Map // clientID -> *internal.Client
	totalMessages        atomic.Int64
	totalConnectionsEver atomic.Int64
	serverStart          time.Time
	onHeartbeatTimeout   []func(*Context)
	upgrader             *internal.WSUpgrader
	pollHandler          *internal.PollHandler
	diag                 Logger
}

// New constructs a Server with defaults applied from cfg.
func New(cfg Config) *Server {
	cfg = cfg.normalized()
	diag := resolveLogger(cfg.Logger)
	hub := internal.NewHub(diag, cfg.HubEmitBufferSize)
	hCtx, cancel := context.WithCancel(context.Background())
	s := &Server{
		cfg:         cfg,
		hub:         hub,
		hubCtx:      hCtx,
		hubCancel:   cancel,
		handlers:    make(map[string]EventHandler),
		diag:        diag,
		serverStart: time.Now(),
		upgrader:    internal.NewWSUpgrader(cfg.ReadBufferSize, cfg.WriteBufferSize, cfg.CheckOrigin),
	}
	s.pollHandler = internal.NewPollHandler(
		cfg.PingInterval,
		cfg.PongWait,
		cfg.MaxMessageSize,
		s.attachPollTransport,
		func() bool { return s.closed.Load() },
	)
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

// EngineIOHandler returns an http.Handler for Engine.IO v4 HTTP long-polling.
// Mount it on a path such as /engine.io/ alongside Handler(); existing
// WebSocket clients are not affected.
func (s *Server) EngineIOHandler() http.Handler {
	return s.pollHandler
}

// GetConnectionStats returns diagnostic metrics for a specific connection.
// Returns nil if the connection does not exist.
func (s *Server) GetConnectionStats(clientID string) *ConnectionStats {
	v, ok := s.clientsByID.Load(clientID)
	if !ok {
		return nil
	}
	ic := v.(*internal.Client)
	st := s.buildConnectionStats(ic)
	s.connStats.Store(clientID, st)
	return st
}

// GetServerStats returns aggregate metrics for the server.
func (s *Server) GetServerStats() *ServerStats {
	return &ServerStats{
		ActiveConnections:      s.conns.Load(),
		TotalConnectionsEver:   s.totalConnectionsEver.Load(),
		TotalMessagesProcessed: s.totalMessages.Load(),
		UptimeSince:            s.serverStart,
	}
}

// GetRoomStats returns metrics for a specific room.
// Returns nil if the room does not exist.
func (s *Server) GetRoomStats(roomID string) *RoomStats {
	if roomID == "" {
		return nil
	}
	clients := s.hub.RoomClients(roomID)
	if len(clients) == 0 {
		return nil
	}
	members := make([]string, len(clients))
	for i, c := range clients {
		members[i] = c.ID()
	}
	created := time.Now()
	if t, ok := s.hub.RoomCreatedAt(roomID); ok {
		created = t
	}
	return &RoomStats{
		RoomID:      roomID,
		MemberCount: len(clients),
		CreatedAt:   created,
		Members:     members,
	}
}

// ListAllRooms returns a snapshot of all active rooms with member counts.
func (s *Server) ListAllRooms() []*RoomStats {
	rooms := s.hub.AllRooms()
	stats := make([]*RoomStats, 0, len(rooms))
	for _, roomID := range rooms {
		if rs := s.GetRoomStats(roomID); rs != nil {
			stats = append(stats, rs)
		}
	}
	return stats
}

func (s *Server) buildConnectionStats(ic *internal.Client) *ConnectionStats {
	st := ic.GetStats()
	return &ConnectionStats{
		ClientID:          ic.ID(),
		BytesSent:         st.BytesSent,
		BytesReceived:     st.BytesReceived,
		MessagesSent:      st.MessagesSent,
		MessagesReceived:  st.MessagesReceived,
		BufferUtilization: st.BufferUtilization,
		ConnectedAt:       st.ConnectedAt,
		LastActivity:      st.LastActivity,
		RoomCount:         s.hub.ClientRoomCount(ic),
	}
}

// OnHeartbeatTimeout registers callbacks invoked when a client misses a heartbeat.
// This typically means the connection is zombie (network dead but not yet detected).
func (s *Server) OnHeartbeatTimeout(fn func(*Context)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onHeartbeatTimeout = append(s.onHeartbeatTimeout, fn)
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

// Shutdown gracefully closes the server and all connections.
// It stops accepting new connections and waits up to ctx timeout for the hub to finish draining.
// After timeout, the context error is returned even though cleanup may still be in progress.
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

// ShutdownWithMessage sends a notification to all clients before closing.
func (s *Server) ShutdownWithMessage(ctx context.Context, eventName string, payload interface{}) error {
	s.closed.Store(true)

	msg := ServerWireMessage{Event: eventName, Payload: payload}
	b, err := json.Marshal(msg)
	if err != nil {
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
			return err
		}
	}
	s.hub.Emit(internal.EmitJob{
		AllClients: true,
		Event:      internal.WireEvent(eventName),
		Data:       b,
	})

	time.Sleep(500 * time.Millisecond)

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

	transport, err := s.upgrader.Upgrade(w, r)
	if err != nil {
		return
	}
	s.serveTransport(transport, r)
}

func (s *Server) attachPollTransport(transport internal.Transport, r *http.Request) {
	if s.closed.Load() {
		_ = transport.Close()
		return
	}
	if int(s.conns.Load()) >= s.cfg.MaxConnections {
		_ = transport.Close()
		return
	}
	s.serveTransport(transport, r)
}

func (s *Server) serveTransport(transport internal.Transport, r *http.Request) {
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
		s.clientsByID.Delete(ic.ID())
		s.connStats.Delete(ic.ID())
		s.fireDisconnect(ic, r)
	}
	opt.OnHeartbeatTimeout = func() {
		s.fireHeartbeatTimeout(ic, r)
	}
	ic = internal.NewClient(s.hub, transport, opt)

	s.hub.Register(ic)

	connectCtx := s.newContext(ic, r, EventConnect, nil, "", "")
	if err := s.withRecover(func() error { return s.runMiddlewares(connectCtx) }); err != nil {
		s.conns.Add(-1)
		s.hub.Unregister(ic)
		ic.Shutdown()
		return
	}

	s.clientsByID.Store(id, ic)
	s.totalConnectionsEver.Add(1)

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

func (s *Server) fireHeartbeatTimeout(ic *internal.Client, r *http.Request) {
	ctx := s.newContext(ic, r, "", nil, "", "")
	s.mu.RLock()
	cbs := make([]func(*Context), len(s.onHeartbeatTimeout))
	copy(cbs, s.onHeartbeatTimeout)
	s.mu.RUnlock()
	for _, fn := range cbs {
		if fn == nil {
			continue
		}
		func() {
			defer func() {
				if rec := recover(); rec != nil {
					s.diag.Printf("gosocket: heartbeat timeout handler panic: %v", rec)
				}
			}()
			fn(ctx)
		}()
	}
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
	s.totalMessages.Add(1)
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
