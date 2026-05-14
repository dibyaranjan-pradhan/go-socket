// Package internal implements the connection hub for gosocket. It must not be imported outside the gosocket module tree.
package internal

import (
	"context"
	"sync"
	"time"
)

// PrintfLogger is an optional sink for hub warnings (same shape as gosocket.Logger).
type PrintfLogger interface {
	Printf(format string, v ...interface{})
}

// Hub tracks clients and room membership and delivers outbound frames without holding locks while writing to client buffers.
type Hub struct {
	mu sync.RWMutex

	clients map[*Client]struct{}
	rooms   map[string]map[*Client]struct{}
	// roomCreated records when each room first received a member.
	roomCreated map[string]time.Time

	register   chan *Client
	unregister chan *Client
	join       chan roomJoin
	leave      chan roomLeave
	emit       chan EmitJob

	closed chan struct{}
	log    PrintfLogger
}

type roomJoin struct {
	c    *Client
	room string
}

type roomLeave struct {
	c    *Client
	room string
}

// EmitJob describes a fan-out delivery request processed by the hub.
type EmitJob struct {
	// Room selects members when Only is nil.
	Room WireRoom
	// Event is the logical event name (for logging when a client send queue is full).
	Event WireEvent
	// Exclude removes a client from a room broadcast (typically the sender).
	Exclude *Client
	// Only targets a single client when non-nil (room is ignored).
	Only *Client
	// AllClients delivers Data to every registered client (Room is ignored).
	AllClients bool
	// Data is the fully serialized wire frame.
	Data []byte
}

// NewHub constructs a hub with configurable emit buffer size.
// Call Run in a dedicated goroutine. log may be nil.
func NewHub(log PrintfLogger, emitBufferSize int) *Hub {
	if emitBufferSize <= 0 {
		emitBufferSize = 256
	}
	return &Hub{
		clients:     make(map[*Client]struct{}),
		rooms:       make(map[string]map[*Client]struct{}),
		roomCreated: make(map[string]time.Time),
		register:    make(chan *Client),
		unregister:  make(chan *Client),
		join:        make(chan roomJoin),
		leave:       make(chan roomLeave),
		emit:        make(chan EmitJob, emitBufferSize),
		closed:      make(chan struct{}),
		log:         log,
	}
}

// Run processes hub operations until Shutdown completes draining.
func (h *Hub) Run(ctx context.Context) {
	defer close(h.closed)

	for {
		select {
		case <-ctx.Done():
			h.drainAndCloseAll()
			return
		case c := <-h.register:
			h.registerClient(c)
		case c := <-h.unregister:
			h.removeClientMaps(c)
		case j := <-h.join:
			h.joinRoom(j)
		case j := <-h.leave:
			h.leaveRoom(j)
		case e := <-h.emit:
			h.emitToTargets(e)
		}
	}
}

func (h *Hub) registerClient(c *Client) {
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()
}

func (h *Hub) joinRoom(j roomJoin) {
	h.mu.Lock()
	defer h.mu.Unlock()
	m := h.rooms[j.room]
	if m == nil {
		m = make(map[*Client]struct{})
		h.rooms[j.room] = m
		h.roomCreated[j.room] = time.Now()
	}
	m[j.c] = struct{}{}
}

func (h *Hub) leaveRoom(j roomLeave) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if m, ok := h.rooms[j.room]; ok {
		delete(m, j.c)
		if len(m) == 0 {
			delete(h.rooms, j.room)
			delete(h.roomCreated, j.room)
		}
	}
}

func (h *Hub) emitToTargets(e EmitJob) {
	targets := h.collectTargets(e)
	for _, c := range targets {
		if c.TrySend(e.Data) {
			continue
		}
		h.logEmitQueueFull(e, c)
		go c.Shutdown()
	}
}

func (h *Hub) logEmitQueueFull(e EmitJob, c *Client) {
	if h.log == nil {
		return
	}
	h.log.Printf("go-socket: [WARN] emit queue full: clientID=%s roomID=%s event=%s", c.ID(), e.Room.OrDash(), e.Event.OrDash())
}

func (h *Hub) collectTargets(e EmitJob) []*Client {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if e.AllClients {
		out := make([]*Client, 0, len(h.clients))
		for c := range h.clients {
			if e.Exclude != nil && c == e.Exclude {
				continue
			}
			out = append(out, c)
		}
		return out
	}

	if e.Only != nil {
		if _, ok := h.clients[e.Only]; ok {
			return []*Client{e.Only}
		}
		return nil
	}

	m := h.rooms[string(e.Room)]
	if len(m) == 0 {
		return nil
	}
	out := make([]*Client, 0, len(m))
	for c := range m {
		if e.Exclude != nil && c == e.Exclude {
			continue
		}
		out = append(out, c)
	}
	return out
}

func (h *Hub) removeClientMaps(c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	delete(h.clients, c)
	for room, m := range h.rooms {
		delete(m, c)
		if len(m) == 0 {
			delete(h.rooms, room)
			delete(h.roomCreated, room)
		}
	}
}

func (h *Hub) drainAndCloseAll() {
	h.mu.Lock()
	clients := make([]*Client, 0, len(h.clients))
	for c := range h.clients {
		clients = append(clients, c)
	}
	h.clients = make(map[*Client]struct{})
	h.rooms = make(map[string]map[*Client]struct{})
	h.roomCreated = make(map[string]time.Time)
	h.mu.Unlock()

	for _, c := range clients {
		c.Shutdown()
	}
}

// Register schedules adding a client. Safe from any goroutine.
func (h *Hub) Register(c *Client) {
	select {
	case <-h.closed:
		return
	case h.register <- c:
	}
}

// Unregister schedules removing a client.
func (h *Hub) Unregister(c *Client) {
	select {
	case <-h.closed:
		return
	case h.unregister <- c:
	}
}

// Join schedules room membership.
func (h *Hub) Join(c *Client, room string) {
	if room == "" {
		return
	}
	select {
	case <-h.closed:
		return
	case h.join <- roomJoin{c: c, room: room}:
	}
}

// Leave schedules leaving a room.
func (h *Hub) Leave(c *Client, room string) {
	if room == "" {
		return
	}
	select {
	case <-h.closed:
		return
	case h.leave <- roomLeave{c: c, room: room}:
	}
}

// Emit schedules delivery to targets derived under lock; sends are non-blocking per client.
func (h *Hub) Emit(job EmitJob) {
	select {
	case <-h.closed:
		return
	case h.emit <- job:
		return
	default:
		h.logHubEmitSaturated(job)
	}
}

func (h *Hub) logHubEmitSaturated(job EmitJob) {
	if h.log == nil {
		return
	}
	h.log.Printf("go-socket: [WARN] hub emit queue saturated: roomID=%s event=%s", job.Room.OrDash(), job.Event.OrDash())
}

// Wait waits until Run has exited after context cancel.
func (h *Hub) Wait() {
	<-h.closed
}

// RoomSize returns the number of clients in a room. Returns 0 if room does not exist.
func (h *Hub) RoomSize(room string) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.rooms[room])
}

// RoomClients returns a snapshot of clients in a room. Returns nil if room does not exist.
func (h *Hub) RoomClients(room string) []*Client {
	h.mu.RLock()
	defer h.mu.RUnlock()
	m := h.rooms[room]
	if len(m) == 0 {
		return nil
	}
	out := make([]*Client, 0, len(m))
	for c := range m {
		out = append(out, c)
	}
	return out
}

// AllRooms returns a list of all active room IDs.
func (h *Hub) AllRooms() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	rooms := make([]string, 0, len(h.rooms))
	for room := range h.rooms {
		rooms = append(rooms, room)
	}
	return rooms
}

// ClientRoomCount returns how many rooms the client is currently joined to.
func (h *Hub) ClientRoomCount(c *Client) int {
	if c == nil {
		return 0
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	n := 0
	for _, m := range h.rooms {
		if _, ok := m[c]; ok {
			n++
		}
	}
	return n
}

// RoomCreatedAt returns the time the room was first created, if known.
func (h *Hub) RoomCreatedAt(room string) (time.Time, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	t, ok := h.roomCreated[room]
	return t, ok
}
