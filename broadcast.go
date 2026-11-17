// Copyright (c) 2026 Dibyaranjan Pradhan. All rights reserved.

package gosocket

import (
	"encoding/json"

	"github.com/dibyaranjan-pradhan/go-socket/internal"
)

// Broadcast is an immutable, composable selector of recipients within one
// namespace, mirroring Socket.IO's chainable server API:
//
//	srv.Of("/chat").To("room1", "room2").Except("room3").Emit("msg", data)
//
// Every chain method returns a new value, so a shared base can be reused safely
// from many goroutines.
type Broadcast struct {
	ns     *Namespace
	rooms  []string
	except []string
	self   *internal.Client
}

// To selects the union of the given rooms. With no To/In, a Broadcast targets the
// whole namespace.
func (n *Namespace) To(rooms ...string) *Broadcast { return (&Broadcast{ns: n}).To(rooms...) }

// In is an alias of To, as in Socket.IO.
func (n *Namespace) In(rooms ...string) *Broadcast { return n.To(rooms...) }

// Except excludes members of the given rooms or the sockets with the given ids.
func (n *Namespace) Except(names ...string) *Broadcast {
	return (&Broadcast{ns: n}).Except(names...)
}

// Broadcast selects every socket in the namespace.
func (n *Namespace) Broadcast() *Broadcast { return &Broadcast{ns: n} }

// Emit sends an event to every socket in the namespace.
func (n *Namespace) Emit(event string, data interface{}) error {
	return n.Broadcast().Emit(event, data)
}

// To adds rooms to the selection.
func (b *Broadcast) To(rooms ...string) *Broadcast {
	nb := *b
	nb.rooms = appendValid(b.rooms, rooms)
	return &nb
}

// In is an alias of To.
func (b *Broadcast) In(rooms ...string) *Broadcast { return b.To(rooms...) }

// Except adds exclusions. Each name is matched as a room and as a socket id.
func (b *Broadcast) Except(names ...string) *Broadcast {
	nb := *b
	nb.except = appendValid(b.except, names)
	return &nb
}

func appendValid(base, add []string) []string {
	out := append([]string(nil), base...)
	for _, v := range add {
		if validRoom(v) {
			out = append(out, v)
		}
	}
	return out
}

// Emit delivers the event to the selected sockets. Delivery is asynchronous through
// the hub; the error reports only encoding failures.
func (b *Broadcast) Emit(event string, data interface{}) error {
	return b.EmitWith(event, data, EmitOptions{})
}

// EmitWith is Emit with per-call backpressure and idempotency options.
func (b *Broadcast) EmitWith(event string, data interface{}, opts EmitOptions) error {
	n := b.ns
	s := n.s
	job := internal.EmitJob{
		Event:   internal.WireEvent(event),
		Exclude: b.self,
		Policy:  s.resolvePolicy(opts.Policy),
	}

	// Callers name rooms plainly, but hub keys are namespaced, so "room1" in
	// "/chat" can never collide with "room1" in "/admin".
	if len(b.rooms) == 0 {
		job.Rooms = []string{n.memberKey()}
		job.IncludeLegacy = n.def
	} else {
		job.Rooms = make([]string, len(b.rooms))
		for i, r := range b.rooms {
			job.Rooms[i] = n.roomKey(r)
		}
	}
	for _, name := range b.except {
		job.ExceptRooms = append(job.ExceptRooms, n.roomKey(name), sidKey(name))
	}

	// Legacy clients only exist in the default namespace.
	if n.def {
		legacy, err := json.Marshal(ServerWireMessage{Event: event, Payload: data, IdempotencyKey: opts.IdempotencyKey})
		if err != nil {
			return err
		}
		job.Data = legacy
	}
	if s.sioMounted.Load() {
		sio, err := encodeSIOEvent(n.name, nil, event, data)
		if err != nil {
			return err
		}
		job.SIOData = sio
	}
	if job.Data == nil && job.SIOData == nil {
		return nil
	}
	s.hub.Emit(job)
	return nil
}
