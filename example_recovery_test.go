// Copyright (c) 2026 Dibyaranjan Pradhan. All rights reserved.

package gosocket_test

import (
	"net/http"
	"time"

	gosocket "github.com/dibyaranjan-pradhan/go-socket"
)

// Clients reconnect with ?gs_session=<id>&gs_seq=<last seq seen> and receive the
// events they missed; see docs/RECOVERY.md for the client algorithm.
func ExampleSessionRecoveryConfig() {
	s := gosocket.New(gosocket.Config{
		SessionRecovery: gosocket.SessionRecoveryConfig{
			Enabled:    true,
			SessionTTL: 2 * time.Minute,
		},
	})
	s.OnConnect(func(ctx *gosocket.Context) {
		ctx.JoinRoom("lobby")
		if ctx.Resumed() {
			return
		}
		_ = ctx.Emit("welcome", nil)
	})
	s.On("cursor", func(ctx *gosocket.Context) {
		// Stale cursor positions are worthless after a reconnect: never buffer them.
		_ = ctx.BroadcastToRoomWith("lobby", "cursor", nil, gosocket.EmitOptions{Policy: gosocket.BackpressureVolatile})
	})
	http.Handle("/ws", s.Handler())
}
