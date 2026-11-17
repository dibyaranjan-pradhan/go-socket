// Copyright (c) 2026 Dibyaranjan Pradhan. All rights reserved.

package gosocket_test

import (
	"net/http"

	gosocket "github.com/dibyaranjan-pradhan/go-socket"
)

// A single server serves legacy JSON clients and official socket.io-client
// peers; handlers, middleware and rooms are shared.
func ExampleServer_SocketIOHandler() {
	srv := gosocket.New(gosocket.Config{})

	srv.On("chat", func(ctx *gosocket.Context) {
		var msg string
		_ = ctx.Bind(&msg)
		_ = ctx.To("lobby").Emit("chat", msg) // everyone in lobby except the sender
		_ = ctx.Ack("ok")                     // no-op error for legacy clients
	})

	admin := srv.Of("/admin")
	admin.Use(gosocket.MiddlewareFunc(func(ctx *gosocket.Context) error {
		if ctx.Event() != gosocket.EventConnect {
			return nil
		}
		var auth struct{ Token string }
		if err := ctx.BindAuth(&auth); err != nil || auth.Token == "" {
			return &gosocket.ConnectError{Message: "forbidden"}
		}
		return nil
	}))

	mux := http.NewServeMux()
	mux.Handle("/socket.io/", srv.SocketIOHandler())
	mux.Handle("/ws", srv.Handler())
}
