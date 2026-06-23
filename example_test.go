package gosocket_test

import (
	"fmt"
	"net/http"

	gosocket "github.com/dibyaranjan-pradhan/go-socket"
)

// ExampleServer_EngineIOHandler shows mounting Engine.IO polling next to WebSocket.
// Clients that speak Engine.IO v4 use GET/POST on this path; WebSocket clients keep using Handler().
func ExampleServer_EngineIOHandler() {
	s := gosocket.New(gosocket.Config{})

	mux := http.NewServeMux()
	mux.Handle("/ws", s.Handler())
	mux.Handle("/engine.io/", s.EngineIOHandler())

	fmt.Println("mounted /ws and /engine.io/")
	// Output: mounted /ws and /engine.io/
}
