// Command echo runs a minimal RFC 6455 echo server for Autobahn Testsuite runs.
// It uses the in-tree codec directly so fuzz results reflect native WebSocket code.
package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/dibyaranjan-pradhan/go-socket/internal/nativews"
)

func main() {
	addr := flag.String("addr", ":9001", "listen address")
	flag.Parse()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		c, err := nativews.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = c.Close() }()
		for {
			op, msg, err := c.ReadMessage()
			if err != nil {
				return
			}
			if err := c.WriteMessage(op, msg); err != nil {
				return
			}
		}
	})

	log.Printf("autobahn echo listening on %s", *addr)
	log.Fatal(http.ListenAndServe(*addr, nil))
}
