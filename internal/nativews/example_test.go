// Copyright (c) 2026 Dibyaranjan Pradhan. All rights reserved.

package nativews_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	"github.com/dibyaranjan-pradhan/go-socket/internal/nativews"
)

// A server upgrades with Upgrader and a client connects with Dial. Closing with a
// status is answered by the peer, so both sides see the same code.
func ExampleUpgrader() {
	up := nativews.Upgrader{ReadBufferSize: 4096}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(w, r)
		if err != nil {
			return
		}
		defer func() { _ = c.Close() }()
		for {
			op, msg, err := c.ReadMessage()
			if err != nil {
				return
			}
			_ = c.WriteMessage(op, append([]byte("echo: "), msg...))
		}
	}))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, _, err := nativews.Dial(ctx, "ws"+strings.TrimPrefix(ts.URL, "http"), nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = c.Close() }()

	_ = c.WriteMessage(nativews.TextMessage, []byte("hello"))
	_, msg, _ := c.ReadMessage()
	fmt.Println(string(msg))

	_ = c.WriteControl(nativews.CloseMessage, nativews.FormatCloseMessage(nativews.CloseNormalClosure, "done"))
	_, _, err = c.ReadMessage()
	var ce *nativews.CloseError
	fmt.Println(errors.As(err, &ce), ce.Code)
	// Output:
	// echo: hello
	// true 1000
}
