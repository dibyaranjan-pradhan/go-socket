package gosocket

import (
	"errors"
	"time"

	"github.com/dibyaranjan-pradhan/go-socket/internal"
)

// Recover enables panic recovery for connect and event handling; panicking clients are disconnected.
// Register with Use(Recover()) once.
func Recover() Middleware {
	return recoverMiddleware{}
}

// RateLimiter enforces a minimum interval between handled messages per client. Violations reject the event with ErrRejected.
func RateLimiter(interval time.Duration) Middleware {
	return MiddlewareFunc(func(ctx *Context) error {
		if ctx.client == nil || interval <= 0 {
			return nil
		}
		if ctx.Event() == EventConnect || ctx.Event() == EventDisconnect {
			return nil
		}
		if err := ctx.client.AllowEvent(interval); err != nil {
			if errors.Is(err, internal.ErrRateLimited) {
				return ErrRejected
			}
			return err
		}
		return nil
	})
}
