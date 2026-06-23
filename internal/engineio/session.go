package engineio

import "time"

// Config tunes handshake values copied into every new session.
type Config struct {
	PingInterval time.Duration
	PingTimeout  time.Duration
	MaxPayload   int64
}

// Session is an Engine.IO connection identified by sid. Transport details live
// outside this package so engineio stays free of import cycles.
type Session struct {
	ID           string
	PingInterval time.Duration
	PingTimeout  time.Duration
	MaxPayload   int64
	CreatedAt    time.Time
}
