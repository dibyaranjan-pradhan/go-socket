// Copyright (c) 2026 Dibyaranjan Pradhan. All rights reserved.

package gosocket

import "time"

// ConnectionStats represents diagnostic metrics for a single WebSocket connection.
type ConnectionStats struct {
	// ClientID is the unique connection identifier.
	ClientID string
	// BytesSent is the total number of bytes transmitted to the client.
	BytesSent int64
	// BytesReceived is the total number of bytes received from the client.
	BytesReceived int64
	// MessagesSent is the count of events sent to the client.
	MessagesSent int64
	// MessagesReceived is the count of events received from the client.
	MessagesReceived int64
	// MessagesDropped counts events discarded by drop/volatile backpressure.
	MessagesDropped int64
	// SessionID is the recovery session token; empty when recovery is disabled.
	// Treat as a secret: it authorizes a resume.
	SessionID string
	// BufferUtilization is the percentage of the write buffer in use (0.0 to 1.0).
	// Values approaching 1.0 indicate the client is slow.
	BufferUtilization float32
	// ConnectedAt is when this connection was established.
	ConnectedAt time.Time
	// LastActivity is when this client last sent or received data.
	LastActivity time.Time
	// RoomCount is the number of rooms this client is joined to.
	RoomCount int
}

// ServerStats represents aggregate metrics for the entire WebSocket server.
type ServerStats struct {
	// ActiveConnections is the current number of connected clients.
	ActiveConnections int64
	// TotalConnectionsEver is cumulative connections since server start.
	TotalConnectionsEver int64
	// TotalMessagesProcessed is cumulative events handled since server start.
	TotalMessagesProcessed int64
	// HubEmitsDropped counts broadcasts lost to a saturated hub queue (raise HubEmitBufferSize).
	HubEmitsDropped int64
	// Sessions is the number of recovery sessions held (live + parked); zero when disabled.
	Sessions int64
	// SessionsParked is the number of disconnected sessions awaiting resume.
	SessionsParked int64
	// SessionsResumed is the cumulative count of successful resumes.
	SessionsResumed int64
	// MessagesReplayed is the cumulative count of frames re-sent on resume.
	MessagesReplayed int64
	// DuplicatesSuppressed is the cumulative count of inbound events skipped by idempotency keys.
	DuplicatesSuppressed int64
	// UptimeSince is when the server started.
	UptimeSince time.Time
}

// RoomStats represents metrics for a single room.
type RoomStats struct {
	// RoomID is the room identifier.
	RoomID string
	// MemberCount is the number of clients in this room.
	MemberCount int
	// CreatedAt is when the first client joined this room.
	CreatedAt time.Time
	// Members is a snapshot of client IDs in this room.
	Members []string
}
