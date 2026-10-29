// Copyright (c) 2026 Dibyaranjan Pradhan. All rights reserved.

package internal

import "time"

// ClientStats is used internally to track client metrics.
type ClientStats struct {
	BytesSent         int64
	BytesReceived     int64
	MessagesSent      int64
	MessagesReceived  int64
	MessagesDropped   int64
	BufferUtilization float32
	ConnectedAt       time.Time
	LastActivity      time.Time
}
