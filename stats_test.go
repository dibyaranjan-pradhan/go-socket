package gosocket

import (
	"context"
	"testing"
	"time"
)

func newTestContext() context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return ctx
}

func TestConnectionStatsZero(t *testing.T) {
	stats := &ConnectionStats{}
	if stats.BytesSent != 0 || stats.MessagesSent != 0 {
		t.Errorf("ConnectionStats should initialize to zero")
	}
}

func TestServerStatsInitialization(t *testing.T) {
	srv := New(Config{})
	defer func() { _ = srv.Shutdown(newTestContext()) }()

	stats := srv.GetServerStats()
	if stats == nil {
		t.Fatal("GetServerStats should not return nil")
	}
	if stats.ActiveConnections != 0 {
		t.Errorf("expected 0 active connections, got %d", stats.ActiveConnections)
	}
}

func TestRoomStatsEmpty(t *testing.T) {
	srv := New(Config{})
	defer func() { _ = srv.Shutdown(newTestContext()) }()

	stats := srv.GetRoomStats("nonexistent")
	if stats != nil {
		t.Errorf("GetRoomStats should return nil for nonexistent room")
	}
}

func TestListAllRoomsEmpty(t *testing.T) {
	srv := New(Config{})
	defer func() { _ = srv.Shutdown(newTestContext()) }()

	rooms := srv.ListAllRooms()
	if len(rooms) != 0 {
		t.Errorf("ListAllRooms should return empty list initially")
	}
}
