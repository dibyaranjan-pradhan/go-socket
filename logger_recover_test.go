package gosocket

import (
	"testing"
)

func TestRecoverMiddlewareType(t *testing.T) {
	if !isRecoverMiddleware(Recover()) {
		t.Fatal("Recover() should be detected")
	}
	if isRecoverMiddleware(MiddlewareFunc(func(*Context) error { return nil })) {
		t.Fatal("plain middleware should not be recover")
	}
}

func TestResolveLoggerDefault(t *testing.T) {
	l := resolveLogger(nil)
	if l == nil {
		t.Fatal("expected default logger")
	}
	l.Printf("test %s", "log")
}
