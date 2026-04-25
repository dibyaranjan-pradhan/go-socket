package gosocket

import "log"

// Logger is a minimal sink for connect/disconnect/event lines and panic reports.
// Implement it to integrate with zerolog, zap, or other centralized logging.
type Logger interface {
	Printf(format string, v ...interface{})
}

type stdLogger struct{}

func (stdLogger) Printf(format string, v ...interface{}) {
	log.Printf(format, v...)
}

func resolveLogger(l Logger) Logger {
	if l == nil {
		return stdLogger{}
	}
	return l
}
