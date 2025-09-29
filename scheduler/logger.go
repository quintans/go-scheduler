package scheduler

import (
	"log"
)

type Logger interface {
	Error(format string, args ...any)
	Warn(format string, args ...any)
	Info(format string, args ...any)
}

type myLogger struct{}

func (myLogger) Error(format string, args ...any) {
	log.Printf("ERROR "+format, args...)
}

func (myLogger) Warn(format string, args ...any) {
	log.Printf("WARN "+format, args...)
}

func (myLogger) Info(format string, args ...any) {
	log.Printf("INFO "+format, args...)
}
