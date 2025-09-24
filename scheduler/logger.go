package scheduler

import (
	"log"
)

type Logger interface {
	Error(format string, args ...interface{})
	Warn(format string, args ...interface{})
	Info(format string, args ...interface{})
}

type myLogger struct{}

func (myLogger) Error(format string, args ...interface{}) {
	log.Printf("ERROR "+format, args...)
}

func (myLogger) Warn(format string, args ...interface{}) {
	log.Printf("WARN "+format, args...)
}

func (myLogger) Info(format string, args ...interface{}) {
	log.Printf("INFO "+format, args...)
}
