// Package logger wraps zap for structured logging.
package logger

import (
	"fmt"
	"os"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	log     *zap.Logger
	once    sync.Once
	logPath = "parity.log" // default path
)

// SetLogPath allows changing the log file location before initialization
func SetLogPath(path string) {
	logPath = path
}

// InitLogger initializes the Zap logger with structured logging.
func InitLogger() {
	once.Do(func() {
		// Define log level (adjustable)
		level := zap.NewAtomicLevelAt(zap.InfoLevel)

		// Configure file logging
		fileEncoder := zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())
		file, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			panic(fmt.Sprintf("Failed to open log file: %v", err))
		}
		fileCore := zapcore.NewCore(fileEncoder, zapcore.AddSync(file), level)

		// Configure console logging
		consoleEncoder := zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig())
		consoleCore := zapcore.NewCore(consoleEncoder, zapcore.Lock(os.Stdout), level)

		// Combine both outputs (console + file)
		core := zapcore.NewTee(consoleCore, fileCore)

		// Initialize global logger
		log = zap.New(core, zap.AddCaller(), zap.AddCallerSkip(1))
	})
}

// GetLogger provides access to the initialized logger.
func GetLogger() *zap.Logger {
	if log == nil {
		InitLogger()
	}
	return log
}

// Sync ensures buffered logs are written before the application exits.
func Sync() {
	if log != nil {
		_ = log.Sync()
	}
}

// ResetLogger resets the logger state for testing
func ResetLogger() {
	if log != nil {
		log.Sync()
		log = nil
	}
	once = sync.Once{}
}

// Debug logs a debug message with structured fields
func Debug(msg string, fields ...zap.Field) {
	if log != nil {
		log.Debug(msg, fields...)
	}
}
