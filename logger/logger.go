// Package logger wraps zap for structured logging.
package logger

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	log     *zap.Logger
	once    sync.Once
	logPath = "parity.log" // default relative path
)

// findProjectRoot looks for the project root by searching for .git or go.mod
func findProjectRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}

	for {
		// Check for common project root indicators
		if _, err := os.Stat(filepath.Join(dir, ".git")); err == nil {
			return dir, nil
		}
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}

		// Move up one directory
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("could not find project root")
		}
		dir = parent
	}
}

// SetLogPath allows changing the log file location before initialization
func SetLogPath(path string) {
	logPath = path
}

// InitLogger initializes the Zap logger with structured logging.
func InitLogger() {
	once.Do(func() {
		var finalPath string

		// If path is absolute, use it directly
		if filepath.IsAbs(logPath) {
			finalPath = logPath
		} else {
			// Find project root for relative paths
			root, err := findProjectRoot()
			if err != nil {
				panic(fmt.Sprintf("Failed to find project root: %v", err))
			}
			finalPath = filepath.Join(root, logPath)
		}

		// Create log directory if it doesn't exist
		logDir := filepath.Dir(finalPath)
		if err := os.MkdirAll(logDir, 0755); err != nil {
			panic(fmt.Sprintf("Failed to create log directory: %v", err))
		}

		// Define log level (adjustable)
		level := zap.NewAtomicLevelAt(zap.InfoLevel)

		// Configure file logging
		fileEncoder := zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())
		file, err := os.OpenFile(finalPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
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

// Info logs an info message with structured fields
func Info(msg string, fields ...zap.Field) {
	if log != nil {
		log.Info(msg, fields...)
	}
}

// Warn logs a warning message with structured fields
func Warn(msg string, fields ...zap.Field) {
	if log != nil {
		log.Warn(msg, fields...)
	}
}

// Error logs an error message with structured fields
func Error(msg string, fields ...zap.Field) {
	if log != nil {
		log.Error(msg, fields...)
	}
}

// Fatal logs a fatal message with structured fields and then calls os.Exit(1)
func Fatal(msg string, fields ...zap.Field) {
	if log != nil {
		log.Fatal(msg, fields...)
	}
	os.Exit(1)
}
