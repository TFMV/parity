package logger

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestInitLogger ensures that the logger initializes properly.
func TestInitLogger(t *testing.T) {
	ResetLogger()
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "parity.log")
	SetLogPath(logPath)

	// Initialize logger
	InitLogger()

	// Check if log is not nil
	if log == nil {
		t.Fatal("Expected logger to be initialized, but got nil")
	}

	// Log a test message
	log.Info("Test log message")

	// Check if the log file exists after initialization
	if _, err := os.Stat(logPath); os.IsNotExist(err) {
		t.Fatal("Log file was not created")
	}
}

// TestGetLogger ensures that GetLogger returns a non-nil instance.
func TestGetLogger(t *testing.T) {
	ResetLogger()

	// Create temp directory for test
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "parity.log")

	// Set log file path before initializing logger
	SetLogPath(logPath)

	// Retrieve logger (this will also initialize it)
	logger := GetLogger()

	if logger == nil {
		t.Fatal("Expected non-nil logger instance, but got nil")
	}

	logger.Info("Logger retrieved successfully")

	// Ensure logs are flushed
	Sync()

	// Verify that the log file exists
	if _, err := os.Stat(logPath); os.IsNotExist(err) {
		t.Fatal("Log file was not created")
	}
}

// TestSync ensures the Sync function flushes logs properly.
func TestSync(t *testing.T) {
	ResetLogger()

	// Create temp directory for test
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "parity.log")
	SetLogPath(logPath)

	// Capture standard output
	var buf bytes.Buffer
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Initialize and test logger
	InitLogger()
	log.Info("Test sync log message")

	// Flush logs
	Sync()

	// Restore stdout
	w.Close()
	os.Stdout = oldStdout
	_, _ = buf.ReadFrom(r)

	// Verify log file exists and contains message
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}

	if !bytes.Contains(data, []byte("Test sync log message")) {
		t.Fatal("Expected log message not found in log file")
	}

	t.Log("Sync function executed successfully")
}

// TestLogOutput checks if logging produces expected results in the log file.
func TestLogOutput(t *testing.T) {
	// Reset logger state
	ResetLogger()

	// Create temp directory for test
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "parity.log")

	// Set log file path before init
	SetLogPath(logPath)

	// Initialize logger
	InitLogger()

	// Write test message
	log.Info("Writing to log file")

	// Ensure logs are flushed and file is closed
	Sync()

	// Small delay to ensure file system catches up
	time.Sleep(100 * time.Millisecond)

	// Read log file
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}

	// Ensure log contains expected content
	if !bytes.Contains(data, []byte("Writing to log file")) {
		t.Fatal("Expected log message not found in log file")
	}
}
