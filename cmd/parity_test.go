// File: parity_test.go
package main

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

var isTest = false

func init() {
	isTest = true
}

// TestRunCLI_Help checks that the --help flag prints usage information.
func TestRunCLI_Help(t *testing.T) {
	// Patch os.Exit
	oldOsExit := osExit
	defer func() { osExit = oldOsExit }()

	var exitCode int
	osExit = func(code int) {
		exitCode = code
		panic("os.Exit called") // Use panic to stop execution
	}

	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()

	os.Args = []string{"parity", "--help"}

	var buf bytes.Buffer
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Recover from the expected panic
	defer func() {
		if r := recover(); r != nil && r != "os.Exit called" {
			t.Fatalf("Unexpected panic: %v", r)
		}
		w.Close()
		os.Stdout = oldStdout
		io.Copy(&buf, r) //nolint:errcheck

		if exitCode != 0 {
			t.Errorf("Expected exit code 0, got %d", exitCode)
		}

		output := buf.String()
		if !strings.Contains(output, "Usage:") {
			t.Errorf("Expected usage output in --help, got: %s", output)
		}
	}()

	_ = runCLI()
}

// TestRunCLI_Version checks that the --version flag prints version information.
func TestRunCLI_Version(t *testing.T) {
	// Patch os.Exit
	oldOsExit := osExit
	defer func() { osExit = oldOsExit }()

	var exitCode int
	osExit = func(code int) {
		exitCode = code
		panic("os.Exit called")
	}

	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()

	os.Args = []string{"parity", "--version"}

	var buf bytes.Buffer
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Recover from the expected panic
	defer func() {
		if r := recover(); r != nil && r != "os.Exit called" {
			t.Fatalf("Unexpected panic: %v", r)
		}
		w.Close()
		os.Stdout = oldStdout
		io.Copy(&buf, r) //nolint:errcheck

		if exitCode != 0 {
			t.Errorf("Expected exit code 0, got %d", exitCode)
		}

		output := buf.String()
		if !strings.Contains(output, "Parity") {
			t.Errorf("Expected version output to contain 'Parity', got: %s", output)
		}
	}()

	_ = runCLI()
}

// TestRunCLI_Default checks that calling without recognized flags prints the usage.
func TestRunCLI_Default(t *testing.T) {
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()

	// No special flags
	os.Args = []string{"parity"}

	// Capture stdout
	var buf bytes.Buffer
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Run the CLI
	err := runCLI()

	// Restore stdout
	w.Close()
	os.Stdout = oldStdout
	io.Copy(&buf, r) //nolint:errcheck

	if err != nil {
		t.Fatalf("Expected no error for default usage, got %v", err)
	}

	output := buf.String()
	if !strings.Contains(output, "Usage:") {
		t.Errorf("Expected usage output, got: %s", output)
	}
}

// TestRunCLI_Start checks that calling --start attempts to start the server and then
// we simulate sending an OS signal to shut it down. This is illustrative and may need
// refining depending on your CI environment or local test setup.
func TestRunCLI_Start(t *testing.T) {
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()

	// Mock command line arguments for --start
	os.Args = []string{"parity", "--start"}

	// We'll run runCLI in a separate goroutine so we can simulate
	// sending an interrupt signal to trigger graceful shutdown.
	errCh := make(chan error, 1)

	go func() {
		errCh <- runCLI()
	}()

	// Give the server a little time to start up (adjust as needed)
	time.Sleep(500 * time.Millisecond)

	// Send an interrupt signal to our own process
	p, _ := os.FindProcess(os.Getpid())
	if err := p.Signal(os.Interrupt); err != nil {
		t.Fatalf("Failed to send interrupt signal: %v", err)
	}

	// Wait for runCLI to return
	err := <-errCh
	if err != nil {
		t.Errorf("Expected no error after graceful shutdown, got %v", err)
	}

	// If needed, you can also hook into logs or other signals to confirm
	// that the server started and was shut down properly.
}
