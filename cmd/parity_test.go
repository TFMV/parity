package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"errors"

	"github.com/TFMV/parity/version"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var isTest = false

func init() {
	isTest = true
	// Set empty config for tests
	viper.SetConfigType("yaml")
	testConfig := strings.NewReader(`
server:
  port: "3000"
  prefork: false
DB1:
  type: "postgres"
  tables:
    - name: "test"
      primary_key: "id"
DB2:
  type: "postgres"
  tables:
    - name: "test"
      primary_key: "id"
`)
	if err := viper.ReadConfig(testConfig); err != nil {
		panic(fmt.Sprintf("Failed to load test config: %v", err))
	}
}

func TestCLI_Help(t *testing.T) {
	rootCmd := newRootCommand()
	output, err := executeCommand(rootCmd, "--help")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if !strings.Contains(output, "Usage:") {
		t.Errorf("Expected usage output in --help, got: %s", output)
	}
}

func TestCLI_Version(t *testing.T) {
	rootCmd := newRootCommand()
	output, err := executeCommand(rootCmd, "version")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if !strings.Contains(output, "Parity") {
		t.Errorf("Expected version output to contain 'Parity', got: %s", output)
	}
}

func TestCLI_Default(t *testing.T) {
	rootCmd := newRootCommand()
	output, err := executeCommand(rootCmd)
	if err != nil {
		t.Fatalf("Expected no error for default usage, got %v", err)
	}
	if !strings.Contains(output, "Usage:") {
		t.Errorf("Expected usage output, got: %s", output)
	}
}

func TestCLI_Start(t *testing.T) {
	// Reset config for this test
	viper.Set("server.port", "3001") // Use different port for test

	rootCmd := newRootCommand()
	errCh := make(chan error, 1)
	done := make(chan struct{})

	// Start server in background
	go func() {
		defer close(done)
		errCh <- executeCommandErr(rootCmd, "start")
	}()

	// Give server time to start
	time.Sleep(200 * time.Millisecond)

	// Send interrupt signal
	process, err := os.FindProcess(os.Getpid())
	if err != nil {
		t.Fatalf("Failed to find process: %v", err)
	}
	process.Signal(os.Interrupt)

	// Wait for shutdown with timeout
	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("Expected no error or context.Canceled, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Test timed out waiting for server shutdown")
	}

	// Ensure server goroutine completed
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Server goroutine did not complete")
	}
}

func executeCommand(rootCmd *cobra.Command, args ...string) (string, error) {
	buf := new(bytes.Buffer)
	rootCmd.SetOut(buf)
	rootCmd.SetErr(buf)
	rootCmd.SetArgs(args)
	err := rootCmd.Execute()
	return buf.String(), err
}

func executeCommandErr(rootCmd *cobra.Command, args ...string) error {
	rootCmd.SetArgs(args)
	return rootCmd.Execute()
}

func newRootCommand() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "parity",
		Short: "Parity CLI for data validation",
	}

	// Version command
	rootCmd.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "Show the version of Parity",
		Run: func(cmd *cobra.Command, args []string) {
			cmd.Printf("Parity %s\n", version.Version)
		},
	})

	// Start command
	startCmd := &cobra.Command{
		Use:   "start",
		Short: "Start the Parity API server",
		RunE:  startServer,
	}
	rootCmd.AddCommand(startCmd)

	// Validate Config command

	validateConfigCmd := &cobra.Command{
		Use:   "validate-config",
		Short: "Validate the Parity configuration file",
		RunE: func(cmd *cobra.Command, args []string) error {
			return validateConfig(cmd, args)
		},
	}
	rootCmd.AddCommand(validateConfigCmd)

	// Run Validation command
	runCmd := &cobra.Command{
		Use:   "run",
		Short: "Run a data validation",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runValidation(cmd, args)
		},
	}
	rootCmd.AddCommand(runCmd)

	return rootCmd
}
