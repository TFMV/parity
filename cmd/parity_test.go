package main

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"errors"

	"github.com/TFMV/parity/version"
	"github.com/spf13/cobra"
)

var isTest = false

func init() {
	isTest = true
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
	rootCmd := newRootCommand()
	errCh := make(chan error, 1)

	// Start server in background
	go func() {
		errCh <- executeCommandErr(rootCmd, "start")
	}()

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	// Send interrupt signal
	process, err := os.FindProcess(os.Getpid())
	if err != nil {
		t.Fatalf("Failed to find process: %v", err)
	}
	process.Signal(os.Interrupt)

	// Wait for shutdown and check error
	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("Expected no error or context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("Test timed out waiting for server shutdown")
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
