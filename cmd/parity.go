// Package main is the entry point for the Parity CLI.
package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/TFMV/parity/api"
	"github.com/TFMV/parity/version"
	"github.com/docopt/docopt-go"
)

var osExit = os.Exit // Allow overriding for tests

func main() {
	if err := runCLI(); err != nil {
		log.Fatalf("Error: %v", err)
	}
}

func runCLI() error {
	usage := `
	Usage:
		parity [options]

	Options:
		-h, --help     Show this help message
		-v, --version  Show the version
		--start        Start the parity server
	`

	// Handle version flag directly first
	for _, arg := range os.Args[1:] {
		if arg == "--version" || arg == "-v" {
			fmt.Println("Parity", version.Version)
			osExit(0)
			return nil
		}
	}

	parser := &docopt.Parser{
		HelpHandler:  func(err error, usage string) {},
		OptionsFirst: true,
	}

	arguments, err := parser.ParseArgs(usage, nil, "Parity "+version.Version)
	if err != nil {
		if strings.Contains(err.Error(), "help requested") {
			fmt.Println(usage)
			osExit(0)
			return nil
		}
		return fmt.Errorf("parsing arguments: %w", err)
	}

	switch {
	case arguments["--help"] != nil && arguments["--help"].(bool):
		fmt.Println(usage)
		osExit(0)
		return nil
	case arguments["--start"] != nil && arguments["--start"].(bool):
		return startServer()
	default:
		fmt.Println(usage)
		return nil
	}
}

// startServer initializes and runs the server with graceful shutdown handling.
func startServer() error {
	opts := api.ServerOptions{
		Port:    "5555",
		Prefork: false,
	}
	server := api.NewServer(opts)

	// Channel to listen for OS termination signals.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	// Start the server in a separate goroutine.
	go func() {
		if err := server.Start(); err != nil {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	// Block until an OS signal is received.
	<-quit
	log.Println("Received shutdown signal, stopping server...")

	// Perform graceful shutdown.
	if err := server.Shutdown(); err != nil {
		log.Fatalf("Error shutting down: %v", err)
	}

	log.Println("Server shutdown successfully")
	return nil
}
