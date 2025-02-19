// Package main is the entry point for the Parity CLI.
package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/TFMV/parity/api"
	"github.com/TFMV/parity/version"
	"github.com/docopt/docopt-go"
)

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

	arguments, err := docopt.ParseArgs(usage, nil, "Parity "+version.Version)
	if err != nil {
		return fmt.Errorf("parsing arguments: %w", err)
	}

	switch {
	case arguments["--version"].(bool):
		fmt.Println("Parity", version.Version)

	case arguments["--start"].(bool):
		return startServer()

	default:
		fmt.Println(usage)
	}

	return nil
}

// startServer initializes and runs the server with graceful shutdown handling.
func startServer() error {
	server := api.NewServer()

	// Channel to listen for OS termination signals.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	// Start the server in a separate goroutine.
	go func() {
		if err := server.Start("5555"); err != nil {
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
