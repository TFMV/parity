package api

import (
	"context"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/TFMV/parity/version"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/middleware/recover"
)

// Server holds the Fiber app instance
type Server struct {
	app *fiber.App
}

// NewServer initializes a new Fiber instance with best practices
func NewServer() *Server {
	app := fiber.New(fiber.Config{
		IdleTimeout:  10 * time.Second, // Prevents idle connections
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		Prefork:      true, // Uses multiple OS processes for better performance
	})

	// Middleware
	app.Use(recover.New()) // Auto-recovers from panics
	app.Use(logger.New())  // Logs all requests

	// Routes
	app.Get("/health", func(c *fiber.Ctx) error {
		return c.SendString("OK")
	})

	app.Get("/version", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"service": "Parity API",
			"version": version.Version,
			"build":   version.BuildDate,
			"time":    time.Now().UTC().Format(time.RFC3339),
		})
	})

	return &Server{app: app}
}

// Start runs the Fiber server and handles graceful shutdown
func (s *Server) Start(port string) error {
	if port == "" {
		port = "3000" // Default port
	}

	// Channel to listen for OS termination signals (graceful shutdown)
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)

	// Start server in a goroutine
	go func() {
		log.Printf("Parity API is running on port %s\n", port)
		if err := s.app.Listen(":" + port); err != nil {
			log.Fatalf("Server error: %v", err)
		}
	}()

	// Wait for shutdown signal
	<-quit
	log.Println("Received shutdown signal, stopping server...")

	// Create a timeout context for the shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.app.ShutdownWithContext(ctx); err != nil {
		log.Fatalf("Error shutting down: %v", err)
	}

	log.Println("Server shutdown successfully")
	return nil
}

func (s *Server) Shutdown() error {
	return s.app.Shutdown()
}
