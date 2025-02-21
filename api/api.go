package api

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	paritylogger "github.com/TFMV/parity/logger"
	"github.com/TFMV/parity/version"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/compress"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/middleware/monitor"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"go.uber.org/zap"
)

// Server holds the Fiber app instance
type Server struct {
	app  *fiber.App
	log  *zap.Logger
	port string
}

type ServerOptions struct {
	Port    string
	Prefork bool
}

// NewServer initializes a new Fiber instance with best practices
func NewServer(opts ServerOptions) *Server {
	// Initialize Zap logger
	log := paritylogger.GetLogger() // Assumes this is already initialized elsewhere

	fiberConfig := fiber.Config{
		IdleTimeout:  10 * time.Second,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		Prefork:      opts.Prefork,
		ErrorHandler: func(c *fiber.Ctx, err error) error { // Custom error handler
			// Default to 500 Internal Server Error
			code := fiber.StatusInternalServerError
			message := "Internal Server Error"

			// Retrieve the custom status code if it's an fiber.Error
			var e *fiber.Error
			if errors.As(err, &e) {
				code = e.Code
				message = e.Message
			}

			log.Error("Request failed",
				zap.String("method", c.Method()),
				zap.String("path", c.Path()),
				zap.Int("status", code),
				zap.Error(err),
			)

			return c.Status(code).JSON(fiber.Map{
				"error":   true,
				"message": message,
			})
		},
	}

	app := fiber.New(fiberConfig)

	// Middleware
	app.Use(recover.New())  // Auto-recovers from panics
	app.Use(compress.New()) // Enable gzip compression
	app.Use(logger.New())   // Use default config

	// Add routes first
	app.Get("/health", func(c *fiber.Ctx) error {
		log.Debug("Health check requested")
		return c.SendString("OK")
	})

	app.Get("/version", func(c *fiber.Ctx) error {
		log.Debug("Version endpoint hit")
		return c.JSON(fiber.Map{
			"service": "Parity API",
			"version": version.Version,
			"build":   version.BuildDate,
			"time":    time.Now().UTC().Format(time.RFC3339),
		})
	})

	// Add monitor middleware last
	app.Use(monitor.New()) // Expose metrics at /metrics

	// Custom structured logging middleware using Zap (modified)
	app.Use(func(c *fiber.Ctx) error {
		start := time.Now()
		err := c.Next() // Execute the next handler
		duration := time.Since(start)

		// Use structured logging with Zap
		fields := []zap.Field{
			zap.String("method", c.Method()),
			zap.String("path", c.Path()),
			zap.Int("status", c.Response().StatusCode()),
			zap.Duration("duration", duration),
			zap.String("client_ip", c.IP()),
		}

		if err != nil { // Log errors
			fields = append(fields, zap.Error(err))
		}

		log.Info("Request handled", fields...) // Combine fields into a single call

		return err
	})

	return &Server{app: app, log: log, port: opts.Port}
}

// Start runs the Fiber server and handles graceful shutdown
func (s *Server) Start() error {
	if s.port == "" {
		s.port = "3000"
	}

	addr := fmt.Sprintf(":%s", s.port)
	s.log.Info("Starting server", zap.String("address", addr))

	// Graceful shutdown handling
	idleConnsClosed := make(chan struct{})

	go func() {
		sigint := make(chan os.Signal, 1)
		signal.Notify(sigint, os.Interrupt, syscall.SIGTERM) // Handle SIGTERM as well
		<-sigint

		s.log.Info("Shutdown signal received")

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second) // Adjusted timeout
		defer cancel()

		if err := s.Shutdown(ctx); err != nil {
			s.log.Error("Server shutdown error", zap.Error(err))
		}

		close(idleConnsClosed)
	}()

	// Start server in a goroutine
	go func() {
		if err := s.app.Listen(addr); err != nil && !errors.Is(err, fiber.ErrServiceUnavailable) {
			s.log.Fatal("Server error", zap.Error(err))
		}
	}()

	<-idleConnsClosed
	s.log.Info("Server stopped")

	return nil
}

// Shutdown stops the server gracefully with context
func (s *Server) Shutdown(ctx context.Context) error {
	s.log.Warn("Server is shutting down...")
	if err := s.app.ShutdownWithContext(ctx); err != nil {
		s.log.Error("Fiber shutdown error", zap.Error(err))
		return fmt.Errorf("fiber shutdown error: %w", err)
	}
	return nil
}

// GetApp returns the underlying Fiber app
func (s *Server) GetApp() *fiber.App {
	return s.app
}
