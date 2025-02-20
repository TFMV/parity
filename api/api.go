package api

import (
	"context"
	"os"
	"os/signal"
	"time"

	"github.com/TFMV/parity/logger"
	"github.com/TFMV/parity/version"
	"github.com/gofiber/fiber/v2"
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
	logger.InitLogger()
	log := logger.GetLogger()

	app := fiber.New(fiber.Config{
		IdleTimeout:  10 * time.Second,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		Prefork:      opts.Prefork,
	})

	// Middleware
	app.Use(recover.New()) // Auto-recovers from panics

	// Fiber's default logger middleware (kept for console logs)
	app.Use(func(c *fiber.Ctx) error {
		start := time.Now()
		err := c.Next()
		duration := time.Since(start)

		// Use structured logging with Zap
		log.Info("Request handled",
			zap.String("method", c.Method()),
			zap.String("path", c.Path()),
			zap.Int("status", c.Response().StatusCode()),
			zap.Duration("duration", duration),
			zap.String("client_ip", c.IP()),
		)

		return err
	})

	// Routes
	app.Get("/health", func(c *fiber.Ctx) error {
		log.Info("Health check requested")
		return c.SendString("OK")
	})

	app.Get("/version", func(c *fiber.Ctx) error {
		log.Info("Version endpoint hit")
		return c.JSON(fiber.Map{
			"service": "Parity API",
			"version": version.Version,
			"build":   version.BuildDate,
			"time":    time.Now().UTC().Format(time.RFC3339),
		})
	})

	return &Server{app: app, log: log, port: opts.Port}
}

// Start runs the Fiber server and handles graceful shutdown
func (s *Server) Start() error {
	if s.port == "" {
		s.port = "3000"
	}

	// Channel to listen for OS termination signals (graceful shutdown)
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)

	// Start server in a goroutine
	go func() {
		s.log.Info("Parity API is running", zap.String("port", s.port))
		if err := s.app.Listen(":" + s.port); err != nil {
			s.log.Fatal("Server error", zap.Error(err))
		}
	}()

	// Wait for shutdown signal
	<-quit
	s.log.Warn("Received shutdown signal, stopping server...")

	// Create a timeout context for shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.app.ShutdownWithContext(ctx); err != nil {
		s.log.Fatal("Error shutting down", zap.Error(err))
	}

	s.log.Info("Server shutdown successfully")
	return nil
}

// Shutdown stops the server gracefully
func (s *Server) Shutdown() error {
	s.log.Warn("Manual shutdown requested")
	return s.app.Shutdown()
}

// GetApp returns the underlying Fiber app
func (s *Server) GetApp() *fiber.App {
	return s.app
}
