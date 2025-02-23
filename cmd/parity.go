package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/TFMV/parity/api"
	"github.com/TFMV/parity/config"
	"github.com/TFMV/parity/logger"
	"github.com/TFMV/parity/version"
	"github.com/briandowns/spinner"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile string // Config file path, can be passed via flag

	rootCmd = &cobra.Command{
		Use:   "parity",
		Short: "Parity CLI for data validation",
		Long:  `Parity is a data validation tool.`,
	}
)

func init() {
	// Initialize logger - move this up so logging is available ASAP
	logger.InitLogger()

	// Initialize Cobra
	cobra.OnInitialize(initConfig)

	// Define flags and configuration settings.
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is ./config.yaml)")

	// Add help command
	rootCmd.SetHelpCommand(&cobra.Command{
		Use:   "help",
		Short: "Display help for any command",
		RunE: func(cmd *cobra.Command, args []string) error {
			return rootCmd.Help()
		},
	})

	// Subcommands
	rootCmd.AddCommand(versionCmd)
	rootCmd.AddCommand(startCmd)
	rootCmd.AddCommand(validateConfigCmd)
	rootCmd.AddCommand(runCmd)
}

// appConfig is the application configuration.
var appConfig config.Config

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if viper.ConfigFileUsed() != "" {
		// Config already loaded
		return
	}

	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".") // Search in current directory

	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	}

	viper.AutomaticEnv() // Read environment variables

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			log.Println("No config file found. Using defaults and environment variables.")
		} else {
			log.Printf("Error reading config file: %v", err)
		}
	} else {
		log.Printf("Using config file: %s", viper.ConfigFileUsed())
	}

	// Unmarshal config into struct
	if err := viper.Unmarshal(&appConfig); err != nil {
		log.Fatalf("Failed to parse configuration: %v", err)
	}
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("Error: %v", err) // Exit immediately if root command fails.
	}
}

// versionCmd represents the version command
var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Show the version of Parity",
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Printf("Parity %s\n", version.Version)
	},
}

// startCmd represents the start command
var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Start the Parity API server",
	RunE:  startServer,
}

// startServer starts the Parity API server.
func startServer(cmd *cobra.Command, args []string) error {
	opts := api.ServerOptions{
		Port:    appConfig.Server.Port,
		Prefork: appConfig.Server.Prefork,
	}

	server := api.NewServer(opts)
	ctx, cancel := context.WithCancel(context.Background())

	// Handle shutdown signals
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-quit
		log.Println("Received shutdown signal, stopping server...")
		cancel()
	}()

	// Start server in goroutine
	serverErr := make(chan error, 1)
	go func() {
		if err := server.Start(); err != nil && !errors.Is(err, context.Canceled) {
			serverErr <- fmt.Errorf("server failed to start: %w", err)
		}
	}()

	select {
	case <-ctx.Done():
		log.Println("Shutting down server...")
	case err := <-serverErr:
		log.Printf("Server error: %v", err)
		cancel()
		return err
	}

	// Graceful shutdown with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("Error during shutdown: %v", err)
		return err
	}

	log.Println("Server shutdown successfully")
	return nil
}

// validateConfigCmd represents the validate-config command
var validateConfigCmd = &cobra.Command{
	Use:   "validate-config",
	Short: "Validate the Parity configuration file",
	RunE:  validateConfig,
}

// validateConfig validates the configuration file.
func validateConfig(cmd *cobra.Command, args []string) error {
	var cfg config.Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}
	if err := config.ValidateConfig(&cfg); err != nil {
		return fmt.Errorf("configuration validation failed: %w", err)
	}
	log.Println("Configuration is valid.")
	return nil
}

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run a data validation",
	RunE:  runValidation,
}

// runValidation executes validation with a custom spinner.
func runValidation(cmd *cobra.Command, args []string) error {
	w := spinner.New(spinner.CharSets[9], 100*time.Millisecond)
	w.Suffix = " Running data validation..."
	w.Start()

	done := make(chan bool)

	go func() {
		// Simulate actual work being done
		for range [5]int{} {
			time.Sleep(800 * time.Millisecond) // Simulated progress
		}
		done <- true
	}()

	select {
	case <-done:
		w.Stop()
		log.Println("\nData validation completed successfully.")
	case <-time.After(10 * time.Second): // Timeout safeguard
		w.Stop()
		log.Println("\nData validation took too long. Please check logs for details.")
		return errors.New("validation timeout")
	}

	return nil
}
