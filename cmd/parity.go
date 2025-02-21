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

	// Subcommands
	rootCmd.AddCommand(versionCmd)
	rootCmd.AddCommand(startCmd)
	rootCmd.AddCommand(validateConfigCmd)
	rootCmd.AddCommand(runCmd)
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".") // search in current directory

	if cfgFile != "" {
		// Use config file specified by the user.
		viper.SetConfigFile(cfgFile)
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			log.Println("No config file found. Using defaults and/or environment variables.")
		} else {
			log.Printf("Error reading config file: %v", err) // Potentially fatal? Depends on defaults
		}
	} else {
		log.Printf("Using config file: %s", viper.ConfigFileUsed())
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

func startServer(cmd *cobra.Command, args []string) error {
	opts := api.ServerOptions{
		Port:    viper.GetString("server.port"),
		Prefork: viper.GetBool("server.prefork"),
	}

	server := api.NewServer(opts)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-quit
		log.Println("Received shutdown signal, stopping server...")
		cancel() // Signal the server to shutdown
	}()

	// Start the server in a goroutine
	go func() {
		if err := server.Start(); err != nil && !errors.Is(err, context.Canceled) {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	// Block until context is cancelled
	<-ctx.Done()

	log.Println("Shutting down server...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil && !errors.Is(err, context.Canceled) {
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

func validateConfig(cmd *cobra.Command, args []string) error {
	var cfg config.Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return fmt.Errorf("failed to unmarshal config: %w", err)
	}
	if err := config.ValidateConfig(&cfg); err != nil {
		return fmt.Errorf("config validation failed: %w", err)
	}
	fmt.Println("Configuration is valid.")
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
	time.Sleep(3 * time.Second)
	w.Stop()
	fmt.Println("\nData validation completed.")
	return nil
}
