package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/TFMV/parity/api"
	"github.com/TFMV/parity/logger"
	"github.com/TFMV/parity/version"
	"github.com/briandowns/spinner"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func main() {
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
			return validateConfig()
		},
	}
	rootCmd.AddCommand(validateConfigCmd)

	// Run Validation command
	runCmd := &cobra.Command{
		Use:   "run",
		Short: "Run a data validation",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runValidation()
		},
	}
	rootCmd.AddCommand(runCmd)

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("Error: %v", err)
	}
}

func startServer(cmd *cobra.Command, args []string) error {
	opts := api.ServerOptions{
		Port:    viper.GetString("server.port"),
		Prefork: viper.GetBool("server.prefork"),
	}
	server := api.NewServer(opts)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	go func() {
		if err := server.Start(); err != nil {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	<-quit
	log.Println("Received shutdown signal, stopping server...")
	if err := server.Shutdown(); err != nil {
		log.Fatalf("Error shutting down: %v", err)
	}

	log.Println("Server shutdown successfully")
	return nil
}

func validateConfig() error {
	fmt.Println("Validating configuration...")
	// Implement config validation logic
	return nil
}

// runValidation executes validation with a custom spinner.
func runValidation() error {
	// Choose a spinner style
	w := spinner.New(spinner.CharSets[9], 100*time.Millisecond)
	w.Start()

	// Simulate validation work
	time.Sleep(3 * time.Second)

	// Stop spinner and persist final output
	w.Stop()

	return nil
}

func init() {
	// Initialize logger
	logger.InitLogger()
	// Initialize Viper for configuration
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")

	if err := viper.ReadInConfig(); err != nil {
		log.Printf("No config file found: %v", err)
	}
}
