# Project Variables
APP_NAME := parity
GO_FILES := $(shell find . -type f -name '*.go')
PKG := github.com/TFMV/$(APP_NAME)

# Commands
GOCMD := go
GOTEST := $(GOCMD) test
GOLINT := golangci-lint

# Default target
.DEFAULT_GOAL := help

# Build the application
build: ## Build the binary
	$(GOCMD) build -o $(APP_NAME)

# Run tests
test: ## Run tests with verbose output
	$(GOTEST) -v ./...

# Run tests with coverage
test-coverage: ## Run tests with coverage report
	$(GOTEST) -cover -coverprofile=coverage.out ./...
	$(GOCMD) tool cover -func=coverage.out

# Lint the code
lint: ## Run linters using golangci-lint
	$(GOLINT) run ./...

# Clean build artifacts
clean: ## Remove binary and coverage files
	rm -f $(APP_NAME)
	rm -f coverage.out

# Install dependencies
deps: ## Install Go modules and tools
	$(GOCMD) mod tidy
	$(GOCMD) mod vendor
	$(GOLINT) --version || (curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(shell go env GOPATH)/bin v1.53.3)

# Format the code
fmt: ## Format code using gofmt
	$(GOCMD) fmt ./...

# Show help
help: ## Show help for each command
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'
