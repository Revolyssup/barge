BINARY_DIR=bin
SERVER_BINARY=$(BINARY_DIR)/server
CLI_BINARY=$(BINARY_DIR)/cli
PROTO_DIR=proto
GO=go
PROTOC=protoc

.PHONY: all build build-server build-cli proto test clean run-server run-cli fmt vet lint help

# Default target
all: proto build

# Build all binaries
build: build-server build-cli

# Build server binary
build-server:
	@echo "Building server..."
	@mkdir -p $(BINARY_DIR)
	$(GO) build -o $(SERVER_BINARY) ./cmd/server

# Build CLI client binary
build-cli:
	@echo "Building CLI client..."
	@mkdir -p $(BINARY_DIR)
	$(GO) build -o $(CLI_BINARY) ./cmd/cli

# Generate protobuf code
proto:
	@echo "Generating protobuf code..."
	$(PROTOC) --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		$(PROTO_DIR)/raft.proto

# Run all tests
test:
	@echo "Running tests..."
	$(GO) test ./... -v

# Run tests with race detector
test-race:
	@echo "Running tests with race detector..."
	$(GO) test -race ./... -v

# Clean build artifacts
clean:
	@echo "Cleaning..."
	rm -rf $(BINARY_DIR)
	rm -f server
	$(GO) clean

# Run the server
run-server: build-server
	@echo "Running server..."
	./$(SERVER_BINARY)

# Run the CLI client
run-cli: build-cli
	@echo "Running CLI client..."
	./$(CLI_BINARY)

# Format code
fmt:
	@echo "Formatting code..."
	$(GO) fmt ./...

# Vet code
vet:
	@echo "Vetting code..."
	$(GO) vet ./...

# Run linter (requires golangci-lint)
lint:
	@echo "Linting code..."
	golangci-lint run ./...

# Show help
help:
	@echo "Available targets:"
	@echo "  all         - Generate protos and build all binaries (default)"
	@echo "  build       - Build all binaries (server and cli)"
	@echo "  build-server - Build the server binary"
	@echo "  build-cli   - Build the CLI client binary"
	@echo "  proto       - Generate protobuf code"
	@echo "  test        - Run all tests"
	@echo "  test-race   - Run all tests with race detector"
	@echo "  clean       - Remove build artifacts"
	@echo "  run-server  - Build and run the server"
	@echo "  run-cli     - Build and run the CLI client"
	@echo "  fmt         - Format Go source files"
	@echo "  vet         - Vet Go source files"
	@echo "  lint        - Run golangci-lint"
	@echo "  help        - Show this help message"
