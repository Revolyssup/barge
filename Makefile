.PHONY: all build clean test run proto

# Binary name
BINARY := raft-server

# Build directory
BUILD_DIR := bin

# Go parameters
GOCMD := go
GOBUILD := $(GOCMD) build
GOCLEAN := $(GOCMD) clean
GOTEST := $(GOCMD) test
GOMOD := $(GOCMD) mod

all: build

build:
	@mkdir -p $(BUILD_DIR)
	$(GOBUILD) -o $(BUILD_DIR)/$(BINARY) ./cmd/server

clean:
	$(GOCLEAN)
	rm -rf $(BUILD_DIR)

test:
	$(GOTEST) -v ./...

tidy:
	$(GOMOD) tidy

proto:
	protoc --go_out=. --go_opt=paths=source_relative \
	       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
	       proto/raft.proto

# Run a 3-node cluster locally
run-cluster:
	@echo "Starting 3-node Raft cluster..."
	@$(BUILD_DIR)/$(BINARY) -id A -addr :9000 -http :8000 -peers :9001,:9002 &
	@$(BUILD_DIR)/$(BINARY) -id B -addr :9001 -http :8001 -peers :9000,:9002 &
	@$(BUILD_DIR)/$(BINARY) -id C -addr :9002 -http :8002 -peers :9000,:9001 &
	@echo "Cluster started."
	@echo "HTTP APIs: http://localhost:8000, http://localhost:8001, http://localhost:8002"
	@echo "Press Ctrl+C to stop."
	@wait
