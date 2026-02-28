BINARY_DIR=bin
SERVER_BINARY=$(BINARY_DIR)/barge-server
CLI_BINARY=$(BINARY_DIR)/barge-cli

.PHONY: all server cli proto clean

all: server cli

server:
	@mkdir -p $(BINARY_DIR)
	go build -o $(SERVER_BINARY) ./cmd/server

cli:
	@mkdir -p $(BINARY_DIR)
	go build -o $(CLI_BINARY) ./cmd/cli

proto:
	protoc --go_out=. --go-grpc_out=. proto/raft.proto

clean:
	rm -rf $(BINARY_DIR)

test:
	go test ./...
