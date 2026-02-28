PROTO_DIR=proto
BIN_DIR=bin

.PHONY: proto server cli clean all

all: proto server cli

proto:
	protoc --go_out=. --go-grpc_out=. $(PROTO_DIR)/raft.proto

server:
	go build -o $(BIN_DIR)/barge-server ./cmd/server

cli:
	go build -o $(BIN_DIR)/barge-cli ./cmd/cli

clean:
	rm -rf $(BIN_DIR)/*
