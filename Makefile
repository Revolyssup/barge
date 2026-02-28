PROTO_DIR=proto
BIN_DIR=bin

.PHONY: proto build clean server cli

proto:
	protoc --go_out=. --go-grpc_out=. $(PROTO_DIR)/raft.proto

build: proto
	go build -o $(BIN_DIR)/server cmd/server/main.go
	go build -o $(BIN_DIR)/cli cmd/cli/main.go

server:
	go build -o $(BIN_DIR)/server cmd/server/main.go

cli:
	go build -o $(BIN_DIR)/cli cmd/cli/main.go

clean:
	rm -rf $(BIN_DIR)/*
