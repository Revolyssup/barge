PROTO_DIR=proto
BIN_DIR=bin

.PHONY: proto build clean test server cli

proto:
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		$(PROTO_DIR)/raft.proto

build: proto
	go build -o $(BIN_DIR)/server ./cmd/server
	go build -o $(BIN_DIR)/barge ./cmd/cli

clean:
	rm -rf $(BIN_DIR)/*

test:
	go test ./...

server:
	go run ./cmd/server

cli:
	go run ./cmd/cli
