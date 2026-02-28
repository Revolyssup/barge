package transport

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	pb "github.com/revolyssup/barge/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ClientOperator is an interface for client-facing operations (get/set).
type ClientOperator interface {
	Get(key string) (string, bool)
	ProposeSet(key, value string) error
}

type GRPCTransport struct {
	pb.UnimplementedRaftServiceServer
	addr       string
	handler    Handler
	clientOp   ClientOperator
	server     *grpc.Server
	mu         sync.RWMutex
	clients    map[string]pb.RaftServiceClient
}

func NewGRPCTransport(addr string) *GRPCTransport {
	return &GRPCTransport{
		addr:    addr,
		clients: make(map[string]pb.RaftServiceClient),
	}
}

func (t *GRPCTransport) SetHandler(h Handler) {
	t.handler = h
}

func (t *GRPCTransport) SetClientOperator(op ClientOperator) {
	t.clientOp = op
}

func (t *GRPCTransport) Start() error {
	lis, err := net.Listen("tcp", t.addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", t.addr, err)
	}

	t.server = grpc.NewServer()
	pb.RegisterRaftServiceServer(t.server, t)

	go func() {
		if err := t.server.Serve(lis); err != nil {
			log.Printf("gRPC server error: %v", err)
		}
	}()

	log.Printf("gRPC transport listening on %s", t.addr)
	return nil
}

func (t *GRPCTransport) Stop() {
	if t.server != nil {
		t.server.GracefulStop()
	}
}

func (t *GRPCTransport) getClient(addr string) (pb.RaftServiceClient, error) {
	t.mu.RLock()
	client, ok := t.clients[addr]
	t.mu.RUnlock()
	if ok {
		return client, nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// Double-check after acquiring write lock
	if client, ok := t.clients[addr]; ok {
		return client, nil
	}

	conn, err := grpc.Dial(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithTimeout(5*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", addr, err)
	}

	client = pb.NewRaftServiceClient(conn)
	t.clients[addr] = client
	return client, nil
}

func (t *GRPCTransport) SendAppendEntries(addr string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	client, err := t.getClient(addr)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return client.AppendEntries(ctx, req)
}

func (t *GRPCTransport) SendRequestVote(addr string, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	client, err := t.getClient(addr)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return client.RequestVote(ctx, req)
}

// gRPC server method implementations

func (t *GRPCTransport) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	if t.handler == nil {
		return nil, fmt.Errorf("no handler registered")
	}
	return t.handler.HandleAppendEntries(req)
}

func (t *GRPCTransport) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	if t.handler == nil {
		return nil, fmt.Errorf("no handler registered")
	}
	return t.handler.HandleRequestVote(req)
}

func (t *GRPCTransport) ClientGet(ctx context.Context, req *pb.ClientGetRequest) (*pb.ClientGetResponse, error) {
	if t.clientOp == nil {
		return &pb.ClientGetResponse{
			Error: "node not ready for client operations",
		}, nil
	}

	value, found := t.clientOp.Get(req.Key)
	return &pb.ClientGetResponse{
		Value: value,
		Found: found,
	}, nil
}

func (t *GRPCTransport) ClientSet(ctx context.Context, req *pb.ClientSetRequest) (*pb.ClientSetResponse, error) {
	if t.clientOp == nil {
		return &pb.ClientSetResponse{
			Success: false,
			Error:   "node not ready for client operations",
		}, nil
	}

	err := t.clientOp.ProposeSet(req.Key, req.Value)
	if err != nil {
		return &pb.ClientSetResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &pb.ClientSetResponse{
		Success: true,
	}, nil
}
