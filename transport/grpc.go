package transport

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	pb "barge/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// NodeHandler defines the interface that the consensus node must implement
// to handle incoming RPCs
type NodeHandler interface {
	HandleAppendEntries(req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error)
	HandleRequestVote(req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error)
	HandleClientGet(req *pb.ClientGetRequest) (*pb.ClientGetResponse, error)
	HandleClientSet(req *pb.ClientSetRequest) (*pb.ClientSetResponse, error)
}

// GRPCTransport implements the Transport interface using gRPC
type GRPCTransport struct {
	pb.UnimplementedRaftServiceServer
	mu      sync.RWMutex
	handler NodeHandler
	server  *grpc.Server
	conns   map[string]*grpc.ClientConn
}

// NewGRPCTransport creates a new gRPC transport
func NewGRPCTransport() *GRPCTransport {
	return &GRPCTransport{
		conns: make(map[string]*grpc.ClientConn),
	}
}

// RegisterHandler registers the node handler for incoming RPCs
func (t *GRPCTransport) RegisterHandler(handler NodeHandler) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.handler = handler
}

// Start starts the gRPC server on the given address
func (t *GRPCTransport) Start(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	t.server = grpc.NewServer()
	pb.RegisterRaftServiceServer(t.server, t)

	go func() {
		if err := t.server.Serve(lis); err != nil {
			log.Printf("gRPC server error: %v", err)
		}
	}()

	log.Printf("gRPC transport listening on %s", addr)
	return nil
}

// Stop stops the gRPC server
func (t *GRPCTransport) Stop() {
	if t.server != nil {
		t.server.GracefulStop()
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	for addr, conn := range t.conns {
		conn.Close()
		delete(t.conns, addr)
	}
}

// getConn returns a cached or new gRPC client connection
func (t *GRPCTransport) getConn(target string) (*grpc.ClientConn, error) {
	t.mu.RLock()
	conn, ok := t.conns[target]
	t.mu.RUnlock()
	if ok {
		return conn, nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// Double-check after acquiring write lock
	if conn, ok := t.conns[target]; ok {
		return conn, nil
	}

	conn, err := grpc.Dial(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(5*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", target, err)
	}

	t.conns[target] = conn
	return conn, nil
}

// --- Client-side Transport methods ---

// SendAppendEntries sends an AppendEntries RPC to the target node
func (t *GRPCTransport) SendAppendEntries(target string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	conn, err := t.getConn(target)
	if err != nil {
		return nil, err
	}

	client := pb.NewRaftServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return client.AppendEntries(ctx, req)
}

// SendRequestVote sends a RequestVote RPC to the target node
func (t *GRPCTransport) SendRequestVote(target string, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	conn, err := t.getConn(target)
	if err != nil {
		return nil, err
	}

	client := pb.NewRaftServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return client.RequestVote(ctx, req)
}

// Get sends a ClientGet RPC to the target node
func (t *GRPCTransport) Get(target string, req *pb.ClientGetRequest) (*pb.ClientGetResponse, error) {
	conn, err := t.getConn(target)
	if err != nil {
		return nil, err
	}

	client := pb.NewRaftServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return client.ClientGet(ctx, req)
}

// Set sends a ClientSet RPC to the target node
func (t *GRPCTransport) Set(target string, req *pb.ClientSetRequest) (*pb.ClientSetResponse, error) {
	conn, err := t.getConn(target)
	if err != nil {
		return nil, err
	}

	client := pb.NewRaftServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return client.ClientSet(ctx, req)
}

// --- Server-side gRPC handlers ---

// AppendEntries handles incoming AppendEntries RPCs
func (t *GRPCTransport) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	t.mu.RLock()
	handler := t.handler
	t.mu.RUnlock()

	if handler == nil {
		return nil, fmt.Errorf("no handler registered")
	}

	return handler.HandleAppendEntries(req)
}

// RequestVote handles incoming RequestVote RPCs
func (t *GRPCTransport) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	t.mu.RLock()
	handler := t.handler
	t.mu.RUnlock()

	if handler == nil {
		return nil, fmt.Errorf("no handler registered")
	}

	return handler.HandleRequestVote(req)
}

// ClientGet handles incoming ClientGet RPCs
func (t *GRPCTransport) ClientGet(ctx context.Context, req *pb.ClientGetRequest) (*pb.ClientGetResponse, error) {
	t.mu.RLock()
	handler := t.handler
	t.mu.RUnlock()

	if handler == nil {
		return nil, fmt.Errorf("no handler registered")
	}

	return handler.HandleClientGet(req)
}

// ClientSet handles incoming ClientSet RPCs
func (t *GRPCTransport) ClientSet(ctx context.Context, req *pb.ClientSetRequest) (*pb.ClientSetResponse, error) {
	t.mu.RLock()
	handler := t.handler
	t.mu.RUnlock()

	if handler == nil {
		return nil, fmt.Errorf("no handler registered")
	}

	return handler.HandleClientSet(req)
}
