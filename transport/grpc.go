package transport

import (
	"context"
	"fmt"
	"net"

	pb "github.com/revolyssup/barge/proto"
	"google.golang.org/grpc"
)

// NodeHandler defines the interface that the consensus node must implement
// to handle incoming RPCs.
type NodeHandler interface {
	HandleAppendEntries(req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error)
	HandleRequestVote(req *pb.VoteRequest) (*pb.VoteResponse, error)
	HandleGet(req *pb.GetRequest) (*pb.GetResponse, error)
	HandleSet(req *pb.SetRequest) (*pb.SetResponse, error)
}

// GRPCTransport implements the Transport interface using gRPC
type GRPCTransport struct {
	pb.UnimplementedRaftServiceServer
	server  *grpc.Server
	handler NodeHandler
}

// NewGRPCTransport creates a new gRPC transport
func NewGRPCTransport(handler NodeHandler) *GRPCTransport {
	t := &GRPCTransport{
		handler: handler,
	}
	t.server = grpc.NewServer()
	pb.RegisterRaftServiceServer(t.server, t)
	return t
}

// Start begins listening on the given address
func (t *GRPCTransport) Start(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}
	go t.server.Serve(lis)
	return nil
}

// Stop gracefully stops the gRPC server
func (t *GRPCTransport) Stop() {
	t.server.GracefulStop()
}

// AppendEntries handles incoming AppendEntries RPCs
func (t *GRPCTransport) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	return t.handler.HandleAppendEntries(req)
}

// RequestVote handles incoming RequestVote RPCs
func (t *GRPCTransport) RequestVote(ctx context.Context, req *pb.VoteRequest) (*pb.VoteResponse, error) {
	return t.handler.HandleRequestVote(req)
}

// ClientGet handles incoming client get RPCs
func (t *GRPCTransport) ClientGet(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	return t.handler.HandleGet(req)
}

// ClientSet handles incoming client set RPCs
func (t *GRPCTransport) ClientSet(ctx context.Context, req *pb.SetRequest) (*pb.SetResponse, error) {
	return t.handler.HandleSet(req)
}
