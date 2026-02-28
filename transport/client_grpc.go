package transport

import (
	"context"

	"github.com/revolyssup/barge/consensus"
	pb "github.com/revolyssup/barge/proto"
)

// ClientServer implements the ClientService gRPC server.
type ClientServer struct {
	pb.UnimplementedClientServiceServer
	node *consensus.Node
}

// NewClientServer creates a new ClientServer.
func NewClientServer(node *consensus.Node) *ClientServer {
	return &ClientServer{node: node}
}

// Get handles a client Get request.
func (s *ClientServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	value, found := s.node.Get(req.Key)
	return &pb.GetResponse{
		Value: value,
		Found: found,
	}, nil
}

// Set handles a client Set request by proposing the command through Raft.
func (s *ClientServer) Set(ctx context.Context, req *pb.SetRequest) (*pb.SetResponse, error) {
	err := s.node.Propose(req.Key, req.Value)
	if err != nil {
		return &pb.SetResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}
	return &pb.SetResponse{
		Success: true,
	}, nil
}
