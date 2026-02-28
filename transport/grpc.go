package transport

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/revolyssup/barge/consensus"
	pb "github.com/revolyssup/barge/proto"
	"google.golang.org/grpc"
)

type GRPCTransport struct {
	pb.UnimplementedRaftServer
	pb.UnimplementedKVServer
	node   *consensus.Node
	server *grpc.Server
	addr   string
}

func NewGRPCTransport(addr string) *GRPCTransport {
	return &GRPCTransport{
		addr: addr,
	}
}

func (t *GRPCTransport) SetNode(node *consensus.Node) {
	t.node = node
}

func (t *GRPCTransport) Start() error {
	lis, err := net.Listen("tcp", t.addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", t.addr, err)
	}

	t.server = grpc.NewServer()
	pb.RegisterRaftServer(t.server, t)
	pb.RegisterKVServer(t.server, t)

	log.Printf("gRPC server listening on %s", t.addr)
	go func() {
		if err := t.server.Serve(lis); err != nil {
			log.Printf("gRPC server error: %v", err)
		}
	}()

	return nil
}

func (t *GRPCTransport) Stop() {
	if t.server != nil {
		t.server.GracefulStop()
	}
}

func (t *GRPCTransport) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	if t.node == nil {
		return nil, fmt.Errorf("node not set")
	}
	resp := t.node.HandleAppendEntries(req)
	return resp, nil
}

func (t *GRPCTransport) RequestVote(ctx context.Context, req *pb.VoteRequest) (*pb.VoteResponse, error) {
	if t.node == nil {
		return nil, fmt.Errorf("node not set")
	}
	resp := t.node.HandleRequestVote(req)
	return resp, nil
}

func (t *GRPCTransport) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if t.node == nil {
		return nil, fmt.Errorf("node not set")
	}
	value, found := t.node.Get(req.Key)
	return &pb.GetResponse{
		Value: value,
		Found: found,
	}, nil
}

func (t *GRPCTransport) Set(ctx context.Context, req *pb.SetRequest) (*pb.SetResponse, error) {
	if t.node == nil {
		return nil, fmt.Errorf("node not set")
	}
	err := t.node.Set(req.Key, req.Value)
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
