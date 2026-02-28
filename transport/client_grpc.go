package transport

import (
	"context"
	"fmt"
	"time"

	pb "github.com/revolyssup/barge/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// getConnection returns a gRPC client connection to the target
func getConnection(target string) (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %v", target, err)
	}
	return conn, nil
}

// SendAppendEntries sends an AppendEntries RPC to the target node
func (t *GRPCTransport) SendAppendEntries(target string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	conn, err := getConnection(target)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := pb.NewRaftServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return client.AppendEntries(ctx, req)
}

// SendRequestVote sends a RequestVote RPC to the target node
func (t *GRPCTransport) SendRequestVote(target string, req *pb.VoteRequest) (*pb.VoteResponse, error) {
	conn, err := getConnection(target)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := pb.NewRaftServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return client.RequestVote(ctx, req)
}

// Get sends a client get request to the target node
func (t *GRPCTransport) Get(target string, req *pb.GetRequest) (*pb.GetResponse, error) {
	conn, err := getConnection(target)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := pb.NewRaftServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return client.ClientGet(ctx, req)
}

// Set sends a client set request to the target node
func (t *GRPCTransport) Set(target string, req *pb.SetRequest) (*pb.SetResponse, error) {
	conn, err := getConnection(target)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := pb.NewRaftServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return client.ClientSet(ctx, req)
}
