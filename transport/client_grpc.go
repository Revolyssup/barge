package transport

import (
	"context"
	"fmt"
	"time"

	pb "github.com/revolyssup/barge/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type GRPCClient struct {
	conns map[string]*grpc.ClientConn
}

func NewGRPCClient() *GRPCClient {
	return &GRPCClient{
		conns: make(map[string]*grpc.ClientConn),
	}
}

func (c *GRPCClient) getConn(addr string) (*grpc.ClientConn, error) {
	if conn, ok := c.conns[addr]; ok {
		return conn, nil
	}

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %v", addr, err)
	}
	c.conns[addr] = conn
	return conn, nil
}

func (c *GRPCClient) SendRequestVote(addr string, req *pb.VoteRequest) (*pb.VoteResponse, error) {
	conn, err := c.getConn(addr)
	if err != nil {
		return nil, err
	}

	client := pb.NewRaftClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return client.RequestVote(ctx, req)
}

func (c *GRPCClient) SendAppendEntries(addr string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	conn, err := c.getConn(addr)
	if err != nil {
		return nil, err
	}

	client := pb.NewRaftClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return client.AppendEntries(ctx, req)
}

// KVClient is a client for the KV service.
type KVClient struct {
	conn *grpc.ClientConn
}

// NewKVClient creates a new KV client connected to the given address.
func NewKVClient(addr string) (*KVClient, error) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %v", addr, err)
	}
	return &KVClient{conn: conn}, nil
}

// Get retrieves a value by key from the barge node.
func (c *KVClient) Get(key string) (*pb.GetResponse, error) {
	client := pb.NewKVClient(c.conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return client.Get(ctx, &pb.GetRequest{Key: key})
}

// Set sets a key-value pair on the barge node.
func (c *KVClient) Set(key, value string) (*pb.SetResponse, error) {
	client := pb.NewKVClient(c.conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return client.Set(ctx, &pb.SetRequest{Key: key, Value: value})
}

// Close closes the client connection.
func (c *KVClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
