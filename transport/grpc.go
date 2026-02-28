package transport

import (
	"context"
	"fmt"
	"net"

	"github.com/revolyssup/barge/consensus"
	pb "github.com/revolyssup/barge/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type GRPCTransport struct {
	pb.UnimplementedRaftServiceServer
	node     *consensus.Node
	server   *grpc.Server
	addr     string
	clients  map[string]pb.RaftServiceClient
}

func NewGRPCTransport(addr string) *GRPCTransport {
	return &GRPCTransport{
		addr:    addr,
		clients: make(map[string]pb.RaftServiceClient),
	}
}

func (t *GRPCTransport) SetNode(node *consensus.Node) {
	t.node = node
}

func (t *GRPCTransport) Start() error {
	lis, err := net.Listen("tcp", t.addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	t.server = grpc.NewServer()
	pb.RegisterRaftServiceServer(t.server, t)

	// Register client service if node is set
	if t.node != nil {
		clientServer := NewClientServer(t.node)
		pb.RegisterClientServiceServer(t.server, clientServer)
	}

	go func() {
		if err := t.server.Serve(lis); err != nil {
			fmt.Printf("gRPC server error: %v\n", err)
		}
	}()

	return nil
}

func (t *GRPCTransport) Stop() {
	if t.server != nil {
		t.server.GracefulStop()
	}
}

func (t *GRPCTransport) getClient(target string) (pb.RaftServiceClient, error) {
	if client, ok := t.clients[target]; ok {
		return client, nil
	}

	conn, err := grpc.Dial(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to dial %s: %v", target, err)
	}

	client := pb.NewRaftServiceClient(conn)
	t.clients[target] = client
	return client, nil
}

func (t *GRPCTransport) SendAppendEntries(target string, req consensus.AppendEntriesRequest) (*consensus.AppendEntriesResponse, error) {
	client, err := t.getClient(target)
	if err != nil {
		return nil, err
	}

	pbEntries := make([]*pb.LogEntry, len(req.Entries))
	for i, entry := range req.Entries {
		pbEntries[i] = &pb.LogEntry{
			Term:  entry.Term,
			Index: entry.Index,
			Data:  entry.Data,
		}
	}

	resp, err := client.AppendEntries(context.Background(), &pb.AppendEntriesRequest{
		Term:         req.Term,
		LeaderId:     req.LeaderID,
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      pbEntries,
		LeaderCommit: req.LeaderCommit,
	})
	if err != nil {
		return nil, err
	}

	return &consensus.AppendEntriesResponse{
		Term:    resp.Term,
		Success: resp.Success,
	}, nil
}

func (t *GRPCTransport) SendRequestVote(target string, req consensus.RequestVoteRequest) (*consensus.RequestVoteResponse, error) {
	client, err := t.getClient(target)
	if err != nil {
		return nil, err
	}

	resp, err := client.RequestVote(context.Background(), &pb.RequestVoteRequest{
		Term:         req.Term,
		CandidateId:  req.CandidateID,
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
	})
	if err != nil {
		return nil, err
	}

	return &consensus.RequestVoteResponse{
		Term:        resp.Term,
		VoteGranted: resp.VoteGranted,
	}, nil
}

func (t *GRPCTransport) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	if t.node == nil {
		return nil, fmt.Errorf("node not set")
	}

	entries := make([]consensus.LogEntry, len(req.Entries))
	for i, entry := range req.Entries {
		entries[i] = consensus.LogEntry{
			Term:  entry.Term,
			Index: entry.Index,
			Data:  entry.Data,
		}
	}

	resp := t.node.HandleAppendEntries(consensus.AppendEntriesRequest{
		Term:         req.Term,
		LeaderID:     req.LeaderId,
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      entries,
		LeaderCommit: req.LeaderCommit,
	})

	return &pb.AppendEntriesResponse{
		Term:    resp.Term,
		Success: resp.Success,
	}, nil
}

func (t *GRPCTransport) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	if t.node == nil {
		return nil, fmt.Errorf("node not set")
	}

	resp := t.node.HandleRequestVote(consensus.RequestVoteRequest{
		Term:         req.Term,
		CandidateID:  req.CandidateId,
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
	})

	return &pb.RequestVoteResponse{
		Term:        resp.Term,
		VoteGranted: resp.VoteGranted,
	}, nil
}
