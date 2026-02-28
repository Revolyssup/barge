package transport

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"

	pb "github.com/example/raft/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// normalizeAddr ensures the address has a host component for dialing.
// Converts ":9000" to "localhost:9000".
func normalizeAddr(addr string) string {
	if strings.HasPrefix(addr, ":") {
		return "localhost" + addr
	}
	return addr
}

// -------------------------------------------------------------------------
// gRPC Client (Transport implementation)
// -------------------------------------------------------------------------

// GRPCTransport implements Transport using gRPC.
// It maintains a pool of connections to peers.
type GRPCTransport struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

func NewGRPCTransport() *GRPCTransport {
	return &GRPCTransport{conns: make(map[string]*grpc.ClientConn)}
}

func (t *GRPCTransport) dial(addr string) (pb.RaftServiceClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if conn, ok := t.conns[addr]; ok {
		return pb.NewRaftServiceClient(conn), nil
	}
	dialAddr := normalizeAddr(addr)
	conn, err := grpc.Dial(dialAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", dialAddr, err)
	}
	t.conns[addr] = conn
	return pb.NewRaftServiceClient(conn), nil
}

func (t *GRPCTransport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, c := range t.conns {
		c.Close()
	}
}

func (t *GRPCTransport) SendRequestVote(ctx context.Context, addr string, args RequestVoteArgs) (RequestVoteReply, error) {
	client, err := t.dial(addr)
	if err != nil {
		return RequestVoteReply{}, err
	}
	resp, err := client.RequestVote(ctx, &pb.RequestVoteRequest{
		Term:         args.Term,
		CandidateId:  args.CandidateID,
		LastLogIndex: args.LastLogIndex,
		LastLogTerm:  args.LastLogTerm,
	})
	if err != nil {
		return RequestVoteReply{}, err
	}
	return RequestVoteReply{Term: resp.Term, VoteGranted: resp.VoteGranted}, nil
}

func (t *GRPCTransport) SendAppendEntries(ctx context.Context, addr string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	client, err := t.dial(addr)
	if err != nil {
		return AppendEntriesReply{}, err
	}
	pbEntries := make([]*pb.LogEntry, len(args.Entries))
	for i, e := range args.Entries {
		pbEntries[i] = &pb.LogEntry{Index: e.Index, Term: e.Term, Command: e.Command}
	}
	resp, err := client.AppendEntries(ctx, &pb.AppendEntriesRequest{
		Term:         args.Term,
		LeaderId:     args.LeaderID,
		PrevLogIndex: args.PrevLogIndex,
		PrevLogTerm:  args.PrevLogTerm,
		Entries:      pbEntries,
		LeaderCommit: args.LeaderCommit,
	})
	if err != nil {
		return AppendEntriesReply{}, err
	}
	return AppendEntriesReply{Term: resp.Term, Success: resp.Success}, nil
}

// -------------------------------------------------------------------------
// gRPC Server
// -------------------------------------------------------------------------

type GRPCServer struct {
	srv *grpc.Server
}

func NewGRPCServer() *GRPCServer { return &GRPCServer{} }

func (s *GRPCServer) Start(addr string, handler Handler) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}
	s.srv = grpc.NewServer()
	pb.RegisterRaftServiceServer(s.srv, &grpcHandler{handler: handler})
	go s.srv.Serve(lis)
	return nil
}

func (s *GRPCServer) Stop() {
	if s.srv != nil {
		s.srv.GracefulStop()
	}
}

// grpcHandler bridges the generated gRPC interface to our transport.Handler.
type grpcHandler struct {
	pb.UnimplementedRaftServiceServer
	handler Handler
}

func (g *grpcHandler) RequestVote(_ context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	reply := g.handler.HandleRequestVote(RequestVoteArgs{
		Term:         req.Term,
		CandidateID:  req.CandidateId,
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
	})
	return &pb.RequestVoteResponse{Term: reply.Term, VoteGranted: reply.VoteGranted}, nil
}

func (g *grpcHandler) AppendEntries(_ context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	entries := make([]LogEntry, len(req.Entries))
	for i, e := range req.Entries {
		entries[i] = LogEntry{Index: e.Index, Term: e.Term, Command: e.Command}
	}
	reply := g.handler.HandleAppendEntries(AppendEntriesArgs{
		Term:         req.Term,
		LeaderID:     req.LeaderId,
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      entries,
		LeaderCommit: req.LeaderCommit,
	})
	return &pb.AppendEntriesResponse{Term: reply.Term, Success: reply.Success}, nil
}
