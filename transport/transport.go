package transport

import (
	pb "github.com/revolyssup/barge/proto"
)

// Handler is an interface that the consensus node implements to handle
// incoming Raft RPCs.
type Handler interface {
	HandleAppendEntries(req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error)
	HandleRequestVote(req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error)
}

// Transport is an interface for sending Raft RPCs to other nodes.
type Transport interface {
	SendAppendEntries(addr string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error)
	SendRequestVote(addr string, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error)
	SetHandler(h Handler)
	Start() error
	Stop()
}
