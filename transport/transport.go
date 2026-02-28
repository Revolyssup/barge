package transport

import (
	pb "github.com/revolyssup/barge/proto"
)

// Transport defines the interface for network communication between Raft nodes
// as well as client-facing get/set operations.
type Transport interface {
	// SendAppendEntries sends an AppendEntries RPC to the target node
	SendAppendEntries(target string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error)

	// SendRequestVote sends a RequestVote RPC to the target node
	SendRequestVote(target string, req *pb.VoteRequest) (*pb.VoteResponse, error)

	// Get sends a get request to the target node
	Get(target string, req *pb.GetRequest) (*pb.GetResponse, error)

	// Set sends a set request to the target node
	Set(target string, req *pb.SetRequest) (*pb.SetResponse, error)
}
