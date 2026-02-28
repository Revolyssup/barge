package transport

import (
	pb "barge/proto"
)

// Transport defines the interface for node-to-node communication
type Transport interface {
	// SendAppendEntries sends an AppendEntries RPC to the target node
	SendAppendEntries(target string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error)

	// SendRequestVote sends a RequestVote RPC to the target node
	SendRequestVote(target string, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error)

	// Get sends a client get request to the target node
	Get(target string, req *pb.ClientGetRequest) (*pb.ClientGetResponse, error)

	// Set sends a client set request to the target node
	Set(target string, req *pb.ClientSetRequest) (*pb.ClientSetResponse, error)
}
