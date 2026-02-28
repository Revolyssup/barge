// Package transport defines the network interface used by the consensus layer.
// The concrete gRPC implementation and the mock used in tests both satisfy this interface.
package transport

import "context"

// RequestVoteArgs mirrors the protobuf message but lives in the transport-agnostic layer
// so that the consensus package does not depend on generated proto code.
type RequestVoteArgs struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

// AppendEntriesArgs mirrors AppendEntriesRequest.
type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

type AppendEntriesReply struct {
	Term    uint64
	Success bool
}

// LogEntry is the wire representation of a log entry.
type LogEntry struct {
	Index   uint64
	Term    uint64
	Command []byte
}

// Transport is the interface the consensus layer uses to reach peers.
type Transport interface {
	// SendRequestVote calls RequestVote on the peer at the given address.
	SendRequestVote(ctx context.Context, addr string, args RequestVoteArgs) (RequestVoteReply, error)

	// SendAppendEntries calls AppendEntries on the peer at the given address.
	SendAppendEntries(ctx context.Context, addr string, args AppendEntriesArgs) (AppendEntriesReply, error)
}

// Handler is implemented by the consensus node and called by the server when
// it receives an inbound RPC.
type Handler interface {
	HandleRequestVote(args RequestVoteArgs) RequestVoteReply
	HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply
}

// Server listens for inbound RPCs and forwards them to a Handler.
type Server interface {
	Start(addr string, handler Handler) error
	Stop()
}
