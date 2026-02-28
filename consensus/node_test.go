package consensus

import (
	"testing"
	"time"

	"github.com/example/raft/storage"
	"github.com/example/raft/transport"
)

// -------------------------------------------------------------------------
// Test cluster helper
// -------------------------------------------------------------------------

type testCluster struct {
	nodes []*Node
	ids   []string
	mt    *mockTransport
}

func newTestCluster(t *testing.T, size int) *testCluster {
	t.Helper()

	ids := make([]string, size)
	for i := range ids {
		ids[i] = string(rune('A' + i))
	}

	mt := newMockTransport()
	nodes := make([]*Node, size)

	for i, id := range ids {
		peers := make([]string, 0, size-1)
		for j, pid := range ids {
			if j != i {
				peers = append(peers, pid)
			}
		}
		cfg := Config{
			ID:                 id,
			Peers:              peers,
			HeartbeatInterval:  50 * time.Millisecond,
			ElectionTimeoutMin: 150 * time.Millisecond,
			ElectionTimeoutMax: 300 * time.Millisecond,
		}
		nodes[i] = NewNode(cfg, storage.NewInMemoryLogStorage(), mt, make(chan ApplyMsg, 128))
	}

	// Wire each node into the mock transport before starting.
	for i, node := range nodes {
		mt.register(ids[i], node)
	}
	for _, node := range nodes {
		node.Start()
	}

	t.Cleanup(func() {
		for _, node := range nodes {
			node.Stop()
		}
	})

	return &testCluster{nodes: nodes, ids: ids, mt: mt}
}

func (tc *testCluster) waitForLeader(t *testing.T, timeout time.Duration) *Node {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range tc.nodes {
			if n.IsLeader() {
				return n
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for a leader")
	return nil
}

// -------------------------------------------------------------------------
// Election tests
// -------------------------------------------------------------------------

func TestElection_OneNode(t *testing.T) {
	tc := newTestCluster(t, 1)
	leader := tc.waitForLeader(t, 2*time.Second)
	if !leader.IsLeader() {
		t.Fatal("single node should become leader")
	}
}

func TestElection_ThreeNodes_ExactlyOneLeader(t *testing.T) {
	tc := newTestCluster(t, 3)
	tc.waitForLeader(t, 3*time.Second)

	leaderCount := 0
	for _, n := range tc.nodes {
		if n.IsLeader() {
			leaderCount++
		}
	}
	if leaderCount != 1 {
		t.Fatalf("want 1 leader, got %d", leaderCount)
	}
}

func TestElection_FiveNodes(t *testing.T) {
	tc := newTestCluster(t, 5)
	tc.waitForLeader(t, 3*time.Second)
}

func TestElection_TermIsPositiveAfterElection(t *testing.T) {
	tc := newTestCluster(t, 3)
	tc.waitForLeader(t, 3*time.Second)

	for _, n := range tc.nodes {
		if n.CurrentTerm() == 0 {
			t.Errorf("node %s has term 0 after election", n.cfg.ID)
		}
	}
}

// -------------------------------------------------------------------------
// Submit / log replication tests
// -------------------------------------------------------------------------

func TestSubmit_LeaderAccepts_FollowerRejects(t *testing.T) {
	tc := newTestCluster(t, 3)
	leader := tc.waitForLeader(t, 3*time.Second)

	if !leader.Submit([]byte("cmd1")) {
		t.Error("leader.Submit should return true")
	}

	for _, n := range tc.nodes {
		if n != leader {
			if n.Submit([]byte("cmd2")) {
				t.Error("follower.Submit should return false")
			}
			break
		}
	}
}

// -------------------------------------------------------------------------
// Unit tests for individual RPC handlers (no goroutines needed)
// -------------------------------------------------------------------------

func makeNode(id string) *Node {
	return NewNode(
		Config{
			ID:                 id,
			Peers:              nil,
			HeartbeatInterval:  50 * time.Millisecond,
			ElectionTimeoutMin: 200 * time.Millisecond,
			ElectionTimeoutMax: 400 * time.Millisecond,
		},
		storage.NewInMemoryLogStorage(),
		newMockTransport(),
		make(chan ApplyMsg, 4),
	)
}

func TestHandleRequestVote_GrantsVote(t *testing.T) {
	node := makeNode("A")
	reply := node.HandleRequestVote(transport.RequestVoteArgs{
		Term: 2, CandidateID: "B",
	})
	if !reply.VoteGranted {
		t.Fatal("expected vote granted")
	}
	if reply.Term != 2 {
		t.Fatalf("expected term 2, got %d", reply.Term)
	}
}

func TestHandleRequestVote_DeniesOnStaleTerm(t *testing.T) {
	node := makeNode("A")
	// Advance to term 5.
	node.HandleRequestVote(transport.RequestVoteArgs{Term: 5, CandidateID: "B"})

	reply := node.HandleRequestVote(transport.RequestVoteArgs{Term: 3, CandidateID: "C"})
	if reply.VoteGranted {
		t.Fatal("should not grant vote for stale term")
	}
}

func TestHandleRequestVote_DeniesDoubleVote(t *testing.T) {
	node := makeNode("A")
	// Vote for B in term 1.
	node.HandleRequestVote(transport.RequestVoteArgs{Term: 1, CandidateID: "B"})
	// C requests vote in the same term.
	reply := node.HandleRequestVote(transport.RequestVoteArgs{Term: 1, CandidateID: "C"})
	if reply.VoteGranted {
		t.Fatal("should not grant second vote in same term")
	}
}

func TestHandleAppendEntries_HeartbeatSucceeds(t *testing.T) {
	node := makeNode("A")
	reply := node.HandleAppendEntries(transport.AppendEntriesArgs{
		Term: 1, LeaderID: "B",
	})
	if !reply.Success {
		t.Fatal("empty heartbeat should succeed")
	}
}

func TestHandleAppendEntries_RejectsStaleTerm(t *testing.T) {
	node := makeNode("A")
	// Advance to term 3.
	node.HandleAppendEntries(transport.AppendEntriesArgs{Term: 3, LeaderID: "B"})

	reply := node.HandleAppendEntries(transport.AppendEntriesArgs{Term: 1, LeaderID: "C"})
	if reply.Success {
		t.Fatal("should reject AppendEntries with stale term")
	}
}

func TestHandleAppendEntries_AppendsEntries(t *testing.T) {
	node := makeNode("A")

	entries := []transport.LogEntry{
		{Index: 1, Term: 1, Command: []byte("set x=1")},
		{Index: 2, Term: 1, Command: []byte("set y=2")},
	}
	reply := node.HandleAppendEntries(transport.AppendEntriesArgs{
		Term:         1,
		LeaderID:     "B",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      entries,
		LeaderCommit: 2,
	})
	if !reply.Success {
		t.Fatal("expected success")
	}
	if node.log.LastIndex() != 2 {
		t.Fatalf("expected last index 2, got %d", node.log.LastIndex())
	}
}
