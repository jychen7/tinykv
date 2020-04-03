// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
    "math/rand"
    "time"
	"errors"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// None is a placeholder node ID used when there is no leader.
// a better name could be NoLeader
const None uint64 = 0
const NoLeader uint64 = None

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft.
	// ID cannot be 0 (None, i.e. NoLeader).
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
    heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
    randomizedElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// number of ticks since it reached last electionTimeout
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	progresses := make(map[uint64]*Progress)
	for _, p := range c.peers {
		progresses[p] = &Progress{Next: 1}
	}
	return &Raft{
		id: c.ID,
		// Term
		// Vote
		RaftLog: newLog(c.Storage),
		Prs: progresses,
		State: StateFollower,
		// votes
		// msgs
		// Lead
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout: c.ElectionTick,
		// heartbeatElapsed
		// electionElapsed
		// leadTransferee
		// PendingConfIndex
	}
}

func (r *Raft) sendTo(to uint64, m pb.Message) {
	m.From = r.id
    m.To = to
	r.msgs = append(r.msgs, m)
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return false
}

// tick advances the internal logical clock by a single tick.
// trigger Step message if timeout
func (r *Raft) tick() {
	switch r.State {
	case StateFollower, StateCandidate:
		r.tickAsFollower()
	case StateLeader:
		r.tickAsLeader()
	}
}

func (r *Raft) tickAsFollower() {
	// vote for self if no heartbeat during randomizedElectionTimeout
    r.electionElapsed += 1
	if r.electionElapsed < r.randomizedElectionTimeout {
		return
	}
    // r.sendTo(r.id, pb.Message{MsgType: pb.MessageType_MsgHup})
    r.onReceiveMsgHup()
}

func (r *Raft) tickAsLeader() {
	// periodically send a heartbeat
	// heartbeatTimeout: heartbeat cool down period
	r.heartbeatElapsed += 1
	if r.heartbeatElapsed < r.heartbeatTimeout {
		return
	}
    // r.sendTo(r.id, pb.Message{MsgType: pb.MessageType_MsgBeat})
    r.onReceiveMsgBeat()
}

// becomeFollower transform this peer's state to Follower
// handle internal states
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.State = StateFollower
    r.Term = term
    r.Vote = lead
	r.Lead = lead
    r.electionElapsed = 0

    rand.Seed(time.Now().UnixNano())
    r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// becomeCandidate transform this peer's state to candidate
// handle internal states
func (r *Raft) becomeCandidate() {
	r.State = StateCandidate
	r.Term += 1
    r.Vote = r.id
    r.Lead = NoLeader
    r.votes = make(map[uint64]bool)
	r.electionElapsed = 0

    rand.Seed(time.Now().UnixNano())
    r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// becomeLeader transform this peer's state to leader
// handle internal states
func (r *Raft) becomeLeader() {
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
    r.Vote = r.id
    r.Lead = NoLeader
	r.heartbeatElapsed = 0
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.onReceiveMsgHup()
	case pb.MessageType_MsgRequestVote:
		r.onReceiveMsgRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.onReceiveMsgRequestVoteResponse(m)
	case pb.MessageType_MsgBeat:
		r.onReceiveMsgBeat()
	case pb.MessageType_MsgHeartbeat:
		r.onReceiveMsgHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
        r.onReceiveMsgHeartbeatResponse(m)
    case pb.MessageType_MsgPropose:
        r.onReceiveMsgPropose()
    case pb.MessageType_MsgAppend:
        r.onReceiveMsgAppend(m)
    case pb.MessageType_MsgAppendResponse:
        r.onReceiveMsgAppendResponse(m)
    }

    return nil
}

// Follower/Candidate -> self: MsgHup
// Candidate -> peers: MsgRequestVote
// peer -> Candidate: MsgRequestVoteResponse
func (r *Raft) onReceiveMsgHup() {
    if r.State == StateLeader {
        return
    }

    r.becomeCandidate()
	for peer_id := range r.Prs {
		if peer_id == r.id {
            // vote for self directly
            // actually it maybe simple to send message to self, but test cases reject it
            r.onReceiveMsgRequestVoteResponse(
                pb.Message{
                    From: r.id,
                    MsgType: pb.MessageType_MsgRequestVoteResponse,
                    Term: r.Term,
                    Reject: false,
                },
            )
            continue
        }
		r.sendTo(peer_id, pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
            Term: r.Term,
            LogTerm: r.RaftLog.LastTerm(),
            Index: r.RaftLog.LastIndex(),
		})
	}
}

func (r *Raft) onReceiveMsgRequestVote(m pb.Message) {
	if m.Term < r.Term {
		r.voteWith(false, m)
		return
	}

    if m.Term > r.Term {
		r.becomeFollower(m.Term, NoLeader)
	}

    will_vote := (r.Vote == m.From) || (r.Vote == NoLeader)
    will_follow := r.RaftLog.behind(m.LogTerm, m.Index)
    if will_vote && will_follow {
        r.voteWith(true, m)
        r.becomeFollower(m.Term, m.From)
        return
    }
    r.voteWith(false, m)
}

func (r *Raft) voteWith(accept bool, m pb.Message) {
	r.sendTo(m.From, pb.Message{
        MsgType: pb.MessageType_MsgRequestVoteResponse,
        Term: m.Term,
		Reject: !accept,
	})
}

func (r *Raft) onReceiveMsgRequestVoteResponse(m pb.Message) {
	r.votes[m.From] = !m.Reject
	if r.currentVotes(true) >= r.majority() {
        r.becomeLeader()
        return
	}

    if r.currentVotes(false) >= r.majority() {
		r.becomeFollower(r.Term, NoLeader)
	}
}

func (r *Raft) currentVotes(accept bool) int {
	num := 0
	for _, v := range r.votes {
		if v == accept {
			num += 1
		}
	}
	return num
}

func (r *Raft) majority() int {
	return len(r.Prs) / 2 + 1
}

// Leader -> Leader: MsgBeat
// Leader -> Follower: MsgHeartbeat
// Follower -> Leader: MsgHeartbeatResponse
func (r *Raft) onReceiveMsgBeat() {
    r.heartbeatElapsed = 0
    if r.State != StateLeader {
        return
    }

    for peer_id := range r.Prs {
		if peer_id == r.id {
			continue
		}
		r.sendTo(peer_id, pb.Message{
			MsgType: pb.MessageType_MsgHeartbeat,
            Term: r.Term,
            Commit: r.RaftLog.committed,
		})
	}
}

func (r * Raft) onReceiveMsgHeartbeat(m pb.Message) {
	r.becomeFollower(m.Term, m.From)
	// reply
	r.handleHeartbeat(m)
}

// keep since test case requires it
func (r *Raft) handleHeartbeat(m pb.Message) {
    if r.RaftLog.committed < m.Commit {
        r.RaftLog.committed = m.Commit
    }
    r.sendTo(m.From, pb.Message{MsgType: pb.MessageType_MsgHeartbeatResponse})
}

func (r *Raft) onReceiveMsgHeartbeatResponse(m pb.Message) {
	// peer_progress = r.Prs[m.From]
}

// Leader -> Leader: MsgPropose
// Leader -> Follower: MsgAppend
// Follower -> Leader: MsgAppendResponse
func (r *Raft) onReceiveMsgPropose() {}

func (r *Raft) onReceiveMsgAppend(m pb.Message) {
    r.becomeFollower(m.Term, m.From)
}

func (r *Raft) onReceiveMsgAppendResponse(m pb.Message) {}

func (r *Raft) handleAppendEntries(m pb.Message) {}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
