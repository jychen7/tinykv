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
    // "fmt"
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

    hard_state, config_state, _ := c.Storage.InitialState()
    raftLog := newLog(c.Storage)
    progresses := make(map[uint64]*Progress)
    peers := c.peers
    if len(config_state.Nodes) > 0 {
        peers = config_state.Nodes
    }
	for _, p := range peers {
		progresses[p] = &Progress{Match: 0, Next: raftLog.LastIndex() + 1}
    }

    term := uint64(0)
    vote := NoLeader
    if !IsEmptyHardState(hard_state) {
        raftLog.committed = hard_state.Commit
        term = hard_state.Term
        vote = hard_state.Vote
    }
	return &Raft{
		id: c.ID,
		Term: term,
		Vote: vote,
		RaftLog: raftLog,
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
    // fmt.Println(m.From, "->", m.To, m)
}

func (r *Raft) sendCommit(peer_id uint64, msg_type pb.MessageType) {
    commit_index := r.RaftLog.committed
    msg := pb.Message{
        MsgType: msg_type,
        Term: r.Term,
        // LogTerm: commit_term,
        // Index: commit_index,
        Commit: min(commit_index, r.Prs[peer_id].Match),
        Entries: []*pb.Entry{},
    }
    if msg_type == pb.MessageType_MsgAppend {
        commit_term, _ := r.RaftLog.Term(r.RaftLog.committed)
        msg = pb.Message{
            MsgType: msg_type,
            Term: r.Term,
            LogTerm: commit_term,
            Index: commit_index,
            Commit: commit_index,
            Entries: []*pb.Entry{},
        }
    }
    r.sendTo(peer_id, msg)
}

func (r *Raft) sendAppend(peer_id uint64) {
    next_index := r.peerNext(peer_id)
    _, err := r.RaftLog.Term(next_index)
    if err != nil {
        // fmt.Println(r.id, "->", peer_id, next_index, err)
        return
    }
    prev_log_index := next_index - 1
    prev_log_term, err := r.RaftLog.Term(prev_log_index)
    if err != nil {
        // fmt.Println(r.id, "->", peer_id, prev_log_index, err)
        return
    }

    last := r.RaftLog.LastIndex()
    pointers := make([]*pb.Entry, 0, last + 1 - next_index)
    // [low, high)
    entries := r.RaftLog.Entries(next_index, last + 1)
    for i, _ := range entries {
        pointers = append(pointers, &entries[i])
    }
    msg := pb.Message{
        MsgType: pb.MessageType_MsgAppend,
        Term: r.Term,
        LogTerm: prev_log_term,
        Index: prev_log_index,
        Commit: r.RaftLog.committed,
        Entries: pointers,
    }
    r.sendTo(peer_id, msg)
    r.increaseNextTo(peer_id, last + 1)
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
    // r.Vote = lead
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
    if r.State == StateLeader {
        return
    }

    r.State = StateLeader
    r.Vote = r.id
    r.Lead = NoLeader
    r.heartbeatElapsed = 0
    
    // noop entry
    // paper 5.4.2, commit entries from previous terms
    r.onReceiveMsgPropose(pb.Message{
        From: r.id,
        To: r.id,
        MsgType: pb.MessageType_MsgPropose,
        Entries: []*pb.Entry{
            {Data: nil},
        },
    })
}

func (r *Raft) appendEntriesAsLeader(entries []pb.Entry) {
    r.RaftLog.appendEntries(entries)
    r.increaseNextTo(r.id, r.RaftLog.LastIndex() + 1)
    r.onReceiveMsgAppendResponse(pb.Message{
        From: r.id,
        To: r.id,
        MsgType: pb.MessageType_MsgAppendResponse,
        Term:    r.Term,
        // LogTerm: r.RaftLog.LastTerm(),
        Index:   r.RaftLog.LastIndex(),
        Reject:  false,
    })
}

func (r *Raft) appendEntriesAsFollower(entries []pb.Entry) {
    r.RaftLog.appendEntries(entries)
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
        r.onReceiveMsgPropose(m)
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
                    To: r.id,
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
            LogTerm: r.RaftLog.LastTerm(), // lastLogTerm
            Index: r.RaftLog.LastIndex(),  // lastLogIndex
		})
	}
}

func (r *Raft) onReceiveMsgRequestVote(m pb.Message) {
	if m.Term < r.Term {
		r.voteWith(false, m)
		return
	}

    if m.Term > r.Term {
        r.Vote = NoLeader
		r.becomeFollower(m.Term, NoLeader)
	}

    will_vote := (r.Vote == m.From) || (r.Vote == NoLeader && r.Lead == NoLeader)
    will_follow := r.RaftLog.behind(m.LogTerm, m.Index)
    if will_vote && will_follow {
        r.voteWith(true, m)
        r.becomeFollower(m.Term, NoLeader)
        return
    }
    r.voteWith(false, m)
}

func (r *Raft) voteWith(accept bool, m pb.Message) {
    if accept {
        r.Vote = m.From
    }
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

    r.broadcastCommit(pb.MessageType_MsgHeartbeat)
    // for peer_id := range r.Prs {
	// 	if peer_id == r.id {
	// 		continue
	// 	}
	// 	r.sendTo(peer_id, pb.Message{
	// 		MsgType: pb.MessageType_MsgHeartbeat,
    //         Term: r.Term,
    //         Commit: r.RaftLog.committed,
	// 	})
	// }
}

func (r * Raft) onReceiveMsgHeartbeat(m pb.Message) {
	// r.becomeFollower(m.Term, m.From)
	// reply
	r.handleHeartbeat(m)
}

// keep since test case requires it
func (r *Raft) handleHeartbeat(m pb.Message) {
    r.onReceiveMsgAppend(m)
    // if r.RaftLog.committed < m.Commit {
    //     r.RaftLog.committed = m.Commit
    // }
    // r.sendTo(m.From, pb.Message{
    //     MsgType: pb.MessageType_MsgHeartbeatResponse,
    //     LogTerm: r.RaftLog.LastTerm(),
    //     Index: r.RaftLog.LastIndex(),
    // })
}

func (r *Raft) onReceiveMsgHeartbeatResponse(m pb.Message) {
    // r.onReceiveMsgAppendResponse(m)
    if r.State != StateLeader {
        return
    }
    if m.Reject {
        return
    }
    peer_id := m.From
    r.increaseNextTo(peer_id, m.Index + 1)
    if r.peerNext(peer_id) <= r.RaftLog.LastIndex() {
        r.sendAppend(peer_id)
    }
}

// Leader -> Leader: MsgPropose
// Leader -> Follower: MsgAppend
// Follower -> Leader: MsgAppendResponse
func (r *Raft) onReceiveMsgPropose(m pb.Message) {
    if r.State != StateLeader {
        return
    }    
    if len(m.Entries) == 0 {
        return
    }

    next_index := r.peerNext(r.id)
    entries := make([]pb.Entry, 0, len(m.Entries))
    for i, pointer := range m.Entries {
        entry := *pointer
        entry.Term = r.Term
        entry.Index = next_index + uint64(i) // i = 0,1,2,...
        entries = append(entries, entry)
    }
    r.appendEntriesAsLeader(entries)
    r.broadcastAppend()
}

func (r *Raft) broadcastCommit(msg_type pb.MessageType) {
    for peer_id := range r.Prs {
        if peer_id == r.id { continue }
        r.sendCommit(peer_id, msg_type)
    }
}

func (r *Raft) broadcastAppend() {
    for peer_id := range r.Prs {
		if peer_id == r.id {
			r.onReceiveMsgAppendResponse(pb.Message{
                From: r.id,
                To: r.id,
                MsgType: pb.MessageType_MsgAppendResponse,
                Term:    r.Term,
                // LogTerm: r.RaftLog.LastTerm(),
                Index:   r.RaftLog.LastIndex(),
                Reject:  false,
            })
            continue
        }
        r.sendAppend(peer_id)
	}    
}

// for raft_test.go
func (r *Raft) handleAppendEntries(m pb.Message) {
    r.onReceiveMsgAppend(m)
}

func (r *Raft) onReceiveMsgAppend(m pb.Message) {
    msg_type := pb.MessageType_MsgAppendResponse
    if m.MsgType == pb.MessageType_MsgHeartbeat {
        msg_type = pb.MessageType_MsgHeartbeatResponse
    }
    if r.State != StateFollower && m.Term < r.Term {
        r.sendTo(m.From, pb.Message{
            MsgType: msg_type,
            Term:    r.Term,
            Reject:  true,
        })
        return
    }
    r.becomeFollower(m.Term, m.From)
    match := r.RaftLog.match(m.LogTerm, m.Index)
    if !match {
        r.sendTo(m.From, pb.Message{
            MsgType: msg_type,
            Term:    r.Term,
            Reject:  true,
        })
        return
    }

    entries := make([]pb.Entry, 0, len(m.Entries))
    for _, pointer := range(m.Entries) {
        entries = append(entries, *pointer)
    }

    // because of newtork delay or timing, part of entries may already store in follower
    // detect possible conflict, then append "new" entries
    conflict_index, append_entries := r.RaftLog.detectConflict(entries)
    if conflict_index > 0 {
        r.RaftLog.deleteSince(conflict_index)
    }
    r.appendEntriesAsFollower(append_entries)
    if r.RaftLog.committed < m.Commit {
        if m.MsgType == pb.MessageType_MsgHeartbeat {
            r.RaftLog.committed = m.Commit
        }else{
            r.RaftLog.committed = min(m.Commit, m.Index + uint64(len(entries)))
        }
    }
    r.sendTo(m.From, pb.Message{
        MsgType: msg_type,
        Term:    r.Term,
        Reject:  false,
        Index:   r.RaftLog.LastIndex(),
    })
}

func (r *Raft) onReceiveMsgAppendResponse(m pb.Message) {
    if r.State != StateLeader {
        return
    }

    if m.Reject {
        r.decreaseNextBy(m.From, 1)
        r.sendAppend(m.From)
        return
    }
    r.increaseMatch(m.From, m.Index)
    if r.canCommitTo(m.Index) {
        r.RaftLog.committed = m.Index
        r.broadcastCommit(pb.MessageType_MsgAppend)
    }
}

func (r *Raft) canCommitTo(index uint64) bool {
    if index <= r.RaftLog.committed {
        return false
    }

    term, _ := r.RaftLog.Term(index)
    if term != r.Term {
        return false
    }

    num := 0
	for _, pr := range r.Prs {
		if index <= pr.Match {
			num += 1
		}
	}
	return num >= r.majority()
}

func (r *Raft) increaseMatch(peer_id uint64, index uint64) {
    r.Prs[peer_id].Match = index
}

func (r *Raft) increaseNextTo(peer_id uint64, n uint64) {
    r.Prs[peer_id].Next = n
}

func (r *Raft) decreaseNextBy(peer_id uint64, n uint64) {
    r.Prs[peer_id].Next -= n
}

func (r *Raft) peerNext(peer_id uint64) (n uint64) {
    if peer_id == r.id {
        return r.RaftLog.LastIndex() + 1
    }
    return max(r.Prs[peer_id].Next, 1)
}

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
