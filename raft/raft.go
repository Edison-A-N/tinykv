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
	"errors"
	"math/rand"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

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

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
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
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	randomElectionTimeout int

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
	r := Raft{
		id:               c.ID,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		State:            StateFollower,

		RaftLog: newLog(c.Storage),

		Prs:   make(map[uint64]*Progress),
		votes: make(map[uint64]bool),
		msgs:  make([]pb.Message, 0),
	}

	storage := c.Storage
	state, _, _ := storage.InitialState()

	for _, i := range c.peers {
		r.Prs[i] = &Progress{
			Next:  r.RaftLog.LastIndex() + 1,
			Match: r.RaftLog.LastIndex(),
		}
	}

	r.resetRandomElectionTimeout()

	r.becomeFollower(state.GetTerm(), state.GetVote())
	return &r
}

func (r *Raft) resetRandomElectionTimeout() {
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

func (r *Raft) checkQuorum() bool {
	votedCount := 0
	for _, voted := range r.votes {
		if voted {
			votedCount++
		}
	}
	return votedCount >= len(r.Prs)/2+1
}

func (r *Raft) proposeEntries(ents ...*pb.Entry) {
	entries := []pb.Entry{}
	for i := range ents {
		ents[i].Term = r.Term
		ents[i].Index = r.RaftLog.LastIndex() + 1 + uint64(i)
		entries = append(entries, *ents[i])
	}
	r.RaftLog.appendEntries(entries...)
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	logIndex := r.Prs[to].Next - 1
	logTerm, err := r.RaftLog.Term(logIndex)
	if err != nil {
		return false
	}

	size := r.RaftLog.LastIndex() - logIndex
	var entries []*pb.Entry
	entries = make([]*pb.Entry, 0, size)
	ents := r.RaftLog.Entries(logIndex+1, size)
	for i := range ents {
		entries = append(entries, &ents[i])
	}

	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		LogTerm: logTerm,
		Index:   logIndex,
		Entries: entries,
		Commit:  r.RaftLog.committed,
	}

	r.msgs = append(r.msgs, msg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {

	if r.State == StateLeader {
		r.heartbeatElapsed++

		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			msg := pb.Message{
				MsgType: pb.MessageType_MsgBeat,
				From:    r.id,
				To:      r.id,
			}
			r.Step(msg)
		}
	} else {
		r.electionElapsed++
		if r.electionElapsed >= r.randomElectionTimeout {
			r.electionElapsed = 0
			r.resetRandomElectionTimeout()
			msg := pb.Message{
				MsgType: pb.MessageType_MsgHup,
				From:    r.id,
				To:      r.id,
			}
			r.Step(msg)
		}

	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	if r.Term > term {
		return
	}

	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.Vote = lead
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	r.State = StateCandidate
	r.Term++
	r.Vote = r.id
	r.votes[r.id] = true
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader

	r.RaftLog.appendEntries(pb.Entry{
		Term:  r.Term,
		Index: r.RaftLog.LastIndex() + 1,
		Data:  nil,
	})
	for i := range r.Prs {
		if i == r.id {
			continue
		}
		r.sendAppend(i)
	}
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1

}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	processHub := func() {
		r.becomeCandidate()
		if len(r.Prs) == 1 {
			r.becomeLeader()
		}
		logIndex := r.RaftLog.LastIndex()
		logTerm, _ := r.RaftLog.Term(logIndex)
		for i := range r.Prs {
			if i == r.id {
				continue
			}
			msg := pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
				From:    r.id,
				To:      i,
				Term:    r.Term,
				Index:   logIndex,
				LogTerm: logTerm,
			}
			r.msgs = append(r.msgs, msg)
		}
	}

	switch r.State {
	case StateFollower:
		if m.GetMsgType() == pb.MessageType_MsgHup && r.State != StateLeader {
			processHub()
		}

	case StateCandidate:
		if m.GetMsgType() == pb.MessageType_MsgHup && r.State != StateLeader {
			processHub()
		}

		if m.GetMsgType() == pb.MessageType_MsgRequestVoteResponse {
			r.votes[m.GetFrom()] = !m.GetReject()
		}

		if r.checkQuorum() {
			r.becomeLeader()
		}
	case StateLeader:
		if m.GetMsgType() == pb.MessageType_MsgBeat {
			for i := range r.Prs {
				if i == r.id {
					continue
				}
				r.sendHeartbeat(i)
			}
		}
		if m.GetMsgType() == pb.MessageType_MsgPropose {
			r.proposeEntries(m.Entries...)
			for i := range r.Prs {
				if i == r.id {
					continue
				}
				r.sendAppend(i)
			}
			if len(r.Prs) == 1 {
				r.RaftLog.commit(r.RaftLog.LastIndex())
			}
			r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
			r.Prs[r.id].Match = r.RaftLog.LastIndex()
		}
		if m.GetMsgType() == pb.MessageType_MsgAppendResponse {
			if !m.Reject {
				r.Prs[m.GetFrom()].Next = m.GetIndex() + 1
				r.Prs[m.GetFrom()].Match = m.GetIndex()
				commitedCount := 0
				nextIdx := m.GetIndex() + 1
				for i := range r.Prs {
					if r.Prs[i].Next >= nextIdx {
						commitedCount += 1
					}
				}
				if commitedCount >= len(r.Prs)/2+1 {
					preparedCommited := nextIdx - 1
					if t, _ := r.RaftLog.Term(preparedCommited); t == r.Term {
						if canCommit := r.RaftLog.commit(nextIdx - 1); canCommit {
							for i := range r.Prs {
								if i == r.id {
									continue
								}
								r.sendAppend(i)
							}
						}
					}
				}
			} else {
				if r.Prs[m.GetFrom()].Next > 1 {
					r.Prs[m.GetFrom()].Match = m.GetCommit()
					r.Prs[m.GetFrom()].Next--
					r.sendAppend(m.GetFrom())
				}
			}
		}
	}

	if m.GetMsgType() == pb.MessageType_MsgHeartbeat {
		r.handleHeartbeat(m)
	}
	if m.GetMsgType() == pb.MessageType_MsgAppend {
		r.handleAppendEntries(m)
	}

	if m.GetMsgType() == pb.MessageType_MsgRequestVote {
		reject := false

		logTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
		if logTerm > m.LogTerm || logTerm == m.LogTerm && r.RaftLog.LastIndex() > m.Index {
			reject = true
		}

		if m.GetTerm() < r.Term {
			reject = true
		} else if m.GetTerm() == r.Term && r.Vote != 0 && r.Vote != m.From {
			reject = true
		}

		if !reject {
			r.becomeFollower(m.GetTerm(), m.GetFrom())
			r.Lead = 0
		}

		msg := pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			From:    r.id,
			To:      m.GetFrom(),
			Term:    m.GetTerm(),
			Reject:  reject,
		}
		r.msgs = append(r.msgs, msg)
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	resp := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
		Term:    m.GetTerm(),
		Commit:  m.Commit,
	}

	if r.Term > m.GetTerm() {
		resp.Reject = true
	} else if r.Term == m.GetTerm() {
		if r.State == StateLeader {
			resp.Reject = true
		}
		if r.State == StateCandidate && m.GetIndex() <= r.RaftLog.LastIndex() {
			r.becomeFollower(m.GetTerm(), m.GetFrom())
		}
	} else if r.Term < m.GetTerm() {
		r.becomeFollower(m.GetTerm(), m.GetFrom())
	}

	if t, _ := r.RaftLog.Term(m.GetIndex()); t != m.GetLogTerm() {
		resp.Index = r.RaftLog.LastIndex()
		resp.Term, _ = r.RaftLog.Term(resp.Index)
		resp.Reject = true
	}

	if resp.Reject {
		r.msgs = append(r.msgs, resp)
		return
	}

	r.heartbeatElapsed = 0
	r.electionElapsed = 0

	ents := make([]pb.Entry, 0, len(m.Entries))
	for i := range m.Entries {
		ents = append(ents, *m.Entries[i])
	}
	r.RaftLog.appendEntries(ents...)

	if t, _ := r.RaftLog.Term(m.GetCommit()); t == r.Term {
		r.RaftLog.commit(m.GetCommit())
	}

	li := r.RaftLog.LastIndex()
	resp.Index = li
	resp.LogTerm, _ = r.RaftLog.Term(li)
	r.msgs = append(r.msgs, resp)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
	}
	if m.GetTerm() < r.Term {
		msg.Reject = true
		r.msgs = append(r.msgs, msg)
		return
	}

	if m.GetTerm() >= r.Term {
		r.becomeFollower(m.GetTerm(), m.GetFrom())
	}

	r.heartbeatElapsed = 0
	r.electionElapsed = 0

	r.msgs = append(r.msgs, msg)
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
