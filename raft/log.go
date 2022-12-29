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

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	fi, _ := storage.FirstIndex()
	li, _ := storage.LastIndex()
	l := RaftLog{
		storage:   storage,
		committed: fi - 1,
		applied:   fi - 1,
		stabled:   li,
	}
	ents, _ := storage.Entries(fi, li+1)
	l.entries = append(l.entries, ents...)
	return &l
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

func (l *RaftLog) appendEntries(ents ...pb.Entry) {
	li := l.LastIndex()
	for i, ent := range ents {
		if ent.Index <= li {
			if !l.isMatch(ent.Index, ent.Term) {
				fi, _ := l.storage.FirstIndex()
				l.stabled = ent.Index - 1
				l.entries = l.entries[:ent.Index-fi]
				l.entries = append(l.entries, ents[i:]...)
				break
			}
		} else {
			l.entries = append(l.entries, ents[i:]...)
			break
		}
	}

	// l.stable(l.LastIndex())
}

func (l *RaftLog) isMatch(i, t uint64) bool {
	tt, _ := l.Term(i)
	return tt == t
}

func (l *RaftLog) commit(committed uint64) bool {
	if committed <= l.LastIndex() {
		if committed > l.committed {
			l.committed = committed
			return true
		}
		return false
	}

	return false
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	return l.entries
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	length := uint64(len(l.entries))
	if length == 0 {
		return []pb.Entry{}
	}
	fi := l.entries[0].Index
	offset := l.stabled - fi + 1
	if offset >= length {
		return []pb.Entry{}
	}
	return l.entries[offset:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	fi, _ := l.storage.FirstIndex()
	lo := l.applied - fi + 1
	hi := l.committed - fi + 1
	return l.entries[lo:hi]
}

func (l *RaftLog) Entries(i, size uint64) []pb.Entry {
	if len(l.entries) == 0 {
		return nil
	}
	if size == 0 {
		return []pb.Entry{}
	}
	offset := i - l.entries[0].Index
	return l.entries[offset : offset+size]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	if length := len(l.entries); length != 0 {
		return l.entries[0].Index + uint64(length) - 1
	}
	li, _ := l.storage.LastIndex()
	return li
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	length := uint64(len(l.entries))
	if length == 0 {
		return l.storage.Term(i)
	}
	if i >= l.entries[0].Index {
		offset := i - l.entries[0].Index
		if offset < length {
			return l.entries[offset].Term, nil
		}
	}
	return l.storage.Term(i)
}
