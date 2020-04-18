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
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
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

    // storage have dummy entry, but raft log entries doesn't
    entries []pb.Entry // all entries that have not yet compact.
    snapIndex uint64   // Normally snapshot index. When newLog, it is refer to the dummy entry index from Storage
    snapTerm uint64

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
    first_index, _ := storage.FirstIndex()
    last_index, _ := storage.LastIndex()
    snap_index := first_index - 1
    snap_term, _ := storage.Term(snap_index)
    // [first_index, last_index) - last_index is exclusive, so here "+1"
    entries, _ := storage.Entries(first_index, last_index+1)
	return &RaftLog{
        storage: storage,
        entries: entries,
        snapIndex: snap_index,
        snapTerm: snap_term,
        stabled: last_index,
        committed: snap_index,
        applied: snap_index,
    }
}

func (l *RaftLog) appendEntries(new_entries []pb.Entry) {
    if len(new_entries) == 0 {
        return
    }
    l.entries = append(l.entries, new_entries...)
}

func (l *RaftLog) deleteSince(index uint64) {
    length := index - l.FirstIndex()
    entries := make([]pb.Entry, 0, length)
    for _, entry := range l.Entries(l.FirstIndex(), index) {
        entries = append(entries, entry)
    }
    l.entries = entries

    // "index" is truncated already, so latest stabled can be at most index - 1
	l.stabled = min(l.stabled, index - 1)
}

func (l *RaftLog) detectConflict(entries []pb.Entry) (conflict_index uint64, append_entries []pb.Entry) {
    if len(l.entries) == 0 {
        return 0, entries
    }
    last_index := l.LastIndex()
    for i, entry := range(entries) {
        if entry.Index > last_index {
            return 0, entries[i:]
        }

        t, _ := l.Term(entry.Index)
        if t != entry.Term {
            return entry.Index, entries[i:]
        }
    }

    return 0, []pb.Entry{}
}

func (l *RaftLog) Entries(low uint64, high uint64) []pb.Entry {
    // [low, high): include low, exclude high
    offset := l.FirstIndex()
    return l.entries[low - offset: high - offset]
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
    if len(l.entries) == 0 {
        return nil
    }
    unstable_index := l.stabled + 1 - l.FirstIndex()
	return l.entries[unstable_index:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	if len(l.entries) == 0 {
        return nil
    }
    not_applied_index := l.applied - l.FirstIndex() + 1
    committed_index := l.committed - l.FirstIndex()
    // [low, high)
	return l.entries[not_applied_index : committed_index+1]
}

func (l *RaftLog) FirstIndex() uint64 {
    if len(l.entries) == 0 {
        return l.snapIndex
    }
    return l.entries[0].Index
}

// LastIndex return the last index of the lon entries
func (l *RaftLog) LastIndex() uint64 {
    if len(l.entries) == 0 {
        return l.snapIndex
    }
    return l.entries[0].Index + uint64(len(l.entries)) - 1
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
    if i == l.snapIndex {
		return l.snapTerm, nil
	}
    _index := i - l.FirstIndex()
    if _index < 0 {
		return 0, ErrCompacted
	}
	if _index >= uint64(len(l.entries)) {
		return 0, ErrUnavailable
	}
    return l.entries[_index].Term, nil
}

func (l *RaftLog) LastTerm() uint64 {
    if len(l.entries) == 0 {
        return 0
    }
    term, _ := l.Term(l.LastIndex())
    return term
}

func (l *RaftLog) behind(term uint64, index uint64) bool {
    last_term := l.LastTerm()
    if last_term != term {
        return last_term < term
    }

    last_index := l.LastIndex()
    return last_index <= index
}

func (l *RaftLog) match(term uint64, index uint64) bool {
    my_term, _ := l.Term(index)
    return my_term == term
}
