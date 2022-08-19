package raft

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastSnapshotIndex || index > rf.lastApplied {
		return
	}

	// 删除index以及index之前的日志
	newLog := make([]Entry, 0, rf.getLastLogEntryIndexLocked()-index)
	copy(newLog, rf.getLogEntriesLocked(index+1, -1))

	rf.lastSnapshotTerm = rf.getLogEntryByIndexLocked(index).Term
	rf.lastSnapshotIndex = index
	rf.log = newLog

	rf.persistStateAndSnapshot(snapshot)
}

func (rf *Raft) getLogEntryByIndexLocked(index int) Entry {
	return rf.log[index-rf.lastSnapshotIndex]
}

func (rf *Raft) getLastLogEntryIndexLocked() int {
	return rf.lastSnapshotIndex + len(rf.log)
}

func (rf *Raft) getLogEntriesLocked(from int, to int) []Entry {
	if to > -1 {
		return rf.log[from-rf.lastSnapshotIndex : to-rf.lastSnapshotIndex]
	} else {
		return rf.log[from-rf.lastSnapshotIndex:]
	}
}
