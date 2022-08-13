package raft

import (
	"fmt"
	"log"
	"time"

	"6.824/pkg/randx"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func formatRaft(rf *Raft) string {
	return fmt.Sprintf("{server:[%d] - role:[%d],term:[%d],vote:[%d],commit:[%d],apply:[%d],log:[%d]}", rf.me, rf.role, rf.currentTerm, rf.votedFor, rf.commitIndex, rf.lastApplied, len(rf.log))
}

func randElectronTimeout() time.Duration {
	return time.Duration(randx.Intn(200, 350)) * time.Millisecond
}

func heartbeatTime() time.Duration {
	return 100 * time.Millisecond
}
