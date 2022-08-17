package raft

import (
	"6.824/pkg/findk"
)

type Entry struct {
	Command interface{}
	Term    int
}

// 广播日志
func (rf *Raft) broadcastLog() {
	// 为每个peer创建一个协程
	for peer := range rf.peers {
		if peer != rf.me {
			go func(server int) {
				for rf.killed() == false {
					rf.mu.Lock()

					for rf.role != Leader || rf.matchIndex[server] >= len(rf.log)-1 {
						rf.broadcastCond.Wait()
					}

					rf.sendLogLocked(server, false)

					rf.mu.Unlock()
				}
			}(peer)
		}
	}
}

func (rf *Raft) sendLogLocked(server int, isHeartbeat bool) {
	// todo: args可以用sync.Pool优化内存使用，或者从调用方传过来，调用方复用args
	args := new(AppendEntriesArgs)
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.PrevLogIndex = rf.nextIndex[server] - 1
	args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
	args.Entries = rf.log[args.PrevLogIndex+1:]
	args.LeaderCommit = rf.commitIndex

	rf.mu.Unlock()

	action := "sendAppendEntries"
	if isHeartbeat {
		action = "sendHeartbeat"
	}

	reply := new(AppendEntriesReply)
	if rf.sendAppendEntries(server, args, reply) {
		rf.mu.Lock()

		DPrintf("%s %s to [%d] reply:{term: %d, success: %v}", formatRaft(rf), action, server, reply.Term, reply.Success)
		if rf.currentTerm == args.Term {
			if reply.Term > rf.currentTerm {
				// 有一个新的任期
				rf.currentTerm, rf.votedFor = reply.Term, -1
				rf.persist()
				rf.changeRoleLocked(Follower)
			} else {
				if reply.Success {
					// 同步成功
					// 注意可能会加多次所以使用args.PrevLogIndex来计算
					newMatchIndex := args.PrevLogIndex + len(args.Entries)
					if newMatchIndex > rf.matchIndex[server] {
						rf.matchIndex[server] = newMatchIndex
						rf.nextIndex[server] = rf.matchIndex[server] + 1
						// 计算新的commitIndex，即所有matchIndex的中位数
						newCommitIndex := findk.Ints(rf.matchIndex, len(rf.matchIndex)/2+1)
						if rf.commitIndex < newCommitIndex && rf.log[newCommitIndex].Term == rf.currentTerm {
							// 只能提交当前任期内的日志
							DPrintf("%s commitIndex: %d => %d", formatRaft(rf), rf.commitIndex, newCommitIndex)
							rf.commitIndex = newCommitIndex
							// 通知applier可以apply
							rf.applyCond.Broadcast()
						}
					}
				} else {
					// 日志不匹配，回退nextIndex
					// nextIndex回退优化 https://thesquareplanet.com/blog/students-guide-to-raft/#an-aside-on-optimizations
					if reply.ConflictTerm > 0 {
						// 找到冲突任期最后一条日志
						conflictTermLastIndex := 0
						for i := args.PrevLogIndex; i > 0; i-- {
							if rf.log[i].Term == reply.ConflictTerm {
								conflictTermLastIndex = i
								break
							}
						}
						// 将下一条日志索引设为找到的
						if conflictTermLastIndex > 0 {
							rf.nextIndex[server] = conflictTermLastIndex + 1
						} else {
							rf.nextIndex[server] = reply.ConflictIndex
						}
					}
					rf.nextIndex[server] = reply.ConflictIndex
				}
			}
		}
	} else {
		rf.mu.Lock()
		DPrintf("%s %s to [%d] failed", formatRaft(rf), action, server)
	}
}

func (rf *Raft) applier(applyCh chan ApplyMsg) {
	for rf.killed() == false {
		rf.mu.Lock()

		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}

		// 收集待apply的日志
		applyMsgs := make([]ApplyMsg, 0, rf.commitIndex-rf.lastApplied)
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			applyMsgs = append(applyMsgs, ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			})
			DPrintf("%s applyMsg's CommandIndex:%d Command:%v", formatRaft(rf), rf.lastApplied, rf.log[rf.lastApplied].Command)
		}

		rf.mu.Unlock()

		for _, msg := range applyMsgs {
			applyCh <- msg
		}
	}
}
