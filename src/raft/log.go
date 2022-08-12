package raft

import (
	"fmt"
	"time"

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
				args := new(AppendEntriesArgs)
				var lastHeartbeatTime time.Time
				for rf.killed() == false {
					rf.mu.Lock()

					for rf.role != Leader || (time.Now().Sub(lastHeartbeatTime) < heartbeatTime() && rf.matchIndex[server] >= len(rf.log)-1) {
						// 只有当节点为leader并且还存在未复制到目标节点的日志时才进行同步否则等待
						rf.broadcastCond.Wait()
					}

					args.Term = rf.currentTerm
					args.LeaderId = rf.me
					args.PrevLogIndex = rf.nextIndex[server] - 1
					args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
					args.Entries = rf.log[args.PrevLogIndex+1:]
					args.LeaderCommit = rf.commitIndex

					rf.mu.Unlock()

					reply := new(AppendEntriesReply)
					if rf.sendAppendEntries(server, args, reply) {
						rf.mu.Lock()

						DPrintf("%s sendAppendEntries to [%d] reply:{term: %d, success: %v}", formatRaft(rf), server, reply.Term, reply.Success)

						if rf.currentTerm == args.Term {
							if reply.Term > rf.currentTerm {
								// 有一个新的任期
								rf.currentTerm, rf.votedFor = reply.Term, -1
								rf.persist()
								rf.changeRoleLocked(Follower)
							} else {
								if reply.Success {
									// 同步成功
									rf.nextIndex[server] += len(args.Entries)
									rf.matchIndex[server] = rf.nextIndex[server] - 1
									// 计算新的commitIndex，即所有matchIndex的中位数
									newCommitIndex := findk.Ints(rf.matchIndex, len(rf.matchIndex)/2+1)
									if rf.commitIndex < newCommitIndex && rf.log[newCommitIndex].Term == rf.currentTerm {
										// 只能提交当前任期内的日志
										DPrintf("%s commitIndex: %d => %d", formatRaft(rf), rf.commitIndex, newCommitIndex)
										rf.commitIndex = newCommitIndex
										// 通知applier可以apply
										rf.applyCond.Broadcast()
									}
								} else {
									// 日志不匹配，回退nextIndex
									rf.nextIndex[server]--
									if rf.nextIndex[server] < 1 {
										panic(fmt.Sprintf("rf.nextIndex[%d] < 1", server))
									}
								}
							}
						}

						rf.mu.Unlock()
					} else {
						DPrintf("%s sendAppendEntries to [%d] failed", formatRaft(rf), server)
					}

					lastHeartbeatTime = time.Now()
				}
			}(peer)
		}
	}

	for {
		// 每隔一定时间广播一次心跳
		rf.broadcastCond.Broadcast()
		time.Sleep(heartbeatTime())
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
