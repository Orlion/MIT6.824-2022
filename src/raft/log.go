package raft

import (
	"fmt"

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
				for rf.killed() == false {
					rf.mu.Lock()

					for rf.role != Leader || rf.matchIndex[server] >= len(rf.log)-1 {
						// 只有当节点为leader并且还存在未复制到目标节点的日志时才进行同步否则等待
						rf.broadcastCond.Wait()
					}

					args.Term = rf.currentTerm
					args.LeaderId = rf.me
					args.PrevLogIndex = rf.nextIndex[server] - 1
					args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
					args.Entries = rf.log[args.PrevLogIndex+1:]
					args.LeaderCommit = rf.commitIndex

					reply := new(AppendEntriesReply)
					if rf.sendAppendEntries(server, args, reply) {
						DPrintf("server:[%d] sendAppendEntries to [%d]", rf.me, server)
						if reply.Success {
							// 同步成功
							rf.nextIndex[server] += len(args.Entries)
							rf.matchIndex[server] = rf.nextIndex[server] - 1
							// 计算新的commitIndex，即所有matchIndex的中位数
							newCommitIndex := findk.Ints(rf.matchIndex, len(rf.matchIndex)/2)
							if rf.commitIndex < newCommitIndex && rf.log[newCommitIndex].Term == rf.currentTerm {
								// 只能提交当前任期内的日志
								DPrintf("server:[%d] commitIndex: %d => %d", rf.me, rf.commitIndex, newCommitIndex)
								rf.commitIndex = newCommitIndex
								// 通知applier可以apply
								rf.applyCond.Broadcast()
							}
						} else {
							// 同步失败
							if reply.Term > rf.currentTerm {
								// 有一个新的任期
								rf.currentTerm, rf.votedFor = reply.Term, -1
								rf.persist()
								rf.changeRoleLocked(Follower)
							} else {
								// 日志不匹配，回退nextIndex
								rf.nextIndex[server]--
								if rf.nextIndex[server] < 1 {
									panic(fmt.Sprintf("rf.nextIndex[%d] < 1", server))
								}
							}
						}
					} else {
						DPrintf("server:[%d] sendAppendEntries to [%d] failed", rf.me, server)
					}

					rf.mu.Unlock()
				}
			}(peer)
		}
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
		}

		rf.mu.Unlock()

		for _, msg := range applyMsgs {
			applyCh <- msg
			DPrintf("server:[%d] applyMsg's CommendIndex %d", rf.me, msg.CommandIndex)
		}
	}
}
