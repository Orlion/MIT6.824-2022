package raft

import (
	"fmt"

	"6.824/pkg/findk"
)

type Entry struct {
	Command interface{}
	Term    int
}

func (rf *Raft) boardcastLog() {
	for peer := range rf.peers {
		go func(server int) {
			args := new(AppendEntriesArgs)
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.LeaderCommit = rf.commitIndex
			args.PrevLogIndex = rf.nextIndex[server] - 1
			if args.PrevLogIndex > -1 {
				args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
			}
			args.Entries = rf.log[args.PrevLogIndex+1:]

			reply := new(AppendEntriesReply)
			if rf.sendAppendEntries(server, args, reply) {
				if reply.Success {
					// 同步成功
					rf.nextIndex[server] += len(args.Entries)
					rf.matchIndex[server] = rf.nextIndex[server] - 1
					// 计算新的commitIndex，即所有matchIndex的中位数
					newCommitIndex := findk.Ints(rf.matchIndex, len(rf.matchIndex)/2)
					if rf.commitIndex < newCommitIndex && rf.log[newCommitIndex].Term == rf.currentTerm {
						// 只能提交当前任期内的日志
						rf.commitIndex = newCommitIndex
						// todo: 应用到状态机
					}
				} else {
					// 同步失败
					if reply.Term > rf.currentTerm {
						// 有一个新的任期
						rf.currentTerm = reply.Term
						rf.votedFor = -1
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
			}
		}(peer)
	}
}
