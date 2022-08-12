package raft

import (
	"fmt"
	"time"
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // 候选人的任期号
	CandidateId  int // 候选人的id
	LastLogIndex int // 候选人的最后一条日志的索引
	LastLogTerm  int // 候选人的最后一条日志的任期号
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 当前任期号，让候选人去更新自己的任期号
	VoteGranted bool // 候选人赢得了此张选票时为true
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	var opposeReason string
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
		DPrintf("%s RequestVote from [%d] args: %v reply: %v, opposeReason:[%s]", formatRaft(rf), args.CandidateId, args, reply, opposeReason)
	}()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		// 如果候选人的任期小于当前任期则拒绝
		opposeReason = fmt.Sprintf("args.Term:[%d] < rf.currentTerm:[%d]", args.Term, rf.currentTerm)
		return
	}

	if args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		// 一个任期只能投出一张票
		// 如果当前任期已经投给了其他人则拒绝
		opposeReason = fmt.Sprintf("当前任期已投给:[%d]", rf.votedFor)
		return
	}

	// 流程走到这里说明当前任期还没有投票或者有新一轮的选举

	if args.Term > rf.currentTerm {
		// 如果候选人的任期大于当前任期，则转为follower
		rf.changeRoleLocked(Follower)
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.persist()
	}

	// 候选人必须拥有所有已提交的日志，所以候选人的日志必须比自己的新
	// 对于日志没有自己新的候选人会拒绝
	// 新：任期较大的比较新，任期相同的话日志较长的较新
	if !rf.isLogUpToDateLocked(args.LastLogTerm, args.LastLogIndex) {
		opposeReason = fmt.Sprintf("日志没有自己新")
		return
	}

	reply.Term, reply.VoteGranted = rf.currentTerm, true
	rf.votedFor = args.CandidateId
	rf.persist()
	rf.electionTimer.Reset(randElectronTimeout())

	return
}

// 判断候选人的日志是否比当前节点的日志新
// 任期较大的比较新，任期相同的话日志较长的较新
func (rf *Raft) isLogUpToDateLocked(term, index int) bool {
	lastLogTerm := rf.getLastLogTermLocked()
	return term >= lastLogTerm || (term == lastLogTerm && index >= rf.getLastLogIndexLocked())
}

// 获取最后一条日志的term
func (rf *Raft) getLastLogTermLocked() int {
	if len(rf.log) > 0 {
		return rf.log[len(rf.log)-1].Term
	} else {
		return 0
	}
}

// 获取最后一条日志的index
func (rf *Raft) getLastLogIndexLocked() int {
	return len(rf.log) - 1
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// 开始选举
func (rf *Raft) startElectionLocked() {
	// 角色转为候选人
	rf.changeRoleLocked(Candidate)
	// 首先增加自己的任期号
	rf.currentTerm, rf.votedFor = rf.currentTerm+1, rf.me
	// 持久化
	rf.persist()

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndexLocked(),
		LastLogTerm:  rf.getLastLogTermLocked(),
	}

	// 统计获取的赞成票票数，默认给自己投一票
	grantedNum := 1
	// 遍历所有节点，发送投票请求
	for peer := range rf.peers {
		if peer != rf.me {
			go func(server int) {
				reply := new(RequestVoteReply)
				if rf.sendRequestVote(server, args, reply) {
					DPrintf("%s sendRequestVote to [%d] args: %v, reply: %v", formatRaft(rf), server, args, reply)
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.currentTerm != args.Term || rf.role != Candidate {
						// 角色已不再是候选人则忽略投票结果
						return
					}

					if reply.VoteGranted {
						// 收到了赞成票，赞成票数加一
						grantedNum++
						if grantedNum*2 > len(rf.peers) {
							// 票数过半，成为leader
							rf.changeRoleLocked(Leader)
							DPrintf("%s 成为leader", formatRaft(rf))
						}
					} else if rf.currentTerm < reply.Term {
						// 收到的回复中有更新的任期则转变角色为follower，并等待leader的心跳
						rf.changeRoleLocked(Follower)
						rf.currentTerm, rf.votedFor = reply.Term, -1
						rf.persist()
					}
				} else {
					DPrintf("%s sendRequestVote to [%d] args: %v failed", formatRaft(rf), server, args)
				}
			}(peer)
		}
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		<-rf.electionTimer.C
		// 心跳已超时，尝试发起选举
		rf.mu.Lock()
		DPrintf("%s 心跳超时", formatRaft(rf))
		if rf.role != Leader {
			DPrintf("%s 心跳超时，尝试发起选举", formatRaft(rf))
			// 没有收到心跳，开启新一轮选举
			rf.startElectionLocked()
		}
		rf.mu.Unlock()

		rf.electionTimer.Reset(randElectronTimeout())
	}
}

// 向其他server发送心跳消息建立自己的权威，并阻止新选举
func (rf *Raft) broadcastHeartbeat() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.role == Leader {
			DPrintf("%s 开始心跳广播", formatRaft(rf))
			for peer := range rf.peers {
				if peer != rf.me {
					go func(server int) {
						args := new(AppendEntriesArgs)
						args.Term = rf.currentTerm
						args.LeaderId = rf.me
						args.PrevLogIndex = rf.nextIndex[server] - 1
						args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
						if args.PrevLogIndex < len(rf.log)-1 {
							args.Entries = rf.log[args.PrevLogIndex+1:]
						}
						args.LeaderCommit = rf.commitIndex

						reply := new(AppendEntriesReply)
						if rf.sendAppendEntries(server, args, reply) {
							DPrintf("%s sendHeartbeat to [%d] args: %v, reply: %v", formatRaft(rf), server, args, reply)
							rf.mu.Lock()
							if rf.currentTerm < reply.Term {
								rf.currentTerm, rf.votedFor = reply.Term, -1
								rf.persist()
								rf.changeRoleLocked(Follower)
							}
							rf.mu.Unlock()
						} else {
							DPrintf("%s sendHeartbeat to [%d] failed", formatRaft(rf), server)
						}
					}(peer)
				}
			}
		}

		rf.mu.Unlock()
		time.Sleep(heartbeatTime())
	}
}
