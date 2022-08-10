package raft

type AppendEntriesArgs struct {
	Term         int     // leader的任期
	LeaderId     int     // leader的id
	PrevLogIndex int     // 上一条日志的索引
	PrevLogTerm  int     // 上一条日志的任期
	Entries      []Entry // 日志
	LeaderCommit int     // 领导人已知的已提交的最高的日志索引
}

type AppendEntriesReply struct {
	Term    int  // 当前任期
	Success bool // 如果follower的prevLogTerm和prevLogIndex匹配上了则返回true
}

//
// AppendEntries handler
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
		DPrintf("server:[%d] AppendEntries from [%d] args: %v, reply: %v", rf.me, args.LeaderId, args, reply)
	}()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		return
	}

	if rf.currentTerm < args.Term {
		// 节点任期已落后于当前最新任期，则转为follower
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.persist()
	}

	rf.changeRoleLocked(Follower)
	rf.electionTimer.Reset(randElectronTimeout())

	if len(args.Entries) < 1 {
		// 没有日志则为心跳包直接返回成功
		reply.Term, reply.Success = rf.currentTerm, true
		return
	}

	// todo: 更新LeadedId

	// 追加日志到本机
	// 检查上一条日志任期是否符合
	if len(rf.log) < args.PrevLogIndex+1 || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return
	}

	// 验证没有问题回复成功
	reply.Success = true

	// 验证没有问题之后将leader同步过来的日志添加到本机
	// 先将PrevLogIndex之后的日志全部删除掉再appendleader同步过来的日志
	rf.log = rf.log[:args.PrevLogIndex+1]
	rf.log = append(rf.log, args.Entries...)
	rf.persist()

	// 更新commitIndex
	if args.LeaderCommit > rf.commitIndex {
		if len(rf.log)-1 < args.LeaderCommit {
			rf.commitIndex = len(rf.log) - 1
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
