package raft

type AppendEntriesArgs struct {
	Term              int
	LeaderId          int
	PrevLogIndex      int
	PrevLogTerm       int
	Entries           []*Entry
	LeaderCommitIndex int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	NextTryIndex int
}

// follower response to Leader's AppendEntries call.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term<rf.currentTerm{
		reply.Term=rf.currentTerm
		reply.Success=false
		return
	}

	//reply.Term=rf.currentTerm
	reply.Success=true
	rf.currentTerm = args.Term
	rf.role=Follower
	rf.refreshElectionTimeout()
	prevlogindex:=args.PrevLogIndex
	rf.logmu.Lock()

	//如果leader已经发送的最后一条日志索引比server的
	// 快照的最后一条还旧，说明发来的日志有一部分是重复的，直接返回
	if prevlogindex<rf.getSnapshotLastIndex(){
		rf.logmu.Unlock()
		return
	}
	if prevlogindex>rf.getLastLogIndex()||//
		args.PrevLogTerm!=rf.getLogTerm(prevlogindex)||
		(prevlogindex==rf.getSnapshotLastIndex()&&args.PrevLogTerm!=rf.getSnapshotLastTerm()){
		var conflict int

		//如果leader已经发送的最后一条日志记录和server的冲突了，同时leader的lastcopyindex大于快照日志
		//索引小于server最大的日志索引直接回退到这个任期的第一条日志（说明下一次leader从这个任期的
		//第一条开始发）。如果同时leader的lastcopyindex大于server最大的日志索引，那么直接返回快照的
		// 最大索引+1。
		if prevlogindex<=rf.getLastLogIndex(){
			conflictTerm:=rf.getLogTerm(prevlogindex)
			conflict=prevlogindex

			for conflict-1>=rf.getSnapshotLastIndex()&&rf.getLogTerm(conflict-1)==conflictTerm{
				conflict--
			}
		}else{
			conflict=rf.getLastLogIndex()+1
		}

		reply.Success=false
		reply.Term=rf.currentTerm
		reply.NextTryIndex=conflict
		rf.logmu.Unlock()
		return
	}
	if len(args.Entries)>0{
		newLog := make([]*Entry, 0)
		for i := rf.logs[0].Index; i <= args.PrevLogIndex; i++ {
			newLog = append(newLog, rf.getLog(i))
		}
		newLog = append(newLog, args.Entries...)
		//前面保证了lastcopyIndex在lastSnapshotIndex与lastLogIndex之间，
		//然后追加日志（如果追加的日志比lastLogIndex小就不用追加）
		if moreUpToDate(newLog[len(newLog)-1].Index,newLog[len(newLog)-1].Term,rf.getLastLogIndex(),rf.getLastLogTerm()){
			rf.logs=newLog
		}
		rf.persist()
	}
	myCommitIndex := rf.commitIndex
	//newCommitIndex := min(args.LeaderCommitIndex, rf.getLastLogIndex())
	newCommitIndex :=args.LeaderCommitIndex
	rf.logmu.Unlock()
	if(myCommitIndex<newCommitIndex){
		for i:=myCommitIndex+1;i<=newCommitIndex;i++{
			rf.logmu.Lock()
			msg:=ApplyMsg{
				CommandValid: true,
				Command:      rf.getLog(i).Command,
				CommandIndex: i,
			}
			rf.logmu.Unlock()
			rf.applyCh<-msg
		}
		rf.commitIndex=newCommitIndex
	}
	return
}
