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

	//追加的日志比快照还旧，或者两者在
	if prevlogindex<rf.getSnapshotLastIndex(){
		rf.logmu.Unlock()
		return
	}
	//日志比最新的日志还新，或者日志符合要求，但是任期却不一样，或者被其他的服务器安装了不同的快照日志
	if prevlogindex>rf.getLastLogIndex()||
		args.PrevLogTerm!=rf.getLogTerm(prevlogindex)||
		(prevlogindex==rf.getSnapshotLastIndex()&&args.PrevLogTerm!=rf.getSnapshotLastTerm()){
		var conflict int
		if prevlogindex<=rf.getLastLogIndex(){
			conflictTerm:=rf.getLogTerm(prevlogindex)
			conflict=prevlogindex
			//这就是在日志符合要求，但是任期却不一样，或者被其他的服务器安装了不同的快照日志的情况下的处理
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
		//只能复制从0到PrevLogIndex再追加日志，多余的日志可能是不一致的
		for i := rf.logs[0].Index; i <= args.PrevLogIndex; i++ {
			newLog = append(newLog, rf.getLog(i))
		}
		newLog = append(newLog, args.Entries...)
		if moreUpToDate(newLog[len(newLog)-1].Index,newLog[len(newLog)-1].Term,rf.getLastLogIndex(),rf.getLastLogTerm()){
			rf.logs=newLog
		}
		rf.persist()
	}
	myCommitIndex := rf.commitIndex
	//在任期和日志符合要求的情况下，将上一次的日志追加到状态机，这次保存的下次追加
	newCommitIndex := min(args.LeaderCommitIndex, rf.getLastLogIndex())
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
