package raft

// Candidate starts Election
func (rf *Raft) startElection(startTerm int) {
	rf.mu.Lock()
	rf.logmu.Lock()
	defer rf.logmu.Unlock()
	defer rf.mu.Unlock()
	//如果在发起选举时任期被改变可能有别的机器发起选举，同时比我的任期更新
	if(rf.currentTerm!=startTerm||rf.role!=Candidate){
		return
	}

	rf.votedFor=rf.me
	rf.getVotedTickets=1
	term:=rf.currentTerm
	lastlogterm:=rf.getLastLogTerm()
	lastlogindex:=rf.getLastLogIndex()
	//保存信息，防止候选者宕机情况
	rf.persist()
	for server:=range rf.peers{
		if server==rf.me{
			continue
		}
		go rf.askForVote(server,term,lastlogindex,lastlogterm)
	}
}

// Candidate send request to Follower for tickets
func (rf *Raft) askForVote(server int, startTerm int, lastLogIndex int, lastLogTerm int) {

	args:=&RequestVoteArgs{
		Term:startTerm,
		CandidateId:rf.me,
		LastLogIndex:lastLogIndex,
		LastLogTerm:lastLogTerm}
	reply:=&RequestVoteReply{}

	ok:=rf.sendRequestVote(server,args,reply)
	if !ok{
		DPrintf("%v send request to %v failed",rf.me,server)
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logmu.Lock()
	defer rf.logmu.Unlock()

	//如果在发起选举时任期被改变可能有别的机器发起选举，同时比我的任期更新
	if rf.currentTerm!=startTerm||rf.role!=Candidate{
		return
	}
	//在每次角色改变时尽可能保存信息，以待下一次的恢复
	if reply.Term>rf.currentTerm{
		rf.currentTerm=max(rf.currentTerm,reply.Term)
		rf.role=Follower
		//保存任期和日志
		rf.persist()
		return
	}
	if reply.VoteGranted &&rf.role==Candidate{
		rf.getVotedTickets++
		if rf.getVotedTickets>=rf.getMajority(){
			rf.role=Leader
			rf.leaderInitialization()
			//每次成为leader，那么旧提交一条空日志
			//go func() { rf.applyCh <- ApplyMsg{} }()

			//一旦成为leader就提交一条空日志
			//var command  interface{}
			//term:=rf.currentTerm
			//index:=rf.getLastLogIndex()+1
			//rf.appendLog(&Entry{
			//	Command:command,
			//	Term:term,
			//	Index:index})
			//go rf.leaderHandler()
			//go func() { rf.applyCh <- ApplyMsg{} }()
			rf.persist()
			go rf.leaderHandler()
			return
		}
	}
}
