package raft

// Candidate starts Election
func (rf *Raft) startElection(startTerm int) {
	rf.mu.Lock()
	rf.logmu.Lock()
	defer rf.logmu.Unlock()
	defer rf.mu.Unlock()
	//如果在选举时任期被改变说明不是上一次的请求
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
		CandidateId:server,
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


	if rf.currentTerm!=startTerm||rf.role!=Candidate{
		return
	}
	//在每次角色改变时尽可能保存信息，以待下一次的恢复
	if reply.Term>rf.currentTerm{
		rf.currentTerm=max(rf.currentTerm,reply.Term)
		rf.role=Follower
		rf.persist()
		return
	}
	if reply.VoteGranted &&rf.role==Candidate{
		rf.getVotedTickets++
		if rf.getVotedTickets>=rf.getMajority(){
			rf.role=Leader
			rf.leaderInitialization()
			rf.persist()
			return
		}
	}
}
