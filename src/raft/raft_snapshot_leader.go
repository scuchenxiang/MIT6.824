package raft


func (rf *Raft) doInstallSnapshot(server int, startTerm int, LastIncludedIndex int, LastIncludedTerm int, LastIncludedData []byte) {
	args:=&InstallSnapshotArgs{
		Term:startTerm,
		LeaderId:rf.me,
		LastIncludedIndex:LastIncludedIndex,
		LastIncludedTerm:LastIncludedTerm,
		Data:LastIncludedData,
	}

	reply:=&InstallSnapshotReply{}

	ok:=rf.sendInstallSnapshot(server,args,reply)
	if !ok{
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if startTerm!=rf.currentTerm{
		return
	}
	if reply.Term>startTerm{
		rf.role=Follower
		rf.votedFor=-1
		rf.currentTerm=max(rf.currentTerm,startTerm)
		return
	}

	rf.nextIndex[server]=LastIncludedIndex+1

}
