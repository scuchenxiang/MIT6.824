package raft

// leader send AppendEntries to one follower, and try to update commitIndex
func (rf *Raft) sendHeartbeat(server int, startTerm int, prevLogIndex int, prevLogTerm int, entries []*Entry, leaderCommitIndex int) {
	args := &AppendEntriesArgs{
		Term:              startTerm,
		LeaderId:          rf.me,
		PrevLogIndex:      prevLogIndex,
		PrevLogTerm:       prevLogTerm,
		Entries:           entries,
		LeaderCommitIndex: leaderCommitIndex,
	}
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, reply)
	if !ok {
		DPrintf("[sendHeartbeat] leader %v send to %v rpc error", rf.me, server)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	//在重新获得锁之后任期被改变
	if rf.currentTerm!=startTerm{
		return
	}
	if reply.Success==false{
		if startTerm<reply.Term{
			rf.currentTerm=max(rf.currentTerm,reply.Term)
			rf.role=Follower
			//rf.votedFor=-1
			return
		}else if reply.NextTryIndex>0{
			//在追加日志和发送心跳失败时，参与者返回的跳过的冲突的日志
			rf.nextIndex[server]=reply.NextTryIndex
		}
		rf.logmu.Lock()
		rf.persist()
		rf.logmu.Unlock()
		return
	}
	//复制的日志太老了，已经被复制了
	if prevLogIndex+len(entries)<rf.matchIndex[server]{
		return
	}
	rf.matchIndex[server]=prevLogIndex+len(entries)
	rf.nextIndex[server]=rf.matchIndex[server]+1

	myCommitIndex:=rf.commitIndex

	serverCommitIndex:=rf.matchIndex[server]
	// 已经提交的日志必须复制到其他的服务器
	if myCommitIndex>serverCommitIndex{
		return
	}
	//复制的日志和当前任期不一致，跨越任期的日志
	rf.logmu.Lock()
	serverCommitTerm:=rf.getLogTerm(serverCommitIndex)
	rf.logmu.Unlock()
	if myCommitIndex>=serverCommitIndex||serverCommitTerm!=rf.currentTerm{
		return
	}
	count:=0
	for server:=range rf.peers{
		if server==rf.me{
			continue
		}
		if rf.matchIndex[server]>=serverCommitIndex{
			count++
		}
	}
	//大多数服务器（同时包括自己）收到日志,将日志追加到状态机
	if count+1>=rf.getMajority(){
		for i:=myCommitIndex+1;i<=serverCommitIndex;i++{
			rf.logmu.Lock()
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.getLog(i).Command,
				CommandIndex: i,
			}
			rf.logmu.Unlock()
			rf.applyCh<-msg
		}
		rf.commitIndex=serverCommitIndex
		rf.logmu.Lock()
		rf.persist()
		rf.logmu.Unlock()
	}
}
