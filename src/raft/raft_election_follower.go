package raft

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// follower node response to RequestVote call.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logmu.Lock()
	defer rf.logmu.Unlock()
	DPrintf("[RequestVote] me=%v, be asked to voted to %v, his term=%v", rf.me, args.CandidateId, args.Term)

	reply.Term = rf.currentTerm
	//rf是被要求投票的，args是请求投票的服务器的参数，任期太老拒绝投票
	if args.Term < rf.currentTerm {
		DPrintf("[RequestVote] me=%v, too old term dont give vote, currentTerm=%v", rf.me, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// rf是被要求投票的，args是请求投票的服务器的参数，任期太新就更新任期
	if args.Term > rf.currentTerm {
		DPrintf("[RequestVote] %v get bigger term=%v from %v", rf.me, args.Term, args.CandidateId)
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.votedFor = -1
		rf.persist()
	}

	// 任期符合要求时应该检查日志
	// refer to 5.4.1
	if moreUpToDate(rf.getLastLogIndex(), rf.getLastLogTerm(), args.LastLogIndex, args.LastLogTerm) {
		DPrintf("[RequestVote] %v cant give vote to %v because he is too old, lastLogIndex=%v, lastLogTerm=%v, his LastLogIndex=%v, LastLogTerm=%v", rf.me, args.CandidateId, rf.getLastLogIndex(), rf.getLastLogTerm(), args.LastLogIndex, args.LastLogTerm)
		reply.VoteGranted = false
	} else if rf.votedFor == args.CandidateId || rf.votedFor == -1 {
		// Follower have voted to him or havenot vote yet, then give out tickect.
		DPrintf("[RequestVote] %v give vote to %v, lastLogIndex=%v, lastLogTerm=%v, his LastLogIndex=%v, LastLogTerm=%v", rf.me, args.CandidateId, rf.getLastLogIndex(), rf.getLastLogTerm(), args.LastLogIndex, args.LastLogTerm)
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		reply.VoteGranted = true
		rf.refreshElectionTimeout()
		rf.persist()
	} else {
		// 在任期日志符合要求但是已经投票并且没有投我的情况下
		DPrintf("[RequestVote] %v this term has voted to %v", rf.me, rf.votedFor)
		reply.VoteGranted = false
	}
	//交换任期
	reply.Term = rf.currentTerm
	return
}
