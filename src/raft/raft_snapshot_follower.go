package raft

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logmu.Lock()
	defer rf.logmu.Unlock()

	if args.Term<rf.currentTerm{
		reply.Term=rf.currentTerm

		return
	}

	reply.Term=rf.currentTerm
	rf.role=Follower
	rf.currentTerm=args.Term
	if args.LastIncludedIndex<=rf.getSnapshotLastIndex(){
		return
	}

	Entries:=[]*Entry{}
	Entries=append(Entries,&Entry{
		Index:args.LastIncludedIndex,
		Term:args.LastIncludedTerm,
		Command:args.Data,
	})
	if args.LastIncludedIndex<rf.getLastLogIndex(){
		for i:=args.LastIncludedIndex+1;i<=rf.getLastLogIndex();i++{
			DPrintf("InstallSnapshot apply entries")
			Entries=append(Entries,rf.getLog(i))
		}
	}

	rf.commitIndex=args.LastIncludedIndex
	rf.snapshotData=args.Data
	rf.logs=Entries
	msg:=ApplyMsg{
		SnapshotValid:true,
		Snapshot:rf.getSnapshotLastData(),
		SnapshotTerm:rf.getSnapshotLastTerm(),
		SnapshotIndex:rf.getSnapshotLastIndex(),
	}
	rf.applyCh<-msg
	rf.persist()
}
