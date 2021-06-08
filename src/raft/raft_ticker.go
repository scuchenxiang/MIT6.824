package raft

import (
	"time"
)

// The ticker for leader to send AppendEntries requests periodly
func (rf *Raft) leaderTicker() {
	for rf.killed() == false {
		time.Sleep(time.Duration(LeaderHeart_Interval) * time.Millisecond)
		rf.leaderHandler()
	}
}

func (rf *Raft) leaderHandler() {
	rf.mu.Lock()

	if rf.role==Leader{
		rf.logmu.Lock()
		term:=rf.currentTerm
		for server:=range rf.peers{
			if server==rf.me{
				continue
			}

			prevLogIndex2Server:=rf.nextIndex[server]-1
			//如果日志比目前的最早的日志还旧就安装快照
			if prevLogIndex2Server<rf.getSnapshotLastIndex(){//rf.getLastLogIndex(),rf.getLastLogTerm()
				go rf.doInstallSnapshot(server,term,rf.getSnapshotLastIndex(),rf.getSnapshotLastTerm(),rf.getSnapshotLastData())
			}else {//服务器i的最后需要发送的日志的上一条日志的任期
				entries:=[]*Entry{}
				prevLogTerm2Server:=rf.getLogTerm(prevLogIndex2Server)
				leaderCommitIndex:=rf.commitIndex
				for i:=prevLogIndex2Server+1;i<=rf.getLastLogIndex();i++{
					entries=append(entries,rf.getLog(i))
				}
				//发送需要补的日志和提交的日志的最大值
				go rf.sendHeartbeat(server,term,prevLogIndex2Server,prevLogTerm2Server,entries,leaderCommitIndex)
			}

		}
		rf.logmu.Unlock()
	}
	rf.mu.Unlock()
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) electionTicker() {
	for !rf.killed() {
		//超时发起选举
		rf.mu.Lock()
		sleepTime:=getElectionTimeout()*time.Millisecond
		rf.mu.Unlock()
		time.Sleep(sleepTime)
		rf.mu.Lock()
		before:=rf.lastHeartbeat
		if(time.Since(before)>=sleepTime&&rf.role!=Leader){

			rf.currentTerm++
			rf.role=Candidate
			go rf.startElection(rf.currentTerm)

		}
		rf.mu.Unlock()
	}
}
