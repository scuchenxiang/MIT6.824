package raft

import (
	"fmt"
	"time"

	//"fmt"
	"sync"
	"6.824/labrpc"
	//"../labrpc"
)


type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	currentTerm     int//当前任期
	role            int//leader,candidate or follower
	votedFor        int//为谁投票
	getVotedTickets int//已经获得的投票的数目
	lastHeartbeat   time.Time//
	logs            []*Entry//日志
	commitIndex     int//提交的日志的最大的索引值
	nextIndex       []int//对每个服务器需要发送的下一个日志的索引
	matchIndex      []int//对每个服务器已经复制的日志的最高索引
	applyCh         chan ApplyMsg//
	snapshotData    []byte//快照数据

	// logmu is used when accessing rf.Logs

	//
	logmu sync.Mutex
}
type Entry struct {
	Index   int
	Term    int
	Command interface{}
}
func (e *Entry) String() string {
	return fmt.Sprintf("{index=%v term=%v command=%v} ", e.Index, e.Term, e.Command)
}
//const(
//	Leader int =iota
//	Follower
//	Candidate
//)
const(
	LastHeart_Min int =300
	LastHeart_Interval int =150//follower的sleep的时延
	LeaderHeart_Interval int=100//leader的发送心跳的时延
)
