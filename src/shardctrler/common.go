package shardctrler

import "6.824/labgob"

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
//Shards获取对应的shard的groupId
//group获取对应group的机器
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

type Config_num struct {
	gid int
	shard_num int
}
type Config_num_list []Config_num
func (p Config_num_list) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p Config_num_list) Len() int           { return len(p) }
func (p Config_num_list) Less(i, j int) bool { return p[i].shard_num > p[j].shard_num }

func init() {
	labgob.Register(Config{})
	labgob.Register(QueryArgs{})
	labgob.Register(QueryReply{})
	labgob.Register(JoinArgs{})
	labgob.Register(JoinReply{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(LeaveReply{})
	labgob.Register(MoveReply{})
}
const (
	OK = "OK"
	ErrNoKey="ErrNoKey"
	ErrWrongLeader="ErrWrongLeader"
	ErrRPC="ErrRPCFailed"
	ErrTimeout="ErrTimeout"
)

type Err string
func (c *Config) Copy() Config {
	//var ss [NShards]int
	//for i, v := range c.Shards {
	//	ss[i] = v
	//}
	config := Config{
		Num:    c.Num,
		Shards: c.Shards,
		Groups: make(map[int][]string),
	}
	for gid, s := range c.Groups {
		config.Groups[gid] = append([]string{}, s...)
	}
	return config
}
type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
	ClientId int64
	MsgId int64
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs []int
	ClientId int64
	MsgId int64
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard int
	GID   int
	ClientId int64
	MsgId int64
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num int // desired config number
	ClientId int64
	MsgId int64
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}