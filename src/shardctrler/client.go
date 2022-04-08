package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.824/labrpc"
)
import "time"
import "crypto/rand"
import "math/big"


//这个客户的程序就是，对于已知的server，不断循环去遍历请求，
// 如果对方不是leader或者返回不对，就休息一段时间，选下一个机器，
// 其中msgid是随机生成的，ckid也是随机生成的

type Clerk struct {
	servers []*labrpc.ClientEnd
	id int64
	// Your data here.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.id=nrand()
	// Your code here.
	return ck
}
func (ck *Clerk) genMsgId() int64 {//随机生成的
	return int64(nrand())
}
func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{
		Num:num,
		ClientId:ck.id,
		MsgId:ck.genMsgId(),
	}
	// Your code here.
	args.Num = num
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)

			if ok && reply.WrongLeader == false {

				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{
		ClientId: ck.id,
		MsgId:ck.genMsgId(),
		Servers:  servers,
	}
	// Your code here.
	args.Servers = servers

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{
		ClientId: ck.id,
		MsgId:ck.genMsgId(),
		GIDs:     gids,
	}
	// Your code here.
	args.GIDs = gids

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{
		Shard:    shard,
		GID:      gid,
		ClientId: ck.id,
		MsgId:ck.genMsgId(),
	}
	// Your code here.
	args.Shard = shard
	args.GID = gid

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
