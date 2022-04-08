package shardctrler

import (
	"6.824/raft"
	"fmt"
	"log"
	"sort"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"
const (
	WaitCmdInterval = time.Millisecond * 500
	MaxLockTime = time.Millisecond * 10 // debug
)
type NotifyMsg struct{
	Err Err
	WrongLeader bool
	Config Config
}
type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	//增加的代码
	stopCh chan struct{}
	msgNotify map[int64] chan NotifyMsg
	lastApplies map[int64]int64
	DebugLog  bool
	lockStart time.Time // debug 用，找出长时间 lock
	lockEnd   time.Time
	lockName  string
	// Your data here.
	//ctrler的配置，数值越大，越新
	configs []Config // indexed by config num
}


type Op struct {
	MsgId int64
	ClientId int64
	ReqId int64
	Args interface{}
	Method string

	// Your data here.
}
//如果只有一个group，那么所有的shard都分配到这上面
//如果group数目为0，此时什么都没有，shard也应该为空
//如果group的数目比shard的数量少，那么求出每个group平均分配的最小数目，以及剩余的余数数目，
//此时分四种情况讨论，
// 如果group的shard和avg一样，continue，
// 如果group的shard大于avg，并且remain=0，将多于avg的group置为0，
// 如果group的shard大于avg，并且remain>0，将多于avg+remain的group置为0，
// 如果group的shard小于avg，从group为0的那些shard选一些补到avg，如果还不足avg，还需要循环一次

//如果shard_num<group_num,那么就把多于一个shard的group的多的shard拿出来，
// shard还没选group的拿出来分别分配到一个都没有的group中。
func (sc *ShardCtrler) adjustConfig(config *Config){
	
	if len(config.Groups)==0{
		config.Shards=[NShards]int{}
	}else if(len(config.Groups)==1){
		for k,_:=range config.Groups{
			for p,_:=range config.Shards{
				config.Shards[p]=k
			}
		}

	}else if(len(config.Groups)<=NShards){
		avg:=NShards/len(config.Groups)
		remain:=NShards-avg*len(config.Groups)

		lastGid:=0
		needLoop:=false

LOOP:
	    var keys [] int
		for k:=range config.Groups{
			keys=append(keys,k)
		}
		sort.Ints(keys)
		//对于每一个group，获取它的shard的数目
		for _,gid:=range keys{
			lastGid=gid
			count:=0
			for _,val:=range config.Shards{
				if val==gid{
					count+=1
				}
			}
			if count==avg{
				continue
			}else if count>avg && remain==0{
				//如果这个group的shard数目大于avg，并且剩余数目为0
				// 把count减为avg，即把大于avg的shard 的group暂时选择为0
				c:=0
				for i,val:=range config.Shards{
					if val==gid{
						if c==avg{
							config.Shards[i]=0
						}else{
							c+=1
						}
					}
				}
			}else if count>avg && remain>0{
				c:=0
				//如果这一部分数目大于平均数，多于remain+avg的就给其他group，否则就给这个group
				for i,val:=range config.Shards{
					if val==gid{
						if c==avg+remain{
							config.Shards[i]=0
						}else{
							if c==avg{
								remain-=1
							}else{
								c+=1
							}
						}
					}
				}
			}else{//count<avg,把其他的补给它
				for i,val:=range config.Shards{
					if count==avg{
						break
					}
					if val==0 && count<avg{
						config.Shards[i]=gid
						count+=1//I add it
					}
				}
				//如果这个时候其他group的位置还没空出来，就再循环
				if count<avg{
					needLoop=true
				}
			}
		}
		if needLoop{
			needLoop=false
			goto LOOP
		}
		//当group的数目大于0的时候
		if lastGid!=0{
			//此时如果还有没选group的,让其选最后一个
			for i,val:=range config.Shards{
				if val==0{
					config.Shards[i]=lastGid
				}
			}
		}
	}else{
		gids:=make(map[int]int)
		//一开始的长度为0，预留长度为Nshard
		emptyShard:=make([]int,0,NShards)

		for i,val:=range config.Shards{
			//如果这个shard的group为0，即需要分配
			if val==0{
				emptyShard=append(emptyShard,i)
				continue
			}
			//如果对应的shard i属于val group，将i group标记为1，
			//如果发现标记之前i group已经为1了，就把它放进emptyshard,因为每个group最多放一个
			if _,ok:=gids[val];ok{
				emptyShard=append(emptyShard,i)//在gid处已经有值了，每处只能放一个值
				config.Shards[i]=0
			}else{
				gids[val]=1
			}
		}
		n:=0
		if len(emptyShard)>0{
			var keys []int
			for k:=range config.Groups{
				keys=append(keys,k)
			}
			sort.Ints(keys)
			for _,gid:=range keys{
				if _,ok:=gids[gid];!ok{//这个group 标记还没有标记为1，即为空，从emptyshard中拿一个给这个group
					config.Shards[emptyShard[n]]=gid
					n+=1
				}
				if n>=len(emptyShard){
					break
				}
			}
		}
	}
}

func sortMapByValue(m map[int]int) Config_num_list {
	p := make(Config_num_list, len(m))
	i := 0
	for k, v := range m {
		p[i] = Config_num{k, v}
	}

	sort.Sort(p)
	return p
}
//func (sc *ShardCtrler) adjustConfig_my(config *Config) {
//	if len(config.Groups)==0{
//		config.Shards=[NShards]int{}
//	}else if(len(config.Groups)==1){
//		for k,_:=range config.Groups{
//			for p,_:=range config.Shards{
//				config.Shards[p]=k
//			}
//		}
//
//	}else{
//		avg:=NShards/len(config.Groups)
//		//remain:=NShards-avg*len(config.Groups)
//		sort_gid_num := make(map[int]int)
//		for _,gid_tmp:= range config.Shards{
//			if i, ok := sort_gid_num[gid_tmp];!ok{
//				sort_gid_num[gid_tmp]=1
//			}else{
//				sort_gid_num[gid_tmp]=i+1
//			}
//		}
//		res:=sortMapByValue(sort_gid_num)
//		for i:=0;i<len(res);i++{
//			//remain=i
//		}
//
//	}
//
//}
//在
func (sc *ShardCtrler) join(args JoinArgs){
	config:=sc.getConfigByIndex(-1)//找最后一个config，即最新的
	config.Num+=1

	for k,v:=range args.Servers{
		config.Groups[k]=v
	}
	//更新配置
	sc.adjustConfig(&config)

	sc.configs=append(sc.configs,config)
}
func (sc *ShardCtrler) leave(args LeaveArgs){
	//获取最新的config
	config:=sc.getConfigByIndex(-1)
	config.Num+=1
	//删除某个group的server，然后将对应的里面的shard标记为未分配，然后调整config，更新config
	for _,gid:=range args.GIDs{
		delete(config.Groups,gid)
		for i,v :=range config.Shards{
			if gid==v{
				config.Shards[i]=0
			}
		}
	}
	sc.adjustConfig(&config)
	sc.configs=append(sc.configs,config)
}
//将某个shard的group改变，更新confg
func (sc *ShardCtrler) move(args MoveArgs){
	//获取最新的config
	config:=sc.getConfigByIndex(-1)
	config.Num+=1
	//移动shard到某个group中
	config.Shards[args.Shard]=args.GID
	sc.configs=append(sc.configs,config)
}
func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	res:=sc.runCmd("Join",args.MsgId,args.ClientId, *args)
	reply.Err,reply.WrongLeader=res.Err, res.WrongLeader
	// Your code here.
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	res:=sc.runCmd("Leave",args.MsgId,args.ClientId, *args)
	reply.Err,reply.WrongLeader=res.Err, res.WrongLeader
	// Your code here.
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	res:=sc.runCmd("Move",args.MsgId,args.ClientId, *args)
	reply.Err,reply.WrongLeader=res.Err, res.WrongLeader
	// Your code here.
}
//如果没超出了config的界限，直接回复配置
//否则运行runcmd
func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	sc.lock("Query")
	if args.Num>0&&args.Num<len(sc.configs){
		reply.Err=OK
		reply.WrongLeader=false
		reply.Config=sc.getConfigByIndex(args.Num)
		sc.unlock("Query")
		return
	}
	sc.unlock("Query")
	res := sc.runCmd("Query", args.MsgId, args.ClientId, *args)
	reply.Err = res.Err
	reply.WrongLeader = res.WrongLeader
	reply.Config = res.Config
}
func(sc *ShardCtrler) runCmd(cmd string,MsgId int64,ClientId int64,args interface{}) (res NotifyMsg) {
	op:=Op{
		MsgId:MsgId,
		ClientId:ClientId,
		ReqId:nrand(),
		Args:args,
		Method:cmd,
	}
	res=sc.waitCmd(op)
	return
}
//交给底层的raft提交日志，如果非leader，或者超时，返回，
// 如果收到返回回复的msg的通道数据，
// 删除通道的数据返回
func(sc *ShardCtrler) waitCmd(op Op) (res NotifyMsg){
	_,_,isLeader:=sc.rf.Start(op)
	if !isLeader{//如果请求的非Leader，就返回
		res.Err=ErrWrongLeader
		res.WrongLeader=true
		return
	}
	sc.lock("waitcmd")
	ch:=make(chan NotifyMsg,1)//创建一个通道
	sc.msgNotify[op.ReqId]=ch
	sc.unlock("waitcmd")
	timer:=time.NewTimer(WaitCmdInterval)//最少过去这么多时间就发送消息
	defer timer.Stop()
	select {
	case res=<-ch://阻塞接收通道数据
		sc.removeCh(op.ReqId)
		return
	case <-timer.C://阻塞接收任意数据，并忽略从通道返回的数据
		sc.removeCh(op.ReqId)
		res.WrongLeader=true
		res.Err=ErrTimeout
		return
	}
}


func (sc *ShardCtrler) removeCh(id int64){
	sc.lock("removeCh")
	delete(sc.msgNotify,id)//删除字典的某个元素
	sc.unlock("removeCh")
}


func (sc *ShardCtrler) apply(){
	for{
		select {//选择某个通信执行
		case <-sc.stopCh:
			return
		case msg:=<-sc.applyCh:
			if !msg.CommandValid{
				continue
			}
			op:=msg.Command.(Op)
			sc.lock("apply")
			//op得msgid是否和op.ClientId得最后一条是一样的
			isRepeated:=sc.isRepeated(op.ClientId,op.MsgId)
			if !isRepeated{
				switch op.Method {
				case "Join":
					sc.join(op.Args.(JoinArgs))//断言转换
				case "Leave":
					sc.leave(op.Args.(LeaveArgs))
				case "Move":
					sc.move(op.Args.(MoveArgs))
				case "Query":
				default:
					panic("unknow method")
				}
			}
			res:=NotifyMsg{
				Err:OK,
				WrongLeader:false,
			}
			if op.Method!="Query"{
				sc.lastApplies[op.ClientId]=op.MsgId
			}else {
				res.Config=sc.getConfigByIndex(op.Args.(QueryArgs).Num)
			}
			if ch,ok:=sc.msgNotify[op.ReqId];ok{
				ch<-res
			}
			sc.unlock("apply")
		}
	}
}
//如果idIndex 不合理就返回最后一条，否则返回对应index的config
func (sc *ShardCtrler)  getConfigByIndex(idInex int) Config{
	if idInex<0 ||idInex>=len(sc.configs){
		return sc.configs[len(sc.configs)-1].Copy()
	}else{
		return sc.configs[idInex].Copy()
	}
}
func (sc *ShardCtrler) isRepeated(clientId int64,id int64) bool{//最后一条是否重复
	if val,ok:=sc.lastApplies[clientId];ok{
		return val==id
	}
	return false
}
func (sc *ShardCtrler) lock(str string){
	sc.mu.Lock()
	sc.lockName=str
	sc.lockStart=time.Now()//用来调试记录，也可以删除
}
func (sc *ShardCtrler) log(m string) {
	if sc.DebugLog {
		log.Printf("shardmaster me: %d, configs:%+v, log:%s", sc.me, sc.configs, m)
	}
}
func (sc *ShardCtrler) unlock(str string){
	sc.lockEnd=time.Now()
	sc.lockName=""
	timeInterval:=sc.lockEnd.Sub(sc.lockStart)
	sc.mu.Unlock()
	if timeInterval>MaxLockTime{
		sc.log(fmt.Sprintf("lock too long:%s:%s\n", str, timeInterval))
	}
	//用来调试记录，也可以删除
}
//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.stopCh=make(chan struct{})
	sc.DebugLog=false
	sc.lastApplies=make(map[int64]int64)
	sc.msgNotify=make(map[int64]chan NotifyMsg)
	go sc.apply()
	// Your code here.

	return sc
}
