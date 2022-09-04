package shardkv

import (
	"6.824/shardctrler"
	"6.824/labrpc"
	"6.824/raft"
	"fmt"
	"sync"
	"encoding/gob"
	"time"
	"bytes"
	"reflect"
	// "fmt"
)
const(
	MaxRaftFactor = 0.8
	ResChanSize = 1
	ResChanTimeout = 1000
	PollConfigTimeout = 100
	PollShardsTimeout = 100
)

// --------------------------------------------------------------------
// Op's
// --------------------------------------------------------------------

type ShardConfigOp struct {
	Config  shardctrler.Config
}

type PullShardOp struct {
	KvDb    map[string]string
	CltSqn  map[int64]int64
	SV 		ShardVer
	Ver 	int
}

type DeleteShardOp struct {
	Shard   int
	ConfNum int
}

type RemovePullMapOp struct {
	SV 		ShardVer
}

type Op struct {
	Request string  // "Put", "Append", "Get"
	Key     string
	Value   string  // set to "" for Get request
	CltId   int64   // client unique identifier
	SeqNum  int64
}
type EmptyLog struct{

}

// --------------------------------------------------------------------
// ShardKV struct
// --------------------------------------------------------------------

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	mck          *shardctrler.Clerk
	config       shardctrler.Config
	shardsVerNum []int            	   		     // version number for each shard
	pullMap      map[ShardVer]ServerValid
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int 				   			 // snapshot if log grows this big
	kvdbs        []map[string]string	   		 // each shard has a db and sqn
	clientSeqNum      []map[int64]int64  	   		 // sequence number log for each client
	raftIndex        int 					   		 // raft grows to index
	//从applydb到客户端的通信
	resChanMap   map [int] chan ReplyRes 		 // communication from applyDb to clients
	pcTimer      *time.Timer  			   		 // timer for polling the configuration
	psTimer      *time.Timer  			   		 // timer for sending PullShards requests
	killIt       chan bool   				   	 // close goroutine
}

// --------------------------------------------------------------------
// Background Functions
// --------------------------------------------------------------------

func (kv *ShardKV) createResChan(cmtidx int) {
	//对于commandindex的位置创建一个通道
	if kv.resChanMap[cmtidx] == nil {
		kv.resChanMap[cmtidx] = make(chan ReplyRes, ResChanSize)
	}

}

func flushChannel(resCh chan ReplyRes) {
	select {
	case <- resCh:
		// flush the channel
	default:
		// no need to flush
	}
}

func (kv *ShardKV) PullConfig() {
	//每隔一段时间，如果ctrler的配置更新了，注意必须每次+1的配置版本进行更新，
	// 如果不需要拉取shard，没有正在传输的config，就更新config，向raft添加日志
	for {
		select{
		case <- kv.killIt://阻塞接收消息
			return
		case <- kv.pcTimer.C://timer到时间阻塞接收消息
			//重新定时
			kv.pcTimer.Reset(time.Duration(PollConfigTimeout)* time.Millisecond)
			kv.mu.Lock()
			nextConfigIdx := kv.config.Num + 1  // next config
			kv.mu.Unlock()
			newConfig := kv.mck.Query(nextConfigIdx)
			//如果得到新的config,不是-1都能得到
			if newConfig.Num == nextConfigIdx {  // got new config
				kv.mu.Lock()
				okToUpdate := true
				//如果对于目前的config中本group中的每个shard版本号不是配置号
				//说明配置正在传输
				for s := 0; s < shardctrler.NShards; s ++ {
					g := kv.config.Shards[s]
					if g == kv.gid {  // in charge of this shard in current config
						if kv.shardsVerNum[s] != kv.config.Num{  // config during transit
							okToUpdate = false
							break
						}
					}
				}
				//如果有需要拉取的shard
				if len(kv.pullMap) != 0 {  // shards are still moving around
					okToUpdate = false
				}
				kv.mu.Unlock()
				//无上述情况就更新config
				if okToUpdate {
					op := ShardConfigOp{Config: newConfig}
					kv.rf.Start(op)
				}
			}
		}
	}
}

func (kv *ShardKV) PullShards() {
	//每隔一段时间，就对pullmap中的需要拉取的shard进行拉取，
	// 给出args参数，对于返回的reply，如果成功，添加到底层的raft日志中
	//如果valid为false，说明需要删除，对于返回的reply，如果成功，也添加到日志中
	for {
		select{
		case <- kv.killIt:
			return
		case <- kv.psTimer.C:
			kv.psTimer.Reset(time.Duration(PollShardsTimeout)* time.Millisecond)
			kv.mu.Lock()
			localPullMap := make(map[ShardVer]ServerValid)
			for k, v := range kv.pullMap {
				localPullMap[k] = v
			}
			kv.mu.Unlock()
			//对于pullmap的每个需要拉取的shard进行拉取
			for shardVer, serversValid := range localPullMap {
				//如果valid为false说明已经拉取过了
				if serversValid.Valid {
					// ---- needs shard from others	----
					for si := 0; si < len(serversValid.Servers); si++ {
						srv := kv.make_end(serversValid.Servers[si])
						//VerNum：旧得config号，ConfNum：已经更新得新得config号
						args := PullShardArgs{Shard:shardVer.Shard, VerNum:shardVer.VerNum, ConfNum:shardVer.ConfNum}
						var reply PullShardReply
						ok := srv.Call("ShardKV.G2GPullShardRPC", &args, &reply)
						if ok && reply.Success {  // got the reply from intended shard group
							//reply.ShardVer 拉到得shard得版本号
							op := PullShardOp{KvDb: reply.KvDb, CltSqn: reply.CltSqn, SV: shardVer, Ver: reply.ShardVer}
							kv.rf.Start(op)
							break  // got response already, no need to try more
						}
					}
				} else {
					// ---- delete shard in the other side ----
					for si := 0; si < len(serversValid.Servers); si++ {
						srv := kv.make_end(serversValid.Servers[si])
						args := DeleteShardArgs{Shard:shardVer.Shard, VerNum:shardVer.VerNum, ConfNum:shardVer.ConfNum}
						var reply DeleteShardReply
						ok := srv.Call("ShardKV.G2GDeleteShardRPC", &args, &reply)
						if ok && reply.Success {  // got the reply from intended shard group
							op := RemovePullMapOp{SV: shardVer}
							kv.rf.Start(op)
							break  // got response already, no need to try more
						}
					}
				}
			}
		}
	}
}

// --------------------------------------------------------------------
// Shard Group RPC Functions
// --------------------------------------------------------------------

func (kv *ShardKV) G2GPullShardRPC(args *PullShardArgs, reply *PullShardReply) {
	//如果我的配置号更新，那么就把kvdbs和clientseqNum发送给对方，回应success，否则false
	kv.mu.Lock()
	if kv.config.Num >= args.ConfNum {//>=
		reply.Success = true
		reply.KvDb = make(map[string]string)
		reply.CltSqn = make(map[int64]int64)
		for k, v := range kv.kvdbs[args.Shard] {
			reply.KvDb[k] = v
		}
		for k, v := range kv.clientSeqNum[args.Shard] {
			reply.CltSqn[k] = v
		}
		reply.ShardVer = kv.shardsVerNum[args.Shard]
	} else {
		reply.Success = false
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) G2GDeleteShardRPC(args *DeleteShardArgs, reply *DeleteShardReply) {
	//如果我的对应的shard的配置号比参数发过来的配置号更旧，说明确实可以删除，
	// 此时让底层的raft进行同步命令，如果是leader，并且对应命令的管道信息没有正在传输，
	// 那么就为success，超时或者其他情况返回false
	kv.mu.Lock()
	localShardVerNum := kv.shardsVerNum[args.Shard]
	localConfNum := args.ConfNum
	kv.mu.Unlock()
	if localShardVerNum < localConfNum {//before is  <=
		DPrintf("G2GDeleteShardRPC do_raft_op args:config %v  localshrdver: %v  ",localConfNum,localShardVerNum)
		op := DeleteShardOp{Shard: args.Shard, ConfNum: args.ConfNum}
		cmtidx, _, isLeader := kv.rf.Start(op)
		if !isLeader{
			reply.Success = false
			return
		}
		kv.mu.Lock()
		kv.createResChan(cmtidx)
		resCh := kv.resChanMap[cmtidx]
		kv.mu.Unlock()
		select{
		case res := <- resCh:
			if res.InTransit {
				fmt.Printf("deleteshardOp intransit")
				reply.Success = false
			} else {
				reply.Success = true
			}
		case <- time.After(ResChanTimeout * time.Millisecond): // RPC timeout
			reply.Success = false
		}
	} else {
		reply.Success = false
	}
}

// --------------------------------------------------------------------
// ApplyCh from Raft
// --------------------------------------------------------------------

func (kv *ShardKV) ApplyDb() {
	for{
		select {
		case <- kv.killIt:
			return
		default:
			applymsg := <- kv.applyCh
			kv.mu.Lock()
			if applymsg.SnapshotValid {
				r := bytes.NewBuffer(applymsg.Snapshot)
				d := gob.NewDecoder(r)
				for i := 0; i < shardctrler.NShards; i ++ {
					kv.kvdbs[i] = make(map[string]string)
					kv.clientSeqNum[i] = make(map[int64]int64)
				}
				kv.pullMap = make(map[ShardVer]ServerValid)
				d.Decode(&kv.kvdbs)
				d.Decode(&kv.raftIndex)
				d.Decode(&kv.clientSeqNum)
				d.Decode(&kv.config)
				d.Decode(&kv.shardsVerNum)
				d.Decode(&kv.pullMap)
			} else {
				kv.raftIndex = applymsg.CommandIndex
				kv.createResChan(applymsg.CommandIndex)
				resCh := kv.resChanMap[applymsg.CommandIndex]
				switch op := applymsg.Command.(type) {
				// ------------------- update config op -------------------

				case ShardConfigOp:  // update config
				//在applymsg请求的配置更新的情况下，把新配置下属于本group的
				//如果原来就是这个group的只更新shard版本号，原来不是的，就添加需要pull的shard
				//配置需要更新的情况下，如果
					if op.Config.Num > kv.config.Num {
						for s := 0; s < shardctrler.NShards; s ++ {
							g := op.Config.Shards[s]
							if g == kv.gid { // in charge of this shard in new config
								if kv.shardsVerNum[s] == kv.config.Num {  // in previous config
									kv.shardsVerNum[s] = op.Config.Num  // no need to pull
								} else {
									shardVer := ShardVer{Shard:s, VerNum:kv.config.Num, ConfNum:op.Config.Num}
									oldServer := kv.config.Groups[kv.config.Shards[s]]
									serversValid := ServerValid{Servers: oldServer, Valid: true}
									//pullmap包含了第s个分配，旧得config号，新config号
									kv.pullMap[shardVer] = serversValid
								}
							}
						}
						kv.config = op.Config
					}
					// ------------------- pull shard op -------------------
					//具体拉取shard，更新了对应shard的kvdb，以及clientseqNum,
					// 并且将kv.pullMap的valid设置为false，同时更新shardsVerNum
				case PullShardOp:
					//拉取shard
					if kv.pullMap[op.SV].Valid && op.Ver == kv.config.Num - 1 {
						kv.kvdbs[op.SV.Shard] = make(map[string]string)
						kv.clientSeqNum[op.SV.Shard] = make(map[int64]int64)
						for k, v := range op.KvDb {
							kv.kvdbs[op.SV.Shard][k] = v
						}
						for k, v := range op.CltSqn {
							kv.clientSeqNum[op.SV.Shard][k] = v
						}
						kv.pullMap[op.SV] = ServerValid{Servers: kv.pullMap[op.SV].Servers,
							Valid: false}  // invalid pullMap
						kv.shardsVerNum[op.SV.Shard] = kv.config.Num  // update version number
					}
					// ------------------- delete shard op -------------------
					//删除shard操作，将对应的kvdbs，clientseqnum置空，
					//刷新resch，同时如果根据配置号，写入resCh
				case DeleteShardOp:
					//刷新resch
					DPrintf("DeleteShardOp  localshard:config %v  op:config %v  ",kv.shardsVerNum[op.Shard],op.ConfNum)

					if kv.shardsVerNum[op.Shard] < op.ConfNum {//<=
						
						kv.kvdbs[op.Shard] = make(map[string]string)
						kv.clientSeqNum[op.Shard] = make(map[int64]int64)
						flushChannel(resCh)
						resCh <- ReplyRes{InTransit: false}
					} else {
						flushChannel(resCh)
						//这里之所以设置成正在传输，是为了让pullshard协程重试
						//这个情况出现可能是因为从A到B到C到A，A的配置号反而更新，但是需要删除这个自然不会删除了
						resCh <- ReplyRes{InTransit: true}
					}
					// ------------------- remove pull map op -------------------
					//删除pullmap的数组
				case RemovePullMapOp:
					delete(kv.pullMap, op.SV)
					// ------------------- client request op -------------------
				case Op:  // user request
					// Check shard config
					//如果是属于本group管理的
					//在shard版本号和config号对不上时，说明在传输中，刷新resch
					//如果配置号一致，在数据库中执行操作
					//如果不是这个group管理的，向结果管道写入错误的group
					shard := key2shard(op.Key)
					gid := kv.config.Shards[shard]
					if kv.gid == gid {
						if kv.shardsVerNum[shard] != kv.config.Num {  // during transition
							flushChannel(resCh)
							resCh <- ReplyRes{InTransit: true}
						} else {
							if val, ok := kv.clientSeqNum[shard][op.CltId]; !ok || op.SeqNum > val {
								kv.clientSeqNum[shard][op.CltId] = op.SeqNum
								if op.Request == "Put" {
									kv.kvdbs[shard][op.Key] = op.Value
								} else if op.Request == "Append" {
									kv.kvdbs[shard][op.Key] += op.Value
								} else if op.Request == "Get" {
									// dummy
								}
							}
							flushChannel(resCh)
							resCh <- ReplyRes{Value:kv.kvdbs[shard][op.Key], InOp:op, WrongGroup: false, InTransit: false}
						}
					} else {
						flushChannel(resCh)
						resCh <- ReplyRes{WrongGroup: true}
					}
				}
			}
			kv.mu.Unlock()
			//如果raft的大小大于最大大小的一定比例，就存为快照
			go kv.CheckSnapshot()
		}
	}
}

// --------------------------------------------------------------------
// Snapshots
// --------------------------------------------------------------------

func (kv *ShardKV) CheckSnapshot() {
	if float64(kv.rf.GetRaftStateSize()) / float64(kv.maxraftstate) > MaxRaftFactor {
		kv.SaveSnapshot()
	}
}

func (kv *ShardKV) SaveSnapshot() {
	kv.mu.Lock()

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(kv.kvdbs)
	e.Encode(kv.raftIndex)
	e.Encode(kv.clientSeqNum)
	e.Encode(kv.config)
	e.Encode(kv.shardsVerNum)
	e.Encode(kv.pullMap)
	data := w.Bytes()
	kvrfidx := kv.raftIndex  // preserve this value outside the lock
	kv.mu.Unlock()  // has to unlock here, otherwise deadlock
	kv.rf.Snapshot(kvrfidx,data)
}

func (kv *ShardKV) ReadSnapshot(data []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&kv.kvdbs)
	d.Decode(&kv.raftIndex)
	d.Decode(&kv.clientSeqNum)
	d.Decode(&kv.config)
	d.Decode(&kv.shardsVerNum)
	d.Decode(&kv.pullMap)
}

// --------------------------------------------------------------------
// Response Functions
// --------------------------------------------------------------------
//应对客户端发起的get请求，先交给底层的raft进行处理，然后等待通道结果
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	op := Op{Request: "Get", Key: args.Key, Value: "", CltId:args.CltId, SeqNum: args.SeqNum}
	cmtidx, _, isLeader := kv.rf.Start(op)
	if !isLeader{
		reply.WrongLeader = true
		return
	}
	kv.mu.Lock()
	kv.createResChan(cmtidx)
	resCh := kv.resChanMap[cmtidx]
	kv.mu.Unlock()
	select{
	case res := <- resCh:
		if res.WrongGroup {
			reply.Err = ErrWrongGroup
		} else if res.InTransit {
			reply.Err = ErrInTransit
		} else if reflect.DeepEqual(op, res.InOp) {
			reply.Value = res.Value
			reply.Err = OK
		} else{
			reply.WrongLeader = true
		}
	case <- time.After(ResChanTimeout * time.Millisecond): // RPC timeout
		reply.WrongLeader = true
	}
}
//与get类似
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{Request: args.Op, Key: args.Key, Value: args.Value, CltId:args.CltId, SeqNum: args.SeqNum}
	cmtidx, _, isLeader := kv.rf.Start(op)
	if !isLeader{
		reply.WrongLeader = true
		return
	}
	kv.mu.Lock()
	kv.createResChan(cmtidx)
	resCh := kv.resChanMap[cmtidx]
	kv.mu.Unlock()
	select{
	case res := <- resCh:
		if res.WrongGroup {
			reply.Err = ErrWrongGroup
		} else if res.InTransit {
			reply.Err = ErrInTransit
		} else if reflect.DeepEqual(op, res.InOp) {
			reply.Err = OK
		} else{
			reply.WrongLeader = true
		}
	case <- time.After(ResChanTimeout * time.Millisecond): // RPC timeout
		reply.WrongLeader = true
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	close(kv.killIt)
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	//这里使用了gob.Register(Inner{})告诉系统：所有的Interface是有可能为Inner结构的。
	gob.Register(Op{})
	gob.Register(ShardConfigOp{})
	gob.Register(PullShardOp{})
	gob.Register(DeleteShardOp{})
	gob.Register(RemovePullMapOp{})
	gob.Register(GetArgs{})
	gob.Register(GetReply{})
	gob.Register(PutAppendArgs{})
	gob.Register(PutAppendReply{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters
	// Use something like this to talk to the shardmaster:
	kv.mck = shardctrler.MakeClerk(kv.masters)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	//key value database初始化
	kv.kvdbs = make([]map[string]string, shardctrler.NShards)
	kv.clientSeqNum = make([]map[int64]int64, shardctrler.NShards)
	//假设数据为10个分片
	for i := 0; i < shardctrler.NShards; i ++ {
		kv.kvdbs[i] = make(map[string]string)
		kv.clientSeqNum[i] = make(map[int64]int64)
	}
	kv.raftIndex = 0
	kv.resChanMap = make(map [int] chan ReplyRes)
	kv.killIt = make(chan bool)
	//获取初始的配置
	kv.config = kv.mck.Query(0)  // get the initial config
	kv.shardsVerNum = make([]int, shardctrler.NShards)
	kv.pullMap = make(map[ShardVer]ServerValid)
	kv.ReadSnapshot(persister.ReadSnapshot())

	kv.pcTimer = time.NewTimer(time.Duration(PollConfigTimeout)* time.Millisecond)
	kv.psTimer = time.NewTimer(time.Duration(PollShardsTimeout)* time.Millisecond)
	
	go kv.PullConfig()
	go kv.PullShards()
	go kv.ApplyDb()
	return kv
}