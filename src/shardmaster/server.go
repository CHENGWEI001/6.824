package shardmaster

import "raft"
import "labrpc"
import "sync"
import "labgob"
import "log"
import "time"
import "fmt"

// import "encoding/gob"

const AwaitLeaderCheckInterval = 10 * time.Millisecond

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	// gidMap         map[int]GidInfo      // gid -> gidInfo
	lastAppliedReq map[int64]Op         // to store last requestId from each client, here assume one client would send one reuqest at a time
	pendingQ       map[int]*SendMsgArgs // to store cmd send to RAFT but not yet complete
	toStopChan     chan bool            // channel for stopping the raft instance thread
	toStop         bool                 // indicator for stopping raft instance
	lastApplyMsg   *raft.ApplyMsg       // the last ApplyMsg
	sendMsgJobQ    []*SendMsgArgs       // SendMsg job queue , will be processed by kv main thread when it is idle
	sendMsgChan    chan *SendMsgArgs    // channel for receiving new msg from client

	configs []Config // indexed by config num
}

type SendMsgResult struct {
	Valid bool
	Reply interface{}
}

type SendMsgArgs struct {
	Command   Op
	ResChan   chan *SendMsgResult
	ExpCmtIdx int
}

type SendMsgReply struct {
	WrongLeader bool
	ReqId       int64
	Reply       interface{}
	ClientId    int64
	Err         Err
}

type GidInfo struct {
	Gid     int
	Servers []string
	Shards  []int
}

type OpCode string

const (
	JOIN  OpCode = "JOIN"
	LEAVE        = "LEAVE"
	MOVE         = "MOVE"
	QUERY        = "QUERY"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your data here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Oc       OpCode
	Args     interface{}
	Reply    interface{}
	ReqId    int64
	ClientId int64
}

// the worker handler to send new request msg to main thread
func (sm *ShardMaster) sendMsgTask(args *SendMsgArgs, reply *SendMsgReply) {
	startAt := time.Now().UnixNano()
	DPrintf("[sm:%v]start SendMsgTask: startAt:%+v, args%+v\n", sm.me, startAt, args)
	timer := time.After(0)
	// 	timeOutTimer := time.After(SendMsgTaskMaxWaitLimit)
	msgSent := false
	for {
		select {
		case res := <-args.ResChan:
			reply.WrongLeader = !res.Valid
			reply.ReqId = args.Command.ReqId
			reply.Reply = res.Reply
			reply.ClientId = args.Command.ClientId
			goto End
		case <-timer:
			_, isLeader := sm.rf.GetState()
			if !isLeader {
				reply.WrongLeader = true
				goto End
			}
			if !msgSent {
				// make buffered channel to prevent dead lock for below scenario
				// kv received sendMsg and call sendMsgToRaft, it call rf.start()
				// when rf.start is done, it will try to send to reply chan make here
				// but due to in this sendMsgTask API, it will change resChan every interval,
				// the old reschan won't have handler to take it, cause KV thread stuck
				args.ResChan = make(chan *SendMsgResult, 2)
				sm.sendMsgChan <- args
				msgSent = true
			}
			timer = time.After(AwaitLeaderCheckInterval)
			// 		case <-timeOutTimer:
			// 			panic(fmt.Sprintf("[kv:%v]start SendMsgTask: startAt:%+v, args%+v not complete in %+v, now:%+v\n",
			// 				kv.me, startAt, args, SendMsgTaskMaxWaitLimit, time.Now()))
		}
	}
End:
	// close(s.ReplyChan)
	if reply.WrongLeader {
		if !msgSent {
			reply.Err = Err(fmt.Sprintf("[sm:%v] is not leader\n", sm.me))
		} else {
			// don't remove from pendingQ, test easier get stuck if do this
			// I am guessing it might remove wrong item
			// 			kv.RemoveFromPendingQChan <- args
			reply.Err = Err(fmt.Sprintf("[sm:%v] was leader not anymore\n", sm.me))
		}
	}
	reply.Err = Err(fmt.Sprintf("[sm:%v] OK", sm.me))
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	DPrintf("[sm:%v][Join]: args:%+v\n", sm.me, args)
	defer DPrintf("[sm:%v][Join]: reply:%+v\n", sm.me, reply)
	s := SendMsgArgs{
		Command: Op{
			Oc:       JOIN,
			Args:     *args,
			ReqId:    args.ReqId,
			ClientId: args.ClientId,
		},
		ResChan:   nil,
		ExpCmtIdx: -1,
	}
	r := SendMsgReply{}
	sm.sendMsgTask(&s, &r)
	if !r.WrongLeader {
		*reply = r.Reply.(JoinReply)
	} else {
		reply.WrongLeader = r.WrongLeader
		reply.ReqId = r.ReqId
		reply.ClientId = r.ClientId
		reply.Err = r.Err
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	DPrintf("[sm:%v][Leave]: args:%+v\n", sm.me, args)
	defer DPrintf("[sm:%v][Leave]: reply:%+v\n", sm.me, reply)
	s := SendMsgArgs{
		Command: Op{
			Oc:       LEAVE,
			Args:     *args,
			ReqId:    args.ReqId,
			ClientId: args.ClientId,
		},
		ResChan:   nil,
		ExpCmtIdx: -1,
	}
	r := SendMsgReply{}
	sm.sendMsgTask(&s, &r)
	if !r.WrongLeader {
		*reply = r.Reply.(LeaveReply)
	} else {
		reply.WrongLeader = r.WrongLeader
		reply.ReqId = r.ReqId
		reply.ClientId = r.ClientId
		reply.Err = r.Err
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	DPrintf("[sm:%v][Move]: args:%+v\n", sm.me, args)
	defer DPrintf("[sm:%v][Move]: reply:%+v\n", sm.me, reply)
	s := SendMsgArgs{
		Command: Op{
			Oc:       MOVE,
			Args:     *args,
			ReqId:    args.ReqId,
			ClientId: args.ClientId,
		},
		ResChan:   nil,
		ExpCmtIdx: -1,
	}
	r := SendMsgReply{}
	sm.sendMsgTask(&s, &r)
	if !r.WrongLeader {
		*reply = r.Reply.(MoveReply)
	} else {
		reply.WrongLeader = r.WrongLeader
		reply.ReqId = r.ReqId
		reply.ClientId = r.ClientId
		reply.Err = r.Err
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	DPrintf("[sm:%v][Query]: args:%+v\n", sm.me, args)
	defer DPrintf("[sm:%v][Query]: reply:%+v\n", sm.me, reply)
	s := SendMsgArgs{
		Command: Op{
			Oc:       QUERY,
			Args:     *args,
			ReqId:    args.ReqId,
			ClientId: args.ClientId,
		},
		ResChan:   nil,
		ExpCmtIdx: -1,
	}
	r := SendMsgReply{}
	sm.sendMsgTask(&s, &r)
	if !r.WrongLeader {
		*reply = r.Reply.(QueryReply)
	} else {
		reply.WrongLeader = r.WrongLeader
		reply.ReqId = r.ReqId
		reply.ClientId = r.ClientId
		reply.Err = r.Err
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	//DPrintf("[KV:%v][Kill]: %+v\n", kv.me, kv)
	go func(sm *ShardMaster) {
		sm.toStopChan <- true
	}(sm)
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) joinHandler(args *JoinArgs, reply *JoinReply) {
	DPrintf("[sm:%v]start joinHandler: args:%+v, config:%+v\n", sm.me, args, sm.configs)

	newConfig := sm.configs[len(sm.configs)-1].Clone()
	newConfig.Num += 1

	for gid, servers := range args.Servers {
		newConfig.Groups[gid] = servers
	}

	sm.rebalanceShards(&newConfig)

	sm.configs = append(sm.configs, newConfig)
	reply.WrongLeader = false
	reply.ReqId = args.ReqId
	reply.ClientId = args.ClientId
	DPrintf("[sm:%v]done joinHandler: reply:%+v, config:%+v\n", sm.me, reply, sm.configs)
}

func (sm *ShardMaster) leaveHandler(args *LeaveArgs, reply *LeaveReply) {
	DPrintf("[sm:%v]start leaveHandler: args:%+v, config:%+v\n", sm.me, args, sm.configs)

	newConfig := sm.configs[len(sm.configs)-1].Clone()
	newConfig.Num += 1

	for _, gid := range args.GIDs {
		for shardNum, assignedGID := range newConfig.Shards {
			if gid == assignedGID {
				newConfig.Shards[shardNum] = 0 // Assign to invalid GID
			}
			delete(newConfig.Groups, gid)
		}
	}

	sm.rebalanceShards(&newConfig)

	sm.configs = append(sm.configs, newConfig)
	reply.WrongLeader = false
	reply.ReqId = args.ReqId
	reply.ClientId = args.ClientId
	DPrintf("[sm:%v]done leaveHandler: reply:%+v, config:%+v\n", sm.me, reply, sm.configs)
}

func (sm *ShardMaster) moveHandler(args *MoveArgs, reply *MoveReply) {
	DPrintf("[sm:%v]start applyMove: args:%+v, config:%+v\n", sm.me, args, sm.configs)

	newConfig := sm.configs[len(sm.configs)-1].Clone()
	newConfig.Num += 1
	newConfig.Shards[args.Shard] = args.GID

	sm.configs = append(sm.configs, newConfig)
	reply.WrongLeader = false
	reply.ReqId = args.ReqId
	reply.ClientId = args.ClientId
	DPrintf("[sm:%v]done applyMove: reply:%+v, config:%+v\n", sm.me, reply, sm.configs)
}

func (sm *ShardMaster) queryHandler(args *QueryArgs, reply *QueryReply) {
	DPrintf("[sm:%v]start queryHandler: args:%+v, config:%+v\n", sm.me, args, sm.configs)
	tgtNum := args.Num
	if tgtNum == -1 || tgtNum >= len(sm.configs) {
		// panic(fmt.Sprintf("[sm:%v][queryHandler] args.Num:%v is over len(sm.configs):%v!\n", sm.me, args.Num, len(sm.configs)))
		tgtNum = len(sm.configs) - 1
	}
	reply.WrongLeader = false
	reply.Config = sm.configs[tgtNum]
	reply.ReqId = args.ReqId
	reply.ClientId = args.ClientId
	DPrintf("[sm:%v]done queryHandler: reply:%+v, config:%+v\n", sm.me, reply, sm.configs)
}

// Note: This is a tad gnarly.
func (sm *ShardMaster) rebalanceShards(config *Config) {
	// if there is no any groups, not need to proceed rebalance
	if len(config.Groups) == 0 {
		return
	}
	minShardsPerGroup := NShards / len(config.Groups) // Minimum shard count per group member

	distribution := make(map[int][]int) // Find the current distribution of GID -> shards
	unassignedShards := make([]int, 0)

	for gid := range config.Groups {
		distribution[gid] = make([]int, 0)
	}

	for shard, gid := range config.Shards {
		if gid != 0 {
			distribution[gid] = append(distribution[gid], shard)
		} else {
			unassignedShards = append(unassignedShards, shard)
		}
	}

	areShardsBalanced := func() bool { // Helper to determine if all shards are assigned + balanced across groups
		sum := 0
		for gid := range distribution {
			shardCount := len(distribution[gid])
			if shardCount < minShardsPerGroup {
				return false
			}
			sum += shardCount
		}
		return sum == NShards
	}

	// Distribute shards such that they're balanced across groups
	for !areShardsBalanced() {
		for gid := range distribution {
			// If this group is under capacity, add as many shards as we can to it from the unassigned
			for len(unassignedShards) > 0 && len(distribution[gid]) < minShardsPerGroup {
				distribution[gid] = append(distribution[gid], unassignedShards[0])
				unassignedShards = unassignedShards[1:]
			}

			// If there aren't any unassigned shards and group is under-capacity, "steal" shard from an over-capacity group
			if len(unassignedShards) == 0 && len(distribution[gid]) < minShardsPerGroup {
				for gid2 := range distribution {
					if len(distribution[gid2]) > minShardsPerGroup {
						distribution[gid] = append(distribution[gid], distribution[gid2][0])
						distribution[gid2] = distribution[gid2][1:]
						break
					}
				}
			}
		}

		// If we still have unassigned shards, assign them one by one to each group
		for gid := range distribution {
			if len(unassignedShards) > 0 {
				distribution[gid] = append(distribution[gid], unassignedShards[0])
				unassignedShards = unassignedShards[1:]
			}
		}
	}

	// Assign distribution to config
	for gid, shards := range distribution {
		for _, i := range shards {
			config.Shards[i] = gid
		}
	}
}

func (sm *ShardMaster) opHandler(op *Op) {
	switch op.Oc {
	case JOIN:
		args := op.Args.(JoinArgs)
		reply := JoinReply{}
		sm.joinHandler(&args, &reply)
		op.Reply = reply
	case LEAVE:
		args := op.Args.(LeaveArgs)
		reply := LeaveReply{}
		sm.leaveHandler(&args, &reply)
		op.Reply = reply
	case MOVE:
		args := op.Args.(MoveArgs)
		reply := MoveReply{}
		sm.moveHandler(&args, &reply)
		op.Reply = reply
	case QUERY:
		args := op.Args.(QueryArgs)
		reply := QueryReply{}
		sm.queryHandler(&args, &reply)
		op.Reply = reply
	default:
		panic(fmt.Sprintf("[sm:%v][opHandler] invalid op.Oc:%+v, op:%+v!\n", sm.me, op.Oc, op))
	}
}

func (sm *ShardMaster) sendMsgToRaft(msg *SendMsgArgs) {
	// the reason here we need to use lock is to have atomic operation for :
	// rf.start() then adding to pendingQ, if not , we might have potential issue that item add to
	// rf log and complete applych , but pendingQ not yet go into pendingQ
	startAt := time.Now().UnixNano()
	DPrintf("[sm:%v]start sendMsgToRaft: startAt:%+v, msg:%+v\n", sm.me, startAt, msg)
	index, _, isLeader := sm.rf.Start(msg.Command)
	if !isLeader {
		msg.ResChan <- &SendMsgResult{
			Valid: false,
		}
	} else {
		msg.ExpCmtIdx = index
		sm.pendingQ[msg.ExpCmtIdx] = msg
	}
	DPrintf("[sm:%v]done sendMsgToRaft: startAt:%+v, index:%+v, isLeader:%+v\n", sm.me, startAt, index, isLeader)
}

func (sm *ShardMaster) processingSendMsgJobQ() {
	DPrintf("[sm:%v] processingSendMsgJobQ: start len(sm.sendMsgJobQ):%+v\n", sm.me, len(sm.sendMsgJobQ))
	for len(sm.sendMsgJobQ) > 0 {
		sm.sendMsgToRaft(sm.sendMsgJobQ[0])
		sm.sendMsgJobQ = sm.sendMsgJobQ[1:]
	}
	DPrintf("[sm:%v] processingSendMsgJobQ: end len(sm.sendMsgJobQ):%+v\n", sm.me, len(sm.sendMsgJobQ))
}

func (sm *ShardMaster) startSmThread() {
	DPrintf("[sm:%v]start of Thread: %+v\n", sm.me, sm)
	defer DPrintf("[sm:%v]End of Thread: %+v\n", sm.me, sm)
	for {
		DPrintf("[sm:%v]start of for loop sm:%+v\n", sm.me, sm)
		select {
		case SendMsg := <-sm.sendMsgChan:
			DPrintf("[sm:%v] received SendMsg: SendMsg:%+v\n", sm.me, SendMsg)
			// since each request from client is sending serialize(it must complete one before send the next one for the same client)
			// it is okay to check lastReq only to see new rquest is the same as last ReqId or not
			if lastReqOp, ok := sm.lastAppliedReq[SendMsg.Command.ClientId]; ok && lastReqOp.ReqId == SendMsg.Command.ReqId {
				SendMsg.ResChan <- &SendMsgResult{
					Valid: true,
					Reply: lastReqOp.Reply,
				}
			} else {
				// if this Msg is new request, we need to open up a new routine to push this
				// to raft instance and push into KV pendingQ, the purpose of pendingQ is to
				// know which client Requst to reply once it is commited by Raft instance
				sm.sendMsgJobQ = append(sm.sendMsgJobQ, SendMsg)
				sm.processingSendMsgJobQ()
			}
			DPrintf("[sm:%v]done handling SendMsg SendMsg:%+v, sm:%+v\n", sm.me, SendMsg, sm)
		case applyCh := <-sm.applyCh:
			DPrintf("[sm:%v]received applyCh: applyCh:%+v\n", sm.me, applyCh)
			if !applyCh.CommandValid {
				continue
			}
			op := applyCh.Command.(Op)
			sm.opHandler(&op)
			sm.lastAppliedReq[op.ClientId] = op
			sm.lastApplyMsg = &applyCh
			// if there is pending item, we need to reply it
			if msg, ok := sm.pendingQ[applyCh.CommandIndex]; ok {
				if msg.Command.ReqId != op.ReqId {
					// seems like it might be the case like KV:0 is leader in the beginning, and those are not committed
					// then partition down, later go out of partition and those cmdIdx are changed to different reqId
					//panic(fmt.Sprintf("[KV:%v][applyCh] reqId not matching! %v != %v!\n", kv.me, msg.Args.Command.ReqId, op.ReqId))
				} else {
					res := &SendMsgResult{
						Valid: true,
						Reply: op.Reply,
					}
					// todo : to make it blocking call?
					go func(args *SendMsgArgs, res *SendMsgResult) {
						args.ResChan <- res
					}(msg, res)
				}
				delete(sm.pendingQ, applyCh.CommandIndex)
			}
			DPrintf("[sm:%v]done handling applyCh applyCh:%+v, sm:%+v\n", sm.me, applyCh, sm)
		case <-sm.toStopChan:
			DPrintf("[sm:%v]received ToStopChan\n", sm.me)
			sm.toStop = true
		}
		if sm.toStop {
			return
		}
		DPrintf("[sm:%v]end of for loop sm:%+v\n", sm.me, sm)
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	// need to register structure into gob otherwise would hit EOF err
	// https://stackoverflow.com/questions/33073434/gob-decoder-returning-eof-error
	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(QueryArgs{})
	labgob.Register(JoinReply{})
	labgob.Register(MoveReply{})
	labgob.Register(LeaveReply{})
	labgob.Register(QueryReply{})

	sm.pendingQ = make(map[int]*SendMsgArgs)
	sm.lastAppliedReq = make(map[int64]Op)
	sm.sendMsgChan = make(chan *SendMsgArgs)
	sm.toStopChan = make(chan bool)
	sm.toStop = false
	sm.sendMsgJobQ = make([]*SendMsgArgs, 0)
	// sm.gidMap = make(map[int]GidInfo)
	DPrintf("[sm:%v] Make sm:%+v\n", sm.me, sm)

	go sm.startSmThread()

	return sm
}
