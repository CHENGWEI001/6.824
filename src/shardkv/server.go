package shardkv

import "shardmaster"
import "labrpc"
import "raft"
import "sync"
import "labgob"
import "time"
import "bytes"
import "log"
import "fmt"
import "strconv"

const AwaitLeaderCheckInterval = 10 * time.Millisecond

// const AwaitCheckSnapshotPendingIntervalMillisecond = 5 * time.Millisecond
const SnapshotSizeTolerancePercentage = 5
const SnapShotCheckIntervalMillisecond = 50 * time.Millisecond
const SendMsgTaskMaxWaitLimit = 5000 * time.Millisecond
const CheckConfigInterval = 100 * time.Millisecond
const LogMasterOnly = true
const Debug = 0
const Debug2 = 0

func testDPrintf(format string, a ...interface{}) (n int, err error) {
	// if Debug > 0 {
	// log.Printf("test: "+format, a...)
	// fmt.Printf("test: "+format, a...)

	// }
	return
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func DPrintf2(format string, a ...interface{}) (n int, err error) {
	if Debug2 > 0 {
		log.Printf(format, a...)
	}
	return
}

func (kv *ShardKV) StateDescription() string {
	if kv.rf != nil {
		if _, isLeader := kv.rf.GetState(); isLeader {
			return "Master"
		}
	}
	return "Replica"
}

func kvInfo(format string, kv *ShardKV, a ...interface{}) {
	if Debug > 0 {
		state := kv.StateDescription()
		if LogMasterOnly && state != "Master" {
			return
		}
		// state := "N/A"
		args := append([]interface{}{kv.gid, kv.me, state, kv.latestConfig.Num}, a...)
		log.Printf("[INFO] KV Server: [GId: %d, Id: %d, %s, Config: %d] "+format, args...)
	}
}

type SendMsgResult struct {
	Valid bool
	Value string
	Err   Err
}

type SendMsgArgs struct {
	Command   Op
	ResChan   chan *SendMsgResult
	ExpCmtIdx int
}

type SendMsgReply struct {
	WrongLeader bool
	ReqId       int64
	Value       string
	ClientId    int64
	Err         Err
}

type SendShardMsgArgs struct {
	args    *SendShardArgs
	ResChan chan *SendShardMsgResult
}

type SendShardMsgResult struct {
	WrongLeader bool
	Err         Err
}

type OpCode string

const (
	GET    OpCode = "GET"
	PUT           = "PUT"
	APPEND        = "APPEND"
)

type KVRaftPersistence struct {
	LastAppliedReq map[int]map[int64]int64
	State          map[int]map[string]string
	ShardStatus    map[int]ShardState
	LastApplyMsg   raft.ApplyMsg
	LatestConfig   shardmaster.Config
}

type CommandType int

const (
	Put CommandType = iota
	Append
	Get
	ConfigUpdate
	ShardTransfer
	MigrationComplete
)

type Op interface {
	getCommand() CommandType
}

type ClientOp struct {
	Command  CommandType
	Key      string
	Value    string
	ReqId    int64
	ClientId int64
}

func (op ClientOp) getCommand() CommandType {
	return op.Command
}

type ConfigOp struct {
	Command CommandType
	Config  shardmaster.Config
}

func (op ConfigOp) getCommand() CommandType {
	return op.Command
}

type ShardTransferOp struct {
	Command        CommandType
	Num            int
	Shard          map[string]string
	LatestRequests map[int64]int64
}

type CfgLis struct {
	ToStop       bool
	latestConfig shardmaster.Config
}

func (op ShardTransferOp) getCommand() CommandType {
	return op.Command
}

type ShardState string

const (
	NotStored     ShardState = "NotStored"
	Available                = "Avail"
	MigratingData            = "Migrate"
	AwaitingData             = "Await"
)

type SendShardArgs struct {
	Num        int
	Data       map[string]string
	LatestReqs map[int64]int64
	Config     shardmaster.Config
	FromServer string
	ToServer   string
}

type SendShardReply struct {
	IsLeader   bool
	Err        Err
	FromServer string
	ToServer   string
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	// LastAppliedReq        map[int64]int64           // to store last requestId from each client, here assume one client would send one reuqest at a time
	PendingQ    map[int]*SendMsgArgs // to store cmd send to RAFT but not yet complete
	SendMsgChan chan *SendMsgArgs    // channel for receiving new msg from client
	// State                 map[string]string
	State                 map[int]map[string]string // Shard ID (int) -> Shard key-values (string -> string)
	LastAppliedReq        map[int]map[int64]int64   // Shard ID (int) -> Last request key-values (Client ID -> Request ID)
	ShardStatus           map[int]ShardState
	ToStopChan            chan bool              // channel for stopping the raft instance thread
	ToStop                bool                   // indicator for stopping raft instance
	persister             *raft.Persister        // persister
	LastApplyMsg          *raft.ApplyMsg         // the last ApplyMsg
	CreateSnapshotPending bool                   // indicating wether any pending CreateSnapshot ongoing
	SendMsgJobQ           []*SendMsgArgs         // SendMsg job queue , will be processed by kv main thread when it is idle
	latestConfig          shardmaster.Config     // configuration query from shardmaster
	mck                   *shardmaster.Clerk     // shardmaster Clerk instance
	SendShardMsgChan      chan *SendShardMsgArgs // channel for receiving SendShardMsg
	serverName            string                 // the server name
	cfgLisChan            chan CfgLis            // channel to talk to configListener
}

// the worker handler to send new request msg to KV raft main thread
func (kv *ShardKV) SendMsgClientOpTask(args *SendMsgArgs, reply *SendMsgReply) {
	startAt := time.Now().UnixNano()
	DPrintf("[kv:%v]start SendMsgClientOpTask: startAt:%+v, args%+v\n", kv.me, startAt, args)
	timer := time.After(0)
	timeOutTimer := time.After(SendMsgTaskMaxWaitLimit)
	msgSent := false
	op := args.Command.(ClientOp)
	// whenever Reply return with valid, kv.State should already been updated with latest
	for {
		select {
		case res := <-args.ResChan:
			reply.WrongLeader = false
			reply.ReqId = op.ReqId
			reply.Value = res.Value
			reply.ClientId = op.ClientId
			reply.Err = res.Err
			goto End
		case <-timer:
			_, isLeader := kv.rf.GetState()
			if !isLeader {
				reply.WrongLeader = true
				if !msgSent {
					reply.Err = Err(fmt.Sprintf("[kv:%v] is not leader", kv.me))
				} else {
					// don't remove from pendingQ, test easier get stuck if do this
					// I am guessing it might remove wrong item
					// 			kv.RemoveFromPendingQChan <- args
					reply.Err = Err(fmt.Sprintf("[kv:%v] was leader not anymore", kv.me))
				}
				goto End
			}
			if !msgSent {
				// if there is pendingSnapshot, wait until it is done
				// for kv.PendingCreateSnapShot() {
				// 	time.Sleep(AwaitCheckSnapshotPendingIntervalMillisecond)
				// }

				// make buffered channel to prevent dead lock for below scenario
				// kv received sendMsg and call sendMsgToRaft, it call rf.start()
				// when rf.start is done, it will try to send to reply chan make here
				// but due to in this sendMsgTask API, it will change resChan every interval,
				// the old reschan won't have handler to take it, cause KV thread stuck
				args.ResChan = make(chan *SendMsgResult, 2)
				kv.SendMsgChan <- args
				msgSent = true
			}
			timer = time.After(AwaitLeaderCheckInterval)
		case <-timeOutTimer:
			panic(fmt.Sprintf("[kv:%v]start SendMsgClientOpTask: startAt:%+v, args%+v not complete in %+v, now:%+v, kv:%+v\n",
				kv.me, startAt, args, SendMsgTaskMaxWaitLimit, time.Now(), kv))
		}
	}
End:
	// close(s.ReplyChan)
	// if reply.WrongLeader {
	// 	if !msgSent {
	// 		reply.Err = Err(fmt.Sprintf("[kv:%v] is not leader\n", kv.me))
	// 	} else {
	// 		// don't remove from pendingQ, test easier get stuck if do this
	// 		// I am guessing it might remove wrong item
	// 		// 			kv.RemoveFromPendingQChan <- args
	// 		reply.Err = Err(fmt.Sprintf("[kv:%v] was leader not anymore\n", kv.me))
	// 	}
	// }
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf2("[kv:%v][Get]: GetArgs:%+v\n", kv.serverName, args)
	defer DPrintf2("[kv:%v][Get]: GetReply:%+v\n", kv.serverName, reply)
	s := SendMsgArgs{
		Command: ClientOp{
			Command:  Get,
			Key:      args.Key,
			ReqId:    args.ReqId,
			ClientId: args.ClientId,
		},
		ResChan:   nil,
		ExpCmtIdx: -1,
	}
	r := SendMsgReply{}
	kv.SendMsgClientOpTask(&s, &r)
	reply.WrongLeader = r.WrongLeader
	reply.ReqId = r.ReqId
	reply.Value = r.Value
	reply.ClientId = r.ClientId
	reply.Err = r.Err
	if reply.WrongLeader {
		reply.ReqId = args.ReqId
		reply.ClientId = args.ClientId
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf2("[kv:%v][PutAppend]: PutAppendArgs:%+v\n", kv.serverName, args)
	defer DPrintf2("[kv:%v][PutAppend]: PutAppendReply:%+v\n", kv.serverName, reply)
	var cmd CommandType
	if args.Op == "Put" {
		cmd = Put
	} else if args.Op == "Append" {
		cmd = Append
	} else {
		panic(fmt.Sprintf("[KV:%v][PutAppend] un-expected args.Op %+v!\n", kv.me, args.Op))
	}
	s := SendMsgArgs{
		Command: ClientOp{
			Command:  cmd,
			Key:      args.Key,
			Value:    args.Value,
			ReqId:    args.ReqId,
			ClientId: args.ClientId,
		},
	}
	r := SendMsgReply{}
	kv.SendMsgClientOpTask(&s, &r)
	reply.WrongLeader = r.WrongLeader
	reply.ReqId = r.ReqId
	reply.ClientId = r.ClientId
	reply.Err = r.Err
	reply.Value = r.Value
	if reply.WrongLeader {
		reply.ReqId = args.ReqId
		reply.ClientId = args.ClientId
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
	// Your code here, if desired.
	//DPrintf("[KV:%v][Kill]: %+v\n", kv.me, kv)
	go func(kv *ShardKV) {
		kv.ToStopChan <- true
	}(kv)
}

func (kv *ShardKV) createSnapshot() {
	// DPrintf2("[kv:%v]start createSnapshot: RaftStateSize:%v\n", kv.serverName, kv.persister.RaftStateSize())
	kvInfo("start createSnapshot: \n", kv)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	p := KVRaftPersistence{
		State:          kv.State,
		LastAppliedReq: kv.LastAppliedReq,
		LastApplyMsg:   *kv.LastApplyMsg,
		ShardStatus:    kv.ShardStatus,
		LatestConfig:   kv.latestConfig,
	}
	e.Encode(p)
	data := w.Bytes()
	kv.SetCreateSnapShot()
	kv.rf.CreateSnapshot(data, kv.LastApplyMsg)
	// DPrintf2("[kv:%v]done createSnapshot: RaftStateSize:%v, data:%+v\n", kv.serverName, kv.persister.RaftStateSize(), data)
	kvInfo("done createSnapshot: p:%+v\n", kv, p)
}

func (kv *ShardKV) SetCreateSnapShot() {
	// 	kv.Lock()
	kv.CreateSnapshotPending = true
	// 	kv.Unlock()
}

func (kv *ShardKV) UnsetCreateSnapShot() {
	// 	kv.Lock()
	kv.CreateSnapshotPending = false
	// 	kv.Unlock()
}

func (kv *ShardKV) PendingCreateSnapShot() (status bool) {
	// 	kv.Lock()
	status = kv.CreateSnapshotPending
	// 	kv.Unlock()
	return
}

func (kv *ShardKV) SendMsgToRaft(msg *SendMsgArgs) {
	// the reason here we need to use lock is to have atomic operation for :
	// rf.start() then adding to pendingQ, if not , we might have potential issue that item add to
	// rf log and complete applych , but pendingQ not yet go into pendingQ
	// 	kv.Lock()
	startAt := time.Now().UnixNano()
	DPrintf("[kv:%v]start SendMsgToRaft: startAt:%+v, msg:%+v\n", kv.me, startAt, msg)
	index, _, isLeader := kv.rf.Start(msg.Command)
	if !isLeader {
		msg.ResChan <- &SendMsgResult{
			Valid: false,
		}
	} else {
		msg.ExpCmtIdx = index
		// 		kv.AddToPendingQChan <- msg
		kv.PendingQ[msg.ExpCmtIdx] = msg
	}
	DPrintf("[kv:%v]done SendMsgToRaft: startAt:%+v, index:%+v, isLeader:%+v\n", kv.me, startAt, index, isLeader)
	//kv.Unlock()
}

// return
// true : not over the limit
// false : close to limit, sent snapshot request to rf
func (kv *ShardKV) CheckRaftStateSize() (status bool) {
	status = true
	if kv.maxraftstate == -1 {
		return
	}
	DPrintf("[kv:%v] CheckRaftStateSize: RaftStateSize:%v, maxraftstate:%v\n",
		kv.me, kv.persister.RaftStateSize(), kv.maxraftstate)
	// 	if kv.persister.RaftStateSize() > kv.maxraftstate {
	// 		panic(fmt.Sprintf("[kv:%v] CheckRaftStateSize: RaftStateSize:%v is over the limit:%v! DecodeState:%+v, kv:%+v\n",
	// 			kv.me, kv.persister.RaftStateSize(), kv.maxraftstate, kv.rf.DecodeState(kv.persister.ReadRaftState()), kv))
	// 	}
	if !kv.PendingCreateSnapShot() &&
		(kv.maxraftstate-kv.persister.RaftStateSize())*100/kv.maxraftstate < SnapshotSizeTolerancePercentage &&
		kv.LastApplyMsg != nil {
		kv.createSnapshot()
		status = false
	}
	return
}

func (kv *ShardKV) specialApplyMsgHandler(applyCh *raft.ApplyMsg) {
	switch applyCh.Code {
	case raft.DONE_INSTALL_SNAPSHOT:
		kv.ReadSnapshot(kv.persister.ReadSnapshot())
	case raft.DONE_CREATE_SNAPSHOT:
		kv.UnsetCreateSnapShot()
		kv.processingSendMsgJobQ()
	case raft.TO_CHECCK_STATE_SIZE:
		kv.CheckRaftStateSize()
	default:
		panic(fmt.Sprintf("[kv:%v] specialApplyMsgHandler: invalid Code:%+v! applyCh:%+v, kv:%+v\n",
			kv.me, applyCh.Code, applyCh, kv))
	}
}

func (kv *ShardKV) processingSendMsgJobQ() {
	DPrintf("[kv:%v] processingSendMsgJobQ: start len(kv.SendMsgJobQ):%+v\n", kv.me, len(kv.SendMsgJobQ))
	// 	for !kv.PendingCreateSnapShot() && len(kv.SendMsgJobQ) > 0 && kv.CheckRaftStateSize() {
	for len(kv.SendMsgJobQ) > 0 {
		kv.SendMsgToRaft(kv.SendMsgJobQ[0])
		kv.SendMsgJobQ = kv.SendMsgJobQ[1:]
	}
	DPrintf("[kv:%v] processingSendMsgJobQ: end len(kv.SendMsgJobQ):%+v\n", kv.me, len(kv.SendMsgJobQ))
}

func (kv *ShardKV) applyClientOp(applyCh *raft.ApplyMsg) {
	// only need to touch state when opCode is PUT, do nothing for GET
	op := applyCh.Command.(ClientOp)
	shardId := key2shard(op.Key)
	shardStatus := kv.ShardStatus[shardId]
	var valid bool
	var Err Err
	var value string
	kvInfo("applyClientOp: applyCh:%+v, shardId:%v, shardStatus:%v", kv, applyCh, shardId, shardStatus)
	if shardStatus == Available {
		if kv.LastAppliedReq[shardId][op.ClientId] != op.ReqId {
			if op.Command == Put {
				kv.State[shardId][op.Key] = op.Value
			} else if op.Command == Append {
				kv.State[shardId][op.Key] = kv.State[shardId][op.Key] + op.Value
			}
		}
		if val, ok := kv.State[shardId][op.Key]; ok {
			value = val
			Err = OK
		} else {
			Err = ErrNoKey
		}
		valid = true
		kv.LastAppliedReq[shardId][op.ClientId] = op.ReqId
	} else {
		valid = false
		if shardStatus == MigratingData || shardStatus == AwaitingData {
			Err = ErrMovingShard
		} else {
			Err = ErrWrongGroup
			value = strconv.Itoa(kv.latestConfig.Shards[key2shard(op.Key)])
		}
	}

	// if there is pending item, we need to reply it
	if msg, ok := kv.PendingQ[applyCh.CommandIndex]; ok {
		msgCmd := msg.Command.(ClientOp)
		if msgCmd.ReqId != op.ReqId {
			// seems like it might be the case like KV:0 is leader in the beginning, and those are not committed
			// then partition down, later go out of partition and those cmdIdx are changed to different reqId
			//panic(fmt.Sprintf("[KV:%v][applyCh] reqId not matching! %v != %v!\n", kv.me, msg.Args.Command.ReqId, op.ReqId))
		} else {
			res := &SendMsgResult{
				Valid: valid,
				Value: value,
				Err:   Err,
			}
			// todo : to make it blocking call?
			go func(args *SendMsgArgs, res *SendMsgResult) {
				args.ResChan <- res
			}(msg, res)
			// msg.ReplyChan <- &SendMsgReply{
			// 	Valid: true,
			// 	Value: kv.State[op.Key],
			// }
		}
		delete(kv.PendingQ, applyCh.CommandIndex)
	}
}

func (kv *ShardKV) cloneKvState(src map[string]string) map[string]string {
	cloneState := make(map[string]string)
	for k, v := range src {
		cloneState[k] = v
	}
	return cloneState
}

func (kv *ShardKV) cloneKvLastAppliedReq(src map[int64]int64) map[int64]int64 {
	cloneLastAppliedReq := make(map[int64]int64)
	for k, v := range src {
		cloneLastAppliedReq[k] = v
	}
	return cloneLastAppliedReq
}

func (kv *ShardKV) applyConfigOp(op ConfigOp) {
	shardTransferInProgress := func() bool {
		for _, status := range kv.ShardStatus {
			if status == MigratingData || status == AwaitingData {
				return true
			}
		}
		return false
	}()

	kvInfo("applyConfigOp: op:%+v, ShardStatus:%+v\n", kv, op, kv.ShardStatus)

	// We want to apply configurations in-order and when previous transfers are done
	if op.Config.Num == kv.latestConfig.Num+1 && !shardTransferInProgress {
		kv.latestConfig = op.Config
		kv.updateConfigListerInfo(false)
		for shard, gid := range op.Config.Shards {
			shardStatus := kv.ShardStatus[shard]
			if gid == kv.gid && shardStatus == NotStored {
				kvInfo("Configuration change: Now owner of shard %d", kv, shard)
				kv.ShardStatus[shard] = AwaitingData
				kv.createShardIfNeeded(shard, op.Config)
			} else if gid != kv.gid && shardStatus == Available {
				kvInfo("Configuration change: No longer owner of shard %d", kv, shard)
				kv.ShardStatus[shard] = MigratingData
				go kv.deleteShard(shard, op.Config, kv.cloneKvState(kv.State[shard]), kv.cloneKvLastAppliedReq(kv.LastAppliedReq[shard]))
			}
		}
	}
}

func (kv *ShardKV) createShardIfNeeded(shardNum int, config shardmaster.Config) {
	createNewShard := func() {
		kv.ShardStatus[shardNum] = Available
		kv.State[shardNum] = make(map[string]string)
		kv.LastAppliedReq[shardNum] = make(map[int64]int64)
	}

	// Query previous configurations until we find either there was a previous owner, or that we're the first owner
	lastConfig := config
	for lastConfig.Num > 1 && lastConfig.Shards[shardNum] == kv.gid {
		lastConfig = kv.mck.Query(lastConfig.Num - 1)
	}

	if lastConfig.Num == 1 && lastConfig.Shards[shardNum] == kv.gid { // If this is the first config, and we're the owner
		kvInfo("Creating new shard: %d", kv, shardNum)
		createNewShard()
		return
	} else {
		kvInfo("Awaiting data for shard: %d from %d", kv, shardNum, lastConfig.Shards[shardNum])
	}
}

func (kv *ShardKV) deleteShard(shardNum int, config shardmaster.Config, data map[string]string, lastAppliedReq map[int64]int64) {
	startAt := time.Now().UnixNano()
	uuid := nrand()
	DPrintf2("[kv:%v] uuid:%+v start deleteShard: startAt:%+v, shardNum:%+v, config.Num:%+v\n", kv.serverName, uuid, startAt, shardNum, config.Num)
	defer DPrintf2("[kv:%v] uuid:%+v done deleteShard: startAt:%+v, shardNum:%+v, config.Num:%+v\n", kv.serverName, uuid, startAt, shardNum, config.Num)
	if _, isLeader := kv.rf.GetState(); !isLeader { // Only the leader should be moving shards
		return
	}

	for {
		servers := config.Groups[config.Shards[shardNum]]
		DPrintf2("[kv:%v] uuid:%+v deleteShard: servers to try:%+v\n", kv.serverName, uuid, servers)
		args := SendShardArgs{
			Num:        shardNum,
			Data:       data,
			LatestReqs: lastAppliedReq,
			Config:     config,
			FromServer: kv.serverName,
		}
		// reply := SendShardReply{}

		// in case of RPC failure(i.e RPC return false) , we better retry on different server instead of keeping trying
		// the same server until RPC pass
		for i := 0; ; i++ {
			reply := SendShardReply{}
			clientEnd := servers[i%len(servers)]
			args.ToServer = clientEnd
			DPrintf2("[kv:%v] uuid:%+v deleteShard: Sending shard %d to client: %s", kv.serverName, uuid, shardNum, clientEnd)
			ok := kv.make_end(clientEnd).Call("ShardKV.SendShard", &args, &reply)
			if ok && reply.Err == OK {
				break
			}
			DPrintf2("[kv:%v] uuid:%+v deleteShard: shard %d to client: %s not working , try again reply:%v, ok:%v", kv.serverName, uuid, shardNum, clientEnd, reply, ok)
		}

		kvInfo("Shard: %d successfully transferred", kv, shardNum)
		kv.rf.Start(ShardTransferOp{Command: MigrationComplete, Num: shardNum})
		return
	}
}

func (kv *ShardKV) SendShard(args *SendShardArgs, reply *SendShardReply) {
	startAt := time.Now().UnixNano()
	DPrintf2("[kv:%v]start SendShard: startAt:%+v, args%+v\n", kv.serverName, startAt, *args)
	timer := time.After(0)
	// 	timeOutTimer := time.After(SendMsgTaskMaxWaitLimit)
	msgSent := false
	resChan := make(chan *SendShardMsgResult, 2)
	reply.FromServer = args.FromServer
	reply.ToServer = kv.serverName
	// whenever Reply return with valid, kv.State should already been updated with latest
	for {
		select {
		case res := <-resChan:
			reply.IsLeader = !res.WrongLeader
			reply.Err = res.Err
			goto End
		case <-timer:
			_, isLeader := kv.rf.GetState()
			if !isLeader {
				reply.IsLeader = false
				goto End
			}
			if !msgSent {
				kv.SendShardMsgChan <- &SendShardMsgArgs{
					args:    args,
					ResChan: resChan,
				}
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
	if !reply.IsLeader {
		if !msgSent {
			reply.Err = Err(fmt.Sprintf("[kv:%v] is not leader", kv.me))
		} else {
			// don't remove from pendingQ, test easier get stuck if do this
			// I am guessing it might remove wrong item
			// 			kv.RemoveFromPendingQChan <- args
			reply.Err = Err(fmt.Sprintf("[kv:%v] was leader not anymore", kv.me))
		}
	}
	DPrintf2("[kv:%v]done SendShard: startAt:%+v, reply%+v\n", kv.serverName, startAt, *reply)
}

func (kv *ShardKV) sendShardHelper(msg *SendShardMsgArgs) {
	args := msg.args
	kvInfo("sendShardHelper: start args:%+v\n", kv, *args)
	_, isLeader := kv.rf.GetState()
	res := SendShardMsgResult{
		WrongLeader: false,
	}
	defer kvInfo("sendShardHelper: done res:%+v\n", kv, res)
	if !isLeader {
		res.WrongLeader = true
		return
	}

	if kv.latestConfig.Num == args.Config.Num {
		// Copy shard data
		data := make(map[string]string)
		for k := range args.Data {
			data[k] = args.Data[k]
		}

		// Copy shard's latest requests
		latestRequests := make(map[int64]int64)
		for k := range args.LatestReqs {
			latestRequests[k] = args.LatestReqs[k]
		}

		op := ShardTransferOp{Command: ShardTransfer, Num: args.Num, Shard: data, LatestRequests: latestRequests}
		kv.rf.Start(op)
		res.Err = OK
	} else if args.Config.Num < kv.latestConfig.Num { // Old config and we've likely already handled this
		res.Err = OK
	}

	msg.ResChan <- &res
}

func (kv *ShardKV) applyShardTransferOp(op ShardTransferOp) {
	switch op.Command {
	case MigrationComplete:
		delete(kv.State, op.Num)
		delete(kv.LastAppliedReq, op.Num)
		kv.ShardStatus[op.Num] = NotStored
	case ShardTransfer:
		if kv.ShardStatus[op.Num] == AwaitingData {
			kv.State[op.Num] = kv.cloneKvState(op.Shard)
			kv.LastAppliedReq[op.Num] = kv.cloneKvLastAppliedReq(op.LatestRequests)
			kv.ShardStatus[op.Num] = Available
			kvInfo("Data for shard: %d successfully received", kv, op.Num)
		}
	}
}

func (kv *ShardKV) updateConfigListerInfo(toStop bool) {
	go func(config shardmaster.Config, toStop bool) {
		kv.cfgLisChan <- CfgLis{
			ToStop:       toStop,
			latestConfig: config,
		}
	}(kv.latestConfig, toStop)
}

func (kv *ShardKV) StartConfigListener(latestConfig shardmaster.Config, cfgLisChan chan CfgLis) {
	DPrintf2("[kv:%+v] StartConfigListener thread start", kv.serverName)
	defer DPrintf2("[kv:%+v] StartConfigListener thread end", kv.serverName)
	for {
		select {
		case cfgLis := <-cfgLisChan:
			DPrintf2("[kv:%+v] StartConfigListener: received cfgLis:%+v", kv.serverName, cfgLis)
			if cfgLis.ToStop {
				return
			}
			latestConfig = cfgLis.latestConfig

		case <-time.After(CheckConfigInterval):
			if _, isLeader := kv.rf.GetState(); !isLeader {
				continue
			}
			config := kv.mck.Query(-1) // Getting latest configuration
			if latestConfigNum := latestConfig.Num; latestConfigNum < config.Num {

				// kvInfo("start Batch applying configs from %d -> %d", kv, latestConfigNum, config.Num)

				// Get all configs from last applied config to the config received and apply them in order
				for i := 0; i < config.Num-latestConfigNum; i++ {
					currentConfigNum := latestConfigNum + i + 1

					var c shardmaster.Config
					if currentConfigNum == config.Num { // Did we already fetch this one?
						c = config
					} else {
						c = kv.mck.Query(currentConfigNum)
					}

					// Apply the configs in-order.
					op := ConfigOp{Command: ConfigUpdate, Config: c}
					expCmtIdx, _, isLeader := kv.rf.Start(op)
					DPrintf2("[kv:%+v] checkConfig: sent to Raft expCmtIdx:%v, isLeader:%v", kv.serverName, expCmtIdx, isLeader)
					if !isLeader {
						return
					}
					break
				}
				// kvInfo("done Batch applying configs from %d -> %d", kv, latestConfigNum, config.Num)
			}
		}
	}

}

func (kv *ShardKV) checkConfig(latestConfig shardmaster.Config) {
	DPrintf2("[kv:%+v] start checkConfig", kv.serverName)
	defer DPrintf2("[kv:%+v] done checkConfig", kv.serverName)

	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}

	config := kv.mck.Query(-1) // Getting latest configuration

	if latestConfigNum := latestConfig.Num; latestConfigNum < config.Num {

		// kvInfo("start Batch applying configs from %d -> %d", kv, latestConfigNum, config.Num)

		// Get all configs from last applied config to the config received and apply them in order
		for i := 0; i < config.Num-latestConfigNum; i++ {
			currentConfigNum := latestConfigNum + i + 1

			var c shardmaster.Config
			if currentConfigNum == config.Num { // Did we already fetch this one?
				c = config
			} else {
				c = kv.mck.Query(currentConfigNum)
			}

			// Apply the configs in-order.
			op := ConfigOp{Command: ConfigUpdate, Config: c}
			expCmtIdx, _, isLeader := kv.rf.Start(op)
			DPrintf2("[kv:%+v] checkConfig: sent to Raft expCmtIdx:%v, isLeader:%v", kv.serverName, expCmtIdx, isLeader)
			if !isLeader {
				return
			}
			break
		}
		// kvInfo("done Batch applying configs from %d -> %d", kv, latestConfigNum, config.Num)
	}
}

// check if this client msg is touch key it is currently has ,
// otherwise reply directly to client side
func (kv *ShardKV) isValidClientRequest(msg *SendMsgArgs) bool {
	op := msg.Command.(ClientOp)
	res := SendMsgResult{}
	switch kv.ShardStatus[key2shard(op.Key)] {
	case NotStored:
		fallthrough
	case MigratingData:
		res.Err = ErrWrongGroup
		res.Value = strconv.Itoa(kv.latestConfig.Shards[key2shard(op.Key)])
	case AwaitingData:
		res.Err = ErrMovingShard
	case Available:
		res.Err = OK
	}
	if res.Err != OK {
		res.Valid = false
		msg.ResChan <- &res
		return false
	}
	return true
}

func (kv *ShardKV) StartKVThread() {
	DPrintf("[kv:%v]start of Thread: %+v\n", kv.me, kv)
	defer DPrintf("[kv:%v]End of Thread: %+v\n", kv.me, kv)
	var snapShotCheckTimer <-chan time.Time
	if kv.maxraftstate > 0 {
		snapShotCheckTimer = time.After(SnapShotCheckIntervalMillisecond)
	}
	// checkConfigTimer := time.After(0)
	for {
		// DPrintf("[kv:%v]start of for loop kv:%+v\n", kv.me, kv)
		select {
		case SendMsg := <-kv.SendMsgChan:
			kvInfo("received SendMsg: SendMsg:%+v\n", kv, SendMsg)
			// if kv.isValidClientRequest(SendMsg) {
			kv.SendMsgJobQ = append(kv.SendMsgJobQ, SendMsg)
			kv.processingSendMsgJobQ()
			// }
			kvInfo("done handling SendMsg SendMsg:%+v, kv:%+v\n", kv, SendMsg)
		case applyCh := <-kv.applyCh:
			// 			kv.Lock()
			if !applyCh.CommandValid {
				kv.specialApplyMsgHandler(&applyCh)
				continue
			} else {
				kvInfo("received applyCh: applyCh:%+v\n", kv, applyCh)
				switch op := applyCh.Command.(type) {
				case ClientOp:
					kv.applyClientOp(&applyCh)
				case ConfigOp:
					// panic("should not be here yet!")
					kv.applyConfigOp(op)
				case ShardTransferOp:
					// panic("should not be here yet!")
					kv.applyShardTransferOp(op)
				default:
					panic(fmt.Sprintf("[kv:%v][StartKVThread] un-expected op:%+v\n", kv.me, op))
				}
				kv.LastApplyMsg = &applyCh
			}
			kvInfo("done handling applyCh applyCh:%+v, kv:%+v\n", kv, applyCh, kv)
		case <-kv.ToStopChan:
			DPrintf("[kv:%v]received ToStopChan\n", kv.me)
			kv.ToStop = true
		case <-snapShotCheckTimer:
			DPrintf("[kv:%v]snapShotCheckTimer is up: RaftStateSize:%v, maxraftstate:%v\n",
				kv.me, kv.persister.RaftStateSize(), kv.maxraftstate)
			kv.CheckRaftStateSize()
			snapShotCheckTimer = time.After(SnapShotCheckIntervalMillisecond)
			// 		default:
			// 			// whenever we don't have pending snapshot, keep processing client request
			// 			// but stop it whenever the Raft state is is tight
			// 			for !kv.PendingCreateSnapShot() && len(kv.SendMsgJobQ) > 0 && kv.CheckRaftStateSize() {
			// 				kv.SendMsgToRaft(kv.SendMsgJobQ[0])
			// 				kv.SendMsgJobQ = kv.SendMsgJobQ[1:]
			// 			}
		// case <-checkConfigTimer:
		// 	kv.checkConfig(kv.latestConfig)
		// 	checkConfigTimer = time.After(CheckConfigInterval)
		case sendShardMsg := <-kv.SendShardMsgChan:
			kvInfo("received sendShardMsg:%+v\n", kv, *sendShardMsg)
			kv.sendShardHelper(sendShardMsg)
		}
		if kv.ToStop {
			kv.updateConfigListerInfo(true)
			return
		}
		// DPrintf("[kv:%v]end of for loop kv:%+v\n", kv.me, kv)
	}
}

func (kv *ShardKV) ReadSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	kvInfo("start ReadSnapshot: \n", kv)
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var obj KVRaftPersistence
	if d.Decode(&obj) != nil {
		panic(fmt.Sprintf("[%v][LoadSnapshot] fail to read snapshot!\n", kv.me))
	}
	kv.LastAppliedReq = obj.LastAppliedReq
	kv.State = obj.State
	kv.LastApplyMsg = &obj.LastApplyMsg
	kv.ShardStatus = obj.ShardStatus
	kv.latestConfig = obj.LatestConfig
	kv.updateConfigListerInfo(false)
	kvInfo("done ReadSnapshot: lastConfig:%+v\n", kv, kv.latestConfig)
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
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
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(ClientOp{})
	labgob.Register(ConfigOp{})
	labgob.Register(ShardTransferOp{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.serverName = fmt.Sprintf("server-%v-%v", gid, kv.me)
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.PendingQ = make(map[int]*SendMsgArgs)
	kv.LastAppliedReq = make(map[int]map[int64]int64)
	kv.SendMsgChan = make(chan *SendMsgArgs)
	kv.ShardStatus = make(map[int]ShardState)
	kv.State = make(map[int]map[string]string)
	kv.ToStopChan = make(chan bool)
	kv.ToStop = false
	kv.persister = persister
	kv.maxraftstate = maxraftstate
	kv.UnsetCreateSnapShot()
	kv.SendMsgJobQ = make([]*SendMsgArgs, 0)
	kv.latestConfig = shardmaster.Config{}
	kv.SendShardMsgChan = make(chan *SendShardMsgArgs, 8192)
	kv.cfgLisChan = make(chan CfgLis)

	for i := 0; i < shardmaster.NShards; i++ {
		// kv.State[i] = make(map[string]string)
		// kv.LastAppliedReq[i] = make(map[int64]int64)
		kv.ShardStatus[i] = NotStored
	}
	kv.ReadSnapshot(kv.persister.ReadSnapshot())
	DPrintf2("[kv:%v] Make kv:%+v\n", kv.serverName, kv)

	go kv.StartKVThread()
	go kv.StartConfigListener(kv.latestConfig, kv.cfgLisChan)

	return kv
}
