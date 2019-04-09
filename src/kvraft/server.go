package raftkv

import (
	"fmt"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)
import "bytes"

const AwaitLeaderCheckInterval = 10 * time.Millisecond

// const AwaitCheckSnapshotPendingIntervalMillisecond = 5 * time.Millisecond
const SnapshotSizeTolerancePercentage = 5
const SnapShotCheckIntervalMillisecond = 50 * time.Millisecond
const SendMsgTaskMaxWaitLimit = 20000 * time.Millisecond
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type SendMsgResult struct {
	Valid bool
	Value string
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

type OpCode string

const (
	GET    OpCode = "GET"
	PUT           = "PUT"
	APPEND        = "APPEND"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Oc       OpCode
	Key      string
	Value    string
	ReqId    int64
	ClientId int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	Uuid                   int64
	LastAppliedReq         map[int64]int64      // to store last requestId from each client, here assume one client would send one reuqest at a time
	PendingQ               map[int]*SendMsgArgs // to store cmd send to RAFT but not yet complete
	SendMsgChan            chan *SendMsgArgs    // channel for receiving new msg from client
	State                  map[string]string
	ToStopChan             chan bool         // channel for stopping the raft instance thread
	ToStop                 bool              // indicator for stopping raft instance
	AddToPendingQChan      chan *SendMsgArgs // channel for add item into PendingQ
	RemoveFromPendingQChan chan *SendMsgArgs // channel for remove item from PendingQ
	persister              *raft.Persister   // persister
	LastApplyMsg           *raft.ApplyMsg    // the last ApplyMsg
	CreateSnapshotPending  bool              // indicating wether any pending CreateSnapshot ongoing
	SendMsgJobQ            []*SendMsgArgs    // SendMsg job queue , will be processed by kv main thread when it is idle
}

type KVRaftPersistence struct {
	LastAppliedReq map[int64]int64
	State          map[string]string
	LastApplyMsg   raft.ApplyMsg
}

// the worker handler to send new request msg to KV raft main thread
func (kv *KVServer) SendMsgTask(args *SendMsgArgs, reply *SendMsgReply) {
	startAt := time.Now().UnixNano()
	DPrintf("[kv:%v]start SendMsgTask: startAt:%+v, args%+v\n", kv.me, startAt, args)
	timer := time.After(0)
	// 	timeOutTimer := time.After(SendMsgTaskMaxWaitLimit)
	msgSent := false
	// whenever Reply return with valid, kv.State should already been updated with latest
	for {
		select {
		case res := <-args.ResChan:
			reply.WrongLeader = !res.Valid
			reply.ReqId = args.Command.ReqId
			reply.Value = res.Value
			reply.ClientId = args.Command.ClientId
			goto End
		case <-timer:
			_, isLeader := kv.rf.GetState()
			if !isLeader {
				reply.WrongLeader = true
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
			// 		case <-timeOutTimer:
			// 			panic(fmt.Sprintf("[kv:%v]start SendMsgTask: startAt:%+v, args%+v not complete in %+v, now:%+v\n",
			// 				kv.me, startAt, args, SendMsgTaskMaxWaitLimit, time.Now()))
		}
	}
End:
	// close(s.ReplyChan)
	if reply.WrongLeader {
		if !msgSent {
			reply.Err = Err(fmt.Sprintf("[kv:%v] is not leader\n", kv.me))
		} else {
			// don't remove from pendingQ, test easier get stuck if do this
			// I am guessing it might remove wrong item
			// 			kv.RemoveFromPendingQChan <- args
			reply.Err = Err(fmt.Sprintf("[kv:%v] was leader not anymore\n", kv.me))
		}
	}
	reply.Err = Err(fmt.Sprintf("[kv:%v] OK", kv.me))
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf("[KV:%v][Get]: GetArgs:%+v\n", kv.me, args)
	defer DPrintf("[KV:%v][Get]: GetReply:%+v\n", kv.me, reply)
	s := SendMsgArgs{
		Command: Op{
			Oc:       GET,
			Key:      args.Key,
			ReqId:    args.ReqId,
			ClientId: args.ClientId,
		},
		ResChan:   nil,
		ExpCmtIdx: -1,
	}
	r := SendMsgReply{}
	kv.SendMsgTask(&s, &r)
	reply.WrongLeader = r.WrongLeader
	reply.ReqId = r.ReqId
	reply.Value = r.Value
	reply.ClientId = r.ClientId
	reply.Err = r.Err
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("[KV:%v][PutAppend]: PutAppendArgs:%+v\n", kv.me, args)
	defer DPrintf("[KV:%v][PutAppend]: PutAppendReply:%+v\n", kv.me, reply)
	var opCode OpCode
	if args.Op == "Put" {
		opCode = PUT
	} else if args.Op == "Append" {
		opCode = APPEND
	} else {
		panic(fmt.Sprintf("[KV:%v][PutAppend] un-expected opCode %+v!\n", kv.me, opCode))
	}
	s := SendMsgArgs{
		Command: Op{
			Oc:       opCode,
			Key:      args.Key,
			Value:    args.Value,
			ReqId:    args.ReqId,
			ClientId: args.ClientId,
		},
	}
	r := SendMsgReply{}
	kv.SendMsgTask(&s, &r)
	reply.WrongLeader = r.WrongLeader
	reply.ReqId = r.ReqId
	reply.ClientId = r.ClientId
	reply.Err = r.Err
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.

	//DPrintf("[KV:%v][Kill]: %+v\n", kv.me, kv)
	go func(kv *KVServer) {
		kv.ToStopChan <- true
	}(kv)
}

func (kv *KVServer) Lock() {
	kv.mu.Lock()
}

func (kv *KVServer) Unlock() {
	kv.mu.Unlock()
}

// func (kv *KVServer) AppendPendingQ(msg SendMsg) {
// 	kv.Lock()
// 	kv.PendingQ = append(kv.PendingQ, msg)
// 	kv.Unock()
// }

func (kv *KVServer) createSnapshot() {
	DPrintf("[kv:%v]start createSnapshot: RaftStateSize:%v\n", kv.me, kv.persister.RaftStateSize())
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(KVRaftPersistence{
		State:          kv.State,
		LastAppliedReq: kv.LastAppliedReq,
		LastApplyMsg:   *kv.LastApplyMsg,
	})
	data := w.Bytes()
	kv.SetCreateSnapShot()
	kv.rf.CreateSnapshot(data, kv.LastApplyMsg)
	DPrintf("[kv:%v]done createSnapshot: RaftStateSize:%v, data:%+v\n", kv.me, kv.persister.RaftStateSize(), data)
}

func (kv *KVServer) SetCreateSnapShot() {
	// 	kv.Lock()
	kv.CreateSnapshotPending = true
	// 	kv.Unlock()
}

func (kv *KVServer) UnsetCreateSnapShot() {
	// 	kv.Lock()
	kv.CreateSnapshotPending = false
	// 	kv.Unlock()
}

func (kv *KVServer) PendingCreateSnapShot() (status bool) {
	// 	kv.Lock()
	status = kv.CreateSnapshotPending
	// 	kv.Unlock()
	return
}

func (kv *KVServer) SendMsgToRaft(msg *SendMsgArgs) {
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
func (kv *KVServer) CheckRaftStateSize() (status bool) {
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
	if !kv.PendingCreateSnapShot() && (kv.maxraftstate-kv.persister.RaftStateSize())*100/kv.maxraftstate < SnapshotSizeTolerancePercentage {
		kv.createSnapshot()
		status = false
	}
	return
}

func (kv *KVServer) specialApplyMsgHandler(applyCh *raft.ApplyMsg) {
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

func (kv *KVServer) processingSendMsgJobQ() {
	DPrintf("[kv:%v] processingSendMsgJobQ: start len(kv.SendMsgJobQ):%+v\n", kv.me, len(kv.SendMsgJobQ))
	// 	for !kv.PendingCreateSnapShot() && len(kv.SendMsgJobQ) > 0 && kv.CheckRaftStateSize() {
	for len(kv.SendMsgJobQ) > 0 {
		kv.SendMsgToRaft(kv.SendMsgJobQ[0])
		kv.SendMsgJobQ = kv.SendMsgJobQ[1:]
	}
	DPrintf("[kv:%v] processingSendMsgJobQ: end len(kv.SendMsgJobQ):%+v\n", kv.me, len(kv.SendMsgJobQ))
}

func (kv *KVServer) StartKVThread() {
	DPrintf("[kv:%v]start of Thread: %+v\n", kv.me, kv)
	defer DPrintf("[kv:%v]End of Thread: %+v\n", kv.me, kv)
	var snapShotCheckTimer <-chan time.Time
	if kv.maxraftstate > 0 {
		snapShotCheckTimer = time.After(SnapShotCheckIntervalMillisecond)
	}
	for {
		DPrintf("[kv:%v]start of for loop kv:%+v\n", kv.me, kv)
		select {
		case SendMsg := <-kv.SendMsgChan:
			DPrintf("[kv:%v] received SendMsg: SendMsg:%+v\n", kv.me, SendMsg)
			// since each request from client is sending serialize(it must complete one before send the next one for the same client)
			// it is okay to check lastReq only to see new rquest is the same as last ReqId or not
			if lastReqId, ok := kv.LastAppliedReq[SendMsg.Command.ClientId]; ok && lastReqId == SendMsg.Command.ReqId {
				SendMsg.ResChan <- &SendMsgResult{
					Valid: true,
					Value: kv.State[SendMsg.Command.Key],
				}
			} else {
				// if this Msg is new request, we need to open up a new routine to push this
				// to raft instance and push into KV pendingQ, the purpose of pendingQ is to
				// know which client Requst to reply once it is commited by Raft instance
				// go kv.SendMsgToRaft(SendMsg)
				// kv.SendMsgToRaft(SendMsg)
				kv.SendMsgJobQ = append(kv.SendMsgJobQ, SendMsg)
				kv.processingSendMsgJobQ()
			}
			DPrintf("[kv:%v]done handling SendMsg SendMsg:%+v, kv:%+v\n", kv.me, SendMsg, kv)
		case applyCh := <-kv.applyCh:
			DPrintf("[kv:%v]received applyCh: applyCh:%+v\n", kv.me, applyCh)
			// 			kv.Lock()
			if !applyCh.CommandValid {
				kv.specialApplyMsgHandler(&applyCh)
				continue
			}
			// only need to touch state when opCode is PUT, do nothing for GET
			//var op Op
			op := applyCh.Command.(Op)
			if kv.LastAppliedReq[op.ClientId] != op.ReqId {
				if op.Oc == PUT {
					kv.State[op.Key] = op.Value
				} else if op.Oc == APPEND {
					kv.State[op.Key] = kv.State[op.Key] + op.Value
				}
			}
			kv.LastAppliedReq[op.ClientId] = op.ReqId
			kv.LastApplyMsg = &applyCh
			// if there is pending item, we need to reply it
			if msg, ok := kv.PendingQ[applyCh.CommandIndex]; ok {
				if msg.Command.ReqId != op.ReqId {
					// seems like it might be the case like KV:0 is leader in the beginning, and those are not committed
					// then partition down, later go out of partition and those cmdIdx are changed to different reqId
					//panic(fmt.Sprintf("[KV:%v][applyCh] reqId not matching! %v != %v!\n", kv.me, msg.Args.Command.ReqId, op.ReqId))
				} else {
					res := &SendMsgResult{
						Valid: true,
						Value: kv.State[op.Key],
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
			// 			kv.CheckRaftStateSize()
			// 			kv.Unlock()
			DPrintf("[kv:%v]done handling applyCh applyCh:%+v, kv:%+v\n", kv.me, applyCh, kv)
		case msg := <-kv.AddToPendingQChan:
			DPrintf("[kv:%v]received AddToPendingQChan: msg:%+v\n", kv.me, msg)
			kv.PendingQ[msg.ExpCmtIdx] = msg
		case msg := <-kv.RemoveFromPendingQChan:
			DPrintf("[kv:%v]received RemoveFromPendingQChan: msg:%+v\n", kv.me, msg)
			// https://stackoverflow.com/questions/1736014/delete-mapkey-in-go
			delete(kv.PendingQ, msg.ExpCmtIdx)
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
		}
		if kv.ToStop {
			return
		}
		DPrintf("[kv:%v]end of for loop kv:%+v\n", kv.me, kv)
	}
}

func (kv *KVServer) ReadSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var obj KVRaftPersistence
	if d.Decode(&obj) != nil {
		panic(fmt.Sprintf("[%v][LoadSnapshot] fail to read snapshot!\n", kv.me))
	}
	kv.LastAppliedReq = obj.LastAppliedReq
	kv.State = obj.State
	kv.LastApplyMsg = &obj.LastApplyMsg
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.Uuid = time.Now().Unix()
	kv.PendingQ = make(map[int]*SendMsgArgs)
	kv.LastAppliedReq = make(map[int64]int64)
	kv.SendMsgChan = make(chan *SendMsgArgs)
	kv.AddToPendingQChan = make(chan *SendMsgArgs)
	kv.RemoveFromPendingQChan = make(chan *SendMsgArgs)
	kv.State = make(map[string]string)
	kv.ToStopChan = make(chan bool)
	kv.ToStop = false
	kv.persister = persister
	kv.maxraftstate = maxraftstate
	kv.ReadSnapshot(kv.persister.ReadSnapshot())
	kv.UnsetCreateSnapShot()
	kv.SendMsgJobQ = make([]*SendMsgArgs, 0)
	DPrintf("[kv:%v] Make kv:%+v\n", kv.me, kv)

	go kv.StartKVThread()
	return kv
}
