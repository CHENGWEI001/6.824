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

const AwaitLeaderCheckInterval = 10 * time.Millisecond
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type SendMsgReply struct {
	Valid bool
	Value string
}
type SendMsgArgs struct {
	Command Op
}

type SendMsg struct {
	Args      *SendMsgArgs
	ReplyChan chan *SendMsgReply
	ExpCmtIdx int
	Done      bool
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
	Uuid           int64
	LastAppliedReq map[int64]int64 // to store last requestId from each client, here assume one client would send one reuqest at a time
	PendingQ       map[int]SendMsg // to store cmd send to RAFT but not yet complete
	SendMsgChan    chan SendMsg    // channel for receiving new msg from client
	State          map[string]string
	ToStopChan     chan bool    // channel for stopping the raft instance thread
	ToStop         bool         // indicator for stopping raft instance
	PendingQChan   chan SendMsg // channel for add item into PendingQ
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf("[KV:%v][Get]: GetArgs:%+v\n", kv.me, args)
	defer DPrintf("[KV:%v][Get]: GetReply:%+v\n", kv.me, reply)
	s := SendMsg{
		Args: &SendMsgArgs{
			Command: Op{
				Oc:       GET,
				Key:      args.Key,
				ReqId:    args.ReqId,
				ClientId: args.ClientId,
			},
		},
	}
	timer := time.After(0)
	msgSent := false
	// whenever Reply return with valid, kv.State should already been updated with latest
	for {
		select {
		case res := <-s.ReplyChan:
			reply.WrongLeader = !res.Valid
			reply.ReqId = args.ReqId
			reply.Value = res.Value
			reply.ClientId = args.ClientId
			goto End
		case <-timer:
			_, isLeader := kv.rf.GetState()
			if !isLeader {
				reply.WrongLeader = true
				goto End
			}
			if !msgSent {
				s.ReplyChan = make(chan *SendMsgReply)
				DPrintf("[KV:%v][Get]: Sending GetArgs:%+v\n", kv.me, args)
				kv.SendMsgChan <- s
				msgSent = true
				DPrintf("[KV:%v][Get]: Comeback GetArgs:%+v\n", kv.me, args)
			}
			timer = time.After(AwaitLeaderCheckInterval)
		}
	}
End:
	// close(s.ReplyChan)
	if reply.WrongLeader {
		reply.Err = Err(fmt.Sprintf("[KV:%v][Get] No longer leader!", kv.me))
	}
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
	s := SendMsg{
		Args: &SendMsgArgs{
			Command: Op{
				Oc:       opCode,
				Key:      args.Key,
				Value:    args.Value,
				ReqId:    args.ReqId,
				ClientId: args.ClientId,
			},
		},
	}
	timer := time.After(0)
	msgSent := false
	for {
		DPrintf("[KV:%v][PutAppend]: Start of the loop PutAppendArgs:%+v\n", kv.me, args)
		select {
		case res := <-s.ReplyChan:
			reply.WrongLeader = !res.Valid
			reply.ReqId = args.ReqId
			reply.ClientId = args.ClientId
			goto End
		case <-timer:
			_, isLeader := kv.rf.GetState()
			if !isLeader {
				reply.WrongLeader = true
				goto End
			}
			if !msgSent {
				s.ReplyChan = make(chan *SendMsgReply)
				DPrintf("[KV:%v][PutAppend]: Sending PutAppendArgs:%+v\n", kv.me, args)
				kv.SendMsgChan <- s
				msgSent = true
				DPrintf("[KV:%v][PutAppend]: Comeback PutAppendArgs:%+v\n", kv.me, args)
			}
			timer = time.After(AwaitLeaderCheckInterval)
		}
		DPrintf("[KV:%v][PutAppend]: End of the loop PutAppendArgs:%+v\n", kv.me, args)
	}
End:
	// close(s.ReplyChan)
	if reply.WrongLeader {
		reply.Err = Err(fmt.Sprintf("[KV:%v][PutAppend] No longer leader!", kv.me))
		return
	}
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

func (kv *KVServer) SendMsgTask(msg SendMsg) {
	DPrintf("[kv:%v]start handling SendMsgTask: msg.Args%+v\n", kv.me, msg.Args)
	index, _, isLeader := kv.rf.Start(msg.Args.Command)
	if !isLeader {
		msg.ReplyChan <- &SendMsgReply{
			Valid: false,
		}
	} else {
		msg.ExpCmtIdx = index
		kv.PendingQChan <- msg
	}
	DPrintf("[kv:%v]done handling SendMsgTask: msg.Args%+v\n", kv.me, msg.Args)
}

func (kv *KVServer) StartKVThread() {
	defer DPrintf("[kv:%v]End of Thread: %+v\n", kv.me, kv)
	for {
		DPrintf("[kv:%v]start of for loop kv:%+v\n", kv.me, kv)
		select {
		case SendMsg := <-kv.SendMsgChan:
			DPrintf("[kv:%v]start handling SendMsg SendMsg:%+v, kv:%+v\n", kv.me, SendMsg.Args, kv)
			if lastReqId, ok := kv.LastAppliedReq[SendMsg.Args.Command.ClientId]; ok && lastReqId == SendMsg.Args.Command.ReqId {
				SendMsg.ReplyChan <- &SendMsgReply{
					Valid: true,
					Value: kv.State[SendMsg.Args.Command.Key],
				}
			} else {
				go kv.SendMsgTask(SendMsg)
			}
			DPrintf("[kv:%v]done handling SendMsg SendMsg:%+v, kv:%+v\n", kv.me, SendMsg.Args, kv)
		case applyCh := <-kv.applyCh:
			DPrintf("[kv:%v]start handling applyCh applyCh:%+v, kv:%+v\n", kv.me, applyCh, kv)
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
			// if there is pending item, we need to reply it
			if msg, ok := kv.PendingQ[applyCh.CommandIndex]; ok {
				if msg.Args.Command.ReqId != op.ReqId {
					// seems like it might be the case like KV:0 is leader in the beginning, and those are not committed
					// then partition down, later go out of partition and those cmdIdx are changed to different reqId
					//panic(fmt.Sprintf("[KV:%v][applyCh] reqId not matching! %v != %v!\n", kv.me, msg.Args.Command.ReqId, op.ReqId))
				} else {
					reply := &SendMsgReply{
						Valid: true,
						Value: kv.State[op.Key],
					}
					go func(msg SendMsg, reply *SendMsgReply) {
						msg.ReplyChan <- reply
					}(msg, reply)
					// msg.ReplyChan <- &SendMsgReply{
					// 	Valid: true,
					// 	Value: kv.State[op.Key],
					// }
				}
				delete(kv.PendingQ, applyCh.CommandIndex)
			}
			DPrintf("[kv:%v]done handling applyCh applyCh:%+v, kv:%+v\n", kv.me, applyCh, kv)
		case msg := <-kv.PendingQChan:
			kv.PendingQ[msg.ExpCmtIdx] = msg
		}
		if kv.ToStop {
			return
		}
	}
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
	kv.PendingQ = make(map[int]SendMsg)
	kv.LastAppliedReq = make(map[int64]int64)
	kv.SendMsgChan = make(chan SendMsg)
	kv.PendingQChan = make(chan SendMsg)
	kv.State = make(map[string]string)
	kv.ToStopChan = make(chan bool)
	kv.ToStop = false

	go kv.StartKVThread()
	return kv
}
