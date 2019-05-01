package shardkv

import "time"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrMovingShard = "ErrShardMigrating"
)

const RPCTimeout = 50 * time.Millisecond
const RPCMaxTries = 3

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ReqId    int64
	ClientId int64
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
	ReqId       int64
	ClientId    int64
	Value       string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ReqId    int64
	ClientId int64
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
	ReqId       int64
	ClientId    int64
}

// SendRPCRequest will keep trying to send an RPC until it succeeds (with timeouts, per request)
func SendRPCRequest(request func(args *interface{}, reply *interface{}) bool, args *interface{}, reply *interface{}) bool {
	makeRequest := func(successChan chan struct{}) {
		if ok := request(args, reply); ok {
			successChan <- struct{}{}
		}
	}

	for attempts := 0; attempts < RPCMaxTries; attempts++ {
		rpcChan := make(chan struct{}, 1)
		go makeRequest(rpcChan)
		select {
		case <-rpcChan:
			return true
		case <-time.After(RPCTimeout):
			continue
		}
	}

	return false
}
