package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "fmt"
import "time"

// import "time"
// import "log"

// idea from https://www.callicoder.com/distributed-unique-id-sequence-number-generator/
// const REQID_TIME_SHIFT = 22
// const REQID_TIME_MASK = 0xFFFFFFFFFFC00000
// const REQID_CLIENTID_SHIFT = 12
// const REQID_CLIENTID_MASK = 0x3FF000
// const REQID_RANDID_SHIFT = 0
// const REQID_RANDID_MASK = 0xFFF

const CLIENT_REQUEST_NO_RESPONSE_MAX_LIMIT_MILLISECOND = 20000 * time.Millisecond

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeader int
	Uuid       int64
}

func applyShiftAndMask(val int64, shift uint, mask int64) int64 {
	return (val << shift) & mask
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
	// You'll have to add code here.
	ck.lastLeader = 0
	ck.Uuid = nrand()
	DPrintf("[CK:%v][MakeClerk]: ck:%+v\n", ck.Uuid, ck)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	startAt := time.Now()
	DPrintf("[CK:%v][Get]: key:%v, startAt:%+v\n", ck.Uuid, key, startAt)
	args := GetArgs{
		Key:      key,
		ReqId:    nrand(),
		ClientId: ck.Uuid,
	}
	ok := false
	for i := ck.lastLeader; ; i = (i + 1) % len(ck.servers) {
		reply := GetReply{}
		// DPrintf("[CK:%v][Get]: ->%v, args:%+v\n", ck.Uuid, i, args)
		ok = ck.servers[i].Call("KVServer.Get", &args, &reply)
		if ok && !reply.WrongLeader {
			if reply.ClientId != args.ClientId || reply.ReqId != args.ReqId {
				panic(fmt.Sprintf("[CK:%v][Get] args:%+v and reply:%+v data not matching!\n", ck.Uuid, args, reply))
			}
			ck.lastLeader = i
			DPrintf("[CK:%v][Get]: <-%v ok:%v, reply:%+v\n", ck.Uuid, i, ok, reply)
			return reply.Value
		}
		if time.Now().Sub(startAt) > CLIENT_REQUEST_NO_RESPONSE_MAX_LIMIT_MILLISECOND {
			panic(fmt.Sprintf("[CK:%v][Get]: key:%v, startAt:%+v not complete in %+v milliseconds, now:%+v\n",
				ck.Uuid, key, startAt, CLIENT_REQUEST_NO_RESPONSE_MAX_LIMIT_MILLISECOND, time.Now()))
		}
	}

}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	startAt := time.Now()
	DPrintf("[CK:%v][PutAppend]: Beginning key:%v, value:%+v, op:%v, startAt:%+v\n", ck.Uuid, key, value, op, startAt)

	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ReqId:    nrand(),
		ClientId: ck.Uuid,
	}
	ok := false
	for i := ck.lastLeader; ; i = (i + 1) % len(ck.servers) {
		reply := PutAppendReply{}
		// DPrintf("[CK:%v][PutAppend]: ->%v ,args:%+v\n", ck.Uuid, i, args)
		ok = ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		if ok && !reply.WrongLeader {
			DPrintf("[CK:%v][PutAppend]: <-%v ok:%v, reply:%+v\n", ck.Uuid, i, ok, reply)
			if reply.ClientId != args.ClientId || reply.ReqId != args.ReqId {
				panic(fmt.Sprintf("[CK:%v][PutAppend] args:%+v and reply:%+v data not matching!\n", ck.Uuid, args, reply))
			}
			ck.lastLeader = i
			DPrintf("[CK:%v][Get]: <-%v ok:%v, reply:%+v\n", ck.Uuid, i, ok, reply)
			return
		}
		if time.Now().Sub(startAt) > CLIENT_REQUEST_NO_RESPONSE_MAX_LIMIT_MILLISECOND {
			panic(fmt.Sprintf("[CK:%v][PutAppend]: Beginning key:%v, value:%+v, op:%v, startAt:%+v not complete in %+v milliseconds, now:%+v\n",
				ck.Uuid, key, value, op, startAt, CLIENT_REQUEST_NO_RESPONSE_MAX_LIMIT_MILLISECOND, time.Now()))
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
