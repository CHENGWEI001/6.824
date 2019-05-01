package shardmaster

//
// Shardmaster clerk.
//

import "labrpc"
import "time"
import "crypto/rand"
import "math/big"
import "fmt"

const CLIENT_REQUEST_NO_RESPONSE_MAX_LIMIT = 20000 * time.Millisecond
const TO_PANIC_IF_CLIENT_REQUEST_OVER_LIMIT = false

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	// lastLeader int
	Uuid int64
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
	// Your code here.
	// ck.lastLeader = 0
	ck.Uuid = nrand()
	DPrintf("[smck:%v][MakeClerk]: smck:%+v\n", ck.Uuid, ck)
	return ck
}

func (ck *Clerk) Query(num int) Config {
	startAt := time.Now()
	reqId := nrand()
	DPrintf("[smck:%v][Query]: num:%+v, startAt:%+v, reqId:%+v\n", ck.Uuid, num, startAt, reqId)
	args := QueryArgs{
		Num:      num,
		ReqId:    reqId,
		ClientId: ck.Uuid,
	}
	for {
		// try each known server.
		for i, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", &args, &reply)
			if ok && reply.WrongLeader == false {
				if reply.ClientId != args.ClientId || reply.ReqId != args.ReqId {
					panic(fmt.Sprintf("[smck:%v][Query] args:%+v and reply:%+v data not matching!\n", ck.Uuid, args, reply))
				}
				DPrintf("[smck:%v][Query]: <-%v ok:%v, reply:%+v\n", ck.Uuid, i, ok, reply)
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
		if time.Now().Sub(startAt) > CLIENT_REQUEST_NO_RESPONSE_MAX_LIMIT {
			if TO_PANIC_IF_CLIENT_REQUEST_OVER_LIMIT {
				panic(fmt.Sprintf("[smck:%v][Query]: args:%+v, startAt:%+v not complete in %+v, now:%+v\n",
					ck.Uuid, args, startAt, CLIENT_REQUEST_NO_RESPONSE_MAX_LIMIT, time.Now()))
			}
			return Config{}
		}
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	startAt := time.Now()
	DPrintf("[smck:%v][Join]: servers:%+v, startAt:%+v\n", ck.Uuid, servers, startAt)
	args := JoinArgs{
		Servers:  servers,
		ReqId:    nrand(),
		ClientId: ck.Uuid,
	}

	for {
		// try each known server.
		for i, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", &args, &reply)
			if ok && reply.WrongLeader == false {
				if reply.ClientId != args.ClientId || reply.ReqId != args.ReqId {
					panic(fmt.Sprintf("[smck:%v][Join] args:%+v and reply:%+v data not matching!\n", ck.Uuid, args, reply))
				}
				DPrintf("[smck:%v][Join]: <-%v ok:%v, reply:%+v\n", ck.Uuid, i, ok, reply)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
		if time.Now().Sub(startAt) > CLIENT_REQUEST_NO_RESPONSE_MAX_LIMIT {
			panic(fmt.Sprintf("[smck:%v][Join]: args:%v, startAt:%+v not complete in %+v, now:%+v\n",
				ck.Uuid, args, startAt, CLIENT_REQUEST_NO_RESPONSE_MAX_LIMIT, time.Now()))
		}
	}
}

func (ck *Clerk) Leave(gids []int) {
	startAt := time.Now()
	DPrintf("[smck:%v][Leave]: gids:%+v, startAt:%+v\n", ck.Uuid, gids, startAt)
	args := LeaveArgs{
		GIDs:     gids,
		ReqId:    nrand(),
		ClientId: ck.Uuid,
	}

	for {
		// try each known server.
		for i, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", &args, &reply)
			if ok && reply.WrongLeader == false {
				if reply.ClientId != args.ClientId || reply.ReqId != args.ReqId {
					panic(fmt.Sprintf("[smck:%v][Leave] args:%+v and reply:%+v data not matching!\n", ck.Uuid, args, reply))
				}
				DPrintf("[smck:%v][Leave]: <-%v ok:%v, reply:%+v\n", ck.Uuid, i, ok, reply)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
		if time.Now().Sub(startAt) > CLIENT_REQUEST_NO_RESPONSE_MAX_LIMIT {
			panic(fmt.Sprintf("[smck:%v][Leave]: args:%v, startAt:%+v not complete in %+v, now:%+v\n",
				ck.Uuid, args, startAt, CLIENT_REQUEST_NO_RESPONSE_MAX_LIMIT, time.Now()))
		}
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	startAt := time.Now()
	DPrintf("[smck:%v][Move]: shard:%+v, gid:%+v, startAt:%+v\n", ck.Uuid, shard, gid, startAt)
	args := MoveArgs{
		Shard:    shard,
		GID:      gid,
		ReqId:    nrand(),
		ClientId: ck.Uuid,
	}

	for {
		// try each known server.
		for i, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", &args, &reply)
			if ok && reply.WrongLeader == false {
				if reply.ClientId != args.ClientId || reply.ReqId != args.ReqId {
					panic(fmt.Sprintf("[smck:%v][Move] args:%+v and reply:%+v data not matching!\n", ck.Uuid, args, reply))
				}
				DPrintf("[smck:%v][Move]: <-%v ok:%v, reply:%+v\n", ck.Uuid, i, ok, reply)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
		if time.Now().Sub(startAt) > CLIENT_REQUEST_NO_RESPONSE_MAX_LIMIT {
			panic(fmt.Sprintf("[smck:%v][Move]: args:%v, startAt:%+v not complete in %+v, now:%+v\n",
				ck.Uuid, args, startAt, CLIENT_REQUEST_NO_RESPONSE_MAX_LIMIT, time.Now()))
		}
	}
}
