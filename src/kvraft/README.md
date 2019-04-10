# TODO
* TO_CHECCK_STATE_SIZE to only when new log added, not to do in every persist
* new CODE to tell KV when step up/down from leader
* clean up Q when step down from leader for KV
* might need to have rf know maxRaftStateSize so follower when doing append can consider if it is going to go over the limit
* if lastApplyMsg is nil, skip sending createsnapshot
* remaining issue
```
panic: [3][sendApplyMsg] start:-3, end:-1 is out of bound rf:&{mu:{state:0 sema:0} peers:[0xc425a0e7c0 0xc425a0e7e0 0xc425a0e800 0xc425a0e820 0xc425a0e840 0xc425a0e860 0xc425a0e880] persister:0xc425a120c0 me:3 CurrentTerm:20 VotedFor:0 CurrentRaftState:follower RequestVoteArgsChan:0xc423a8f140 RequestVoteReplyChan:0xc423a8f1a0 AppendEntriesArgsChan:0xc423a8f200 AppendEntriesReplyChan:0xc423a8f260 Log:[{Term:17 Command:{Oc:GET Key:14 Value: ReqId:1043275027529894426 ClientId:4176700636993497660} Index:73} {Term:17 Command:{Oc:APPEND Key:2 Value:x 2 0 y ReqId:3803419040381244477 ClientId:2251850483052948290} Index:74} {Term:17 Command:{Oc:GET Key:12 Value: ReqId:961768185878954139 ClientId:4318741969735371264} Index:75} {Term:17 Command:{Oc:GET Key:0 Value: ReqId:9080361772084258 ClientId:432346059155110483} Index:76} {Term:20 Command:{Oc:APPEND Key:9 Value:x 14 0 y ReqId:1296778536608823500 ClientId:28590891016536490} Index:77} {Term:20 Command:{Oc:GET Key:0 Value: ReqId:9080361772084258 ClientId:432346059155110483} Index:78} {Term:20 Command:{Oc:APPEND Key:7 Value:x 4 0 y ReqId:2849830087548023913 ClientId:1483357863157896697} Index:79} {Term:20 Command:{Oc:GET Key:12 Value: ReqId:3958666311541413780 ClientId:1646914343915344446} Index:80} {Term:20 Command:{Oc:PUT Key:9 Value:x 11 0 y ReqId:1404011541096929158 ClientId:4176700636993497660} Index:81} {Term:20 Command:{Oc:PUT Key:14 Value:x 10 1 y ReqId:1249219876096538808 ClientId:2365983681010572452} Index:82} {Term:20 Command:{Oc:GET Key:12 Value: ReqId:961768185878954139 ClientId:4318741969735371264} Index:83}] CommitIndex:72 LastApplied:2 NextIndex:[62 62 62 62 62 62 62] MatchIndex:[0 0 0 0 0 0 0] ApplyChChan:0xc423a8f2c0 ApplyMsgChan:0xc423a8f0e0 ToStopChan:0xc423a8f320 ToStop:false GetStateReqChan:0xc423a8f380 GetStateReplyChan:0xc423a8f3e0 StartReqChan:0xc423a8f440 StartReplyChan:0xc423a8f4a0 PrevApplyMsgToken:0xc423a8f500 NextApplyMsgToken:0xc423a8f560 LastAppendEntrySentTime:[0001-01-01 00:00:00 +0000 UTC 0001-01-01 00:00:00 +0000 UTC 0001-01-01 00:00:00 +0000 UTC 0001-01-01 00:00:00 +0000 UTC 0001-01-01 00:00:00 +0000 UTC 0001-01-01 00:00:00 +0000 UTC 0001-01-01 00:00:00 +0000 UTC] SelfWorkerCloseChan:0xc423a8f5c0 SelfWorkerChan:0xc425a144e0 SnapshotLastIndex:72 SnapshotLastTerm:17 MsgChan:0xc425a14480 appendReplyChan:<nil> installSnapshotReplyChan:<nil>}



	test_test.go:418: logs were not trimmed (2411 > 2*1000)



panic: runtime error: invalid memory address or nil pointer dereference
[signal SIGSEGV: segmentation violation code=0x1 addr=0x0 pc=0x649539]

goroutine 154354 [running]:
kvraft.(*KVServer).createSnapshot(0xc42226a3c0)
	/home/ec2-user/environment/6.824/src/kvraft/server.go:242 +0x279
kvraft.(*KVServer).CheckRaftStateSize(0xc42226a3c0, 0x18)
	/home/ec2-user/environment/6.824/src/kvraft/server.go:305 +0x2bd
kvraft.(*KVServer).specialApplyMsgHandler(0xc42226a3c0, 0xc4225cd140)
	/home/ec2-user/environment/6.824/src/kvraft/server.go:319 +0x8f
kvraft.(*KVServer).StartKVThread(0xc42226a3c0)
	/home/ec2-user/environment/6.824/src/kvraft/server.go:369 +0xf65
created by kvraft.StartKVServer
	/home/ec2-user/environment/6.824/src/kvraft/server.go:499 +0x801
```

# Steps
```
GOPATH=/home/ec2-user/environment/6.824/
export GOPATH

go test -run 3A 1> out.txt 2> log.txt -v

```

# Notes
* Very critical about RPC warning, basically it means there should not be any value left in the structure when putting to RPC
```
// warn if the value contains non-default values,
// as it would if one sent an RPC but the reply
// struct was already modified. if the RPC reply
// contains default values, GOB won't overwrite
// the non-default value.
```

* refer to raft/config.go , below API about how to setup RPC
    ```
    func (cfg *config) start1(i int) {
    ```

* My original idea is to have KV thread and Raft thread and each touch its own data, one possible deadlock as below:
  * KV thread is sending msg to raft thread for it to hanlde , so wait
  * meantime raft thread is send applych to KV thread , but KV thread is waiting , the applyCh doesn't get chance to be executed. => deadlock
  *

* How to handle below scenario?
  Client send put(key=3, y = a) , but fail to get response from kv server( though data already written to kv server, reply RPC fail case)
  Client also in the mean time has get(key=3) request, this case what the value should return? I think it is okay to either has result or get -> put or put -> get since in lineariblity it only require to have a valid result

* Consider case where client send request to KV, kv may receive but fail to reply RPC, and this kv failed, later the other KV task over, in that case , how should we handle. ( my idea is to update per client last request info for other slave as well)

* Need to use ReqId to check whether this ReqId has been applied to KV state or not, since same reuqest might go into RAFT multiple times, we reply on reqId to detect duplication

* there is one issue puzzled me a while, since in my early implementation somehow when rf instance is in candidate state, even if it recieved append from leader with equal or higher term , I didn't update votedFor to that leader for the rf instance, as a result , it is too easy to start new election, and the issue is that eventually when it settle, term is higher then all log items, and since per rule we can't commit log item which is not in the current term , it get stuck
  Below line I changed ```reqAppend.Term >= rf.CurrentTerm``` to ```reqAppend.Term >= rf.CurrentTerm``` and update votedFor to fix the issue.
  ```
  			if reqAppend.Term >= rf.CurrentTerm {  < - - chage to include equal
  				rf.VotedFor = reqAppend.LeadId  < - - key here is to update votedFor
  				rf.updateState(follower)
  				// if ack an Append RPC, return to follower state and handle the req
  				go rf.redirectAppendHelper(reqAppend)
  				return
  			}
  ```

* I need to check update interval for every 10ms and not to resend the msg ( original checking every 100ms too long)
* the get request should only get data from KV raft after the command commited, if I get data when just received request but not yet commit, the kv state might not get up to data until that get request command index, that case may get stale data ( like get something which is not has previous put command)

# DEBUG quick pad
```
func testPrintf(format string, a ...interface{}) (n int, err error) {
	format = "[TestDebug]" + format
	log.Printf(format, a...)
	return
}
```