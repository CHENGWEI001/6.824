# TODO
* TO_CHECCK_STATE_SIZE to only when new log added, not to do in every persist
* new CODE to tell KV when step up/down from leader
* clean up Q when step down from leader for KV
* might need to have rf know maxRaftStateSize so follower when doing append can consider if it is going to go over the limit
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