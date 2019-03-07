# Steps
```
GOPATH=/home/ec2-user/environment/6.824/
export GOPATH

cd ~/environment/6.824/src/raft
go test
go test -run 2A

go test -run 2A 1> out.txt 2> log.txt -v
```

# note
* While in candidate, what to do when receive other RequestVote RPC? Maybe ignore RequestVote while in Cand state?
* for candidate, when sending vote, if RPC fail ( no response from follower ), I assume shouldn't re-send the vote ( can't find into in the paper)
* struct to RPC need to have all filed with first letter capitalized
* If leaderId != rf.votedFor, but term the same , should reject and check it ?
```
Test (2B): leader backs up quickly over incorrect follower logs ...
exit status 1
FAIL	raft	46.101s
```