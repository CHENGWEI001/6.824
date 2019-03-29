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
* Below case seems sometimes stuck
```
=== RUN   TestReliableChurn2C
Test (2C): churn ...
panic: [1][leaderHandler] MatchIdex:282 is higher than NextIndex:281, it is unexpected!

goroutine 173372 [running]:
raft.leaderHandler(0xc420c8d900)
	/home/ec2-user/environment/6.824/src/raft/raft.go:1159 +0x1ed3
raft.startRaftThread(0xc420c8d900)
	/home/ec2-user/environment/6.824/src/raft/raft.go:662 +0x259
created by raft.Make
	/home/ec2-user/environment/6.824/src/raft/raft.go:517 +0x926
exit status 2
FAIL	raft	219.853s


=== RUN   TestUnreliableChurn2C
Test (2C): unreliable churn ...
panic: [2][leaderHandler] MatchIdex:293 is higher than NextIndex:292, it is unexpected!

goroutine 144055 [running]:
raft.leaderHandler(0xc4233c6b40)
	/home/ec2-user/environment/6.824/src/raft/raft.go:1159 +0x1ed3
raft.startRaftThread(0xc4233c6b40)
	/home/ec2-user/environment/6.824/src/raft/raft.go:662 +0x259
created by raft.Make
	/home/ec2-user/environment/6.824/src/raft/raft.go:517 +0x926
exit status 2
FAIL	raft	216.601s

Test (2C): Figure 8 (unreliable) ...
--- FAIL: TestFigure8Unreliable2C (120.39s)
	config.go:220: test took longer than 120 seconds
```