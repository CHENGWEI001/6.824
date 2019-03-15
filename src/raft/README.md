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
=== RUN   TestInitialElection2A
warning: only one CPU, which may conceal locking bugs
Test (2A): initial election ...
  ... Passed --   3.5  3   30    0
--- PASS: TestInitialElection2A (3.53s)
=== RUN   TestReElection2A
Test (2A): election after network failure ...
  ... Passed --   5.4  3   74    0
--- PASS: TestReElection2A (5.39s)
=== RUN   TestBasicAgree2B
Test (2B): basic agreement ...
  ... Passed --   2.0  5   32    3
--- PASS: TestBasicAgree2B (1.97s)
=== RUN   TestFailAgree2B
Test (2B): agreement despite follower disconnection ...
  ... Passed --   7.8  3   82    8
--- PASS: TestFailAgree2B (7.82s)
=== RUN   TestFailNoAgree2B
Test (2B): no agreement if too many followers disconnect ...
  ... Passed --   4.3  5  116    4
--- PASS: TestFailNoAgree2B (4.31s)
=== RUN   TestConcurrentStarts2B
Test (2B): concurrent Start()s ...
  ... Passed --   1.3  3   10    6
--- PASS: TestConcurrentStarts2B (1.27s)
=== RUN   TestRejoin2B
Test (2B): rejoin of partitioned leader ...
  ... Passed --   7.0  3  104    4
--- PASS: TestRejoin2B (7.03s)
=== RUN   TestBackup2B
Test (2B): leader backs up quickly over incorrect follower logs ...
  ... Passed --  48.0  5 2044  102
--- PASS: TestBackup2B (47.98s)
=== RUN   TestCount2B
Test (2B): RPC counts aren't too high ...
  ... Passed --   2.9  3   26   12
--- PASS: TestCount2B (2.91s)
=== RUN   TestPersist12C
Test (2C): basic persistence ...
  ... Passed --   9.0  3  270    6
--- PASS: TestPersist12C (8.96s)
=== RUN   TestPersist22C
Test (2C): more persistence ...
  ... Passed --  25.7  5  728   16
--- PASS: TestPersist22C (25.69s)
=== RUN   TestPersist32C
Test (2C): partitioned leader and one follower crash, leader restarts ...
  ... Passed --   4.6  3   68    4
--- PASS: TestPersist32C (4.62s)
=== RUN   TestFigure82C
Test (2C): Figure 8 ...
  ... Passed --  47.0  5 21685   10
--- PASS: TestFigure82C (47.02s)
=== RUN   TestUnreliableAgree2C
Test (2C): unreliable agreement ...
  ... Passed --  20.3  5  380  246
--- PASS: TestUnreliableAgree2C (20.31s)
=== RUN   TestFigure8Unreliable2C
Test (2C): Figure 8 (unreliable) ...
--- FAIL: TestFigure8Unreliable2C (267.09s)
	config.go:220: test took longer than 120 seconds
=== RUN   TestReliableChurn2C
Test (2C): churn ...
  ... Passed --  16.7  5  188    3
--- PASS: TestReliableChurn2C (16.74s)
=== RUN   TestUnreliableChurn2C
Test (2C): unreliable churn ...
  ... Passed --  17.6  5  324  113
--- PASS: TestUnreliableChurn2C (17.63s)
FAIL
exit status 1
FAIL	raft	490.469s

=== RUN   TestUnreliableAgree2C
Test (2C): unreliable agreement ...
--- FAIL: TestUnreliableAgree2C (16.28s)
	config.go:471: one(800) failed to reach agreement
	config.go:471: one(803) failed to reach agreement
	config.go:471: one(801) failed to reach agreement
	config.go:471: one(8) failed to reach agreement
	testing.go:699: race detected during execution of test
exit status 2
FAIL	raft	207.477s


```