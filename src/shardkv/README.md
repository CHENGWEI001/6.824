# steps
```
GOPATH=/home/ec2-user/environment/6.824/ && export GOPATH
```

# links
* https://github.com/sunhay/mit-6.824-2017/blob/master/shardmaster/server.go

# notes
* be aware that when doing multiple test back to back , since checkconfig would do query to shardmaster, and when test is tear down, the RPC from shardmaster to shardkv is down, it would cause checkconfig of previous test hang over there
* hit issue that due to I was using KV main thread to do configCheck every 100ms, it would cause KV main thread not able to catch up new config probably, in case like server shutdown and replaying the log. Need to have config thread to do that, and kv main thread just need to keep update latestConfig to configCheck thread whenever changed
* remaining failing test cases:
```
TestConcurrent2
TestChallenge1Concurrent
```
