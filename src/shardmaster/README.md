# steps
```
GOPATH=/home/ec2-user/environment/6.824/ && export GOPATH
```

# links
* https://github.com/sunhay/mit-6.824-2017/blob/master/shardmaster/server.go

# reference
```
func (sm *ShardMaster) addNewGid(gidMap map[int]GidInfo, newGid int, serverNames []string) (result map[int][]int) {
	if _, ok := gidMap[newGid]; ok {
		panic(fmt.Sprintf("[sm:%v][addNewGid] newGid:%v already in gidMap:%+v!\n", sm.me, newGid, gidMap))
	}
	newBucket := make([]int, 0)
	numBuckets := len(gidMap) + 1
	tgtSize := NShards / numBuckets
	remain := NShards % numBuckets
	for gid, gidInfo := range gidMap {
		bucket := gidInfo.Shards
		numToShift := len(gidInfo.Shards) - tgtSize
		if numToShift > 0 && remain > 0 {
			numToShift -= 1
			remain -= 1
		}
		result[gid] = make([]int, 0)
		for numToShift > 0 {
			item := bucket[len(bucket)-1]
			bucket = bucket[:len(bucket)-1]
			newBucket = append(newBucket, item)
			result[gid] = append(result[gid], item)
			numToShift -= 1
		}
	}
	gidMap[newGid] = GidInfo{
		Gid:     newGid,
		Servers: serverNames,
		Shards:  newBucket,
	}
	return
}

func (sm *ShardMaster) removeGid(gidMap map[int]GidInfo, gidToRemove int) (result map[int][]int) {
	if _, ok := gidMap[gidToRemove]; !ok {
		panic(fmt.Sprintf("[sm:%v][removeGid] gidToRemove:%v not in gidMap:%+v!\n", sm.me, gidToRemove, gidMap))
	}
	bucketToRemove := gidMap[gidToRemove].Shards
	delete(gidMap, gidToRemove)
	numBuckets := len(gidMap)
	tgtSize := NShards / numBuckets
	remain := len(bucketToRemove)
	// first round make all remaining bucket has same tgt size
	for gid, gidInfo := range gidMap {
		bucket := gidInfo.Shards
		numToShift := tgtSize - len(bucket)
		result[gid] = make([]int, 0)
		for numToShift > 0 {
			item := bucketToRemove[len(bucketToRemove)-1]
			bucketToRemove = bucketToRemove[:len(bucketToRemove)-1]
			bucket = append(bucket, item)
			result[gid] = append(result[gid], item)
			numToShift -= 1
		}
	}
	// second to distribute remain to all bucket one by one
	if remain != len(bucketToRemove) {
		panic(fmt.Sprintf("[sm:%v][removeGid] remain:%v != len(bucketToRemove):%v!\n", sm.me, remain, len(bucketToRemove)))
	}
	for gid, gidInfo := range gidMap {
		if remain <= 0 {
			return
		}
		bucket := gidInfo.Shards
		item := bucketToRemove[len(bucketToRemove)-1]
		bucketToRemove = bucketToRemove[:len(bucketToRemove)-1]
		bucket = append(bucket, item)
		result[gid] = append(result[gid], item)
	}
	return
}

func (sm *ShardMaster) makeConfig(gidMap map[int]GidInfo) Config {
	newConfig := Config{
		Num:    sm.configs[len(sm.configs)-1].Num + 1,
		Groups: make(map[int][]string),
	}
	for gid, gidInfo := range gidMap {
		for shard := range gidInfo.Shards {
			newConfig.Shards[shard] = gid
		}
		newConfig.Groups[gid] = gidInfo.Servers
	}
	return newConfig
}

func (sm *ShardMaster) joinHandler(args *JoinArgs, reply *JoinReply) {
	for gid, serverNames := range args.Servers {
		sm.addNewGid(sm.gidMap, gid, serverNames)
		reply.WrongLeader = false
	}
	sm.configs = append(sm.configs, sm.makeConfig(sm.gidMap))
}

func (sm *ShardMaster) leaveHandler(args *LeaveArgs, reply *LeaveReply) {
	for gid := range args.GIDs {
		sm.removeGid(sm.gidMap, gid)
		reply.WrongLeader = false
	}
	sm.configs = append(sm.configs, sm.makeConfig(sm.gidMap))
}
```