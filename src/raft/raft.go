package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "labrpc"
import "time"
import "math/rand"
import "fmt"

//import "log"
// import "math"

import "bytes"
import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	Code         int // for raft to tell kv what to do for this special MSG
}

const DONE_INSTALL_SNAPSHOT = 0
const DONE_CREATE_SNAPSHOT = 1
const TO_CHECCK_STATE_SIZE = 2

//
// A Go object implementing a single Raft peer.
//

const ELECTION_TIMEOUT_MILISECONDS = 500
const ELECTION_TIMEOUT_RAND_RANGE_MILISECONDS = 500
const HEARTBEAT_TIMEOUT_MILISECONDS = 200
const LEADER_PEER_TICK_MILISECONDS = 200
const EXTERNAL_API_TIMEOUT_MILLISECONDS = 1000 * time.Millisecond
const INITIAL_VOTED_FOR = -1

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	CurrentTerm              int                        // current term of raft instance
	VotedFor                 int                        // previous vote for who, initial to -1
	CurrentRaftState         RaftState                  // current raft state
	RequestVoteArgsChan      chan *RequestVoteArgs      // channel for receiving Request Vote
	RequestVoteReplyChan     chan *RequestVoteReply     // channel for response Request Vote
	AppendEntriesArgsChan    chan *AppendEntriesArgs    // channel for receiving Append Entry
	AppendEntriesReplyChan   chan *AppendEntriesReply   // channel for response Append Entry
	Log                      []LogEntry                 // log entries
	CommitIndex              int                        // indx of highest log entry known to be committed
	LastApplied              int                        // index of highest log entry applied to state machine
	NextIndex                []int                      // index of the next log entry to send to the server
	MatchIndex               []int                      // index of highest log entry known to be replicated on server
	ApplyChChan              chan *ApplychArgs          // channel for receiving ApplyCh from client
	ApplyMsgChan             chan ApplyMsg              // channel for updating to tester
	ToStopChan               chan bool                  // channel for stopping the raft instance thread
	ToStop                   bool                       // indicator for stopping raft instance
	GetStateReqChan          chan *GetStateReq          // channel for GetState API input
	GetStateReplyChan        chan *GetStateReply        // channel for GetState API output
	StartReqChan             chan *StartReq             // channel for Start API input
	StartReplyChan           chan *StartReply           // channel for Start API output
	PrevApplyMsgToken        chan int64                 // channel for scheduling go routine to sendMsg to tester
	NextApplyMsgToken        chan int64                 // channel for scheduling go routine to sendMsg to tester
	LastAppendEntrySentTime  []time.Time                // last time stamp sent appendEntry
	SelfWorkerCloseChan      chan bool                  // chan for closing self worker ( which is to handle applych)
	SelfWorkerChan           chan interface{}           // channel to worker for sending ApplyMsg back to tester
	SnapshotLastIndex        int                        // index of last log entry for the snapshot, init to -1
	SnapshotLastTerm         int                        // term of last log entry for the snapshot
	MsgChan                  chan interface{}           // channel for raft main thread to handle incoming request/msg
	appendReplyChan          chan *AppendEntriesReply   // channel for append reply when it is leader
	installSnapshotReplyChan chan *InstallSnapshotReply // channel for install snapshot reply when it is leader
}

type GetStateReq struct {
}

type GetStateReply struct {
	term     int
	isLeader bool
}

type StartReq struct {
	command interface{}
}

type StartReply struct {
	index    int
	term     int
	isLeader bool
}

type CreateSnapshotReq struct {
	kvState      []byte
	lastApplyMsg *ApplyMsg
}

type InstallSnapshotReq struct {
	args      *InstallSnapshotArgs
	replyChan chan *InstallSnapshotReply
}

type StartReuqest struct {
	args      *StartReq
	replyChan chan *StartReply
}

type GetStateReuqest struct {
	args      *GetStateReq
	replyChan chan *GetStateReply
}

// serverState indicating current raft instance state
type RaftState string

type LogEntry struct {
	Term    int         // term of the given log entry
	Command interface{} // command for state machine
	Index   int         // index of this logEntry in log
}

// RaftPersistence is persisted to the `persister`, and contains all necessary data to restart a failed node
type RaftPersistence struct {
	CurrentTerm       int
	Log               []LogEntry
	VotedFor          int
	SnapshotLastIndex int
	SnapshotLastTerm  int
}

const (
	follower  RaftState = "follower"
	candidate           = "candidate"
	leader              = "leader"
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	req := &GetStateReuqest{
		args:      &GetStateReq{},
		replyChan: make(chan *GetStateReply),
	}
	DPrintf("[rf:%v]start GetState: args: %+v\n", rf.me, req.args)
	rf.MsgChan <- req
	timer := time.After(EXTERNAL_API_TIMEOUT_MILLISECONDS)
	select {
	case reply := <-req.replyChan:
		DPrintf("[rf:%v]done GetState: reply: %+v\n", rf.me, reply)
		return reply.term, reply.isLeader
	case <-timer:
		DPrintf("[rf:%v] GetState timeout: req.args: %+v\n", rf.me, req.args)
		return 0, false
	}

}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	// DPrintf("[%v][persist] : to save to persist rf:%+v\n", rf.me, rf)
	data := rf.EncodeState()
	rf.persister.SaveRaftState(data)

	rf.SelfWorkerChan <- &ApplyMsg{
		CommandValid: false,
		Code:         TO_CHECCK_STATE_SIZE,
	}
	// DPrintf("[%v][persist] done RaftStateSize:%+v, DecodeState:%+v\n",
	// 	rf.me, rf.persister.RaftStateSize())
	DPrintf("[%v][persist] done RaftStateSize:%+v, DecodeState:%+v\n",
		rf.me, rf.persister.RaftStateSize(),
		rf.DecodeState(rf.persister.ReadRaftState()))
	// DPrintf("[%v][persist] : done save to persist rf:%+v\n", rf.me, rf)
}

func (rf *Raft) DecodeState(data []byte) *RaftPersistence {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	// var term int
	// var votedFor int
	// var rfLog []LogEntry
	var obj RaftPersistence
	if d.Decode(&obj) != nil {
		panic(fmt.Sprintf("[%v][DecodeState] fail to read Persist!\n", rf.me))
	}
	return &obj
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	// var term int
	// var votedFor int
	// var rfLog []LogEntry
	var obj RaftPersistence
	if d.Decode(&obj) != nil {
		panic(fmt.Sprintf("[%v][readPersist] fail to read Persist!\n", rf.me))
	}
	rf.CurrentTerm = obj.CurrentTerm
	rf.VotedFor = obj.VotedFor
	rf.Log = obj.Log
	rf.SnapshotLastIndex = obj.SnapshotLastIndex
	rf.SnapshotLastTerm = obj.SnapshotLastTerm
	rf.CommitIndex = max(rf.SnapshotLastIndex, 0)
	// DPrintf("[%v][readPersist] : read from persist rf:%+v\n", rf.me, rf)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	Term         int // candidate's term
	CandidateId  int // Id who send this request
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidates's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).

	Term        int  // currentTerm of the raft instance to response to candidate
	VoteGranted bool // true if the raft instance agree to vote the candidate
	FromId      int  // indicating this reply is from which raft instance
}

type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeadId       int        // leader Id
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store ( empty for heartbeat )
	LeaderCommit int        // leader's commitIndex
	TimeStamp    int64      // time this req sent
}

type AppendEntriesReply struct {
	Term                   int   // current term , for leader to update itself
	Success                bool  // [ToDo]
	FromId                 int   // indicating this reply is from which raft instance
	NextIndex              int   // if success: nextIndex for this follower, if failure: next Index to try
	VotedFor               int   // Replay current votedFor for the raft Instace
	TimeStamp              int64 // time this req sent
	IsValid                bool  // to indidate if sender can ignore this Reply, this is to handle out of order AppendEntry case
	ConflictTerm           int   // follower conflicting term
	ConflictTermFirstIndex int   // follower's first index of conflicting term
}

type ApplychArgs struct {
	command interface{}
	status  chan int
}

type AppendEntriesWorkerReq struct {
	args      *AppendEntriesArgs
	replyChan chan *AppendEntriesReply
}

type RequestVoteWorkerReq struct {
	args      *RequestVoteArgs
	replyChan chan *RequestVoteReply
}

type InstallSnapshotWorkerReq struct {
	args      *InstallSnapshotArgs
	replyChan chan *InstallSnapshotReply
}

type InstallSnapshotArgs struct {
	Term              int    // leader's term
	LeadId            int    // leader's id
	LastIncludedIndex int    // last included index in the snapshot
	LastIncludedTerm  int    // term of last included index of the snapshot
	Data              []byte // raw bytes of the snapshot chunk
	TimeStamp         int64  // time this req sent
}

type InstallSnapshotReply struct {
	Term      int   // currentTerm for the follower for leader to update
	FromId    int   // indicating this reply is from which raft instance
	TimeStamp int64 // time this req sent
	IsValid   bool  // to indidate if sender can ignore this Reply, this is to handle out of order case
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	DPrintf("[RequestVote][%v -> %v]: %+v\n", args.CandidateId, rf.me, *args)
	rf.RequestVoteArgsChan <- args
	*reply = *(<-rf.RequestVoteReplyChan)
	DPrintf("[RequestVote][%v <- %v]: %+v\n", args.CandidateId, reply.FromId, *reply)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("[AppendEntries][%v -> %v]: %+v\n", args.LeadId, rf.me, *args)
	rf.AppendEntriesArgsChan <- args
	*reply = *(<-rf.AppendEntriesReplyChan)
	DPrintf("[AppendEntries][%v <- %v]: %+v\n", args.LeadId, reply.FromId, *reply)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	DPrintf("[InstallSnapshot][%v -> %v]: %+v\n", args.LeadId, rf.me, *args)
	req := InstallSnapshotReq{
		args:      args,
		replyChan: make(chan *InstallSnapshotReply),
	}
	rf.MsgChan <- &req
	*reply = *(<-req.replyChan)
	DPrintf("[InstallSnapshot][%v <- %v]: %+v\n", args.LeadId, reply.FromId, *reply)
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	DPrintf("[RPC][RequestVote][%v -> %v]: args: %+v\n", args.CandidateId, server, *args)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	DPrintf("[RPC][RequestVote][%v <- %v]: ok: %v, reply: %+v\n", args.CandidateId, server, ok, *reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf("[RPC][AppendEntries][%v -> %v]: args: %+v\n", args.LeadId, server, *args)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	DPrintf("[RPC][AppendEntries][%v <- %v]: ok: %v, reply: %+v\n", args.LeadId, server, ok, *reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	DPrintf("[RPC][InstallSnapshot][%v -> %v]: args: %+v\n", args.LeadId, server, *args)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	DPrintf("[RPC][InstallSnapshot][%v <- %v]: ok: %v, reply: %+v\n", args.LeadId, server, ok, *reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	req := &StartReuqest{
		args: &StartReq{
			command: command,
		},
		replyChan: make(chan *StartReply),
	}
	DPrintf("[rf:%v]start Start: args: %+v\n", rf.me, req.args)
	rf.MsgChan <- req
	timer := time.After(EXTERNAL_API_TIMEOUT_MILLISECONDS)
	select {
	case reply := <-req.replyChan:
		DPrintf("[rf:%v]done Start: req: %+v, reply: %+v\n", rf.me, req, reply)
		return reply.index, reply.term, reply.isLeader
	case <-timer:
		DPrintf("[rf:%v]Start timeout: req: %+v\n", rf.me, req)
		return 0, 0, false
	}
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	// DPrintf("[Kill][%v]: %+v\n", rf.me, rf)
	go func(rf *Raft) {
		rf.ToStopChan <- true
	}(rf)
}

func (rf *Raft) CreateSnapshot(kvState []byte, lastApplyMsg *ApplyMsg) {
	rf.MsgChan <- &CreateSnapshotReq{
		kvState:      kvState,
		lastApplyMsg: lastApplyMsg,
	}
}

func (rf *Raft) updateTerm(term int) {
	// rf.Lock()
	// defer rf.Unlock()
	rf.CurrentTerm = term
}

func (rf *Raft) updateState(state RaftState) {
	// rf.Lock()
	// defer rf.Unlock()
	rf.CurrentRaftState = state
}

// init NextIndex and MatchIndex after every election
func (rf *Raft) initIndex() {
	lastIndex, _ := rf.getLastLogEntryInfo()
	rf.NextIndex = make([]int, len(rf.peers))
	rf.MatchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.NextIndex[i] = lastIndex + 1
	}

}

func (rf *Raft) GetStateHelper(req *GetStateReuqest) {
	req.replyChan <- &GetStateReply{
		term:     rf.CurrentTerm,
		isLeader: rf.CurrentRaftState == leader,
	}
}

func (rf *Raft) StartHelper(req *StartReuqest) {
	if rf.CurrentRaftState == leader {
		rf.Log = append(rf.Log, LogEntry{
			Term:    rf.CurrentTerm,
			Command: req.args.command,
			Index:   rf.NextIndex[rf.me],
		})
		rf.MatchIndex[rf.me] = rf.NextIndex[rf.me]
		rf.NextIndex[rf.me]++
		rf.leaderBroadcast(rf.appendReplyChan, rf.installSnapshotReplyChan)
	}
	req.replyChan <- &StartReply{
		index:    rf.MatchIndex[rf.me],
		term:     rf.CurrentTerm,
		isLeader: rf.CurrentRaftState == leader,
	}
	// since we add new entry to log, need to save to persist
	rf.persist()
}

func (rf *Raft) getLastLogEntryInfo() (index int, term int) {
	if len(rf.Log) > 0 {
		index = rf.Log[len(rf.Log)-1].Index
		term = rf.Log[len(rf.Log)-1].Term
	} else {
		index = rf.SnapshotLastIndex
		term = rf.SnapshotLastTerm
	}
	if index < 0 {
		panic(fmt.Sprintf("[rf:%v] getLastLogEntryInfo: invalid index:%v! rf:%+v\n", rf.me, index, rf))
	}
	return
}

func (rf *Raft) getLogEntryIndexFromGlobalIndex(globalIndex int) (logEntryIndex int) {
	logEntryIndex = globalIndex - rf.SnapshotLastIndex - 1
	return
}

func (rf *Raft) getGlobalIndexFromLogEntryIndex(logEntryIndex int) (globalIndex int) {
	globalIndex = logEntryIndex + rf.SnapshotLastIndex + 1
	return
}

func (rf *Raft) getLogEntryInfo(globalIndex int) (index int, term int) {
	logEntryIndex := rf.getLogEntryIndexFromGlobalIndex(globalIndex)
	if logEntryIndex >= 0 && logEntryIndex < len(rf.Log) {
		index = rf.Log[logEntryIndex].Index
		term = rf.Log[logEntryIndex].Term
	} else if logEntryIndex == -1 {
		index = rf.SnapshotLastIndex
		term = rf.SnapshotLastTerm
	} else { // when we don't have any info for this index, already disgard due to snapshot
		index = -1
		term = 0
	}
	if index < -1 {
		panic(fmt.Sprintf("[rf:%v] getPrevLogEntryInfo: invalid index:%v! logEntryIndex:%v, globalIndex:%v, rf:%+v\n",
			rf.me, index, logEntryIndex, globalIndex, rf))
	}
	return
}

func (rf *Raft) EncodeState() (state []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(RaftPersistence{
		CurrentTerm:       rf.CurrentTerm,
		VotedFor:          rf.VotedFor,
		Log:               rf.Log,
		SnapshotLastIndex: rf.SnapshotLastIndex,
		SnapshotLastTerm:  rf.SnapshotLastTerm,
	})
	state = w.Bytes()
	return
}

func (rf *Raft) createSnapshotHelper(req *CreateSnapshotReq) {
	// prepare the data to store
	if req.lastApplyMsg == nil {
		panic(fmt.Sprintf("[rf:%v][createSnapshotHelper] req.lastApplyMsg is nil! req:%+v, rf:%+v\n",
			rf.me, req, rf))
	}
	DPrintf("[rf:%v] createSnapshotHelper: to handle CreateSnapshotReq:%+v, lastApplyMsg:%+v, RaftStateSize:%+v\n",
		rf.me, req, req.lastApplyMsg, rf.persister.RaftStateSize())
	localCutOffIndex := rf.getLogEntryIndexFromGlobalIndex(req.lastApplyMsg.CommandIndex)
	// because for follower createSnapshot and installSnapshot migth be conflict with each other
	// so when ever recieve createSnapshot with index lower than lastSnapshot index, skip saving
	// and reply to kv directly
	if localCutOffIndex >= 0 {
		if localCutOffIndex < 0 || localCutOffIndex >= len(rf.Log) {
			panic(fmt.Sprintf("[rf:%v][createSnapshotHelper] invalid index for log:%v, rf:%+v\n",
				rf.me, localCutOffIndex, rf))
		}
		rf.SnapshotLastTerm = rf.Log[localCutOffIndex].Term
		rf.SnapshotLastIndex = req.lastApplyMsg.CommandIndex
		// cut off older log which is included in snapshot
		rf.Log = rf.Log[localCutOffIndex+1:]

		state := rf.EncodeState()
		// save both rf state and kv state into persist snapshot
		rf.persister.SaveStateAndSnapshot(state, req.kvState)
	}
	// ask self worker to send applymsg to tell KV snapshot is done
	rf.SelfWorkerChan <- &ApplyMsg{
		CommandValid: false,
		Code:         DONE_CREATE_SNAPSHOT,
	}
	DPrintf("[rf:%v] createSnapshotHelper: done handle CreateSnapshotReq: RaftStateSize:%+v, rf%+v\n",
		rf.me, rf.persister.RaftStateSize(), rf)
}

func (rf *Raft) InstallSnapshotHelper(req *InstallSnapshotReq) {
	// per Fig2 rule when receiving higher term
	if req.args.Term > rf.CurrentTerm {
		rf.updateState(follower)
		return
	}
	if req.args.Term < rf.CurrentTerm {
		return
	}
	if req.args.LeadId == rf.VotedFor {
		if rf.CurrentRaftState != follower {
			panic(fmt.Sprintf("[%v][InstallSnapshotHelper] not expect go to this point under state:%v, rf:%+v",
				rf.me, rf.CurrentRaftState, rf))
		}
		// if recieved install snapshot request with snapshot included index smaller than itself
		// snapshotLastIndex, we probably could just ignore it
		if req.args.LastIncludedIndex <= rf.CommitIndex {
			return
		}
		// localCutOffIndex >= len(rf.Log) would be the case where follower is lagging behind
		localCutOffIndex := min(rf.getLogEntryIndexFromGlobalIndex(req.args.LastIncludedIndex), len(rf.Log)-1)
		rf.SnapshotLastIndex = req.args.LastIncludedIndex
		rf.SnapshotLastTerm = req.args.LastIncludedTerm
		// cut off older log which is included in snapshot
		if localCutOffIndex < -1 || localCutOffIndex >= len(rf.Log) {
			panic(fmt.Sprintf("[rf:%v][InstallSnapshotHelper] invalid index for log:%v, rf:%+v\n",
				rf.me, localCutOffIndex, rf))
		}
		rf.Log = rf.Log[localCutOffIndex+1:]
		rf.CommitIndex = rf.SnapshotLastIndex
		state := rf.EncodeState()
		rf.persister.SaveStateAndSnapshot(state, req.args.Data)
		// ask self worker to send applymsg with update data from snapshot to KV peer
		rf.SelfWorkerChan <- &ApplyMsg{
			CommandValid: false,
			Code:         DONE_INSTALL_SNAPSHOT,
		}
	}
}

func (rf *Raft) MsgFlusher() {
	startAt := time.Now()
loop:
	for {
		select {
		case msg := <-rf.MsgChan:
			switch msgType := msg.(type) {
			case *StartReuqest:
				if req, ok := msg.(*StartReuqest); ok {
					DPrintf("[%v] MsgFlusher: to handle StartReuqest: args:%+v, startAt:%+v\n",
						rf.me, req.args, startAt)
					req.replyChan <- &StartReply{
						isLeader: false,
					}
					DPrintf("[%v] MsgFlusher: done handle StartReuqest: startAt:%+v\n",
						rf.me, startAt)
				} else {
					panic(fmt.Sprintf("[%v] MsgFlusher: can't handle msgType:%+v, msg:%+v\n",
						rf.me, msgType, req))
				}
			case *GetStateReuqest:
				if req, ok := msg.(*GetStateReuqest); ok {
					DPrintf("[%v] MsgFlusher: to handle GetStateReuqest: args:%+v, startAt:%+v\n",
						rf.me, req.args, startAt)
					req.replyChan <- &GetStateReply{
						isLeader: false,
					}
					DPrintf("[%v] MsgFlusher: done handle GetStateReuqest: startAt:%+v\n",
						rf.me, startAt)
				} else {
					panic(fmt.Sprintf("[%v] MsgFlusher: can't handle msgType:%+v, msg:%+v\n",
						rf.me, msgType, req))
				}
			default:
				DPrintf("[%v] MsgHandler: ignore msgType:%+v, msg:%+v\n",
					rf.me, msgType, msg)
			}
		default:
			break loop
		}
	}
}

func (rf *Raft) MsgHandler(msg interface{}) {
	startAt := time.Now()
	switch msgType := msg.(type) {
	case *CreateSnapshotReq:
		if req, ok := msg.(*CreateSnapshotReq); ok {
			DPrintf("[%v] MsgHandler: to handle CreateSnapshotReq: kvState:%+v, lastApplyMsg:%+v, startAt:%+v\n",
				rf.me, req.kvState, req.lastApplyMsg, startAt)
			rf.createSnapshotHelper(req)
			DPrintf("[%v] MsgHandler: done handle CreateSnapshotReq: startAt:%+v\n",
				rf.me, startAt)
		} else {
			panic(fmt.Sprintf("[%v] MsgHandler: can't handle msgType:%+v, msg:%+v\n",
				rf.me, msgType, req))
		}
	case *InstallSnapshotReq:
		if req, ok := msg.(*InstallSnapshotReq); ok {
			DPrintf("[%v] MsgHandler: to handle InstallSnapshotReq: args:%+v, startAt:%+v\n",
				rf.me, req.args, startAt)
			rf.InstallSnapshotHelper(req)
			DPrintf("[%v] MsgHandler: done handle InstallSnapshotReq: startAt:%+v\n",
				rf.me, startAt)
		} else {
			panic(fmt.Sprintf("[%v] MsgHandler: can't handle msgType:%+v, msg:%+v\n",
				rf.me, msgType, req))
		}
	case *StartReuqest:
		if req, ok := msg.(*StartReuqest); ok {
			DPrintf("[%v] MsgHandler: to handle StartReuqest: args:%+v, startAt:%+v\n",
				rf.me, req.args, startAt)
			rf.StartHelper(req)
			DPrintf("[%v] MsgHandler: done handle StartReuqest: startAt:%+v\n",
				rf.me, startAt)
		} else {
			panic(fmt.Sprintf("[%v] MsgHandler: can't handle msgType:%+v, msg:%+v\n",
				rf.me, msgType, req))
		}
	case *GetStateReuqest:
		if req, ok := msg.(*GetStateReuqest); ok {
			DPrintf("[%v] MsgHandler: to handle GetStateReuqest: args:%+v, startAt:%+v\n",
				rf.me, req.args, startAt)
			rf.GetStateHelper(req)
			DPrintf("[%v] MsgHandler: done handle GetStateReuqest: startAt:%+v\n",
				rf.me, startAt)
		} else {
			panic(fmt.Sprintf("[%v] MsgHandler: can't handle msgType:%+v, msg:%+v\n",
				rf.me, msgType, req))
		}
	default:
		panic(fmt.Sprintf("[%v] MsgHandler: unexpected msgType:%+v, msg:%+v\n",
			rf.me, msgType, msg))
	}

}

func (rf *Raft) Lock() {
	rf.mu.Lock()
}

func (rf *Raft) Unlock() {
	rf.mu.Unlock()
}

func (rf *Raft) binarySearchForTerm(LogEntries []LogEntry, start int, end int, tgtTerm int, searchForFirst bool) int {
	if start < 0 || start >= len(LogEntries) || end < 0 || end >= len(LogEntries) {
		panic(fmt.Sprintf("[rf:%v]binarySearchForTerm: invalid input! start:%+v, end:%+v\n", rf.me, start, end))
	}
	lo := start
	hi := end
	for lo+1 < hi {
		mi := lo + (hi-lo)/2
		if LogEntries[mi].Term < tgtTerm {
			lo = mi + 1
		} else if LogEntries[mi].Term > tgtTerm {
			hi = mi - 1
		} else {
			if searchForFirst {
				hi = mi
			} else {
				lo = mi
			}
		}
	}
	if LogEntries[lo].Term != tgtTerm && LogEntries[hi].Term != tgtTerm {
		return -1
	}
	if searchForFirst {
		if LogEntries[lo].Term == tgtTerm {
			return lo
		} else {
			return hi
		}
	}
	if LogEntries[hi].Term == tgtTerm {
		return hi
	} else {
		return lo
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).

	rf.VotedFor = INITIAL_VOTED_FOR
	rf.CurrentTerm = 0
	rf.CurrentRaftState = follower
	rf.RequestVoteArgsChan = make(chan *RequestVoteArgs)
	rf.RequestVoteReplyChan = make(chan *RequestVoteReply)
	rf.AppendEntriesArgsChan = make(chan *AppendEntriesArgs)
	rf.AppendEntriesReplyChan = make(chan *AppendEntriesReply)
	rf.ApplyChChan = make(chan *ApplychArgs)
	rf.ApplyMsgChan = applyCh
	rf.CommitIndex = 0
	rf.LastApplied = 0
	// initial with index 1, concept is that when up , there is already index = 0 done with term 0
	rf.Log = make([]LogEntry, 1)
	rf.ToStopChan = make(chan bool)
	rf.ToStop = false
	rf.GetStateReqChan = make(chan *GetStateReq)
	rf.GetStateReplyChan = make(chan *GetStateReply)
	rf.StartReqChan = make(chan *StartReq)
	rf.StartReplyChan = make(chan *StartReply)
	rf.PrevApplyMsgToken = make(chan int64)
	rf.NextApplyMsgToken = make(chan int64)
	rf.LastAppendEntrySentTime = make([]time.Time, len(peers))
	rf.SnapshotLastIndex = -1
	rf.SnapshotLastTerm = 0
	rf.MsgChan = make(chan interface{}, 8192)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.initIndex()
	DPrintf("[%v][Make]: rf:%+v\n", rf.me, rf)

	// start self worker which is for applyCh
	rf.startSelfWorker()

	// start raft main thread
	go startRaftThread(rf)

	// start first sendMsg scheduling thread
	// go func(nextToken chan int64) {
	// 	nextToken <- time.Now().Unix()
	// }(rf.PrevApplyMsgToken)

	return rf
}

// self Worker should start along with main rf thread
func (rf *Raft) startSelfWorker() {
	rf.SelfWorkerCloseChan = make(chan bool)
	rf.SelfWorkerChan = make(chan interface{}, 8192)
	go rf.startRaftWorkerThread(rf.me, rf.SelfWorkerCloseChan, rf.SelfWorkerChan, rf.CurrentTerm, rf.CurrentRaftState)
}

// self worker should stop along with main rf thread
func (rf *Raft) stopSelfWorker() {
	close(rf.SelfWorkerCloseChan)
}

func (rf *Raft) startRaftWorkerThread(id int, workerCloseChan chan bool, workerChan chan interface{},
	term int, state RaftState) {
	for {
		select {
		case msg := <-workerChan:
			// DPrintf("[%v] Worker:(id:%v, term:%v, state:%v), to handle TypeOf(msg):%+v, msg:%+v\n",
			// 	rf.me, id, term, state, reflect.TypeOf(msg), msg)
			switch msgType := msg.(type) {
			case *ApplyMsg:
				if applyMsg, ok := msg.(*ApplyMsg); ok {
					DPrintf("[%v] Worker:(id:%v, term:%v, state:%v), to handle applyMsg:%+v\n",
						rf.me, id, term, state, applyMsg)
					rf.ApplyMsgChan <- *applyMsg
					DPrintf("[%v] Worker:(id:%v, term:%v, state:%v), done handle applyMsg:%+v\n",
						rf.me, id, term, state, applyMsg)
				} else {
					panic(fmt.Sprintf("[%v] Worker:(id:%v, term:%v, state:%v), can't handle msgType:%+v, msg:%+v\n",
						rf.me, id, term, state, msgType, msg))
				}
			case *AppendEntriesWorkerReq:
				if appendEntriesWorkerReq, ok := msg.(*AppendEntriesWorkerReq); ok {
					DPrintf("[%v] Worker:(id:%v, term:%v, state:%v), to handle appendEntriesWorkerReq args:%+v\n",
						rf.me, id, term, state, appendEntriesWorkerReq.args)
					reply := AppendEntriesReply{}
					ok := rf.sendAppendEntries(id, appendEntriesWorkerReq.args, &reply)
					if ok {
						appendEntriesWorkerReq.replyChan <- &reply
					}
					DPrintf("[%v] Worker:(id:%v, term:%v, state:%v), done handle appendEntriesWorkerReq ok:%+v, reply:%+v\n",
						rf.me, id, term, state, ok, reply)
				} else {
					panic(fmt.Sprintf("[%v] Worker:(id:%v, term:%v, state:%v), can't handle msgType:%+v, msg:%+v\n",
						rf.me, id, term, state, msgType, msg))
				}
			case RequestVoteWorkerReq:
				if requestVoteWorkerReq, ok := msg.(RequestVoteWorkerReq); ok {
					DPrintf("[%v] Worker:(id:%v, term:%v, state:%v), to handle requestVoteWorkerReq args:%+v\n",
						rf.me, id, term, state, requestVoteWorkerReq.args)
					reply := RequestVoteReply{}
					ok := rf.sendRequestVote(id, requestVoteWorkerReq.args, &reply)
					if ok {
						requestVoteWorkerReq.replyChan <- &reply
					}
					DPrintf("[%v] Worker:(id:%v, term:%v, state:%v), done handle requestVoteWorkerReq ok:%+v, reply:%+v\n",
						rf.me, id, term, state, ok, reply)
				} else {
					panic(fmt.Sprintf("[%v] Worker:(id:%v, term:%v, state:%v), can't handle msgType:%+v, msg:%+v\n",
						rf.me, id, term, state, msgType, msg))
				}
			default:
				panic(fmt.Sprintf("[%v] Worker:(id:%v, term:%v, state:%v), unexpected msgType:%+v, msg:%+v\n",
					rf.me, id, term, state, msgType, msg))
			}
			// DPrintf("[%v] Worker:(id:%v, term:%v, state:%v), done handle TypeOf(msg):%+v, msg:%+v\n",
			// 	rf.me, id, term, state, reflect.TypeOf(msg), msg)
		case <-workerCloseChan:
			DPrintf("[%v]Close Worker:(id:%v, term:%v, state:%v) thread\n", rf.me, id, term, state)
			return
		}
	}
}

func (rf *Raft) startRaftOneTimeWorkerThread(id int, msg interface{}, term int, state RaftState) {
	startAt := time.Now()
	switch msgType := msg.(type) {
	case *ApplyMsg:
		if applyMsg, ok := msg.(*ApplyMsg); ok {
			DPrintf("[%v] One Time Worker:(id:%v, term:%v, state:%v, startAt:%+v), to handle applyMsg:%+v\n",
				rf.me, id, term, state, startAt, applyMsg)
			rf.ApplyMsgChan <- *applyMsg
			DPrintf("[%v] One Time Worker:(id:%v, term:%v, state:%v, startAt:%+v), done handle applyMsg:%+v\n",
				rf.me, id, term, state, startAt, applyMsg)
		} else {
			panic(fmt.Sprintf("[%v] One Time Worker:(id:%v, term:%v, state:%v, startAt:%+v), can't handle msgType:%+v, msg:%+v\n",
				rf.me, id, term, state, startAt, msgType, msg))
		}
	case AppendEntriesWorkerReq:
		if appendEntriesWorkerReq, ok := msg.(AppendEntriesWorkerReq); ok {
			DPrintf("[%v] One Time Worker:(id:%v, term:%v, state:%v, startAt:%+v), to handle appendEntriesWorkerReq args:%+v\n",
				rf.me, id, term, state, startAt, appendEntriesWorkerReq.args)
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(id, appendEntriesWorkerReq.args, &reply)
			if ok && reply.IsValid {
				appendEntriesWorkerReq.replyChan <- &reply
			}
			DPrintf("[%v] One Time Worker:(id:%v, term:%v, state:%v, startAt:%+v), done handle appendEntriesWorkerReq ok:%+v, reply:%+v\n",
				rf.me, id, term, state, startAt, ok, reply)
		} else {
			panic(fmt.Sprintf("[%v] One Time Worker:(id:%v, term:%v, state:%v, startAt:%+v), can't handle msgType:%+v, msg:%+v\n",
				rf.me, id, term, state, startAt, msgType, msg))
		}
	case RequestVoteWorkerReq:
		if requestVoteWorkerReq, ok := msg.(RequestVoteWorkerReq); ok {
			DPrintf("[%v] One Time Worker:(id:%v, term:%v, state:%v, startAt:%+v), to handle requestVoteWorkerReq args:%+v\n",
				rf.me, id, term, state, requestVoteWorkerReq.args)
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(id, requestVoteWorkerReq.args, &reply)
			if ok {
				requestVoteWorkerReq.replyChan <- &reply
			}
			DPrintf("[%v] One Time Worker:(id:%v, term:%v, state:%v, startAt:%+v), done handle requestVoteWorkerReq ok:%+v, reply:%+v\n",
				rf.me, id, term, state, startAt, ok, reply)
		} else {
			panic(fmt.Sprintf("[%v] One Time Worker:(id:%v, term:%v, state:%v, startAt:%+v), can't handle msgType:%+v, msg:%+v\n",
				rf.me, id, term, state, startAt, msgType, msg))
		}
	case InstallSnapshotWorkerReq:
		if installSnapshotWorkerReq, ok := msg.(InstallSnapshotWorkerReq); ok {
			DPrintf("[%v] One Time Worker:(id:%v, term:%v, state:%v, startAt:%+v), to handle installSnapshotWorkerReq args:%+v\n",
				rf.me, id, term, state, installSnapshotWorkerReq.args)
			reply := InstallSnapshotReply{}
			ok := rf.sendInstallSnapshot(id, installSnapshotWorkerReq.args, &reply)
			if ok && reply.IsValid {
				installSnapshotWorkerReq.replyChan <- &reply
			}
			DPrintf("[%v] One Time Worker:(id:%v, term:%v, state:%v, startAt:%+v), done handle installSnapshotWorkerReq ok:%+v, reply:%+v\n",
				rf.me, id, term, state, startAt, ok, reply)
		} else {
			panic(fmt.Sprintf("[%v] One Time Worker:(id:%v, term:%v, state:%v, startAt:%+v), can't handle msgType:%+v, msg:%+v\n",
				rf.me, id, term, state, startAt, msgType, msg))
		}
	default:
		panic(fmt.Sprintf("[%v] One Time Worker:(id:%v, term:%v, state:%v, startAt:%+v), unexpected msgType:%+v, msg:%+v\n",
			rf.me, id, term, state, startAt, msgType, msg))
	}
}

// start point of raft thread, each raft instance is one thread, and it would
// go into different handler base on current state
func startRaftThread(rf *Raft) {
	defer DPrintf("[%v]End of Thread: %+v\n", rf.me, rf)
	for {
		switch state := rf.CurrentRaftState; state {
		case follower:
			followerHandler(rf)
		case candidate:
			candidateHandler(rf)
		case leader:
			leaderHandler(rf)
		default:
			panic(fmt.Sprintf("unexpected raft state: %v", state))
		}
		if rf.ToStop {
			rf.stopSelfWorker()
			// rf.MsgFlusher()
			return
		}
	}
}

func getRandElectionTimeoutMiliSecond() time.Duration {
	return time.Duration(rand.Intn(ELECTION_TIMEOUT_RAND_RANGE_MILISECONDS)+ELECTION_TIMEOUT_MILISECONDS) * time.Millisecond
	// return ELECTION_TIMEOUT_MILISECONDS * time.Millisecond
}

func (rf *Raft) agreeVote(reqVote *RequestVoteArgs, rspVote *RequestVoteReply) bool {
	DPrintf("[%v]agreeVote: reqVote:%+v, rspVote:%+v, rf:%+v\n", rf.me, reqVote, rspVote, rf)
	if reqVote.Term < rf.CurrentTerm {
		return false
	}
	if rf.VotedFor != INITIAL_VOTED_FOR && rf.VotedFor != reqVote.CandidateId {
		return false
	}
	lastLogEntryIndex, lastLogEntryTerm := rf.getLastLogEntryInfo()
	if reqVote.LastLogTerm == lastLogEntryTerm {
		// if last log term is the same , see whose log entry is longer ( need to consider snapshot)
		return reqVote.LastLogIndex >= lastLogEntryIndex
	} else {
		return reqVote.LastLogTerm > lastLogEntryTerm
	}
	return true
}

// start and end are pos index in rf.Log
func (rf *Raft) sendApplyMsg(start int, end int) {
	if start < 0 || end >= len(rf.Log) {
		panic(fmt.Sprintf("[%v][sendApplyMsg] start:%v, end:%v is out of bound rf:%+v",
			rf.me, start, end, rf))
	}
	if start > end {
		return
	}
	DPrintf("[%v][sendApplyMsg] to sendApplyMsg: start:%v, end:%v\n",
		rf.me, start, end)
	for i := start; i <= end; i++ {
		if rf.Log[i].Index != i+rf.SnapshotLastIndex+1 {
			panic(fmt.Sprintf("[%v][sendApplyMsg] rf.Log[%v].Index:%v != i:%v rf:%+v", rf.me, i, rf.Log[i].Index, i, rf))
		}
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.Log[i].Command,
			CommandIndex: rf.Log[i].Index,
		}
		// go func(msg ApplyMsg, prevToken chan int64, nextToken chan int64) {
		// 	// DPrintf("[%v]sendApplyMsg: %+v, %+v\n", rf.me, msg, rf)
		// 	<-prevToken
		// 	DPrintf("[%v]to sendApplyMsg: %+v\n", rf.me, msg)
		// 	rf.ApplyMsgChan <- msg
		// 	DPrintf("[%v]after sendApplyMsg: %+v\n", rf.me, msg)
		// 	nextToken <- time.Now().Unix()
		// }(msg, rf.PrevApplyMsgToken, rf.NextApplyMsgToken)
		// rf.PrevApplyMsgToken, rf.NextApplyMsgToken = rf.NextApplyMsgToken, make(chan int64)
		rf.SelfWorkerChan <- &msg
		DPrintf("[%v][sendApplyMsg] sent msg: msg:%+v\n",
			rf.me, msg)
	}
	// update LastApplied after we apply to state machine
	rf.LastApplied = end
	DPrintf("[%v][sendApplyMsg] to sendApplyMsg: start:%v, end:%v\n",
		rf.me, start, end)
}

func (rf *Raft) followerAppendLogEntry(reqAppend *AppendEntriesArgs, rspAppend *AppendEntriesReply) {
	// refer to https://stackoverflow.com/questions/16248241/concatenate-two-slices-in-go
	if reqAppend.PrevLogIndex >= rf.SnapshotLastIndex {
		rf.Log = append(rf.Log[:rf.getLogEntryIndexFromGlobalIndex(reqAppend.PrevLogIndex)+1], reqAppend.Entries...)
	} else {
		rf.Log = reqAppend.Entries[min(rf.SnapshotLastIndex-reqAppend.PrevLogIndex, len(reqAppend.Entries)):]
	}
	lastLogEntryIndex, _ := rf.getLastLogEntryInfo()
	DPrintf("[%v][followerAppendLogEntry] lastLogEntryIndex:%v, reqAppend.LeaderCommit:%v, rf.CommitIndex:%v\n",
		rf.me, lastLogEntryIndex, reqAppend.LeaderCommit, rf.CommitIndex)
	if reqAppend.LeaderCommit > rf.CommitIndex {
		oldCommitIndex := rf.CommitIndex
		rf.CommitIndex = min(reqAppend.LeaderCommit, lastLogEntryIndex)
		rf.sendApplyMsg(rf.getLogEntryIndexFromGlobalIndex(oldCommitIndex+1), rf.getLogEntryIndexFromGlobalIndex(rf.CommitIndex))
	}
	rspAppend.NextIndex = lastLogEntryIndex + 1
	rspAppend.Success = true
	// save to persist when follower successfully append new Entry from leader
	rf.persist()
	DPrintf("[%v][followerAppendLogEntry] done raftStateSize:%+v, reqAppend:%+v, rspAppend:%+v\n",
		rf.me, rf.persister.RaftStateSize(), reqAppend, rspAppend)
}

func (rf *Raft) followerAppendEntryHandler(reqAppend *AppendEntriesArgs, rspAppend *AppendEntriesReply, lastAppendEntryReq **AppendEntriesArgs) {
	// per Fig2 rule when receiving higher term
	if reqAppend.Term > rf.CurrentTerm {
		rf.updateTerm(reqAppend.Term)
		rf.VotedFor = INITIAL_VOTED_FOR
	}
	// DPrintf("[followerHandler][%v]: %+v, %+v\n", reqAppend, rf)
	// what if leaderId != rf.VotedFor, but term the same , should reject and check it ?
	if reqAppend.Term >= rf.CurrentTerm && reqAppend.LeadId == rf.VotedFor {
		if *lastAppendEntryReq != nil && (*lastAppendEntryReq).TimeStamp >= reqAppend.TimeStamp {
			DPrintf("[%v][followerAppendEntryHandler] received older reqAppend:%+v, lastAppendEntryReq:%+v", rf.me, reqAppend, **lastAppendEntryReq)
			rspAppend.IsValid = false
			rf.AppendEntriesReplyChan <- rspAppend
			// do I need to reset election timer here? but since it is older request, I think we should not reset it
			// timer = time.After(getRandElectionTimeoutMiliSecond())
			return
		}
		*lastAppendEntryReq = reqAppend
		// seperate to below cases:
		// 1) if reqAppend.PrevLogIndex == rf.SnapshotLastIndex, it should
		//    have same term, if not panic, otherwise append entry and
		//    return success
		// 2) else if reqAppend.PrevLogIndex < rf.SnapshotLastIndex, that would
		//    means prev index is included in snapshot, so follower just take over
		//    the log entry after snapshotlast index
		// 3) else if localPrevLogIndex within rf.Log range and term are the
		//    same, return success
		// 4) for the rest of case, first get the min(localPrevLogIndex, localLastIndex)
		//    then do the search for conflict term
		// [Note] : here local means the index for rf.Log, global means
		//          the index among all the raft instance
		localAppendEntryPrevLogIndex := rf.getLogEntryIndexFromGlobalIndex(reqAppend.PrevLogIndex)
		lastLogEntryIndex, _ := rf.getLastLogEntryInfo()
		localLastLogEntryIndex := rf.getLogEntryIndexFromGlobalIndex(lastLogEntryIndex)

		if reqAppend.PrevLogIndex == rf.SnapshotLastIndex {
			// panic(fmt.Sprintf("[%v][followerHandler] shouldn't go into case(1) yet rf:%+v\n",
			// 	rf.me, rf))
			// case(1)
			if rf.SnapshotLastTerm != reqAppend.PrevLogTerm {
				panic(fmt.Sprintf("[%v][followerAppendEntryHandler] snapshot (index:%v, term:%v) term != reqAppend.PrevLogTerm:%v, rf:%+v\n",
					rf.me, rf.SnapshotLastIndex, rf.SnapshotLastTerm, reqAppend.PrevLogTerm, rf))
			}
			rf.followerAppendLogEntry(reqAppend, rspAppend)
			// } else if lastLogEntryIndex == rf.SnapshotLastIndex {
		} else if reqAppend.PrevLogIndex < rf.SnapshotLastIndex {
			// panic(fmt.Sprintf("[%v][followerHandler] shouldn't go into case(2) yet rf:%+v\n",
			// 	rf.me, rf))
			// case(2)
			// rspAppend.NextIndex = rf.SnapshotLastIndex + 1
			rf.followerAppendLogEntry(reqAppend, rspAppend)

		} else if localAppendEntryPrevLogIndex >= 0 && localAppendEntryPrevLogIndex < len(rf.Log) && rf.Log[localAppendEntryPrevLogIndex].Term == reqAppend.PrevLogTerm {
			// case(3)
			rf.followerAppendLogEntry(reqAppend, rspAppend)
		} else {
			// case(4)
			localCheckIndex := min(localAppendEntryPrevLogIndex, localLastLogEntryIndex)
			if localCheckIndex < -1 || localCheckIndex >= len(rf.Log) {
				panic(fmt.Sprintf("[%v][followerAppendEntryHandler] localCheckIndex:%v is out of bound, len(rf.Log):%v, rf:%+v\n",
					rf.me, localCheckIndex, len(rf.Log), rf))
			}
			// if we have conflict term, search for first index that has leader prevLogTerm
			// this rule is per https://pdos.csail.mit.edu/6.824/notes/l-raft2.txt
			if localCheckIndex >= 0 && reqAppend.PrevLogTerm != rf.Log[localCheckIndex].Term {
				rspAppend.ConflictTerm = rf.Log[localCheckIndex].Term
				// try to find first index of conflict term
				localConflictTermFirstIndex := rf.binarySearchForTerm(rf.Log, 0, localCheckIndex, rspAppend.ConflictTerm, true)
				if localConflictTermFirstIndex == -1 {
					panic(fmt.Sprintf("[%v][followerAppendEntryHandler] can't find conflict term:%v in its log(%v,%v) in rf:%+v", rf.me, rspAppend.ConflictTerm, 0, localCheckIndex, rf))
				}
				rspAppend.ConflictTermFirstIndex = rf.getGlobalIndexFromLogEntryIndex(localConflictTermFirstIndex)
			}
			rspAppend.NextIndex = rf.getGlobalIndexFromLogEntryIndex(localCheckIndex + 1)
		}
	}
	rspAppend.Term = rf.CurrentTerm
	rspAppend.VotedFor = rf.VotedFor
	rf.AppendEntriesReplyChan <- rspAppend
}

func followerHandler(rf *Raft) {
	// - handle timeout, then start new election and change to candidate
	// - handle AppendEntries ( heartbeat only for 2A)
	// - handle RequestVote
	rf.printInfo()
	timer := time.After(getRandElectionTimeoutMiliSecond())
	var lastAppendEntryReq *AppendEntriesArgs
	for {
		DPrintf("[%v][followerHandler] start of for loop: rf:%+v", rf.me, rf)
		select {
		case <-timer:
			DPrintf("[%v][followerHandler] timer is up", rf.me)
			rf.CurrentRaftState = candidate
			rf.persist()
			return
		case reqVote := <-rf.RequestVoteArgsChan:
			DPrintf("[%v][followerHandler] received reqVote:%+v", rf.me, reqVote)
			rspVote := RequestVoteReply{
				Term:        rf.CurrentTerm,
				VoteGranted: false,
				FromId:      rf.me,
			}
			// per Fig2 rule when receiving higher term
			if reqVote.Term > rf.CurrentTerm {
				// per paper Fig2 votedFor definition, it should be candidate Id this raft instance voted for the given term
				// so whenever change the term , need to initialize the votedFor value
				rf.updateTerm(reqVote.Term)
				rf.VotedFor = INITIAL_VOTED_FOR
			}
			// per Fig2 Request RPC rule, only this condition to grant vote
			if rf.agreeVote(reqVote, &rspVote) {
				rspVote.VoteGranted = true
				rf.VotedFor = reqVote.CandidateId
			}
			rspVote.Term = rf.CurrentTerm
			rf.RequestVoteReplyChan <- &rspVote
			rf.persist()
			// if grant vote for a leader, reset timer
			if rspVote.VoteGranted {
				// return
				timer = time.After(getRandElectionTimeoutMiliSecond())
			}
		case reqAppend := <-rf.AppendEntriesArgsChan:
			DPrintf("[%v][followerHandler] received reqAppend:%+v, lastAppendEntryReq:%+v", rf.me, reqAppend, lastAppendEntryReq)
			rspAppend := AppendEntriesReply{
				Term:                   rf.CurrentTerm,
				Success:                false,
				FromId:                 rf.me,
				NextIndex:              reqAppend.PrevLogIndex + 1,
				VotedFor:               rf.VotedFor,
				TimeStamp:              time.Now().UnixNano(),
				IsValid:                true,
				ConflictTerm:           -1,
				ConflictTermFirstIndex: -1,
			}
			rf.followerAppendEntryHandler(reqAppend, &rspAppend, &lastAppendEntryReq)
			// if ack an Append RPC, reset timer
			if rspAppend.Success || rf.VotedFor == reqAppend.LeadId {
				// return
				timer = time.After(getRandElectionTimeoutMiliSecond())
			}
		case <-rf.ToStopChan:
			DPrintf("[%v][followerHandler] received ToStopChan", rf.me)
			rf.ToStop = true
			return
		// case <-rf.GetStateReqChan:
		// 	DPrintf("[%v][followerHandler] received GetStateReqChan", rf.me)
		// 	rf.GetStateHelper()
		// case StartReq := <-rf.StartReqChan:
		// 	DPrintf("[%v][followerHandler] received StartReqChan:%+v", rf.me, StartReq)
		// 	rf.StartHelper(StartReq)
		case msg := <-rf.MsgChan:
			DPrintf("[%v][followerHandler] received msg:%+v", rf.me, msg)
			rf.MsgHandler(msg)
		}
		DPrintf("[%v][followerHandler] end of for loop: rf:%+v", rf.me, rf)
	}
}

func candidateHandler(rf *Raft) {
	// - every time go into it, meaning new election ,so increase its term
	// - send the vote out to all peers
	// - handle timeout, then start new election and still in candidate state
	// - handle AppendEntries,
	// - handle RequestVote
	rf.CurrentTerm += 1
	rf.printInfo()
	timer := time.After(getRandElectionTimeoutMiliSecond())
	voteReplyChan := make(chan *RequestVoteReply, 8192)
	// vote for itself and send vote to followers
	rf.VotedFor = rf.me
	voteCount := 1
	// sendVoteTask := func(term int, me int, destServer int, voteReplyChan chan *RequestVoteReply, argvs *RequestVoteArgs) {
	// 	// argvs := RequestVoteArgs{
	// 	// 	Term:         term,
	// 	// 	CandidateId:  me,
	// 	// 	LastLogIndex: len(rf.Log) - 1,
	// 	// 	LastLogTerm:  rf.Log[len(rf.Log)-1].Term,
	// 	// }
	// 	reply := RequestVoteReply{}
	// 	if rf.sendRequestVote(destServer, argvs, &reply) {
	// 		voteReplyChan <- &reply
	// 	}
	// }
	lastLogEntryIndex, lastLogEntryTerm := rf.getLastLogEntryInfo()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		argvs := RequestVoteArgs{
			Term:         rf.CurrentTerm,
			CandidateId:  rf.me,
			LastLogIndex: lastLogEntryIndex,
			LastLogTerm:  lastLogEntryTerm,
		}
		// go sendVoteTask(rf.CurrentTerm, rf.me, i, voteReplyChan, &argvs)
		workerReq := RequestVoteWorkerReq{
			args:      &argvs,
			replyChan: voteReplyChan,
		}
		go rf.startRaftOneTimeWorkerThread(i, workerReq, rf.CurrentTerm, rf.CurrentRaftState)

	}

	for {
		DPrintf("[%v][candidateHandler] start of for loop: rf:%+v", rf.me, rf)
		select {
		case <-timer:
			DPrintf("[%v][candidateHandler] timer is up", rf.me)
			return
		case reqVote := <-rf.RequestVoteArgsChan:
			DPrintf("[%v][candidateHandler] received reqVote:%+v", rf.me, reqVote)
			rspVote := RequestVoteReply{
				Term:        rf.CurrentTerm,
				VoteGranted: false,
				FromId:      rf.me,
			}
			// per Fig2 rule when receiving higher term
			if reqVote.Term > rf.CurrentTerm {
				rf.updateState(follower)
				// step down and handle the reqVote in follower step
				go rf.redirectReqVoteHelper(reqVote)
				return
			}
			rf.RequestVoteReplyChan <- &rspVote
		case reqAppend := <-rf.AppendEntriesArgsChan:
			DPrintf("[%v][candidateHandler] received reqAppend:%+v", rf.me, reqAppend)
			rspAppend := AppendEntriesReply{
				Term:     rf.CurrentTerm,
				Success:  false,
				FromId:   rf.me,
				VotedFor: rf.VotedFor,
			}
			// per Fig2 rule when receiving higher term
			// but per Fig 2 rules, it is saying if Append RPC received from new leader, cover to follower
			if reqAppend.Term >= rf.CurrentTerm {
				rf.VotedFor = reqAppend.LeadId
				rf.updateState(follower)
				// if ack an Append RPC, return to follower state and handle the req
				go rf.redirectAppendHelper(reqAppend)
				return
			}
			rf.AppendEntriesReplyChan <- &rspAppend
		case voteReply := <-voteReplyChan:
			DPrintf("[%v][candidateHandler] received voteReply:%+v", rf.me, voteReply)
			if voteReply.VoteGranted {
				voteCount += 1
				if voteCount >= len(rf.peers)/2+1 {
					rf.CurrentRaftState = leader
					return
				}
			} else {
				if voteReply.Term > rf.CurrentTerm {
					rf.updateTerm(voteReply.Term)
					rf.updateState(follower)
					rf.VotedFor = INITIAL_VOTED_FOR
					return
				}
			}
		case <-rf.ToStopChan:
			DPrintf("[%v][candidateHandler] received ToStopChan", rf.me)
			rf.ToStop = true
			return
		// case <-rf.GetStateReqChan:
		// 	DPrintf("[%v][candidateHandler] received GetStateReqChan", rf.me)
		// 	rf.GetStateHelper()
		// case StartReq := <-rf.StartReqChan:
		// 	DPrintf("[%v][candidateHandler] received StartReqChan:%+v", rf.me, StartReq)
		// 	rf.StartHelper(StartReq)
		case msg := <-rf.MsgChan:
			DPrintf("[%v][candidateHandler] received msg:%+v", rf.me, msg)
			rf.MsgHandler(msg)
		}
		DPrintf("[%v][candidateHandler] start of for loop: rf:%+v", rf.me, rf)
		rf.persist()
	}
}

func (rf *Raft) redirectReqVoteHelper(reqVote *RequestVoteArgs) {
	rf.RequestVoteArgsChan <- reqVote
}

func (rf *Raft) redirectAppendHelper(reqVote *AppendEntriesArgs) {
	rf.AppendEntriesArgsChan <- reqVote
}

// helper to prepare appendEntries and send out reqeust ,
// this API should only be called in raft instance thread
func (rf *Raft) appendEntriesHelper(destServer int, appendReplyChan chan *AppendEntriesReply, installSnapReplyChan chan *InstallSnapshotReply) {
	prevLogIndex, prevLogTerm := rf.getLogEntryInfo(rf.NextIndex[destServer] - 1)
	// lastLogEntryIndex, _ := rf.getLastLogEntryInfo()
	var workerReq interface{}

	// don't need to send the appendEntry if the interval from last sent smaller than heartbeat interval
	// if lastLogEntryIndex == rf.NextIndex[destServer]-1 && time.Now().Sub(rf.LastAppendEntrySentTime[destServer]) < HEARTBEAT_TIMEOUT_MILISECONDS*time.Millisecond {
	// if time.Now().Sub(rf.LastAppendEntrySentTime[destServer]) < HEARTBEAT_TIMEOUT_MILISECONDS*time.Millisecond {
	// 	return
	// }

	DPrintf("[%v][appendEntriesHelper] destServer:%v, prevLogIndex:%v, prevLogTerm:%v\n",
		rf.me, destServer, prevLogIndex, prevLogTerm)

	// startIndexForNextLogEntries < 0 would mean current log doesn't have the entry,
	// we need to install snapshot
	if prevLogIndex < 0 {
		// go with install snapshot
		// panic(fmt.Sprintf("[%v][appendEntriesHelper] prevLogIndex:%v, should not go into install snapshot yet, destServer:%v, rf:%+v",
		// 	rf.me, prevLogIndex, destServer, rf))

		argvs := InstallSnapshotArgs{
			Term:              rf.CurrentTerm,
			LeadId:            rf.me,
			LastIncludedIndex: rf.SnapshotLastIndex,
			LastIncludedTerm:  rf.SnapshotLastTerm,
			Data:              rf.persister.ReadSnapshot(),
			TimeStamp:         time.Now().UnixNano(),
		}
		workerReq = InstallSnapshotWorkerReq{
			args:      &argvs,
			replyChan: installSnapReplyChan,
		}
		rf.NextIndex[destServer] = rf.SnapshotLastIndex + 1
		// rf.MatchIndex[destServer] = rf.SnapshotLastIndex
	} else {
		// if startIndexForNextLogEntries-1 < 0 || startIndexForNextLogEntries-1 >= len(rf.Log) {
		// 	panic(fmt.Sprintf("[%v][appendEntriesHelper] (startIndexForNextLogEntries-1):%v is out of bound, destServer:%v, rf:%+v",
		// 		rf.me, startIndexForNextLogEntries-1, destServer, rf))
		// }
		argvs := AppendEntriesArgs{
			Term:         rf.CurrentTerm,
			LeadId:       rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			LeaderCommit: rf.CommitIndex,
			TimeStamp:    time.Now().UnixNano(),
		}
		tmpLog := rf.Log[rf.getLogEntryIndexFromGlobalIndex(rf.NextIndex[destServer]):len(rf.Log)]
		argvs.Entries = make([]LogEntry, len(tmpLog))
		copy(argvs.Entries, tmpLog)
		workerReq = AppendEntriesWorkerReq{
			args:      &argvs,
			replyChan: appendReplyChan,
		}
	}

	go rf.startRaftOneTimeWorkerThread(destServer, workerReq, rf.CurrentTerm, rf.CurrentRaftState)
	rf.LastAppendEntrySentTime[destServer] = time.Now()
}

// to update leader's commit index whenever received AppendEntries success
func (rf *Raft) updateLeaderCommitIndex() {
	// DPrintf("[%v] updateCommitIndex: CommitIndex:%v, matchIdx:%+v, currentTerm:%v\n", rf.me, rf.CommitIndex, rf.MatchIndex, rf.CurrentTerm)
	if rf.CurrentRaftState != leader {
		panic(fmt.Sprintf("unexpected raft state for updateLeaderCommitIndex: %v", rf.CurrentRaftState))
	}
	oldCommitIndex := rf.CommitIndex
	for i := rf.getLogEntryIndexFromGlobalIndex(rf.CommitIndex + 1); i < len(rf.Log); i++ {
		if rf.Log[i].Term != rf.CurrentTerm {
			// DPrintf("[%v][updateLeaderCommitIndex] rf.Log[%v].Term:%v != rf.CurrentTerm:%v\n", rf.me, i, rf.Log[i].Term, rf.CurrentTerm)
			// there is a bug here I dig a while to figure out:
			// The leader can't commit an log entry which is not the same term as current term.
			// But that doesn't mean we should stop whenever see this( I failed in TestFailAgree2B because of this)
			// Per paper fig2 rule for leader, it is mentioning as long as there exist a N can fit requirement,
			// set commitIndex to N
			continue
		}
		cnt := 0
		for peerIdx := range rf.peers {
			if rf.MatchIndex[peerIdx] >= rf.Log[i].Index {
				cnt++
			}
		}
		DPrintf("[%v][updateLeaderCommitIndex] cnt:%v, rf.Log[%v].Index:%v\n", rf.me, cnt, i, rf.Log[i].Index)
		if cnt > len(rf.peers)/2 {
			rf.CommitIndex = rf.Log[i].Index
		} else {
			break
		}
	}
	// DPrintf("[%v][updateLeaderCommitIndex] start:%v, end:%v, rf:%+v\n", rf.me, oldCommitIndex+1, rf.CommitIndex, rf)
	rf.sendApplyMsg(rf.getLogEntryIndexFromGlobalIndex(oldCommitIndex+1), rf.getLogEntryIndexFromGlobalIndex(rf.CommitIndex))
}

func (rf *Raft) leaderBroadcast(appendReplyChan chan *AppendEntriesReply, installSnapReplyChan chan *InstallSnapshotReply) {
	for i := range rf.peers {
		if i == rf.me {
			lastLogEntryIndex, _ := rf.getLastLogEntryInfo()
			rf.NextIndex[rf.me] = lastLogEntryIndex + 1
			rf.MatchIndex[rf.me] = lastLogEntryIndex
			continue
		}
		rf.appendEntriesHelper(i, appendReplyChan, installSnapReplyChan)
	}
}

func leaderHandler(rf *Raft) {
	// - send out heatbeat immediately as initial step then send out periodcally
	// - handle heartbeat timeout, then send it out peridically
	// - handle AppendEntries,
	// - handle RequestVote
	rf.initIndex()
	rf.printInfo()
	timer := time.After(0)
	rf.appendReplyChan = make(chan *AppendEntriesReply, 8192)
	rf.installSnapshotReplyChan = make(chan *InstallSnapshotReply, 8192)
	lastAppendReqly := make([]*AppendEntriesReply, len(rf.peers))
	lastInstallshotReqly := make([]*InstallSnapshotReply, len(rf.peers))

	for {
		DPrintf("[%v][leaderHandler] start of for loop: rf:%+v", rf.me, rf)
		select {
		case <-timer:
			DPrintf("[%v][leaderHandler] timer is up", rf.me)
			// for i := range rf.peers {
			// 	if i == rf.me {
			// 		lastLogEntryIndex, _ := rf.getLastLogEntryInfo()
			// 		rf.NextIndex[rf.me] = lastLogEntryIndex + 1
			// 		rf.MatchIndex[rf.me] = lastLogEntryIndex
			// 		continue
			// 	}
			// 	// if time.Now().Sub(rf.LastAppendEntrySentTime[i]) < HEARTBEAT_TIMEOUT_MILISECONDS*time.Millisecond {
			// 	// 	continue
			// 	// }
			// 	rf.appendEntriesHelper(i, appendReplyChan, installSnapshotReplyChan)
			// }
			rf.leaderBroadcast(rf.appendReplyChan, rf.installSnapshotReplyChan)
			timer = time.After(LEADER_PEER_TICK_MILISECONDS * time.Millisecond)
		case reqVote := <-rf.RequestVoteArgsChan:
			DPrintf("[%v][leaderHandler] received reqVote:%+v", rf.me, reqVote)
			rspVote := RequestVoteReply{
				Term:        rf.CurrentTerm,
				VoteGranted: false,
				FromId:      rf.me,
			}
			// per Fig2 rule when receiving higher term
			if reqVote.Term > rf.CurrentTerm {
				rf.updateState(follower)
				// step down and handle the reqVote in follower step
				go rf.redirectReqVoteHelper(reqVote)
				return
			}
			rf.RequestVoteReplyChan <- &rspVote
		case reqAppend := <-rf.AppendEntriesArgsChan:
			DPrintf("[%v][leaderHandler] received reqAppend:%+v", rf.me, reqAppend)
			rspAppend := AppendEntriesReply{
				Term:     rf.CurrentTerm,
				Success:  false,
				FromId:   rf.me,
				VotedFor: rf.VotedFor,
			}
			// per Fig2 rule when receiving higher term
			if reqAppend.Term > rf.CurrentTerm {
				rf.updateState(follower)
				// if ack an Append RPC, return to follower state and handle the req
				go rf.redirectAppendHelper(reqAppend)
				return
			}
			rf.AppendEntriesReplyChan <- &rspAppend
		case appendReply := <-rf.appendReplyChan:
			DPrintf("[%v][leaderHandler] received appendReply:%+v, lastAppendReqly[%v]:%+v", rf.me, appendReply, appendReply.FromId, lastAppendReqly[appendReply.FromId])
			if lastAppendReqly[appendReply.FromId] != nil && lastAppendReqly[appendReply.FromId].TimeStamp >= appendReply.TimeStamp {
				DPrintf("[%v][leaderHandler] received older appendReply:%+v, lastAppendReqly:%+v", rf.me, appendReply, *lastAppendReqly[appendReply.FromId])
				continue
			}
			lastAppendReqly[appendReply.FromId] = appendReply
			if appendReply.Term > rf.CurrentTerm {
				rf.updateTerm(appendReply.Term)
				rf.updateState(follower)
				rf.VotedFor = INITIAL_VOTED_FOR
				return
			}
			if appendReply.Success {
				// when passing, follower tell leader what is the nextIndex
				rf.NextIndex[appendReply.FromId] = appendReply.NextIndex
				rf.MatchIndex[appendReply.FromId] = appendReply.NextIndex - 1
				rf.updateLeaderCommitIndex()
				// rf.persist()
				// even we receive success, if we have more item for the follower
				// need to send to them
				// lastLogEntryIndex, _ := rf.getLastLogEntryInfo()
				// if rf.NextIndex[appendReply.FromId] <= lastLogEntryIndex {
				// 	rf.appendEntriesHelper(appendReply.FromId, appendReplyChan, installSnapshotReplyChan)
				// }
			} else {
				// if fail, follower tell leader from where to try is better,
				// ignore the rsp if this follower is not voting to it
				if appendReply.VotedFor == rf.me {
					// rf.NextIndex[appendReply.FromId] = appendReply.NextIndex
					// per https://pdos.csail.mit.edu/6.824/notes/l-raft2.txt rule
					// but the one differene is that, I choose to use if Leader has the conflict term entry,
					// which means we can use that entry as prevIdex, so set nextIndex to that found conflict entry index + 1
					if appendReply.ConflictTerm != -1 {
						localLastConflictTermEntryIndex := -1
						if len(rf.Log) > 0 {
							localLastConflictTermEntryIndex = rf.binarySearchForTerm(rf.Log, 0, len(rf.Log)-1, appendReply.ConflictTerm, false)
						}
						if localLastConflictTermEntryIndex != -1 {
							rf.NextIndex[appendReply.FromId] = rf.getGlobalIndexFromLogEntryIndex(localLastConflictTermEntryIndex + 1)
						} else {
							rf.NextIndex[appendReply.FromId] = appendReply.ConflictTermFirstIndex
						}
					} else {
						rf.NextIndex[appendReply.FromId] = appendReply.NextIndex
					}
					// make sure NextIndex should always bigger than current MatchIndex
					if rf.NextIndex[appendReply.FromId] < rf.MatchIndex[appendReply.FromId] {
						panic(fmt.Sprintf("[%v][leaderHandler] MatchIdex:%v is higher than NextIndex:%v, it is unexpected!",
							rf.me, rf.MatchIndex[appendReply.FromId], rf.NextIndex[appendReply.FromId]))
					}
					rf.NextIndex[appendReply.FromId] = max(rf.NextIndex[appendReply.FromId], rf.MatchIndex[appendReply.FromId])
					rf.appendEntriesHelper(appendReply.FromId, rf.appendReplyChan, rf.installSnapshotReplyChan)
				}
			}
		case installSnapshotReply := <-rf.installSnapshotReplyChan:
			DPrintf("[%v][leaderHandler] received installSnapshotReply:%+v, lastInstallshotReqly[%v]:%+v",
				rf.me, installSnapshotReply, installSnapshotReply.FromId, lastInstallshotReqly[installSnapshotReply.FromId])
			if lastInstallshotReqly[installSnapshotReply.FromId] != nil && lastInstallshotReqly[installSnapshotReply.FromId].TimeStamp >= installSnapshotReply.TimeStamp {
				DPrintf("[%v][leaderHandler] received older installSnapshotReply:%+v, lastInstallshotReqly:%+v",
					rf.me, installSnapshotReply, *lastInstallshotReqly[installSnapshotReply.FromId])
				continue
			}
			lastInstallshotReqly[installSnapshotReply.FromId] = installSnapshotReply
			if installSnapshotReply.Term > rf.CurrentTerm {
				rf.updateTerm(installSnapshotReply.Term)
				rf.updateState(follower)
				rf.VotedFor = INITIAL_VOTED_FOR
				return
			}
		case <-rf.ToStopChan:
			DPrintf("[%v][leaderHandler] received ToStopChan", rf.me)
			rf.ToStop = true
			return
		// case <-rf.GetStateReqChan:
		// 	DPrintf("[%v][leaderHandler] received GetStateReqChan", rf.me)
		// 	rf.GetStateHelper()
		case msg := <-rf.MsgChan:
			DPrintf("[%v][leaderHandler] received msg:%+v", rf.me, msg)
			rf.MsgHandler(msg)
			// case StartReq := <-rf.StartReqChan:
			// 	DPrintf("[%v][leaderHandler] received StartReq:%+v", rf.me, StartReq)
			// 	rf.StartHelper(StartReq)
			// 	// once we receive new entry, we should send out append Entry to peers immediately
			// 	// timer = time.After(0)
			// 	// for i := range rf.peers {
			// 	// 	if i == rf.me {
			// 	// 		lastLogEntryIndex, _ := rf.getLastLogEntryInfo()
			// 	// 		rf.NextIndex[rf.me] = lastLogEntryIndex + 1
			// 	// 		rf.MatchIndex[rf.me] = lastLogEntryIndex
			// 	// 		continue
			// 	// 	}
			// 	// 	rf.appendEntriesHelper(i, appendReplyChan, installSnapshotReplyChan)
			// 	// }
		}
		DPrintf("[%v][leaderHandler] end of for loop, rf:%+v", rf.me, rf)
	}
}

func (rf *Raft) printInfo() {
	DPrintf("[%v] enter %s state , term:%v, rf:%+v\n", rf.me, rf.CurrentRaftState, rf.CurrentTerm, rf)
}
