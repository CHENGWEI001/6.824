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
import "log"
import "math"

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
}

//
// A Go object implementing a single Raft peer.
//

const ELECTION_TIMEOUT_MILISECONDS = 500
const ELECTION_TIMEOUT_RAND_RANGE_MILISECONDS = 500
const HEARTBEAT_TIMEOUT_MILISECONDS = 200
const INITIAL_VOTED_FOR = -1

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	CurrentTerm            int                      // current term of raft instance
	VotedFor               int                      // previous vote for who, initial to -1
	CurrentRaftState       RaftState                // current raft state
	RequestVoteArgsChan    chan *RequestVoteArgs    // channel for receiving Request Vote
	RequestVoteReplyChan   chan *RequestVoteReply   // channel for response Request Vote
	AppendEntriesArgsChan  chan *AppendEntriesArgs  // channel for receiving Append Entry
	AppendEntriesReplyChan chan *AppendEntriesReply // channel for response Append Entry
	Log                    []LogEntry               // log entries
	CommitIndex            int                      // indx of highest log entry known to be committed
	LastApplied            int                      // index of highest log entry applied to state machine
	NextIndex              []int                    // index of the next log entry to send to the server
	MatchIndex             []int                    // index of highest log entry known to be replicated on server
	ApplyChChan            chan *ApplychArgs        // channel for receiving ApplyCh from client
	ApplyMsgChan           chan ApplyMsg            // channel for updating to tester
	ToStopChan             chan bool                // channel for stopping the raft instance thread
	ToStop                 bool                     // indicator for stopping raft instance
}

// serverState indicating current raft instance state
type RaftState string

type LogEntry struct {
	Term    int         // term of the given log entry
	Command interface{} // command for state machine
}

const (
	follower  RaftState = "follower"
	candidate           = "candidate"
	leader              = "leader"
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.Lock()
	defer rf.Unlock()
	isleader = rf.CurrentRaftState == leader
	term = rf.CurrentTerm

	return term, isleader
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
	log.Printf("[%v][persist] : save to persist rf:%+v\n", rf.me, rf)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	if d.Decode(&rf.CurrentTerm) != nil ||
		d.Decode(&rf.VotedFor) != nil ||
		d.Decode(&rf.Log) != nil {
		panic(fmt.Sprintf("[%v][readPersist] fail to read Persist!\n", rf.me))
	}
	log.Printf("[%v][readPersist] : read from persist rf:%+v\n", rf.me, rf)
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
}

type AppendEntriesReply struct {
	Term      int  // current term , for leader to update itself
	Success   bool // [ToDo]
	FromId    int  // indicating this reply is from which raft instance
	NextIndex int  // if success: nextIndex for this follower, if failure: next Index to try
	VotedFor  int  // Replay current votedFor for the raft Instace
}

type ApplychArgs struct {
	command interface{}
	status  chan int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	log.Printf("[RequestVote][%v -> %v]: %+v\n", args.CandidateId, rf.me, *args)
	rf.RequestVoteArgsChan <- args
	*reply = *(<-rf.RequestVoteReplyChan)
	log.Printf("[RequestVote][%v <- %v]: %+v\n", args.CandidateId, reply.FromId, *reply)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	log.Printf("[AppendEntries][%v -> %v]: %+v\n", args.LeadId, rf.me, *args)
	rf.AppendEntriesArgsChan <- args
	*reply = *(<-rf.AppendEntriesReplyChan)
	log.Printf("[AppendEntries][%v <- %v]: %+v\n", args.LeadId, reply.FromId, *reply)
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	index := -1
	// Your code here (2B).

	applyChReq := ApplychArgs{
		command: command,
		status:  make(chan int),
	}

	sendApplyCh := func(rf *Raft, applyChReq *ApplychArgs) {
		rf.ApplyChChan <- applyChReq
		<-applyChReq.status
	}
	rf.Lock()
	defer rf.Unlock()
	term := rf.CurrentTerm
	isLeader := rf.CurrentRaftState == leader

	if isLeader {
		index = len(rf.Log)
		sendApplyCh(rf, &applyChReq)
	}
	log.Printf("[%v][Start]: index:%v, term:%v, isLeader:%v, command:%+v\n", rf.me, index, term, isLeader, command)

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	log.Printf("[Kill][%v]: %+v\n", rf.me, rf)
	go func(rf *Raft) {
		rf.ToStopChan <- true
	}(rf)
}

func (rf *Raft) updateTerm(term int) {
	rf.Lock()
	defer rf.Unlock()
	rf.CurrentTerm = term
}

func (rf *Raft) updateState(state RaftState) {
	rf.Lock()
	defer rf.Unlock()
	rf.CurrentRaftState = state
}

// init NextIndex and MatchIndex after every election
func (rf *Raft) initIndex() {
	rf.NextIndex = make([]int, len(rf.peers))
	rf.MatchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.NextIndex[i] = 1
	}
}

func (rf *Raft) Lock() {
	rf.mu.Lock()
}

func (rf *Raft) Unlock() {
	rf.mu.Unlock()
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
	rf.initIndex()
	// initial with index 1, concept is that when up , there is already index = 0 done with term 0
	rf.Log = make([]LogEntry, 1)
	rf.ToStopChan = make(chan bool)
	rf.ToStop = false

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	log.Printf("[%v][Make]: rf:%+v\n", rf.me, rf)

	go startRaftThread(rf)

	return rf
}

// start point of raft thread, each raft instance is one thread, and it would
// go into different handler base on current state
func startRaftThread(rf *Raft) {
	defer log.Printf("[%v]End of Thread: %+v\n", rf.me, rf)
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
			return
		}
	}
}

func getRandElectionTimeoutMiliSecond() time.Duration {
	return time.Duration(rand.Intn(ELECTION_TIMEOUT_RAND_RANGE_MILISECONDS)+ELECTION_TIMEOUT_MILISECONDS) * time.Millisecond
	// return ELECTION_TIMEOUT_MILISECONDS * time.Millisecond
}

func (rf *Raft) agreeVote(reqVote *RequestVoteArgs, rspVote *RequestVoteReply) bool {
	log.Printf("[%v]agreeVote: reqVote:%+v, rspVote:%+v, rf:%+v\n", rf.me, reqVote, rspVote, rf)
	if reqVote.Term < rf.CurrentTerm {
		return false
	}
	if rf.VotedFor != INITIAL_VOTED_FOR && rf.VotedFor != reqVote.CandidateId {
		return false
	}
	if reqVote.LastLogTerm == rf.Log[len(rf.Log)-1].Term {
		return reqVote.LastLogIndex+1 >= len(rf.Log)
	} else {
		return reqVote.LastLogTerm > rf.Log[len(rf.Log)-1].Term
	}

	// if reqVote.LastLogIndex < len(rf.Log) {
	// 	if rf.Log[reqVote.LastLogIndex].Term == reqVote.LastLogTerm {
	// 		return reqVote.LastLogIndex+1 >= len(rf.Log)
	// 	} else {
	// 		return rf.Log[reqVote.LastLogIndex].Term < reqVote.LastLogTerm
	// 	}
	// }
	return true
}

func (rf *Raft) sendApplyMsg(start int, end int) {
	if start > end {
		return
	}
	for i := start; i <= end; i++ {
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.Log[i].Command,
			CommandIndex: i,
		}
		func(msg ApplyMsg) {
			// log.Printf("[%v]sendApplyMsg: %+v, %+v\n", rf.me, msg, rf)
			log.Printf("[%v]sendApplyMsg: %+v\n", rf.me, msg)
			rf.ApplyMsgChan <- msg
		}(msg)
	}
}

func followerHandler(rf *Raft) {
	// - handle timeout, then start new election and change to candidate
	// - handle AppendEntries ( heartbeat only for 2A)
	// - handle RequestVote
	rf.printInfo()
	timer := time.After(getRandElectionTimeoutMiliSecond())
	for {
		rf.persist()
		select {
		case <-timer:
			rf.CurrentRaftState = candidate
			return
		case reqVote := <-rf.RequestVoteArgsChan:
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
			// if grant vote for a leader, reset timer
			if rspVote.VoteGranted {
				return
			}
		case reqAppend := <-rf.AppendEntriesArgsChan:
			rspAppend := AppendEntriesReply{
				Term:      rf.CurrentTerm,
				Success:   false,
				FromId:    rf.me,
				NextIndex: reqAppend.PrevLogIndex + 1,
				VotedFor:  rf.VotedFor,
			}
			// per Fig2 rule when receiving higher term
			if reqAppend.Term > rf.CurrentTerm {
				rf.updateTerm(reqAppend.Term)
				rf.VotedFor = INITIAL_VOTED_FOR
			}
			// log.Printf("[followerHandler][%v]: %+v, %+v\n", reqAppend, rf)
			// what if leaderId != rf.VotedFor, but term the same , should reject and check it ?
			if reqAppend.Term >= rf.CurrentTerm && reqAppend.LeadId == rf.VotedFor {
				if rf.Log[reqAppend.PrevLogIndex].Term == reqAppend.PrevLogTerm {
					// refer to https://stackoverflow.com/questions/16248241/concatenate-two-slices-in-go
					rf.Log = append(rf.Log[:reqAppend.PrevLogIndex+1], reqAppend.Entries...)
					if reqAppend.LeaderCommit > rf.CommitIndex {
						oldCommitIndex := rf.CommitIndex
						rf.CommitIndex = int(math.Min(float64(reqAppend.LeaderCommit), float64(len(rf.Log)-1)))
						rf.sendApplyMsg(oldCommitIndex+1, rf.CommitIndex)
					}
					// increase after applied to state machine [ToDo]
					rf.LastApplied = len(rf.Log) - 1
					rspAppend.NextIndex = len(rf.Log)
					rspAppend.Success = true
				} else {
					nextIdx := reqAppend.PrevLogIndex
					// since log idx 0 is already done, just need to try up to 2
					for ; nextIdx > 1; nextIdx-- {
						if rf.Log[nextIdx-1].Term == reqAppend.PrevLogTerm {
							break
						}
					}
					rspAppend.NextIndex = nextIdx
				}
			}
			rspAppend.Term = rf.CurrentTerm
			rspAppend.VotedFor = rf.VotedFor
			rf.AppendEntriesReplyChan <- &rspAppend
			// if ack an Append RPC, reset timer
			if rspAppend.Success {
				return
			}
		case <-rf.ToStopChan:
			rf.ToStop = true
		}
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
	voteReplyChan := make(chan *RequestVoteReply)
	// vote for itself and send vote to followers
	rf.VotedFor = rf.me
	voteCount := 1
	sendVoteTask := func(term int, me int, destServer int, voteReplyChan chan *RequestVoteReply) {
		argvs := RequestVoteArgs{
			Term:         term,
			CandidateId:  me,
			LastLogIndex: len(rf.Log) - 1,
			LastLogTerm:  rf.Log[len(rf.Log)-1].Term,
		}
		reply := RequestVoteReply{}
		if rf.sendRequestVote(destServer, &argvs, &reply) {
			voteReplyChan <- &reply
		}
	}

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go sendVoteTask(rf.CurrentTerm, rf.me, i, voteReplyChan)
	}

	for {
		select {
		case <-timer:
			rf.persist()
			return
		case reqVote := <-rf.RequestVoteArgsChan:
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
		case voteReply := <-voteReplyChan:
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
			rf.ToStop = true
		}

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
func (rf *Raft) appendEntriesHelper(destServer int, appendReplyChan chan *AppendEntriesReply) {
	argvs := AppendEntriesArgs{
		Term:         rf.CurrentTerm,
		LeadId:       rf.me,
		PrevLogIndex: rf.NextIndex[destServer] - 1,
		PrevLogTerm:  rf.Log[rf.NextIndex[destServer]-1].Term,
		LeaderCommit: rf.CommitIndex,
	}
	tmpLog := rf.Log[rf.NextIndex[destServer]:len(rf.Log)]
	argvs.Entries = make([]LogEntry, len(tmpLog))
	copy(argvs.Entries, tmpLog)
	reply := AppendEntriesReply{}
	go func() {
		if rf.sendAppendEntries(destServer, &argvs, &reply) {
			appendReplyChan <- &reply
		}
	}()
}

// to update leader's commit index whenever received AppendEntries success
func (rf *Raft) updateLeaderCommitIndex() {
	// log.Printf("[%v] updateCommitIndex: CommitIndex:%v, matchIdx:%+v, currentTerm:%v\n", rf.me, rf.CommitIndex, rf.MatchIndex, rf.CurrentTerm)
	if rf.CurrentRaftState != leader {
		panic(fmt.Sprintf("unexpected raft state for updateLeaderCommitIndex: %v", rf.CurrentRaftState))
	}
	oldCommitIndex := rf.CommitIndex
	for i := rf.CommitIndex + 1; i < len(rf.Log); i++ {
		if rf.Log[i].Term != rf.CurrentTerm {
			// log.Printf("[%v][updateLeaderCommitIndex] rf.Log[%v].Term:%v != rf.CurrentTerm:%v\n", rf.me, i, rf.Log[i].Term, rf.CurrentTerm)
			// there is a bug here I dig a while to figure out:
			// The leader can't commit an log entry which is not the same term as current term.
			// But that doesn't mean we should stop whenever see this( I failed in TestFailAgree2B because of this)
			// Per paper fig2 rule for leader, it is mentioning as long as there exist a N can fit requirement,
			// set commitIndex to N
			continue
		}
		cnt := 0
		for peerIdx := range rf.peers {
			if rf.MatchIndex[peerIdx] >= i {
				cnt++
			}
		}
		log.Printf("[%v][updateLeaderCommitIndex] cnt:%v, i:%v\n", rf.me, cnt, i)
		if cnt > len(rf.peers)/2 {
			rf.CommitIndex = i
		} else {
			break
		}
	}
	// log.Printf("[%v][updateLeaderCommitIndex] start:%v, end:%v, rf:%+v\n", rf.me, oldCommitIndex+1, rf.CommitIndex, rf)
	rf.sendApplyMsg(oldCommitIndex+1, rf.CommitIndex)
}

func leaderHandler(rf *Raft) {
	// - send out heatbeat immediately as initial step then send out periodcally
	// - handle heartbeat timeout, then send it out peridically
	// - handle AppendEntries,
	// - handle RequestVote
	rf.initIndex()
	rf.printInfo()
	timer := time.After(0)
	appendReplyChan := make(chan *AppendEntriesReply)

	for {
		select {
		case <-timer:
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				rf.appendEntriesHelper(i, appendReplyChan)
			}
			timer = time.After(HEARTBEAT_TIMEOUT_MILISECONDS * time.Millisecond)
			rf.persist()
		case reqVote := <-rf.RequestVoteArgsChan:
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
		case appendReply := <-appendReplyChan:
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
			} else {
				// if fail, follower tell leader from where to try is better,
				// ignore the rsp if this follower is not voting to it
				if appendReply.VotedFor == rf.me {
					rf.NextIndex[appendReply.FromId] = appendReply.NextIndex
					rf.appendEntriesHelper(appendReply.FromId, appendReplyChan)
				}
			}
		case applyChReq := <-rf.ApplyChChan:
			log.Printf("[%v] receive applyChReq:%v\n", rf.me, applyChReq)
			rf.Log = append(rf.Log, LogEntry{
				Term:    rf.CurrentTerm,
				Command: applyChReq.command,
			})
			rf.MatchIndex[rf.me] = len(rf.Log) - 1
			rf.NextIndex[rf.me] = len(rf.Log)
			// increase after applied to state machine [ToDo]
			rf.LastApplied = len(rf.Log) - 1
			applyChReq.status <- 1
		case <-rf.ToStopChan:
			rf.ToStop = true
		}
	}
}

func (rf *Raft) printInfo() {
	log.Printf("[%v] enter %s state , term:%v, rf:%+v\n", rf.me, rf.CurrentRaftState, rf.CurrentTerm, rf)
}
