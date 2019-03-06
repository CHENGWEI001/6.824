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

// import "bytes"
// import "labgob"

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

const INITIAL_VOTED_FOR = -1

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm            int                      // current term of raft instance
	votedFor               int                      // previous vote for who, initial to -1
	currentRaftState       raftState                // current raft state
	RequestVoteArgsChan    chan *RequestVoteArgs    // channel for receiving Request Vote
	RequestVoteReplyChan   chan *RequestVoteReply   // channel for response Request Vote
	AppendEntriesArgsChan  chan *AppendEntriesArgs  // channel for receiving Append Entry
	AppendEntriesReplyChan chan *AppendEntriesReply // channel for response Append Entry
}

// serverState indicating current raft instance state
type raftState string

const (
	follower  raftState = "follower"
	candidate           = "candidate"
	leader              = "leader"
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isleader = rf.currentRaftState == leader
	term = rf.currentTerm

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
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	Term        int // candidate's term
	CandidateId int // Id who send this request

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
	Term   int // leader's term
	LeadId int // leader Id
}

type AppendEntriesReply struct {
	Term    int  // current term , for leader to update itself
	Success bool // [ToDo]
	FromId  int  // indicating this reply is from which raft instance
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
	rf.AppendEntriesArgsChan <- args
	*reply = *(<-rf.AppendEntriesReplyChan)
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
	term := -1
	isLeader := true

	// Your code here (2B).

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
}

func (rf *Raft) updateTerm(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = term
}

func (rf *Raft) updateState(state raftState) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentRaftState = state
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

	rf.votedFor = INITIAL_VOTED_FOR
	rf.currentTerm = 0
	rf.currentRaftState = follower
	rf.RequestVoteArgsChan = make(chan *RequestVoteArgs)
	rf.RequestVoteReplyChan = make(chan *RequestVoteReply)
	rf.AppendEntriesArgsChan = make(chan *AppendEntriesArgs)
	rf.AppendEntriesReplyChan = make(chan *AppendEntriesReply)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go startRaftThread(rf)

	return rf
}

// start point of raft thread, each raft instance is one thread, and it would
// go into different handler base on current state
func startRaftThread(rf *Raft) {
	for {
		switch state := rf.currentRaftState; state {
		case follower:
			followerHandler(rf)
		case candidate:
			candidateHandler(rf)
		case leader:
			leaderHandler(rf)
		default:
			panic(fmt.Sprintf("unexpected raft state: %v", state))
		}
	}
}

const ELECTION_TIMEOUT_MILISECONDS = 500
const ELECTION_TIMEOUT_RAND_RANGE_MILISECONDS = 500
const HEARTBEAT_TIMEOUT_MILISECONDS = 200

func getRandElectionTimeoutMiliSecond() time.Duration {
	return time.Duration(rand.Intn(ELECTION_TIMEOUT_RAND_RANGE_MILISECONDS)+ELECTION_TIMEOUT_MILISECONDS) * time.Millisecond
	// return ELECTION_TIMEOUT_MILISECONDS * time.Millisecond
}

func followerHandler(rf *Raft) {
	// - handle timeout, then start new election and change to candidate
	// - handle AppendEntries ( heartbeat only for 2A)
	// - handle RequestVote
	rf.printInfo()
	timer := time.After(getRandElectionTimeoutMiliSecond())
	for {
		select {
		case <-timer:
			rf.currentRaftState = candidate
			return
		case reqVote := <-rf.RequestVoteArgsChan:
			rspVote := RequestVoteReply{
				Term:        rf.currentTerm,
				VoteGranted: false,
				FromId:      rf.me,
			}
			// per Fig2 rule when receiving higher term
			if reqVote.Term > rf.currentTerm {
				rf.currentTerm = reqVote.Term
				rf.votedFor = reqVote.CandidateId
			}
			// per Fig2 Request RPC rule, only this condition to grant vote
			if reqVote.Term >= rf.currentTerm &&
				(rf.votedFor == INITIAL_VOTED_FOR || rf.votedFor == reqVote.CandidateId) {
				rspVote.VoteGranted = true
				rf.votedFor = reqVote.CandidateId
			}
			rspVote.Term = rf.currentTerm
			rf.RequestVoteReplyChan <- &rspVote
			// if grant vote for a leader, reset timer
			if rspVote.VoteGranted {
				return
			}
		case reqAppend := <-rf.AppendEntriesArgsChan:
			rspAppend := AppendEntriesReply{
				Term:    rf.currentTerm,
				Success: false,
				FromId:  rf.me,
			}
			// per Fig2 rule when receiving higher term
			if reqAppend.Term > rf.currentTerm {
				rf.currentTerm = reqAppend.Term
				rf.votedFor = reqAppend.LeadId
			}

			if reqAppend.Term >= rf.currentTerm && reqAppend.LeadId == rf.votedFor {
				rspAppend.Success = true
			}
			rspAppend.Term = rf.currentTerm
			rf.AppendEntriesReplyChan <- &rspAppend
			// if ack an Append RPC, reset timer
			if rspAppend.Success {
				return
			}
		}
	}
}

func candidateHandler(rf *Raft) {
	// - every time go into it, meaning new election ,so increase its term
	// - send the vote out to all peers
	// - handle timeout, then start new election and still in candidate state
	// - handle AppendEntries,
	// - handle RequestVote
	rf.printInfo()
	rf.currentTerm += 1
	timer := time.After(getRandElectionTimeoutMiliSecond())
	voteReplyChan := make(chan *RequestVoteReply)
	// vote for itself and send vote to followers
	rf.votedFor = rf.me
	voteCount := 1
	sendVoteTask := func(term int, me int, destServer int, voteReplyChan chan *RequestVoteReply) {
		argvs := RequestVoteArgs{
			Term:        term,
			CandidateId: me,
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
		go sendVoteTask(rf.currentTerm, rf.me, i, voteReplyChan)
	}

	for {
		select {
		case <-timer:
			return
		case reqVote := <-rf.RequestVoteArgsChan:
			rspVote := RequestVoteReply{
				Term:        rf.currentTerm,
				VoteGranted: false,
				FromId:      rf.me,
			}
			// per Fig2 rule when receiving higher term
			if reqVote.Term > rf.currentTerm {
				rf.currentTerm = reqVote.Term
				rf.currentRaftState = follower
				rf.votedFor = reqVote.CandidateId
				rspVote.VoteGranted = true
			}
			rspVote.Term = rf.currentTerm
			rf.RequestVoteReplyChan <- &rspVote
			// if granted, reset timer and step down as follower
			if rspVote.VoteGranted {
				return
			}
		case reqAppend := <-rf.AppendEntriesArgsChan:
			rspAppend := AppendEntriesReply{
				Term:    rf.currentTerm,
				Success: false,
				FromId:  rf.me,
			}
			// per Fig2 rule when receiving higher term
			if reqAppend.Term > rf.currentTerm {
				rf.currentTerm = reqAppend.Term
				rf.currentRaftState = follower
				rf.votedFor = reqAppend.LeadId
				rspAppend.Success = true
			}
			rspAppend.Term = rf.currentTerm
			rf.AppendEntriesReplyChan <- &rspAppend
			// if ack an Append RPC, return to follower state
			if rspAppend.Success {
				return
			}
		case voteReply := <-voteReplyChan:
			if voteReply.VoteGranted {
				voteCount += 1
				if voteCount >= len(rf.peers)/2+1 {
					rf.currentRaftState = leader
					return
				}
			} else {
				if voteReply.Term > rf.currentTerm {
					rf.currentTerm = voteReply.Term
					rf.currentRaftState = follower
					rf.votedFor = voteReply.FromId
					return
				}
			}
		}

	}
}

func leaderHandler(rf *Raft) {
	// - send out heatbeat immediately as initial step then send out periodcally
	// - handle heartbeat timeout, then send it out peridically
	// - handle AppendEntries,
	// - handle RequestVote
	rf.printInfo()
	timer := time.After(0)
	appendReplyChan := make(chan *AppendEntriesReply)
	sendAppendTask := func(term int, me int, destServer int, appendReplyChan chan *AppendEntriesReply) {
		argvs := AppendEntriesArgs{
			Term:   term,
			LeadId: me,
		}
		reply := AppendEntriesReply{}
		if rf.sendAppendEntries(destServer, &argvs, &reply) {
			appendReplyChan <- &reply
		}
	}

	for {
		select {
		case <-timer:
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				go sendAppendTask(rf.currentTerm, rf.me, i, appendReplyChan)
			}
			timer = time.After(HEARTBEAT_TIMEOUT_MILISECONDS)
		case reqVote := <-rf.RequestVoteArgsChan:
			rspVote := RequestVoteReply{
				Term:        rf.currentTerm,
				VoteGranted: false,
				FromId:      rf.me,
			}
			// per Fig2 rule when receiving higher term
			if reqVote.Term > rf.currentTerm {
				rf.currentTerm = reqVote.Term
				rf.currentRaftState = follower
				rf.votedFor = reqVote.CandidateId
				rspVote.VoteGranted = true
			}
			rspVote.Term = rf.currentTerm
			rf.RequestVoteReplyChan <- &rspVote
			// if grant vote for a leader, return to be follower
			if rspVote.VoteGranted {
				return
			}
		case reqAppend := <-rf.AppendEntriesArgsChan:
			rspAppend := AppendEntriesReply{
				Term:    rf.currentTerm,
				Success: false,
				FromId:  rf.me,
			}
			// per Fig2 rule when receiving higher term
			if reqAppend.Term > rf.currentTerm {
				rf.currentTerm = reqAppend.Term
				rf.currentRaftState = follower
				rf.votedFor = reqAppend.LeadId
				rspAppend.Success = true
			}
			rspAppend.Term = rf.currentTerm
			rf.AppendEntriesReplyChan <- &rspAppend
			// if ack an Append RPC, return to follower state
			if rspAppend.Success {
				return
			}
		case appendReply := <-appendReplyChan:
			if appendReply.Term > rf.currentTerm {
				rf.currentRaftState = follower
				rf.currentTerm = appendReply.Term
				rf.votedFor = appendReply.FromId
				return
			}
		}
	}
}

func (rf *Raft) printInfo() {
	log.Printf("[%v] enter %s state , term:%v\n", rf.me, rf.currentRaftState, rf.currentTerm)
}
