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

import (
	"6.824/labgob"
	"bytes"
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"
	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	follower int = iota
	candidate
	leader
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state int
	// persistent state
	currentTerm int
	votedFor    int
	voteCount   int
	logs        []logEntry

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex  map[int]int
	matchIndex map[int]int

	// channels
	applyChan         chan ApplyMsg
	grantVoteChan     chan struct{}
	winElectionChan   chan struct{}
	recvHeartbeatChan chan struct{}
}

// logEntry Defines the log entry
type logEntry struct {
	command interface{}
	term    int
	index   int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == leader
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	currentTerm := rf.currentTerm
	votedFor := rf.votedFor
	voteCount := rf.voteCount
	logs := rf.logs
	e.Encode(currentTerm)
	e.Encode(votedFor)
	e.Encode(logs)
	e.Encode(voteCount)
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
	if d.Decode(&rf.currentTerm) != nil ||
		d.Decode(&rf.votedFor) != nil ||
		d.Decode(&rf.logs) != nil ||
		d.Decode(&rf.voteCount) != nil {
		panic("error!")
	}
}

func (rf *Raft) getLastLogIndex() int {
	return rf.logs[len(rf.logs)-1].index
}

func (rf *Raft) getLastLogTerm() int {
	return rf.logs[len(rf.logs)-1].term
}

type AppendEntryRequestArgs struct {
	Term         int
	LeaderId     int
	PreLogIndex  int
	PreLogTerm   int
	Entries      []logEntry
	LeaderCommit int
}

type AppendEntryReply struct {
	Term         int
	Success      bool
	NextTryIndex int
}

// AppendEntries is a RPC handler to append entries
func (rf *Raft) AppendEntries(args *AppendEntryRequestArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Success = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.NextTryIndex = len(rf.logs)
		return
	}
	// maybe there is new leader already
	// so become a follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = follower
		rf.votedFor = -1
	}
	reply.Term = rf.currentTerm
	rf.recvHeartbeatChan <- struct{}{}

	if args.PreLogIndex > rf.getLastLogIndex() {
		reply.NextTryIndex = rf.getLastLogIndex() + 1
		return
	}
	baseIndex := rf.logs[0].index
	if args.PreLogIndex >= baseIndex && args.PreLogTerm != rf.logs[args.PreLogIndex-baseIndex].term {
		term := rf.logs[args.PreLogIndex-baseIndex].term
		for i := args.PreLogIndex - 1; i >= baseIndex; i-- {
			if rf.logs[i-baseIndex].term != term {
				reply.NextTryIndex = i + 1
				break
			}
		}
	} else if args.PreLogIndex >= baseIndex-1 {

	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Grant bool
	Term  int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	currentTerm := rf.currentTerm
	votedFor := rf.votedFor
	lastLogIndex, lastLogTerm := rf.lastLogIndexAndTerm()

	if args.Term < currentTerm {
		reply.Term = currentTerm
		reply.Grant = false
		return
	} else if args.Term > currentTerm {
		// this raft becomes to a follower
		reply.Term = args.Term
		rf.state = follower
		rf.votedFor = -1
	}
	// case rf.currentTerm == args.Term
	if currentTerm == args.Term && (votedFor == -1 || votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		rf.votedFor = args.CandidateId
		reply.Grant = true
	} else {
		reply.Grant = false
	}
	reply.Term = currentTerm
	rf.persist()
	return
}

// util function
func (rf *Raft) lastLogIndexAndTerm() (int, int) {
	if len(rf.logs) > 0 {
		lastLogIndex := len(rf.logs) - 1
		return lastLogIndex, rf.logs[lastLogIndex].term
	} else {
		return -1, -1
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if rf.state != candidate || rf.currentTerm != args.Term {
			return ok
		}
		if rf.currentTerm < reply.Term {
			rf.currentTerm = reply.Term
			rf.state = follower
			rf.votedFor = -1
			return ok
		}
		if reply.Grant {
			rf.voteCount++
			if rf.voteCount >= len(rf.peers)/2+1 {
				// win the election
				rf.state = leader
				rf.persist()
				rf.nextIndex = make(map[int]int, len(rf.peers))
				rf.matchIndex = make(map[int]int, len(rf.peers))
				nextIndex := len(rf.logs)
				for i, _ := range rf.nextIndex {
					rf.nextIndex[i] = nextIndex
				}
			}
		}
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == leader {
		term = rf.currentTerm
		index = len(rf.logs) - 1
	} else {
		isLeader = false
	}
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

	}
}

func (rf *Raft) Run() {
	for {
		switch rf.state {
		case follower:
			select {
			case <-rf.grantVoteChan:
			case <-rf.recvHeartbeatChan:
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(150)+150)):
				rf.state = candidate
				rf.persist()
			}
		case candidate:
			rf.mu.Lock()
			rf.votedFor = rf.me
			rf.voteCount = 1
			rf.currentTerm++
			rf.persist()
			rf.mu.Unlock()
			go rf.boardVoteRequest()
			select {
			case <-rf.winElectionChan:
			case <-rf.recvHeartbeatChan:
				rf.state = follower
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(150)+150)):
			}
		case leader:
			go rf.boardHeartbeat()
			time.Sleep(time.Millisecond * 50)
		}
	}
}

func (rf *Raft) boardHeartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	baseIndex := rf.logs[0].index
	for peer := range rf.peers {
		if peer != rf.me && rf.state == leader {
			if rf.nextIndex[peer] > baseIndex {

			}
		}
	}

}

func (rf *Raft) boardVoteRequest() {

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

	rf.state = follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.voteCount = 0
	rf.logs = append(rf.logs, logEntry{term: 0})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.applyChan = make(chan ApplyMsg)
	rf.grantVoteChan = make(chan struct{})
	rf.winElectionChan = make(chan struct{})
	rf.recvHeartbeatChan = make(chan struct{})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.persist()
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
