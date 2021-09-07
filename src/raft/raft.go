package raft

import (
	"6.824/labgob"
	"bytes"
	"encoding/gob"
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

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	state     int
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
	Command interface{}
	Term    int
	Index   int
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

func (rf *Raft) persist() {
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

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&rf.currentTerm) != nil ||
		d.Decode(&rf.votedFor) != nil ||
		d.Decode(&rf.logs) != nil ||
		d.Decode(&rf.voteCount) != nil {
		panic("error!")
	}
}

func (rf *Raft) getRaftState() []byte {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil ||
		e.Encode(rf.votedFor) != nil ||
		e.Encode(rf.voteCount) != nil ||
		e.Encode(rf.logs) != nil {
		panic("encode error")
	}
	return w.Bytes()
}

func (rf *Raft) getLastLogIndex() int {
	return rf.logs[len(rf.logs)-1].Index
}

func (rf *Raft) getLastLogTerm() int {
	return rf.logs[len(rf.logs)-1].Term
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
	baseIndex := rf.logs[0].Index
	if args.PreLogIndex >= baseIndex && args.PreLogTerm != rf.logs[args.PreLogIndex-baseIndex].Term {
		term := rf.logs[args.PreLogIndex-baseIndex].Term
		for i := args.PreLogIndex - 1; i >= baseIndex; i-- {
			if rf.logs[i-baseIndex].Term != term {
				reply.NextTryIndex = i + 1
				break
			}
		}
	} else if args.PreLogIndex >= baseIndex-1 {
		rf.logs = rf.logs[:args.PreLogIndex-baseIndex+1]
		rf.logs = append(rf.logs, args.Entries...)
		reply.Success = true
		reply.NextTryIndex = args.PreLogIndex + len(args.Entries)
		if rf.commitIndex < args.LeaderCommit {
			rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
			go rf.applyLog()
		}
	}
}

func (rf *Raft) applyLog() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	baseIndex := rf.logs[0].Index
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		applyMsg := ApplyMsg{}
		applyMsg.CommandIndex = i
		applyMsg.CommandValid = true
		applyMsg.Command = rf.logs[i-baseIndex].Command
		rf.applyChan <- applyMsg
	}
	rf.lastApplied = rf.commitIndex
}

func (rf *Raft) sendAppendEntries(peer int, args *AppendEntryRequestArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[peer].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok || rf.state != leader || args.Term != rf.currentTerm {
		return ok
	}
	if reply.Term > rf.currentTerm {
		rf.state = follower
		rf.votedFor = -1
		rf.currentTerm = reply.Term
		rf.persist()
		return ok
	}
	if reply.Success {
		if len(args.Entries) != 0 {
			rf.nextIndex[peer] = args.PreLogIndex + len(args.Entries) + 1
			rf.matchIndex[peer] = rf.nextIndex[peer] - 1
		}
	} else {
		rf.nextIndex[peer] = min(reply.NextTryIndex, rf.getLastLogIndex())
	}
	baseIndex := rf.logs[0].Index
	for newCommitIndex := rf.getLastLogIndex(); newCommitIndex > rf.commitIndex && rf.logs[newCommitIndex-baseIndex].Term == rf.currentTerm; newCommitIndex-- {
		cnt := 1
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= newCommitIndex {
				cnt++
			}
		}
		if cnt >= len(rf.peers)/2+1 {
			rf.commitIndex = newCommitIndex
			go rf.applyLog()
			break
		}
	}
	return ok
}

type InstallSnapshotArgs struct {
	Term              int
	LastIncludedIndex int
	LastIncludedTerm  int
	LeaderId          int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

//func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
//
//	return true
//}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.state = follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	rf.recvHeartbeatChan <- struct{}{}

	reply.Term = rf.currentTerm

	if args.LastIncludedIndex > rf.commitIndex {
		rf.trimLog(args.LastIncludedIndex, args.LastIncludedTerm)
		rf.lastApplied = args.LastIncludedIndex
		rf.commitIndex = args.LastIncludedIndex
		rf.persister.SaveStateAndSnapshot(rf.getRaftState(), args.Data)

		// remind kv server to use snapshot
		msg := ApplyMsg{SnapshotValid: true, Snapshot: args.Data}
		rf.applyChan <- msg
	}
}

// trim old entries according to lastIncludedIndex
func (rf *Raft) trimLog(lastIncludedIndex int, lastIncludedTerm int) {
	newLog := make([]logEntry, 0)
	newLog = append(newLog, logEntry{Index: lastIncludedIndex, Term: lastIncludedTerm})

	for i := len(rf.logs) - 1; i >= 0; i-- {
		if rf.logs[i].Index == lastIncludedIndex && rf.logs[i].Term == lastIncludedTerm {
			newLog = append(newLog, rf.logs[i+1:]...)
			break
		}
	}
	rf.logs = newLog
}

//func (rf *Raft) Snapshot(index int, snapshot []byte) {
//	// Your code here (2D).
//
//}

func (rf *Raft) sendInstallSnapshot(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[peer].Call("Raft.InstallSnapshot", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok || rf.state != leader || args.Term != rf.currentTerm {
		// invalid request
		return ok
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = follower
		rf.votedFor = -1
		rf.persist()
		return ok
	}

	rf.nextIndex[peer] = args.LastIncludedIndex + 1
	rf.matchIndex[peer] = args.LastIncludedIndex
	return ok
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Grant bool
	Term  int
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
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
		return lastLogIndex, rf.logs[lastLogIndex].Term
	} else {
		return -1, -1
	}
}

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

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

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

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
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
			go rf.broadVoteRequest()
			select {
			case <-rf.winElectionChan:
			case <-rf.recvHeartbeatChan:
				rf.state = follower
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(150)+150)):
			}
		case leader:
			go rf.broadcastHeartbeat()
			time.Sleep(time.Millisecond * 50)
		}
	}
}

func (rf *Raft) broadcastHeartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	baseIndex := rf.logs[0].Index
	for peer := range rf.peers {
		if peer != rf.me && rf.state == leader {
			if rf.nextIndex[peer] > baseIndex {
				args := new(AppendEntryRequestArgs)
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.PreLogIndex = rf.nextIndex[peer] - 1
				if args.PreLogIndex >= baseIndex {
					args.PreLogTerm = rf.logs[args.PreLogIndex].Term
				}
				if args.PreLogIndex <= rf.getLastLogIndex() {
					args.Entries = rf.logs[rf.nextIndex[peer]-baseIndex:]
				}
				args.LeaderCommit = rf.commitIndex
				go rf.sendAppendEntries(peer, args, &AppendEntryReply{})
			} else {
				snapshot := rf.persister.ReadSnapshot()
				args := new(InstallSnapshotArgs)
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.LastIncludedIndex = rf.logs[0].Index
				args.LastIncludedTerm = rf.logs[0].Term
				args.Data = snapshot

				go rf.sendInstallSnapshot(peer, args, &InstallSnapshotReply{})
			}
		}
	}

}

func (rf *Raft) broadVoteRequest() {
	rf.mu.Lock()
	args := &RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.getLastLogIndex()
	args.LastLogTerm = rf.getLastLogTerm()
	rf.mu.Unlock()

	for server := range rf.peers {
		if server != rf.me && rf.state == candidate {
			go rf.sendRequestVote(server, args, &RequestVoteReply{})
		}
	}
}

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
	rf.logs = append(rf.logs, logEntry{Term: 0})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.applyChan = applyCh
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
