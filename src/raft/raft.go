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
	"fmt"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (role Role) String() string {
	var ret string
	switch role {
	case Follower:
		ret = "Follower"
	case Candidate:
		ret = "Candidate"
	case Leader:
		ret = "Leader"
	}
	return ret
}

type LogEntry struct {
	Command interface{}
	Term    int
}

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

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	rpcIdx      int
	log         []LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// Other state
	initialTime time.Time
	role        Role
	voteNum     int
	applyCh     chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.role == Leader
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
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.rpcIdx)
	e.Encode(rf.log)
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
	var currentTerm int
	var votedFor int
	var rpcIdx int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&rpcIdx) != nil ||
		d.Decode(&log) != nil {
		panic("[Raft.readPersist] Decode error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.rpcIdx = rpcIdx
		rf.log = log
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
	isLeader := false

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		rf.DPrintf("[S%v T%v Raft.Start] Fail to append the command \"%v\" (isn't the leader)\n",
			rf.me, rf.currentTerm, Command2Str(command))
	} else if !rf.killed() {
		index = rf.GetLastLogIndex() + 1
		term = rf.currentTerm
		isLeader = true
		logEntry := LogEntry{
			Command: command,
			Term:    term,
		}
		rf.log = append(rf.log, logEntry)
		rf.persist()
		rf.DPrintf("[S%v T%v Raft.Start] Append the command \"%v\" | index: %v\n",
			rf.me, rf.currentTerm, Command2Str(command), index)
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
func (rf *Raft) electTicker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		sleepTime := RandomTimeout()
		rf.DPrintf("[S%v] election timeout: %v\n",
			rf.me, sleepTime)
		tempTime := time.Now()
		time.Sleep(sleepTime)
		//_sleepTime := syscall.NsecToTimespec(int64(sleepTime))
		//syscall.Nanosleep(&_sleepTime, nil)

		span := time.Now().Sub(tempTime)
		if span > time.Second {
			panic("Sleep span is abnormal\n")
		}
		//rf.DPrintf("[S%v] span after sleeping: %v\n",
		//	rf.me, span)

		rf.mu.Lock()
		if rf.role == Leader || rf.initialTime.After(tempTime) {
			rf.mu.Unlock()
			continue
		}
		rf.BecomeCandidate()
		rf.ResetInitialTime()

		for server := range rf.peers {
			if server == rf.me {
				continue
			}
			go func(server int) {
				args, reply, preOk := rf.sendRequestVotePre(server)
				if !preOk {
					return
				}

				ok := rf.sendRequestVote(server, &args, &reply)

				rf.sendRequestVotePro(server, &args, &reply, ok)
			}(server)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) appendTicker() {
	for rf.killed() == false {
		time.Sleep(AppendPeriod * time.Millisecond)

		rf.mu.Lock()
		if rf.role != Leader {
			rf.mu.Unlock()
			continue
		}

		for server := range rf.peers {
			if server == rf.me {
				continue
			}
			go func(server int) {
				args, reply, preOk := rf.sendAppendEntriesPre(server)
				if !preOk {
					return
				}

				ok := rf.sendAppendEntries(server, &args, &reply)

				rf.sendAppendEntriesPro(server, &args, &reply, ok)
			}(server)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) applyTicker() {
	for rf.killed() == false {
		time.Sleep(ApplyPeriod * time.Millisecond)

		rf.mu.Lock()
		if rf.lastApplied > rf.commitIndex {
			errorMsg := fmt.Sprintf("[S%v T%v Raft.applyTikcer] lastApplied > commitIndex\n",
				rf.me, rf.currentTerm)
			rf.mu.Unlock()
			panic(errorMsg)
		}

		lastLogIndex := rf.GetLastLogIndex()
		if rf.commitIndex > lastLogIndex {
			errorMsg := fmt.Sprintf("[S%v T%v Raft.applyTikcer] commitIndex > lastLogIndex: %v VS %v\n",
				rf.me, rf.currentTerm, rf.commitIndex, lastLogIndex)
			rf.mu.Unlock()
			panic(errorMsg)
		}

		applyMsgs := make([]ApplyMsg, 0)
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			command := rf.log[i].Command
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      command,
				CommandIndex: i,
			}
			applyMsgs = append(applyMsgs, applyMsg)
		}
		rf.lastApplied = rf.commitIndex
		rf.mu.Unlock()

		for _, applyMsg := range applyMsgs {
			rf.applyCh <- applyMsg
		}
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
	rand.Seed(time.Now().Unix() + int64(rf.me))

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.rpcIdx = 0
	rf.log = make([]LogEntry, 1)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.ResetInitialTime()
	rf.role = Follower
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electTicker()
	go rf.appendTicker()
	go rf.applyTicker()

	return rf
}
