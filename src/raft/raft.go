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
	"sync"
	"sync/atomic"
	"time"

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
	currentTerm       int
	votedFor          int
	nextRpcId         int
	log               []LogEntry
	lastIncludedIndex int
	lastIncludedTerm  int

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

	applyCh            chan ApplyMsg
	nextApplyOrder     int
	finishedApplyOrder int
	applyCond          *sync.Cond

	newCommand chan bool

	gid int
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
func (rf *Raft) stateData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.nextRpcId)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	return w.Bytes()
}

func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	data := rf.stateData()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	basicInfo := rf.BasicInfo("readPersist")

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
	var rpcId int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&rpcId) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		errorMsg := fmt.Sprintf("[%v] Decode error\n", basicInfo)
		panic(errorMsg)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.nextRpcId = rpcId
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
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
	methodName := "Snapshot"

	// Your code here (2D).
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index > rf.commitIndex {
		errorMsg := fmt.Sprintf("[%v] index > commitIndex: %v VS %v\n",
			rf.BasicInfoWithTerm(methodName), index, rf.commitIndex)
		rf.mu.Unlock()
		panic(errorMsg)
	}

	if index > rf.lastApplied {
		errorMsg := fmt.Sprintf("[%v] index > lastApplied: %v VS %v\n",
			rf.BasicInfoWithTerm(methodName), index, rf.lastApplied)
		rf.mu.Unlock()
		panic(errorMsg)
	}

	if index <= rf.lastIncludedIndex {
		rf.DPrintf("[%v] Fail to make the snapshot (have ahead lastIncludedIndex: %v VS %v)\n",
			rf.BasicInfoWithTerm(methodName), rf.lastIncludedIndex, index)
		return
	}

	rf.DPrintf("[%v] Before making the snapshot: "+
		"lastLogIndex: %v | lastIncludedIndex: %v | lastIncludedTerm: %v | len(log): %v\n",
		rf.BasicInfoWithTerm(methodName), rf.GetLastLogIndex(), rf.lastIncludedIndex, rf.lastIncludedTerm, len(rf.log))

	newLog := make([]LogEntry, 1)
	lastLogIndex := rf.GetLastLogIndex()
	if index < lastLogIndex {
		newLog = append(newLog, rf.GetRightSubLog(index+1)...)
	} else if index > lastLogIndex {
		errorMsg := fmt.Sprintf("[%v] index > lastLogIndex: %v VS %v\n",
			rf.BasicInfoWithTerm(methodName), index, lastLogIndex)
		rf.mu.Unlock()
		panic(errorMsg)
	}
	rf.lastIncludedIndex, rf.lastIncludedTerm = index, rf.GetTerm(index)
	rf.log = newLog
	rf.persister.SaveStateAndSnapshot(rf.stateData(), snapshot)

	rf.DPrintf("[%v] Make the snapshot | index: %v | term: %v | len(log): %v\n",
		rf.BasicInfoWithTerm(methodName), rf.lastIncludedIndex, rf.lastIncludedTerm, len(rf.log))
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
	methodName := "Start"

	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).
	if rf.killed() {
		return index, term, isLeader
	}

	rf.mu.Lock()
	if rf.role != Leader {
		rf.DPrintf("[%v] Fail to append the command \"%v\" (isn't the leader)\n",
			rf.BasicInfoWithTerm(methodName), Command2Str(command))
	} else {
		defer func() {
			if len(rf.newCommand) != cap(rf.newCommand) {
				rf.newCommand <- true
			}
		}()

		index = rf.GetLastLogIndex() + 1
		term = rf.currentTerm
		isLeader = true
		logEntry := LogEntry{
			Command: command,
			Term:    term,
		}
		rf.log = append(rf.log, logEntry)
		rf.persist()
		rf.DPrintf("[%v] Append the command \"%v\" | index: %v\n",
			rf.BasicInfoWithTerm(methodName), Command2Str(command), index)
	}
	rf.mu.Unlock()

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
	basicInfo := rf.BasicInfo("Kill")

	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.applyCond.Broadcast()
	rf.DPrintf("[%v] Be killed\n", basicInfo)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) electTicker() {
	methodName := "electTicker"

	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		sleepTime := RandomTimeout()
		//rf.DPrintf("[R%v] election timeout: %v\n",
		//	rf.me, sleepTime)
		tempTime := time.Now()
		time.Sleep(sleepTime)
		//_sleepTime := syscall.NsecToTimespec(int64(sleepTime))
		//syscall.Nanosleep(&_sleepTime, nil)

		span := time.Now().Sub(tempTime)
		if span > time.Second {
			panic("Sleep span is abnormal\n")
		}
		//rf.DPrintf("[R%v] span after sleeping: %v\n",
		//	rf.me, span)

		rf.mu.Lock()
		if rf.role == Leader || rf.initialTime.After(tempTime) {
			rf.mu.Unlock()
			continue
		}
		rf.BecomeCandidate()
		rf.ResetInitialTime()

		rf.DPrintf("[%v] Start to request vote\n", rf.BasicInfoWithTerm(methodName))
		for server := range rf.peers {
			if server == rf.me {
				continue
			}
			go func(server int) {
				rf.mu.Lock()
				args, reply, preOk := rf.sendRequestVotePre(server)
				rf.mu.Unlock()
				if !preOk {
					return
				}

				ok := rf.sendRequestVote(server, args, reply)

				rf.mu.Lock()
				rf.sendRequestVotePro(server, args, reply, ok)
				rf.mu.Unlock()
			}(server)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) appendTicker() {
	methodName := "appendTicker"

	for rf.killed() == false {
		timer := time.NewTimer(AppendPeriod * time.Millisecond)
		select {
		case <-rf.newCommand:
		case <-timer.C:
		}
		timer.Stop()

		rf.mu.Lock()
		if rf.role != Leader {
			rf.mu.Unlock()
			continue
		}

		rf.DPrintf("[%v] Start to append entries\n", rf.BasicInfoWithTerm(methodName))
		for server := range rf.peers {
			if server == rf.me {
				continue
			}
			go func(server int) {
				rf.mu.Lock()
				if rf.nextIndex[server] <= rf.lastIncludedIndex {
					args, reply, preOk := rf.sendInstallSnapshotPre(server)
					rf.mu.Unlock()
					if !preOk {
						return
					}

					ok := rf.sendInstallSnapshot(server, args, reply)

					rf.mu.Lock()
					rf.sendInstallSnapshotPro(server, args, reply, ok)
					rf.mu.Unlock()
				} else {
					args, reply, preOk := rf.sendAppendEntriesPre(server)
					rf.mu.Unlock()
					if !preOk {
						return
					}

					ok := rf.sendAppendEntries(server, args, reply)

					rf.mu.Lock()
					rf.sendAppendEntriesPro(server, args, reply, ok)
					rf.mu.Unlock()
				}
			}(server)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) applyTicker() {
	methodName := "applyTicker"

	for rf.killed() == false {
		time.Sleep(ApplyPeriod * time.Millisecond)

		rf.mu.Lock()
		if rf.lastApplied > rf.commitIndex {
			errorMsg := fmt.Sprintf("[%v] lastApplied > commitIndex: %v VS %v\n",
				rf.BasicInfoWithTerm(methodName), rf.lastApplied, rf.commitIndex)
			rf.mu.Unlock()
			panic(errorMsg)
		}

		lastLogIndex := rf.GetLastLogIndex()
		if rf.commitIndex > lastLogIndex {
			errorMsg := fmt.Sprintf("[%v] commitIndex > lastLogIndex: %v VS %v\n",
				rf.BasicInfoWithTerm(methodName), rf.commitIndex, lastLogIndex)
			rf.mu.Unlock()
			panic(errorMsg)
		}

		applyMsgs := make([]ApplyMsg, 0)

		if rf.lastApplied < rf.lastIncludedIndex {
			applyMsg := ApplyMsg{
				CommandValid:  false,
				SnapshotValid: true,
				Snapshot:      rf.persister.ReadSnapshot(),
				SnapshotIndex: rf.lastIncludedIndex,
				SnapshotTerm:  rf.lastIncludedTerm,
			}
			applyMsgs = append(applyMsgs, applyMsg)
			oriLastApplied := rf.lastApplied
			oriCommitIndex := rf.commitIndex
			rf.lastApplied = rf.lastIncludedIndex
			rf.commitIndex = Max(rf.commitIndex, rf.lastApplied)
			rf.ApplyDPrintf("[%v] Prepare to apply the snapshot | index: %v | term: %v | "+
				"lastApplied: %v -> %v | commitIndex: %v -> %v\n",
				rf.BasicInfoWithTerm(methodName), applyMsg.SnapshotIndex, applyMsg.SnapshotTerm,
				oriLastApplied, rf.lastApplied, oriCommitIndex, rf.commitIndex)
		}

		for rf.lastApplied < rf.commitIndex {
			applyIndex := rf.lastApplied + 1
			command := rf.GetCommand(applyIndex)
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      command,
				CommandIndex: applyIndex,
			}
			applyMsgs = append(applyMsgs, applyMsg)
			oriLastApplied := rf.lastApplied
			rf.lastApplied = applyIndex
			rf.ApplyDPrintf("[%v] Prepare to apply the command \"%v\" | index: %v | lastApplied: %v -> %v\n",
				rf.BasicInfoWithTerm(methodName), Command2Str(applyMsg.Command), applyMsg.CommandIndex,
				oriLastApplied, rf.lastApplied)
		}

		if len(applyMsgs) == 0 {
			rf.mu.Unlock()
			continue
		}

		applyOrder := rf.nextApplyOrder
		rf.nextApplyOrder++
		for applyOrder != rf.finishedApplyOrder+1 {
			if rf.killed() {
				rf.mu.Unlock()
				return
			}
			rf.applyCond.Wait()
		}
		rf.mu.Unlock()

		for _, applyMsg := range applyMsgs {
			rf.applyCh <- applyMsg
			if applyMsg.SnapshotValid {
				rf.ApplyDPrintf("[%v] Apply the snapshot | applyOrder: %v | index: %v | term: %v\n",
					rf.BasicInfo(methodName), applyOrder, applyMsg.SnapshotIndex, applyMsg.SnapshotTerm)
			} else {
				rf.ApplyDPrintf("[%v] Apply the command \"%v\" | applyOrder: %v | index: %v\n",
					rf.BasicInfo(methodName), Command2Str(applyMsg.Command), applyOrder, applyMsg.CommandIndex)
			}
		}

		rf.mu.Lock()
		rf.finishedApplyOrder = applyOrder
		rf.applyCond.Broadcast()
		rf.mu.Unlock()
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
	return newRaft(peers, me, persister, applyCh, -1)
}

func MakeWithGID(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg, gid int) *Raft {
	return newRaft(peers, me, persister, applyCh, gid)
}

func newRaft(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg, gid int) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rand.Seed(time.Now().Unix() + int64(rf.me))

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.nextRpcId = 0
	rf.log = make([]LogEntry, 1)
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.ResetInitialTime()
	rf.role = Follower
	rf.voteNum = 0
	rf.applyCh = applyCh
	rf.nextApplyOrder = 1
	rf.finishedApplyOrder = 0
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.newCommand = make(chan bool, 1)
	rf.gid = gid

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.DPrintf("[%v] Make new raft | currentTerm: %v | votedFor: %v | lastIncludedIndex: %v | lastIncludedTerm: %v\n",
		rf.BasicInfo(""), rf.currentTerm, rf.votedFor, rf.lastIncludedIndex, rf.lastIncludedTerm)

	// start ticker goroutine to start elections
	go rf.electTicker()
	go rf.appendTicker()
	go rf.applyTicker()

	return rf
}
