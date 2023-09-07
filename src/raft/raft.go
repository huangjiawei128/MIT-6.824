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

type AppendStatus int

const (
	Success AppendStatus = iota
	TermLagFailure
	LogMatchFailure
)

func (appendStatus AppendStatus) String() string {
	var ret string
	switch appendStatus {
	case Success:
		ret = "Success"
	case TermLagFailure:
		ret = "TermLagFailure"
	case LogMatchFailure:
		ret = "LogMatchFailure"
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
	rpcIdx      int
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
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		panic("[Raft.readPersist] Decode error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	RpcIdx       int
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
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	RpcIdx       int
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int
	Status    AppendStatus
	NextIndex int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.DPrintf("[S%v T%v Raft.RequestVote(S%v-%v)] Receive RequestVote RPC from S%v\n",
		rf.me, rf.currentTerm, args.CandidateId, args.RpcIdx, args.CandidateId)

	reply.VoteGranted = true
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		rf.DPrintf("[S%v T%v Raft.RequestVote(S%v-%v)] Refuse to vote for S%v (have ahead term: %v VS %v)\n",
			rf.me, rf.currentTerm, args.CandidateId, args.RpcIdx, args.CandidateId, rf.currentTerm, args.Term)
		return
	}

	if args.Term > rf.currentTerm {
		oriRole := rf.role
		rf.BecomeFollower(args.Term)
		if oriRole != Follower {
			rf.ResetInitialTime()
		}
		reply.Term = rf.currentTerm
	}

	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.VoteGranted = false
		rf.DPrintf("[S%v T%v Raft.RequestVote(S%v-%v)] Refuse to vote for S%v (have already voted for S%v)\n",
			rf.me, rf.currentTerm, args.CandidateId, args.RpcIdx, args.CandidateId, rf.votedFor)
		return
	}

	if !rf.UpToDate(args.LastLogIndex, args.LastLogTerm) {
		reply.VoteGranted = false
		lastLogIndex, lastLogTerm := rf.GetLastLogInfo()
		rf.DPrintf("[S%v T%v Raft.RequestVote(S%v-%v)] Refuse to vote for S%v "+
			"(have ahead (lastLogIndex,lastLogTerm): (%v,%v) VS (%v,%v))\n",
			rf.me, rf.currentTerm, args.CandidateId, args.RpcIdx, args.CandidateId,
			lastLogIndex, lastLogTerm, args.LastLogIndex, args.LastLogTerm)
		return
	}

	rf.votedFor = args.CandidateId
	rf.persist()
	rf.ResetInitialTime()
	reply.VoteGranted = true
	rf.DPrintf("[S%v T%v Raft.RequestVote(S%v-%v)] Vote for S%v\n",
		rf.me, rf.currentTerm, args.CandidateId, args.RpcIdx, args.CandidateId)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.DPrintf("[S%v T%v Raft.AppendEntries(S%v-%v)] Receive AppendEntries RPC from S%v\n",
		rf.me, rf.currentTerm, args.LeaderId, args.RpcIdx, args.LeaderId)

	reply.Status = Success
	reply.Term = rf.currentTerm
	reply.NextIndex = args.PrevLogIndex

	if args.Term < rf.currentTerm {
		reply.Status = TermLagFailure
		rf.DPrintf("[S%v T%v Raft.AppendEntries(S%v-%v)] Refuse to append entries from S%v "+
			"(have ahead term: %v VS %v)\n",
			rf.me, rf.currentTerm, args.LeaderId, args.RpcIdx, args.LeaderId, rf.currentTerm, args.Term)
		return
	}

	rf.BecomeFollower(args.Term)
	rf.ResetInitialTime()
	reply.Term = rf.currentTerm

	if !rf.MatchTerm(args.PrevLogIndex, args.PrevLogTerm) {
		reply.Status = LogMatchFailure
		rf.DPrintf("[S%v T%v Raft.AppendEntries(S%v-%v)] Refuse to append entries from S%v "+
			"(don't match term at I%v: %v VS %v)\n",
			rf.me, rf.currentTerm, args.LeaderId, args.RpcIdx, args.LeaderId,
			args.PrevLogIndex, rf.GetTerm(args.PrevLogIndex), args.PrevLogTerm)

		//	optimize reply.NextIndex
		lastLogIndex := rf.GetLastLogIndex()
		if lastLogIndex < args.PrevLogIndex {
			reply.NextIndex = lastLogIndex + 1
		} else {
			index := args.PrevLogIndex
			term := rf.GetTerm(index)
			for ; index > 0; index-- {
				if rf.GetTerm(index) != term {
					break
				}
			}
			reply.NextIndex = index + 1
		}

		rf.log = rf.log[:Min(lastLogIndex+1, args.PrevLogIndex)]
		rf.persist()
		return
	}

	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	rf.persist()

	lastLogIndex := rf.GetLastLogIndex()
	if args.LeaderCommit > rf.commitIndex {
		oriCommitIndex := rf.commitIndex
		rf.commitIndex = Min(args.LeaderCommit, lastLogIndex)
		rf.DPrintf("[S%v T%v Raft.AppendEntries(S%v-%v)] commitIndex: %v -> %v\n",
			rf.me, rf.currentTerm, args.LeaderId, args.RpcIdx, oriCommitIndex, rf.commitIndex)
	}

	if rf.commitIndex > lastLogIndex {
		errorMsg := fmt.Sprintf("[S%v T%v Raft.AppendEntries(S%v-%v)] commitIndex > lastLogIndex: %v VS %v\n",
			rf.me, rf.currentTerm, args.LeaderId, args.RpcIdx, rf.commitIndex, lastLogIndex)
		panic(errorMsg)
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
	rf.mu.Lock()
	rf.DPrintf("[S%v T%v Raft.sendRequestVote(%v)] Send RequestVote RPC to S%v\n",
		rf.me, rf.currentTerm, args.RpcIdx, server)
	rf.mu.Unlock()

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	rf.mu.Lock()
	if ok {
		rf.DPrintf("[S%v T%v Raft.sendRequestVote(%v)] Receive RequestVote ACK from S%v | voteGranted: %v\n",
			rf.me, rf.currentTerm, args.RpcIdx, server, reply.VoteGranted)
	} else {
		rf.DPrintf("[S%v T%v Raft.sendRequestVote(%v)] Fail to receive RequestVote ACK from S%v\n",
			rf.me, rf.currentTerm, args.RpcIdx, server)
	}
	rf.mu.Unlock()
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.mu.Lock()
	rf.DPrintf("[S%v T%v Raft.sendAppendEntries(%v)] Send AppendEntries RPC to S%v\n",
		rf.me, rf.currentTerm, args.RpcIdx, server)
	rf.mu.Unlock()

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()
	if ok {
		rf.DPrintf("[S%v T%v Raft.sendAppendEntries(%v)] Receive AppendEntries ACK from S%v "+
			"| status: %v | nextIndex: %v\n",
			rf.me, rf.currentTerm, args.RpcIdx, server, reply.Status, reply.NextIndex)
	} else {
		rf.DPrintf("[S%v T%v Raft.sendAppendEntries(%v)] Fail to receive AppendEntries ACK from S%v\n",
			rf.me, rf.currentTerm, args.RpcIdx, server)
	}
	rf.mu.Unlock()
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
	isLeader := false

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		rf.DPrintf("[S%v T%v Raft.Start] Fail to append the command \"%v\" (isn't the leader)\n",
			rf.me, rf.currentTerm, Command2Str(command))
	} else if !rf.killed() {
		index = len(rf.log)
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
		rf.DPrintf("[S%v] election timeout: %v\n", rf.me, sleepTime)
		tempTime := time.Now()
		time.Sleep(sleepTime)
		//_sleepTime := syscall.NsecToTimespec(int64(sleepTime))
		//syscall.Nanosleep(&_sleepTime, nil)

		//rf.DPrintf("[S%v->ALL] span after sleeping: %v\n",
		//	rf.me, time.Now().Sub(tempTime))

		rf.mu.Lock()
		if rf.role == Leader || rf.initialTime.After(tempTime) {
			rf.mu.Unlock()
			continue
		}
		//rf.DPrintf("[S%v->ALL T%v] span before election: %v\n",
		//	rf.me, rf.currentTerm, time.Now().Sub(tempTime))
		rf.BecomeCandidate()
		rf.ResetInitialTime()

		//rf.DPrintf("[S%v->ALL T%v] span before creating go routines: %v\n",
		//	rf.me, requestVoteArgs.Term, time.Now().Sub(tempTime))
		for server := range rf.peers {
			if server == rf.me {
				continue
			}
			go func(server int) {
				//rf.DPrintf("[S%v->S%v T%v] span before sending: %v\n",
				//	rf.me, server, requestVoteArgs.Term, time.Now().Sub(tempTime))
				rf.mu.Lock()
				if rf.role != Candidate {
					rf.mu.Unlock()
					return
				}
				lastLogIndex, lastLogTerm := rf.GetLastLogInfo()
				requestVoteArgs := RequestVoteArgs{
					RpcIdx:       rf.rpcIdx,
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: lastLogIndex,
					LastLogTerm:  lastLogTerm,
				}
				rf.rpcIdx++
				requestVoteReply := RequestVoteReply{}
				rf.mu.Unlock()

				ok := rf.sendRequestVote(server, &requestVoteArgs, &requestVoteReply)
				//rf.DPrintf("[S%v->S%v T%v] span after sending: %v\n",
				//	rf.me, server, requestVoteArgs.Term, time.Now().Sub(tempTime))

				if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.role != Candidate || rf.currentTerm != requestVoteArgs.Term {
						return
					}

					if requestVoteReply.Term > rf.currentTerm {
						rf.BecomeFollower(requestVoteReply.Term)
						rf.ResetInitialTime()
						return
					}

					if requestVoteReply.VoteGranted {
						rf.voteNum++
						if rf.voteNum > len(rf.peers)>>1 {
							rf.BecomeLeader()
						}
					}
				}
			}(server)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) appendTicker() {
	for rf.killed() == false {
		time.Sleep(HeartbeatPeriod * time.Millisecond)

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
				rf.mu.Lock()
				if rf.role != Leader {
					rf.mu.Unlock()
					return
				}
				lastLogIndex := rf.GetLastLogIndex()
				nextIndex := rf.nextIndex[server]
				appendEntriesArgs := AppendEntriesArgs{
					RpcIdx:       rf.rpcIdx,
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: nextIndex - 1,
					PrevLogTerm:  rf.log[nextIndex-1].Term,
					LeaderCommit: rf.commitIndex,
					Entries:      []LogEntry{},
				}
				rf.rpcIdx++
				if lastLogIndex >= rf.nextIndex[server] {
					appendEntriesArgs.Entries = rf.log[nextIndex : lastLogIndex+1]
				}
				appendEntriesReply := AppendEntriesReply{}
				rf.mu.Unlock()

				ok := rf.sendAppendEntries(server, &appendEntriesArgs, &appendEntriesReply)
				if !ok {
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.role != Leader || rf.currentTerm != appendEntriesArgs.Term {
					return
				}

				if appendEntriesReply.Term > rf.currentTerm {
					rf.BecomeFollower(appendEntriesReply.Term)
					rf.ResetInitialTime()
					return
				}

				if appendEntriesReply.Status == Success {
					oriMatchIndex := rf.matchIndex[server]
					oriNextIndex := rf.nextIndex[server]
					rf.matchIndex[server] = appendEntriesArgs.PrevLogIndex + len(appendEntriesArgs.Entries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1
					rf.DPrintf("[S%v T%v Raft.appendTicker] matchIndex[%v]: %v -> %v, nextIndex[%v]: %v -> %v\n",
						rf.me, rf.currentTerm, server, oriMatchIndex, rf.matchIndex[server],
						server, oriNextIndex, rf.nextIndex[server])

					lastLogIndex = rf.GetLastLogIndex()
					for index := lastLogIndex; index > rf.commitIndex; index-- {
						if rf.log[index].Term > rf.currentTerm {
							errorMsg := fmt.Sprintf("[S%v T%v Raft.appendTicker] log[%v].Term > currentTerm\n",
								rf.me, rf.currentTerm, index)
							panic(errorMsg)
						}

						if rf.log[index].Term < rf.currentTerm {
							break
						}

						matchNum := 0
						for server_ := range rf.peers {
							if server_ == rf.me {
								matchNum++
								continue
							}
							if rf.matchIndex[server_] >= index {
								matchNum++
							}
						}

						if matchNum > len(rf.peers)>>1 {
							oriCommitIndex := rf.commitIndex
							rf.commitIndex = index
							rf.DPrintf("[S%v T%v Raft.appendTicker] commitIndex: %v -> %v \n",
								rf.me, rf.currentTerm, oriCommitIndex, rf.commitIndex)
							break
						}
					}
				} else if appendEntriesReply.Status == LogMatchFailure {
					oriNextIndex := rf.nextIndex[server]
					//	rf.nextIndex[server]--
					rf.nextIndex[server] = appendEntriesReply.NextIndex
					rf.DPrintf("[S%v T%v Raft.appendTicker] nextIndex[%v]: %v -> %v \n",
						rf.me, rf.currentTerm, server, oriNextIndex, rf.nextIndex[server])
				}
			}(server)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) commitTicker() {
	for rf.killed() == false {
		time.Sleep(CommitPeriod * time.Millisecond)

		rf.mu.Lock()
		if rf.lastApplied > rf.commitIndex {
			errorMsg := fmt.Sprintf("[S%v T%v Raft.commitTikcer] lastApplied > commitIndex\n",
				rf.me, rf.currentTerm)
			rf.mu.Unlock()
			panic(errorMsg)
		}

		lastLogIndex := rf.GetLastLogIndex()
		if rf.commitIndex > lastLogIndex {
			errorMsg := fmt.Sprintf("[S%v T%v Raft.commitTikcer] commitIndex > lastLogIndex: %v VS %v\n",
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
	rf.log = make([]LogEntry, 1)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.ResetInitialTime()
	rf.role = Follower
	rf.applyCh = applyCh
	rf.rpcIdx = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electTicker()
	go rf.appendTicker()
	go rf.commitTicker()

	return rf
}
