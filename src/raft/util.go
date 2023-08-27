package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	MinTimeout      = 250
	MaxTimeout      = 600
	HeartbeatPeriod = 100
)

func (rf *Raft) RandomTimeout() time.Duration {
	timeout := time.Duration(MinTimeout+rand.Intn(MaxTimeout-MinTimeout)) * time.Millisecond
	//	DPrintf("S%v: %v\n", rf.me, timeout)
	return timeout
}

func (rf *Raft) ResetElectionTimer() {
	rf.electionTimer.Reset(rf.RandomTimeout())
}

func (rf *Raft) DiscoverNewTerm(term int) {
	oriTerm := rf.currentTerm
	rf.currentTerm = term
	rf.votedFor = -1
	rf.ResetElectionTimer()
	oriRole := rf.role
	rf.role = Follower
	rf.voteNum = 0
	DPrintf("[S%v Raft.DiscoverNewTerm] role: %v -> Follower | term: %v -> %v (discover new term)\n",
		rf.me, oriRole, oriTerm, rf.currentTerm)
}

func (rf *Raft) StartElection() {
	oriTerm := rf.currentTerm
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.ResetElectionTimer()
	oriRole := rf.role
	rf.role = Candidate
	rf.voteNum = 1
	DPrintf("[S%v Raft.StartElection] role: %v -> Candidate | term: %v -> %v (start election)\n",
		rf.me, oriRole, oriTerm, rf.currentTerm)
}

func (rf *Raft) BecomeLeader() {
	oriRole := rf.role
	rf.role = Leader
	DPrintf("[S%v Raft.BecomeLeader] role: %v -> Leader | term: %v\n", rf.me, oriRole, rf.currentTerm)
}

func (rf *Raft) GetLastLogInfo() (int, int) {
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = rf.log[lastLogIndex].Term
	}
	return lastLogIndex, lastLogTerm
}

func (rf *Raft) UpToDate(index int, term int) bool {
	lastLogIndex, lastLogTerm := rf.GetLastLogInfo()
	if term != lastLogTerm {
		return term > lastLogTerm
	}
	return index >= lastLogIndex
}
