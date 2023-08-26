package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	MinTimeout      = 250
	MaxTimeout      = 400
	HeartbeatPeriod = 100
)

func (rf *Raft) ResetTimeout() {
	rand.Seed(time.Now().Unix())
	rf.timeout = time.Duration(MinTimeout+rand.Intn(MaxTimeout-MinTimeout)) * time.Millisecond
}

func (rf *Raft) ResetInitialTime() {
	rf.initialTime = time.Now()
}

func (rf *Raft) DiscoverNewTerm(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
	rf.ResetTimeout()
	rf.ResetInitialTime()
	oriRole := rf.role
	rf.role = Follower
	rf.voteNum = 0
	DPrintf("[Server %v] %v -> Follower, term: %v (discover new term)\n", rf.me, oriRole, rf.currentTerm)
}

func (rf *Raft) StartElection() {
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.ResetTimeout()
	rf.ResetInitialTime()
	oriRole := rf.role
	rf.role = Candidate
	rf.voteNum = 0
	DPrintf("[Server %v] %v -> Candidate, term: %v (start election)\n", rf.me, oriRole, rf.currentTerm)
}

func (rf *Raft) BecomeLeader() {
	oriRole := rf.role
	rf.role = Leader
	DPrintf("[Server %v] %v -> Leader, term: %v\n", rf.me, oriRole, rf.currentTerm)
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

func (rf *Raft) TimeoutElapses() bool {
	return time.Now().Sub(rf.initialTime) > rf.timeout
}
