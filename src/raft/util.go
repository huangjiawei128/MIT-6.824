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

func (rf *Raft) DPrintf(format string, a ...interface{}) (n int, err error) {
	if !rf.killed() {
		DPrintf(format, a...)
	}
	return
}

const (
	MinTimeout      = 250
	MaxTimeout      = 500
	HeartbeatPeriod = 100
)

func (rf *Raft) RandomTimeout() time.Duration {
	timeout := time.Duration(MinTimeout+rand.Intn(MaxTimeout-MinTimeout)) * time.Millisecond
	return timeout
}

func (rf *Raft) ResetInitialTime() {
	rf.initialTime = time.Now()
}

func (rf *Raft) DiscoverNewTerm(term int) {
	oriTerm := rf.currentTerm
	rf.currentTerm = term
	rf.votedFor = -1
	rf.ResetInitialTime()
	oriRole := rf.role
	rf.role = Follower
	rf.voteNum = 0
	rf.DPrintf("[S%v T%v->T%v Raft.DiscoverNewTerm] role: %v -> Follower (discover new term)\n",
		rf.me, oriTerm, rf.currentTerm, oriRole)
}

func (rf *Raft) StartElection() {
	oriTerm := rf.currentTerm
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.ResetInitialTime()
	oriRole := rf.role
	rf.role = Candidate
	rf.voteNum = 1
	rf.DPrintf("[S%v T%v->T%v Raft.StartElection] role: %v -> Candidate (start election)\n",
		rf.me, oriTerm, rf.currentTerm, oriRole)
}

func (rf *Raft) BecomeLeader() {
	oriRole := rf.role
	rf.role = Leader
	rf.DPrintf("[S%v T%v Raft.BecomeLeader] role: %v -> Leader\n",
		rf.me, rf.currentTerm, oriRole)
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
