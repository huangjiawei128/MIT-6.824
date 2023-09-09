package raft

import (
	"fmt"
	"log"
	"math/rand"
	"sort"
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
	MinTimeout   = 250
	MaxTimeout   = 500
	AppendPeriod = 100
	ApplyPeriod  = 10
)

func Max(x1 int, x2 int) int {
	if x1 > x2 {
		return x1
	}
	return x2
}

func Min(x1 int, x2 int) int {
	if x1 < x2 {
		return x1
	}
	return x2
}

func RandomTimeout() time.Duration {
	return time.Duration(MinTimeout+rand.Intn(MaxTimeout-MinTimeout)) * time.Millisecond
}

func Command2Str(command interface{}) string {
	ret := fmt.Sprintf("%v", command)
	if len(ret) > 10 {
		ret = ret[0:10] + "......"
	}
	return ret
}

func (rf *Raft) ResetInitialTime() {
	rf.initialTime = time.Now()
}

func (rf *Raft) BecomeFollower(term int) {
	oriTerm := rf.currentTerm
	oriRole := rf.role
	if term < oriTerm {
		errorMsg := fmt.Sprintf("[S%v T%v->T%v Raft.BecomeFollower] Term can't decrease\n",
			rf.me, oriTerm, term)
		panic(errorMsg)
	}
	if term > oriTerm {
		rf.currentTerm = term
		rf.votedFor = -1
		rf.persist()
	}
	rf.role = Follower
	rf.voteNum = 0
	rf.DPrintf("[S%v T%v->T%v Raft.BecomeFollower] role: %v -> Follower\n",
		rf.me, oriTerm, rf.currentTerm, oriRole)
}

func (rf *Raft) BecomeCandidate() {
	oriTerm := rf.currentTerm
	oriRole := rf.role
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	rf.role = Candidate
	rf.voteNum = 1
	rf.DPrintf("[S%v T%v->T%v Raft.BecomeCandidate] role: %v -> Candidate\n",
		rf.me, oriTerm, rf.currentTerm, oriRole)
}

func (rf *Raft) BecomeLeader() {
	oriRole := rf.role
	rf.role = Leader
	initialNextIndex := rf.GetLastLogIndex() + 1
	for server := range rf.peers {
		rf.nextIndex[server] = initialNextIndex
		rf.matchIndex[server] = 0
	}
	rf.DPrintf("[S%v T%v Raft.BecomeLeader] role: %v -> Leader\n",
		rf.me, rf.currentTerm, oriRole)
}

func (rf *Raft) GetLastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) GetLastLogInfo() (int, int) {
	lastLogIndex := rf.GetLastLogIndex()
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = rf.log[lastLogIndex].Term
	}
	return lastLogIndex, lastLogTerm
}

func (rf *Raft) GetTerm(index int) int {
	if index > rf.GetLastLogIndex() {
		return -1
	}
	return rf.log[index].Term
}

func (rf *Raft) GetMajorityMatchIndex() int {
	matchIndexCopy := make([]int, len(rf.peers))
	copy(matchIndexCopy, rf.matchIndex)
	matchIndexCopy[rf.me] = 0
	sort.Ints(matchIndexCopy)
	return matchIndexCopy[len(rf.peers)>>1+1]
}

func (rf *Raft) UpToDate(index int, term int) bool {
	lastLogIndex, lastLogTerm := rf.GetLastLogInfo()
	if term != lastLogTerm {
		return term > lastLogTerm
	}
	return index >= lastLogIndex
}

func (rf *Raft) MatchTerm(index int, term int) bool {
	term_ := rf.GetTerm(index)
	return term_ == term
}
