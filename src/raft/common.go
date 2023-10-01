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

const ApplyDebug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func ApplyDPrintf(format string, a ...interface{}) (n int, err error) {
	if ApplyDebug {
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

func (rf *Raft) ApplyDPrintf(format string, a ...interface{}) (n int, err error) {
	if !rf.killed() {
		ApplyDPrintf(format, a...)
	}
	return
}

func (rf *Raft) BasicInfo(methodName string) string {
	if methodName == "" {
		return fmt.Sprintf("R%v Raft", rf.me)
	}
	return fmt.Sprintf("R%v Raft.%v", rf.me, methodName)
}

func (rf *Raft) BasicInfoWithTerm(methodName string) string {
	if methodName == "" {
		return fmt.Sprintf("R%v T%v Raft", rf.me, rf.currentTerm)
	}
	return fmt.Sprintf("R%v T%v Raft.%v", rf.me, rf.currentTerm, methodName)
}

func (rf *Raft) BasicInfoWithTermChange(methodName string, oriTerm int) string {
	if methodName == "" {
		return fmt.Sprintf("R%v T%v->T%v Raft", rf.me, oriTerm, rf.currentTerm)
	}
	return fmt.Sprintf("R%v T%v->T%v Raft.%v", rf.me, oriTerm, rf.currentTerm, methodName)
}

const (
	MinTimeout   = 250
	MaxTimeout   = 400
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
	maxLen := 1500
	if len(ret) > maxLen {
		ret = ret[0:maxLen] + "..."
	}
	return ret
}

func (rf *Raft) ResetInitialTime() {
	rf.initialTime = time.Now()
}

func (rf *Raft) BecomeFollower(term int) {
	oriTerm := rf.currentTerm
	oriRole := rf.role
	rf.currentTerm = term
	basicInfo := rf.BasicInfoWithTermChange("BecomeFollower", oriTerm)

	if rf.currentTerm < oriTerm {
		errorMsg := fmt.Sprintf("[%v] Term can't decrease\n", basicInfo)
		panic(errorMsg)
	}
	if rf.currentTerm > oriTerm {
		rf.votedFor = -1
		rf.persist()
	}
	rf.role = Follower
	rf.voteNum = 0
	rf.DPrintf("[%v] role: %v -> Follower\n",
		basicInfo, oriRole)
}

func (rf *Raft) BecomeCandidate() {
	oriTerm := rf.currentTerm
	oriRole := rf.role
	rf.currentTerm++
	basicInfo := rf.BasicInfoWithTermChange("BecomeCandidate", oriTerm)

	rf.votedFor = rf.me
	rf.persist()
	rf.role = Candidate
	rf.voteNum = 1
	rf.DPrintf("[%v] role: %v -> Candidate\n",
		basicInfo, oriRole)
}

func (rf *Raft) BecomeLeader() {
	basicInfo := rf.BasicInfoWithTerm("BecomeLeader")

	oriRole := rf.role
	rf.role = Leader
	initialNextIndex := rf.GetLastLogIndex() + 1
	for server := range rf.peers {
		rf.nextIndex[server] = initialNextIndex
		rf.matchIndex[server] = 0
	}
	rf.DPrintf("[%v] role: %v -> Leader | commitIndex: %v\n",
		basicInfo, oriRole, rf.commitIndex)
}

func (rf *Raft) GetLastLogIndex() int {
	return len(rf.log) - 1 + rf.lastIncludedIndex
}

func (rf *Raft) GetLastLogInfo() (int, int) {
	lastLogIndex := rf.GetLastLogIndex()
	lastLogTerm := 0
	if lastLogIndex <= rf.lastIncludedIndex {
		lastLogTerm = rf.lastIncludedTerm
	} else {
		lastLogTerm = rf.log[lastLogIndex-rf.lastIncludedIndex].Term
	}
	return lastLogIndex, lastLogTerm
}

func (rf *Raft) GetCommand(index int) interface{} {
	if index > rf.GetLastLogIndex() || index <= rf.lastIncludedIndex {
		return nil
	}
	return rf.log[index-rf.lastIncludedIndex].Command
}

func (rf *Raft) GetTerm(index int) int {
	if index > rf.GetLastLogIndex() {
		return -1
	}
	if index <= rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}
	return rf.log[index-rf.lastIncludedIndex].Term
}

func (rf *Raft) GetLeftSubLog(rightIndex int) []LogEntry {
	return rf.log[:rightIndex-rf.lastIncludedIndex]
}

func (rf *Raft) GetRightSubLog(leftIndex int) []LogEntry {
	return rf.log[leftIndex-rf.lastIncludedIndex:]
}

func (rf *Raft) GetSubLog(leftIndex int, rightIndex int) []LogEntry {
	return rf.log[leftIndex-rf.lastIncludedIndex : rightIndex-rf.lastIncludedIndex]
}

func (rf *Raft) GetMajorityMatchIndex() int {
	matchIndexCopy := make([]int, len(rf.peers))
	copy(matchIndexCopy, rf.matchIndex)
	matchIndexCopy[rf.me] = rf.GetLastLogIndex()
	sort.Ints(matchIndexCopy)
	return matchIndexCopy[len(rf.peers)>>1]
}

func (rf *Raft) GetPersister() *Persister {
	return rf.persister
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
