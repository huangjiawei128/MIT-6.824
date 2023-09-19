package raft

import "fmt"

//	==============================
//	RequestVote RPC
//	==============================
//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	RpcId        int
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

func (rf *Raft) sendRequestVotePre(server int) (RequestVoteArgs, RequestVoteReply, bool) {
	if rf.role != Candidate {
		return RequestVoteArgs{}, RequestVoteReply{}, false
	}

	lastLogIndex, lastLogTerm := rf.GetLastLogInfo()
	args := RequestVoteArgs{
		RpcId:        rf.nextRpcId,
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	rf.nextRpcId++
	reply := RequestVoteReply{}

	rf.DPrintf("[R%v T%v Raft.sendRequestVotePre(T%v-%v)] Send RequestVote RPC to R%v\n",
		rf.me, rf.currentTerm, args.Term, args.RpcId, server)
	return args, reply, true
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
// handler function on the server side does not return.  Thus, there
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
	return ok
}

func (rf *Raft) sendRequestVotePro(server int, args *RequestVoteArgs, reply *RequestVoteReply, ok bool) {
	if ok {
		rf.DPrintf("[R%v T%v Raft.sendRequestVotePro(T%v-%v)] Receive RequestVote ACK from R%v | voteGranted: %v\n",
			rf.me, rf.currentTerm, args.Term, args.RpcId, server, reply.VoteGranted)
	} else {
		rf.DPrintf("[R%v T%v Raft.sendRequestVotePro(T%v-%v)] Fail to receive RequestVote ACK from R%v\n",
			rf.me, rf.currentTerm, args.Term, args.RpcId, server)
		return
	}

	if rf.role != Candidate || rf.currentTerm != args.Term {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.BecomeFollower(reply.Term)
		rf.ResetInitialTime()
		return
	}

	if reply.VoteGranted {
		rf.voteNum++
		if rf.voteNum > len(rf.peers)>>1 {
			rf.BecomeLeader()
		}
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.DPrintf("[R%v T%v Raft.RequestVote(R%v-T%v-%v)] Receive RequestVote RPC from R%v\n",
		rf.me, rf.currentTerm, args.CandidateId, args.Term, args.RpcId, args.CandidateId)

	reply.VoteGranted = true
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		rf.DPrintf("[R%v T%v Raft.RequestVote(R%v-T%v-%v)] Refuse to vote for R%v (have ahead term: %v > %v)\n",
			rf.me, rf.currentTerm, args.CandidateId, args.Term, args.RpcId, args.CandidateId, rf.currentTerm, args.Term)
		return
	}

	if args.Term > rf.currentTerm {
		oriRole := rf.role
		rf.BecomeFollower(args.Term)
		if oriRole != Follower {
			//	rf.ResetInitialTime()
		}
		reply.Term = rf.currentTerm
	}

	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.VoteGranted = false
		rf.DPrintf("[R%v T%v Raft.RequestVote(R%v-T%v-%v)] Refuse to vote for R%v (have already voted for R%v)\n",
			rf.me, rf.currentTerm, args.CandidateId, args.Term, args.RpcId, args.CandidateId, rf.votedFor)
		return
	}

	if !rf.UpToDate(args.LastLogIndex, args.LastLogTerm) {
		reply.VoteGranted = false
		lastLogIndex, lastLogTerm := rf.GetLastLogInfo()
		rf.DPrintf("[R%v T%v Raft.RequestVote(R%v-T%v-%v)] Refuse to vote for R%v "+
			"(have ahead (lastLogIndex,lastLogTerm): (%v,%v) VS (%v,%v))\n",
			rf.me, rf.currentTerm, args.CandidateId, args.Term, args.RpcId, args.CandidateId,
			lastLogIndex, lastLogTerm, args.LastLogIndex, args.LastLogTerm)
		return
	}

	rf.votedFor = args.CandidateId
	rf.persist()
	rf.ResetInitialTime()
	reply.VoteGranted = true
	rf.DPrintf("[R%v T%v Raft.RequestVote(R%v-T%v-%v)] Vote for R%v\n",
		rf.me, rf.currentTerm, args.CandidateId, args.Term, args.RpcId, args.CandidateId)
}

//	==============================
//	AppendEntries RPC
//	==============================
type AppendStatus int

const (
	Success AppendStatus = iota
	TermLag
	LogLag
	EntrySnapshot
	EntryMissmatch
)

func (appendStatus AppendStatus) String() string {
	var ret string
	switch appendStatus {
	case Success:
		ret = "Success"
	case TermLag:
		ret = "TermLag"
	case LogLag:
		ret = "LogLag"
	case EntrySnapshot:
		ret = "EntrySnapshot"
	case EntryMissmatch:
		ret = "EntryMissmatch"
	}
	return ret
}

type AppendEntriesArgs struct {
	RpcId        int
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

func (rf *Raft) sendAppendEntriesPre(server int) (AppendEntriesArgs, AppendEntriesReply, bool) {
	if rf.role != Leader {
		return AppendEntriesArgs{}, AppendEntriesReply{}, false
	}

	lastLogIndex := rf.GetLastLogIndex()
	nextIndex := rf.nextIndex[server]
	args := AppendEntriesArgs{
		RpcId:        rf.nextRpcId,
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: nextIndex - 1,
		PrevLogTerm:  rf.GetTerm(nextIndex - 1),
		LeaderCommit: rf.commitIndex,
		Entries:      []LogEntry{},
	}
	rf.nextRpcId++
	if lastLogIndex >= rf.nextIndex[server] {
		args.Entries = rf.GetSubLog(nextIndex, lastLogIndex+1)
	}
	reply := AppendEntriesReply{}
	rf.DPrintf("[R%v T%v Raft.sendAppendEntriesPre(T%v-%v)] Send AppendEntries RPC to R%v\n",
		rf.me, rf.currentTerm, args.Term, args.RpcId, server)
	return args, reply, true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntriesPro(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, ok bool) {
	if ok {
		rf.DPrintf("[R%v T%v Raft.sendAppendEntriesPro(T%v-%v)] Receive AppendEntries ACK from R%v "+
			"| status: %v | nextIndex: %v\n",
			rf.me, rf.currentTerm, args.Term, args.RpcId, server, reply.Status, reply.NextIndex)
	} else {
		rf.DPrintf("[R%v T%v Raft.sendAppendEntriesPro(T%v-%v)] Fail to receive AppendEntries ACK from R%v\n",
			rf.me, rf.currentTerm, args.Term, args.RpcId, server)
		return
	}

	if rf.role != Leader || rf.currentTerm != args.Term {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.BecomeFollower(reply.Term)
		//	rf.ResetInitialTime()
		return
	}

	if reply.Status == EntrySnapshot || reply.Status == EntryMissmatch {
		oriNextIndex := rf.nextIndex[server]
		if reply.NextIndex <= rf.matchIndex[server] {
			return
		}
		rf.nextIndex[server] = reply.NextIndex
		rf.DPrintf("[R%v T%v Raft.sendAppendEntriesPro(T%v-%v)] matchIndex[%v]: %v | nextIndex[%v]: %v -> %v\n",
			rf.me, rf.currentTerm, args.Term, args.RpcId, server, rf.matchIndex[server],
			server, oriNextIndex, rf.nextIndex[server])
		return
	}

	if reply.Status == Success {
		oriMatchIndex := rf.matchIndex[server]
		oriNextIndex := rf.nextIndex[server]
		newMatchIndex := reply.NextIndex - 1
		if newMatchIndex <= oriMatchIndex {
			return
		}
		rf.matchIndex[server] = newMatchIndex
		rf.nextIndex[server] = reply.NextIndex
		rf.DPrintf("[R%v T%v Raft.sendAppendEntriesPro(T%v-%v)] matchIndex[%v]: %v -> %v | nextIndex[%v]: %v -> %v\n",
			rf.me, rf.currentTerm, args.Term, args.RpcId, server, oriMatchIndex, rf.matchIndex[server],
			server, oriNextIndex, rf.nextIndex[server])

		newCommitIndex := rf.GetMajorityMatchIndex()
		term := rf.GetTerm(newCommitIndex)
		if term > rf.currentTerm {
			errorMsg := fmt.Sprintf("[R%v T%v Raft.sendAppendEntriesPro(T%v-%v)] log[%v].Term > currentTerm\n",
				rf.me, rf.currentTerm, args.Term, args.RpcId, newCommitIndex)
			panic(errorMsg)
		}
		if newCommitIndex > rf.commitIndex && term == rf.currentTerm {
			oriCommitIndex := rf.commitIndex
			rf.commitIndex = newCommitIndex
			rf.DPrintf("[R%v T%v Raft.sendAppendEntriesPro(T%v-%v)] commitIndex: %v -> %v\n",
				rf.me, rf.currentTerm, args.Term, args.RpcId, oriCommitIndex, rf.commitIndex)
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.DPrintf("[R%v T%v Raft.AppendEntries(R%v-T%v-%v)] Receive AppendEntries RPC from R%v\n",
		rf.me, rf.currentTerm, args.LeaderId, args.Term, args.RpcId, args.LeaderId)

	appendIndex := args.PrevLogIndex + 1
	reply.Status = Success
	reply.Term = rf.currentTerm
	reply.NextIndex = appendIndex

	if args.Term < rf.currentTerm {
		reply.Status = TermLag
		rf.DPrintf("[R%v T%v Raft.AppendEntries(R%v-T%v-%v)] Refuse to append entries from R%v "+
			"(have ahead term: %v > %v)\n",
			rf.me, rf.currentTerm, args.LeaderId, args.Term, args.RpcId, args.LeaderId, rf.currentTerm, args.Term)
		return
	}

	rf.BecomeFollower(args.Term)
	rf.ResetInitialTime()
	reply.Term = rf.currentTerm

	if appendIndex <= rf.lastIncludedIndex {
		reply.Status = EntrySnapshot
		reply.NextIndex = rf.lastIncludedIndex + 1
		rf.DPrintf("[R%v T%v Raft.AppendEntries(R%v-T%v-%v)] Refuse to append entries from R%v "+
			"(lastIncludedIndex: %v >= appendIndex: %v)\n",
			rf.me, rf.currentTerm, args.LeaderId, args.Term, args.RpcId, args.LeaderId,
			rf.lastIncludedIndex, appendIndex)
		return
	}

	if !rf.MatchTerm(args.PrevLogIndex, args.PrevLogTerm) {
		reply.Status = EntryMissmatch
		rf.DPrintf("[R%v T%v Raft.AppendEntries(R%v-T%v-%v)] Refuse to append entries from R%v "+
			"(don't match term at I%v: %v VS %v)\n",
			rf.me, rf.currentTerm, args.LeaderId, args.Term, args.RpcId, args.LeaderId,
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

		rf.log = rf.GetLeftSubLog(Min(lastLogIndex+1, Max(args.PrevLogIndex, rf.lastIncludedIndex+1)))
		rf.persist()
		return
	}

	oriLastLogIndex := rf.GetLastLogIndex()
	reply.NextIndex = appendIndex + len(args.Entries)
	if reply.NextIndex <= rf.GetLastLogIndex() {
		rf.log = append(append(rf.GetLeftSubLog(appendIndex), args.Entries...), rf.GetRightSubLog(reply.NextIndex)...)
	} else {
		rf.log = append(rf.GetLeftSubLog(appendIndex), args.Entries...)
	}
	rf.persist()
	rf.DPrintf("[R%v T%v Raft.AppendEntries(R%v-T%v-%v)] lastLogIndex: %v -> %v | appendIndex: %v | nextIndex: %v\n",
		rf.me, rf.currentTerm, args.LeaderId, args.Term, args.RpcId, oriLastLogIndex, rf.GetLastLogIndex(),
		appendIndex, reply.NextIndex)

	lastLogIndex := rf.GetLastLogIndex()
	if args.LeaderCommit > rf.commitIndex {
		oriCommitIndex := rf.commitIndex
		rf.commitIndex = Min(args.LeaderCommit, lastLogIndex)
		rf.DPrintf("[R%v T%v Raft.AppendEntries(R%v-T%v-%v)] commitIndex: %v -> %v\n",
			rf.me, rf.currentTerm, args.LeaderId, args.Term, args.RpcId, oriCommitIndex, rf.commitIndex)
	}

	if rf.commitIndex > lastLogIndex {
		errorMsg := fmt.Sprintf("[R%v T%v Raft.AppendEntries(R%v-T%v-%v)] commitIndex > lastLogIndex: %v VS %v\n",
			rf.me, rf.currentTerm, args.LeaderId, args.Term, args.RpcId, rf.commitIndex, lastLogIndex)
		panic(errorMsg)
	}
}

//	==============================
//	InstallSnapshot RPC
//	==============================
type InstallSnapshotArgs struct {
	RpcId             int
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term              int
	LastIncludedIndex int
}

func (rf *Raft) sendInstallSnapshotPre(server int) (InstallSnapshotArgs, InstallSnapshotReply, bool) {
	if rf.role != Leader {
		return InstallSnapshotArgs{}, InstallSnapshotReply{}, false
	}

	args := InstallSnapshotArgs{
		RpcId:             rf.nextRpcId,
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	rf.nextRpcId++
	reply := InstallSnapshotReply{}
	rf.DPrintf("[R%v T%v Raft.sendInstallSnapshotPre(T%v-%v)] Send InstallSnapshot RPC to R%v\n",
		rf.me, rf.currentTerm, args.Term, args.RpcId, server)
	return args, reply, true
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshotPro(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply, ok bool) {
	if ok {
		rf.DPrintf("[R%v T%v Raft.sendInstallSnapshotPro(T%v-%v)] Receive InstallSnapshot ACK from R%v\n",
			rf.me, rf.currentTerm, args.Term, args.RpcId, server)
	} else {
		rf.DPrintf("[R%v T%v Raft.sendInstallSnapshotPro(T%v-%v)] Fail to receive InstallSnapshot ACK from R%v\n",
			rf.me, rf.currentTerm, args.Term, args.RpcId, server)
		return
	}

	if rf.role != Leader || rf.currentTerm != args.Term {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.BecomeFollower(reply.Term)
		//	rf.ResetInitialTime()
		return
	}

	oriMatchIndex := rf.matchIndex[server]
	oriNextIndex := rf.nextIndex[server]
	rf.nextIndex[server] = reply.LastIncludedIndex + 1
	rf.matchIndex[server] = reply.LastIncludedIndex
	rf.DPrintf("[R%v T%v Raft.sendInstallSnapshotPro(T%v-%v)] matchIndex[%v]: %v -> %v | nextIndex[%v]: %v -> %v\n",
		rf.me, rf.currentTerm, args.Term, args.RpcId, server, oriMatchIndex, rf.matchIndex[server],
		server, oriNextIndex, rf.nextIndex[server])
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	rf.DPrintf("[R%v T%v Raft.InstallSnapshot(R%v-T%v-%v)] Receive InstallSnapshot RPC from R%v\n",
		rf.me, rf.currentTerm, args.LeaderId, args.Term, args.RpcId, args.LeaderId)

	reply.Term = rf.currentTerm
	reply.LastIncludedIndex = rf.lastIncludedIndex

	if args.Term < rf.currentTerm {
		rf.DPrintf("[R%v T%v Raft.InstallSnapshot(R%v-T%v-%v)] Refuse to install snapshot from R%v "+
			"(have ahead term: %v > %v)\n",
			rf.me, rf.currentTerm, args.LeaderId, args.Term, args.RpcId, args.LeaderId, rf.currentTerm, args.Term)
		rf.mu.Unlock()
		return
	}

	rf.BecomeFollower(args.Term)
	rf.ResetInitialTime()
	reply.Term = rf.currentTerm

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		rf.DPrintf("[R%v T%v Raft.InstallSnapshot(R%v-T%v-%v)] Refuse to install snapshot from R%v "+
			"(have ahead lastIncludedIndex: %v > %v)\n",
			rf.me, rf.currentTerm, args.LeaderId, args.Term, args.RpcId, args.LeaderId,
			rf.lastIncludedIndex, args.LastIncludedIndex)
		rf.mu.Unlock()
		return
	}

	newLog := make([]LogEntry, 1)
	if rf.GetLastLogIndex() > args.LastIncludedIndex {
		newLog = append(newLog, rf.GetRightSubLog(args.LastIncludedIndex+1)...)
	}
	rf.lastIncludedIndex, rf.lastIncludedTerm = args.LastIncludedIndex, args.LastIncludedTerm
	rf.log = newLog
	rf.persister.SaveStateAndSnapshot(rf.stateData(), args.Data)
	reply.LastIncludedIndex = rf.lastIncludedIndex

	if rf.lastApplied >= rf.lastIncludedIndex {
		rf.mu.Unlock()
		return
	}
	oriLastApplied := rf.lastApplied
	oriCommitIndex := rf.commitIndex
	rf.lastApplied = rf.lastIncludedIndex
	rf.commitIndex = Max(rf.commitIndex, rf.lastIncludedIndex)

	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  rf.lastIncludedTerm,
		SnapshotIndex: rf.lastIncludedIndex,
	}
	rf.ApplyDPrintf("[R%v T%v Raft.InstallSnapshot] Prepare to apply the snapshot | index: %v | term: %v | "+
		"lastApplied: %v -> %v | commitIndex: %v -> %v\n",
		rf.me, rf.currentTerm, applyMsg.SnapshotIndex, applyMsg.SnapshotTerm, oriLastApplied, rf.lastApplied,
		oriCommitIndex, rf.commitIndex)
	rf.mu.Unlock()

	rf.applyCh <- applyMsg
}
