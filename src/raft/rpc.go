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

func (rf *Raft) sendRequestVotePre(server int) (RequestVoteArgs, RequestVoteReply, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Candidate {
		return RequestVoteArgs{}, RequestVoteReply{}, false
	}

	lastLogIndex, lastLogTerm := rf.GetLastLogInfo()
	args := RequestVoteArgs{
		RpcIdx:       rf.rpcIdx,
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	rf.rpcIdx++
	reply := RequestVoteReply{}

	rf.DPrintf("[S%v T%v Raft.sendRequestVotePre(T%v-%v)] Send RequestVote RPC to S%v\n",
		rf.me, rf.currentTerm, args.Term, args.RpcIdx, server)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok {
		rf.DPrintf("[S%v T%v Raft.sendRequestVotePro(T%v-%v)] Receive RequestVote ACK from S%v | voteGranted: %v\n",
			rf.me, rf.currentTerm, args.Term, args.RpcIdx, server, reply.VoteGranted)
	} else {
		rf.DPrintf("[S%v T%v Raft.sendRequestVotePro(T%v-%v)] Fail to receive RequestVote ACK from S%v\n",
			rf.me, rf.currentTerm, args.Term, args.RpcIdx, server)
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
	rf.DPrintf("[S%v T%v Raft.RequestVote(S%v-T%v-%v)] Receive RequestVote RPC from S%v\n",
		rf.me, rf.currentTerm, args.CandidateId, args.Term, args.RpcIdx, args.CandidateId)

	reply.VoteGranted = true
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		rf.DPrintf("[S%v T%v Raft.RequestVote(S%v-T%v-%v)] Refuse to vote for S%v (have ahead term: %v VS %v)\n",
			rf.me, rf.currentTerm, args.CandidateId, args.Term, args.RpcIdx, args.CandidateId, rf.currentTerm, args.Term)
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
		rf.DPrintf("[S%v T%v Raft.RequestVote(S%v-T%v-%v)] Refuse to vote for S%v (have already voted for S%v)\n",
			rf.me, rf.currentTerm, args.CandidateId, args.Term, args.RpcIdx, args.CandidateId, rf.votedFor)
		return
	}

	if !rf.UpToDate(args.LastLogIndex, args.LastLogTerm) {
		reply.VoteGranted = false
		lastLogIndex, lastLogTerm := rf.GetLastLogInfo()
		rf.DPrintf("[S%v T%v Raft.RequestVote(S%v-T%v-%v)] Refuse to vote for S%v "+
			"(have ahead (lastLogIndex,lastLogTerm): (%v,%v) VS (%v,%v))\n",
			rf.me, rf.currentTerm, args.CandidateId, args.Term, args.RpcIdx, args.CandidateId,
			lastLogIndex, lastLogTerm, args.LastLogIndex, args.LastLogTerm)
		return
	}

	rf.votedFor = args.CandidateId
	rf.persist()
	rf.ResetInitialTime()
	reply.VoteGranted = true
	rf.DPrintf("[S%v T%v Raft.RequestVote(S%v-T%v-%v)] Vote for S%v\n",
		rf.me, rf.currentTerm, args.CandidateId, args.Term, args.RpcIdx, args.CandidateId)
}

//	==============================
//	AppendEntries RPC
//	==============================
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

func (rf *Raft) sendAppendEntriesPre(server int) (AppendEntriesArgs, AppendEntriesReply, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		return AppendEntriesArgs{}, AppendEntriesReply{}, false
	}

	lastLogIndex := rf.GetLastLogIndex()
	nextIndex := rf.nextIndex[server]
	args := AppendEntriesArgs{
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
		args.Entries = rf.log[nextIndex : lastLogIndex+1]
	}
	reply := AppendEntriesReply{}
	rf.DPrintf("[S%v T%v Raft.sendAppendEntriesPre(T%v-%v)] Send AppendEntries RPC to S%v\n",
		rf.me, rf.currentTerm, args.Term, args.RpcIdx, server)
	return args, reply, true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntriesPro(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, ok bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok {
		rf.DPrintf("[S%v T%v Raft.sendAppendEntriesPro(T%v-%v)] Receive AppendEntries ACK from S%v "+
			"| status: %v | nextIndex: %v\n",
			rf.me, rf.currentTerm, args.Term, args.RpcIdx, server, reply.Status, reply.NextIndex)
	} else {
		rf.DPrintf("[S%v T%v Raft.sendAppendEntriesPro(T%v-%v)] Fail to receive AppendEntries ACK from S%v\n",
			rf.me, rf.currentTerm, args.Term, args.RpcIdx, server)
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

	if reply.Status == Success {
		oriMatchIndex := rf.matchIndex[server]
		oriNextIndex := rf.nextIndex[server]
		newMatchIndex := args.PrevLogIndex + len(args.Entries)
		if newMatchIndex <= oriMatchIndex {
			return
		}
		rf.matchIndex[server] = newMatchIndex
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		rf.DPrintf("[S%v T%v Raft.sendAppendEntriesPro(T%v-%v)] matchIndex[%v]: %v -> %v | nextIndex[%v]: %v -> %v\n",
			rf.me, rf.currentTerm, args.Term, args.RpcIdx, server, oriMatchIndex, rf.matchIndex[server],
			server, oriNextIndex, rf.nextIndex[server])
		
		newCommitIndex := rf.GetMajorityMatchIndex()
		term := rf.GetTerm(newCommitIndex)
		if term > rf.currentTerm {
			errorMsg := fmt.Sprintf("[S%v T%v Raft.sendAppendEntriesPro(T%v-%v)] log[%v].Term > currentTerm\n",
				rf.me, rf.currentTerm, args.Term, args.RpcIdx, newCommitIndex)
			panic(errorMsg)
		}
		if newCommitIndex > rf.commitIndex && term == rf.currentTerm {
			oriCommitIndex := rf.commitIndex
			rf.commitIndex = newCommitIndex
			rf.DPrintf("[S%v T%v Raft.sendAppendEntriesPro(T%v-%v)] commitIndex: %v -> %v\n",
				rf.me, rf.currentTerm, args.Term, args.RpcIdx, oriCommitIndex, rf.commitIndex)
		}
	} else if reply.Status == LogMatchFailure {
		oriNextIndex := rf.nextIndex[server]
		newNextIndex := reply.NextIndex
		if newNextIndex <= rf.matchIndex[server] {
			return
		}
		rf.nextIndex[server] = newNextIndex
		rf.DPrintf("[S%v T%v Raft.sendAppendEntriesPro(T%v-%v)] matchIndex[%v]: %v | nextIndex[%v]: %v -> %v\n",
			rf.me, rf.currentTerm, args.Term, args.RpcIdx, server, rf.matchIndex[server],
			server, oriNextIndex, rf.nextIndex[server])
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.DPrintf("[S%v T%v Raft.AppendEntries(S%v-T%v-%v)] Receive AppendEntries RPC from S%v\n",
		rf.me, rf.currentTerm, args.LeaderId, args.Term, args.RpcIdx, args.LeaderId)

	reply.Status = Success
	reply.Term = rf.currentTerm
	reply.NextIndex = args.PrevLogIndex

	if args.Term < rf.currentTerm {
		reply.Status = TermLagFailure
		rf.DPrintf("[S%v T%v Raft.AppendEntries(S%v-T%v-%v)] Refuse to append entries from S%v "+
			"(have ahead term: %v VS %v)\n",
			rf.me, rf.currentTerm, args.LeaderId, args.Term, args.RpcIdx, args.LeaderId, rf.currentTerm, args.Term)
		return
	}

	rf.BecomeFollower(args.Term)
	rf.ResetInitialTime()
	reply.Term = rf.currentTerm

	if !rf.MatchTerm(args.PrevLogIndex, args.PrevLogTerm) {
		reply.Status = LogMatchFailure
		rf.DPrintf("[S%v T%v Raft.AppendEntries(S%v-T%v-%v)] Refuse to append entries from S%v "+
			"(don't match term at I%v: %v VS %v)\n",
			rf.me, rf.currentTerm, args.LeaderId, args.Term, args.RpcIdx, args.LeaderId,
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

	appendIdx := args.PrevLogIndex + 1
	rf.log = append(rf.log[:appendIdx], args.Entries...)
	rf.persist()

	lastLogIndex := rf.GetLastLogIndex()
	if args.LeaderCommit > rf.commitIndex {
		oriCommitIndex := rf.commitIndex
		rf.commitIndex = Min(args.LeaderCommit, lastLogIndex)
		rf.DPrintf("[S%v T%v Raft.AppendEntries(S%v-T%v-%v)] commitIndex: %v -> %v\n",
			rf.me, rf.currentTerm, args.LeaderId, args.Term, args.RpcIdx, oriCommitIndex, rf.commitIndex)
	}

	if rf.commitIndex > lastLogIndex {
		errorMsg := fmt.Sprintf("[S%v T%v Raft.AppendEntries(S%v-T%v-%v)] commitIndex > lastLogIndex: %v VS %v\n",
			rf.me, rf.currentTerm, args.LeaderId, args.Term, args.RpcIdx, rf.commitIndex, lastLogIndex)
		panic(errorMsg)
	}
}
