package shardctrler

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead int32 // set by Kill()

	configs               []Config // indexed by config num
	clientId2executedOpId map[Int64Id]int
	index2processedOpCh   map[int]chan Op
}

type OpType int

const (
	JoinRG = iota
	LeaveRG
	MoveSD
	QueryCF
)

func (opType OpType) String() string {
	var ret string
	switch opType {
	case JoinRG:
		ret = "Join"
	case LeaveRG:
		ret = "Leave"
	case MoveSD:
		ret = "Move"
	case QueryCF:
		ret = "Query"
	}
	return ret
}

type Op struct {
	// Your data here.
	Id        int
	ClientId  Int64Id
	Type      OpType
	Servers   map[int][]string
	GIDs      []int
	Shard     int
	ConfigNum int
	Config    *Config
}

func (op Op) String() string {
	ret := ""
	ret = fmt.Sprintf("Id: %v | ClientId: %v | Type: %v | ",
		op.Id, op.ClientId, op.Type)
	switch op.Type {
	case QueryCF:
		ret += fmt.Sprintf("ConfigNum: %v | Config: %v", op.ConfigNum, op.Config)
	case JoinRG:
		ret += fmt.Sprintf("Servers: %v", op.Servers)
	case LeaveRG:
		ret += fmt.Sprintf("GIDs: %v", op.GIDs)
	case MoveSD:
		ret += fmt.Sprintf("Shard: %v | GID: %v", op.Shard, op.GIDs[0])
	}
	return ret
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	basicInfo := sc.BasicInfo("Join")

	// Your code here.
	opType := OpType(JoinRG)
	startOp := Op{
		Id:       args.OpId,
		ClientId: args.ClientId,
		Type:     opType,
		Servers:  args.Servers,
	}
	index, _, isLeader := sc.rf.Start(startOp)
	if !isLeader {
		reply.Err = ErrWrongLeader
		sc.DPrintf("[%v(C%v-%v)] Refuse to %v RG(servers: %v) for C%v (isn't the leader)\n",
			basicInfo, args.ClientId, args.OpId, opType, args.Servers, args.ClientId)
		return
	}
	sc.mu.Lock()
	ch := sc.GetProcessedOpCh(index)
	sc.mu.Unlock()

	timer := time.NewTimer(RpcTimeout * time.Millisecond)
	select {
	case executedOp := <-ch:
		if executedOp.ClientId != startOp.ClientId || executedOp.Id != startOp.Id {
			reply.Err = ErrWrongLeader
			sc.DPrintf("[%v(C%v-%v)] Refuse to %v RG(servers: %v) for C%v "+
				"(don't match op identifier at I%v: (%v,%v) VS (%v,%v))\n",
				basicInfo, args.ClientId, args.OpId, opType, args.Servers, args.ClientId,
				index, executedOp.ClientId, executedOp.Id, startOp.ClientId, startOp.Id)
		} else {
			reply.Err = OK
			sc.DPrintf("[%v(C%v-%v)] %v RG(servers: %v) for C%v\n",
				basicInfo, args.ClientId, args.OpId, opType, args.Servers, args.ClientId)
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
		sc.DPrintf("[%v(C%v-%v)] Refuse to %v RG(servers: %v) for C%v (rpc timeout)\n",
			basicInfo, args.ClientId, args.OpId, opType, args.Servers, args.ClientId)
	}
	timer.Stop()

	sc.mu.Lock()
	sc.DeleteProcessedOpCh(index)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	basicInfo := sc.BasicInfo("Leave")

	// Your code here.
	opType := OpType(LeaveRG)
	startOp := Op{
		Id:       args.OpId,
		ClientId: args.ClientId,
		Type:     opType,
		GIDs:     args.GIDs,
	}
	index, _, isLeader := sc.rf.Start(startOp)
	if !isLeader {
		reply.Err = ErrWrongLeader
		sc.DPrintf("[%v(C%v-%v)] Refuse to %v RG(gids: %v) for C%v (isn't the leader)\n",
			basicInfo, args.ClientId, args.OpId, opType, args.GIDs, args.ClientId)
		return
	}
	sc.mu.Lock()
	ch := sc.GetProcessedOpCh(index)
	sc.mu.Unlock()

	timer := time.NewTimer(RpcTimeout * time.Millisecond)
	select {
	case executedOp := <-ch:
		if executedOp.ClientId != startOp.ClientId || executedOp.Id != startOp.Id {
			reply.Err = ErrWrongLeader
			sc.DPrintf("[%v(C%v-%v)] Refuse to %v RG(gids: %v) for C%v "+
				"(don't match op identifier at I%v: (%v,%v) VS (%v,%v))\n",
				basicInfo, args.ClientId, args.OpId, opType, args.GIDs, args.ClientId,
				index, executedOp.ClientId, executedOp.Id, startOp.ClientId, startOp.Id)
		} else {
			reply.Err = OK
			sc.DPrintf("[%v(C%v-%v)] %v RG(gids: %v) for C%v\n",
				basicInfo, args.ClientId, args.OpId, opType, args.GIDs, args.ClientId)
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
		sc.DPrintf("[%v(C%v-%v)] Refuse to %v RG(gids: %v) for C%v (rpc timeout)\n",
			basicInfo, args.ClientId, args.OpId, opType, args.GIDs, args.ClientId)
	}
	timer.Stop()

	sc.mu.Lock()
	sc.DeleteProcessedOpCh(index)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	basicInfo := sc.BasicInfo("Move")

	// Your code here.
	opType := OpType(MoveSD)
	startOp := Op{
		Id:       args.OpId,
		ClientId: args.ClientId,
		Type:     opType,
		Shard:    args.Shard,
		GIDs:     make([]int, 1),
	}
	startOp.GIDs[0] = args.GID
	index, _, isLeader := sc.rf.Start(startOp)
	if !isLeader {
		reply.Err = ErrWrongLeader
		sc.DPrintf("[%v(C%v-%v)] Refuse to %v SD%v to RG%v for C%v (isn't the leader)\n",
			basicInfo, args.ClientId, args.OpId, opType, args.Shard, args.GID, args.ClientId)
		return
	}
	sc.mu.Lock()
	ch := sc.GetProcessedOpCh(index)
	sc.mu.Unlock()

	timer := time.NewTimer(RpcTimeout * time.Millisecond)
	select {
	case executedOp := <-ch:
		if executedOp.ClientId != startOp.ClientId || executedOp.Id != startOp.Id {
			reply.Err = ErrWrongLeader
			sc.DPrintf("[%v(C%v-%v)] Refuse to %v SD%v to RG%v for C%v "+
				"(don't match op identifier at I%v: (%v,%v) VS (%v,%v))\n",
				basicInfo, args.ClientId, args.OpId, opType, args.Shard, args.GID, args.ClientId,
				index, executedOp.ClientId, executedOp.Id, startOp.ClientId, startOp.Id)
		} else {
			reply.Err = OK
			sc.DPrintf("[%v(C%v-%v)] %v SD%v to RG%v for C%v\n",
				basicInfo, args.ClientId, args.OpId, opType, args.Shard, args.GID, args.ClientId)
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
		sc.DPrintf("[%v(C%v-%v)] Refuse to %v SD%v to RG%v for C%v (rpc timeout)\n",
			basicInfo, args.ClientId, args.OpId, opType, args.Shard, args.GID, args.ClientId)
	}
	timer.Stop()

	sc.mu.Lock()
	sc.DeleteProcessedOpCh(index)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	basicInfo := sc.BasicInfo("Query")

	// Your code here.
	opType := OpType(QueryCF)
	startOp := Op{
		Id:        args.OpId,
		ClientId:  args.ClientId,
		Type:      opType,
		ConfigNum: args.Num,
	}
	index, _, isLeader := sc.rf.Start(startOp)
	if !isLeader {
		reply.Err = ErrWrongLeader
		sc.DPrintf("[%v(C%v-%v)] Refuse to %v CF%v for C%v (isn't the leader)\n",
			basicInfo, args.ClientId, args.OpId, opType, args.Num, args.ClientId)
		return
	}
	sc.mu.Lock()
	ch := sc.GetProcessedOpCh(index)
	sc.mu.Unlock()

	timer := time.NewTimer(RpcTimeout * time.Millisecond)
	select {
	case executedOp := <-ch:
		if executedOp.ClientId != startOp.ClientId || executedOp.Id != startOp.Id {
			reply.Err = ErrWrongLeader
			sc.DPrintf("[%v(C%v-%v)] Refuse to %v CF%v for C%v "+
				"(don't match op identifier at I%v: (%v,%v) VS (%v,%v))\n",
				basicInfo, args.ClientId, args.OpId, opType, args.Num, args.ClientId,
				index, executedOp.ClientId, executedOp.Id, startOp.ClientId, startOp.Id)
		} else {
			reply.Err = OK
			reply.Config = *executedOp.Config
			sc.DPrintf("[%v(C%v-%v)] %v CF%v(%v) for C%v\n",
				basicInfo, args.ClientId, args.OpId, opType, args.Num, reply.Config, args.ClientId)
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
		sc.DPrintf("[%v(C%v-%v)] Refuse to %v CF%v for C%v (rpc timeout)\n",
			basicInfo, args.ClientId, args.OpId, opType, args.Num, args.ClientId)
	}
	timer.Stop()

	sc.mu.Lock()
	sc.DeleteProcessedOpCh(index)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) GetNewConfigAfterJoinOp(servers map[int][]string) *Config {
	basicInfo := sc.BasicInfo("GetNewConfigAfterJoinOp")

	ret := &Config{}
	lastConfigIndex := len(sc.configs) - 1
	if lastConfigIndex < 0 {
		errMsg := fmt.Sprintf("[%v] lastConfigIndex < 0\n", basicInfo)
		panic(errMsg)
	}
	lastConfig := &sc.configs[lastConfigIndex]

	sc.DeepCopyConfig(ret, lastConfig)
	ret.Num++

	for gid, serverList := range servers {
		ret.Groups[gid] = serverList
	}

	gid2shards := make(map[int][]int)
	leftShards := make([]int, 0)
	for gid, _ := range ret.Groups {
		gid2shards[gid] = make([]int, 0)
	}
	for shard := 0; shard < NShards; shard++ {
		gid := ret.Shards[shard]
		if gid == 0 {
			leftShards = append(leftShards, shard)
		} else {
			gid2shards[gid] = append(gid2shards[gid], shard)
		}
	}

	//	Do load re-balance to adjust ret.Shards
	sc.DPrintf("[%v] New config before rebalance: %v | gid2shards: %v | leftShards: %v\n",
		basicInfo, ret, gid2shards, leftShards)
	ret.Rebalance(gid2shards, leftShards)
	sc.DPrintf("[%v] New config After rebalance: %v\n",
		basicInfo, ret)

	return ret
}

func (sc *ShardCtrler) GetNewConfigAfterLeaveOp(gids []int) *Config {
	basicInfo := sc.BasicInfo("GetNewConfigAfterLeaveOp")

	ret := &Config{}
	lastConfigIndex := len(sc.configs) - 1
	if lastConfigIndex < 0 {
		errMsg := fmt.Sprintf("[%v] lastConfigIndex < 0\n", basicInfo)
		panic(errMsg)
	}
	lastConfig := &sc.configs[lastConfigIndex]

	sc.DeepCopyConfig(ret, lastConfig)
	ret.Num++

	gid2shards := make(map[int][]int)
	for gid, _ := range ret.Groups {
		gid2shards[gid] = make([]int, 0)
	}
	for shard := 0; shard < NShards; shard++ {
		gid := ret.Shards[shard]
		gid2shards[gid] = append(gid2shards[gid], shard)
	}
	leftShards := make([]int, 0)

	for _, gid := range gids {
		leftShards = append(leftShards, gid2shards[gid]...)
		delete(gid2shards, gid)
		delete(ret.Groups, gid)
	}

	//	Do load re-balance to adjust ret.Shards
	sc.DPrintf("[%v] New config before rebalance: %v\n",
		basicInfo, ret)
	ret.Rebalance(gid2shards, leftShards)
	sc.DPrintf("[%v] New config After rebalance: %v\n",
		basicInfo, ret)

	return ret
}

func (sc *ShardCtrler) GetNewConfigAfterMoveOp(shard int, gid int) *Config {
	basicInfo := sc.BasicInfo("GetNewConfigAfterMoveOp")

	ret := &Config{}
	lastConfigIndex := len(sc.configs) - 1
	if lastConfigIndex < 0 {
		errMsg := fmt.Sprintf("[%v] lastConfigIndex < 0\n", basicInfo)
		panic(errMsg)
	}
	lastConfig := &sc.configs[lastConfigIndex]

	sc.DeepCopyConfig(ret, lastConfig)
	ret.Num++
	ret.Shards[shard] = gid

	return ret
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	basicInfo := sc.BasicInfo("Kill")

	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
	sc.DPrintf("[%v] Be killed\n", basicInfo)
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) processor() {
	basicInfo := sc.BasicInfo("processor")

	lastProcessed := 0
	for {
		m := <-sc.applyCh
		if m.CommandValid && m.CommandIndex > lastProcessed {
			op := m.Command.(Op)
			sc.DPrintf("[%v] Receive the op to be processed \"%v\" | index: %v | lastProcessed: %v\n",
				basicInfo, op, m.CommandIndex, lastProcessed)

			sc.mu.Lock()
			oriExecutedOpId, ok := sc.clientId2executedOpId[op.ClientId]
			if !ok {
				sc.DPrintf("[%v] Haven't executed any ops of C%v\n",
					basicInfo, op.ClientId)
			} else {
				sc.DPrintf("[%v] The max executed op.Id of C%v is %v\n",
					basicInfo, op.ClientId, oriExecutedOpId)
			}

			if op.Type == QueryCF {
				configNum := sc.GetValidConfigNum(op.ConfigNum)
				op.Config = &sc.configs[configNum]
				sc.clientId2executedOpId[op.ClientId] = op.Id
			} else {
				opBeforeExecuted := sc.OpExecuted(op.ClientId, op.Id)
				if !opBeforeExecuted {
					var newConfig *Config
					switch op.Type {
					case JoinRG:
						newConfig = sc.GetNewConfigAfterJoinOp(op.Servers)
					case LeaveRG:
						newConfig = sc.GetNewConfigAfterLeaveOp(op.GIDs)
					case MoveSD:
						newConfig = sc.GetNewConfigAfterMoveOp(op.Shard, op.GIDs[0])
					}
					sc.configs = append(sc.configs, *newConfig)
					sc.clientId2executedOpId[op.ClientId] = op.Id
					sc.DPrintf("[%v] Execute the op \"%v\" | newConfig: %v\n",
						basicInfo, op, newConfig)
				} else {
					sc.DPrintf("[%v] Refuse to execute the duplicated op \"%v\"\n",
						basicInfo, op)
				}
			}
			ch := sc.GetProcessedOpCh(m.CommandIndex)
			sc.mu.Unlock()

			ch <- op
			sc.DPrintf("[%v] After return the processed op \"%v\"\n",
				basicInfo, op)
			lastProcessed = m.CommandIndex
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.clientId2executedOpId = make(map[Int64Id]int)
	sc.index2processedOpCh = make(map[int]chan Op)
	sc.DPrintf("[%v] Start new shard controller\n", sc.BasicInfo(""))

	go sc.processor()

	return sc
}
