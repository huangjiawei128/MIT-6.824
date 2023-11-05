package shardctrler

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"fmt"
	"sync"
	"sync/atomic"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead int32 // set by Kill()

	configs                   []Config // indexed by config num
	clientId2executedOpId     map[Int64Id]int
	index2processedOpResultCh map[int]chan OpResult
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
			sc.DPrintf("[%v] Receive the op to be processed %v | index: %v | lastProcessed: %v\n",
				basicInfo, &op, m.CommandIndex, lastProcessed)
			sc.processOpCommand(&op, m.CommandIndex)
			lastProcessed = m.CommandIndex
		}
	}
}

func (sc *ShardCtrler) processOpCommand(op *Op, index int) {
	basicInfo := sc.BasicInfo("processOpCommand")

	opResult := OpResult{
		Id:       op.Id,
		ClientId: op.ClientId,
	}

	sc.mu.Lock()
	defer func() {
		sc.mu.Unlock()
		sc.NotifyProcessedOpResultCh(index, &opResult)
		sc.DPrintf("[%v] Finish processing the op %v\n",
			basicInfo, op)
	}()

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
		opResult.Config = &sc.configs[configNum]
		sc.clientId2executedOpId[op.ClientId] = op.Id
	} else {
		opBeforeExecuted := sc.OpExecuted(op.ClientId, op.Id)
		if !opBeforeExecuted {
			var newConfig *Config
			switch op.Type {
			case JoinRG:
				newConfig = sc.getNewConfigAfterJoinOp(op.Servers)
			case LeaveRG:
				newConfig = sc.getNewConfigAfterLeaveOp(op.GIDs)
			case MoveSD:
				newConfig = sc.getNewConfigAfterMoveOp(op.Shard, op.GIDs[0])
			}
			sc.configs = append(sc.configs, *newConfig)
			sc.clientId2executedOpId[op.ClientId] = op.Id
			sc.DPrintf("[%v] Execute the op %v | newConfig: %v\n",
				basicInfo, op, newConfig)
		} else {
			sc.DPrintf("[%v] Refuse to execute the duplicated op %v\n",
				basicInfo, op)
		}
	}
}

func (sc *ShardCtrler) getNewConfigAfterJoinOp(servers map[int][]string) *Config {
	basicInfo := sc.BasicInfo("getNewConfigAfterJoinOp")

	ret := &Config{}
	lastConfigIndex := len(sc.configs) - 1
	if lastConfigIndex < 0 {
		errMsg := fmt.Sprintf("[%v] lastConfigIndex < 0\n", basicInfo)
		panic(errMsg)
	}
	lastConfig := &sc.configs[lastConfigIndex]

	lastConfig.DeepCopyConfig(ret)
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

func (sc *ShardCtrler) getNewConfigAfterLeaveOp(gids []int) *Config {
	basicInfo := sc.BasicInfo("getNewConfigAfterLeaveOp")

	ret := &Config{}
	lastConfigIndex := len(sc.configs) - 1
	if lastConfigIndex < 0 {
		errMsg := fmt.Sprintf("[%v] lastConfigIndex < 0\n", basicInfo)
		panic(errMsg)
	}
	lastConfig := &sc.configs[lastConfigIndex]

	lastConfig.DeepCopyConfig(ret)
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

func (sc *ShardCtrler) getNewConfigAfterMoveOp(shard int, gid int) *Config {
	basicInfo := sc.BasicInfo("getNewConfigAfterMoveOp")

	ret := &Config{}
	lastConfigIndex := len(sc.configs) - 1
	if lastConfigIndex < 0 {
		errMsg := fmt.Sprintf("[%v] lastConfigIndex < 0\n", basicInfo)
		panic(errMsg)
	}
	lastConfig := &sc.configs[lastConfigIndex]

	lastConfig.DeepCopyConfig(ret)
	ret.Num++
	ret.Shards[shard] = gid

	return ret
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
	sc.configs[0].Groups = make(map[int][]string)

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.clientId2executedOpId = make(map[Int64Id]int)
	sc.index2processedOpResultCh = make(map[int]chan OpResult)
	sc.DPrintf("[%v] Start new shard controller\n", sc.BasicInfo(""))

	go sc.processor()

	return sc
}
