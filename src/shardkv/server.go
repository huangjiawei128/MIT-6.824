package shardkv

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead int32 // set by Kill()

	mck                     *shardctrler.Clerk
	clientId2executedOpId   map[Int64Id]int
	index2processedResultCh map[int]chan ProcessResult
	kvStore                 KVStore

	curConfig        shardctrler.Config
	prevConfig       shardctrler.Config
	inShards         map[int]int //	shard -> from GID
	outShards        map[int]int //	shard -> to GID
	newConfig        chan bool
	gid2targetLeader sync.Map
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	basicInfo := kv.BasicInfo("Kill")

	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	kv.DPrintf("[%v] Be killed\n", basicInfo)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) snapshotData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.clientId2executedOpId)
	e.Encode(kv.kvStore)
	e.Encode(kv.curConfig)
	e.Encode(kv.prevConfig)
	e.Encode(kv.inShards)
	e.Encode(kv.outShards)
	return w.Bytes()
}

func (kv *ShardKV) readSnapshot(data []byte) {
	basicInfo := kv.BasicInfo("readSnapshot")

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var clientId2executedOpId map[Int64Id]int
	var kvStore KVStore
	var curConfig shardctrler.Config
	var prevConfig shardctrler.Config
	var inShards map[int]int
	var outShards map[int]int
	if d.Decode(&clientId2executedOpId) != nil ||
		d.Decode(&kvStore) != nil {
		errorMsg := fmt.Sprintf("[%v] Decode error\n", basicInfo)
		panic(errorMsg)
	} else {
		kv.clientId2executedOpId = clientId2executedOpId
		kv.kvStore = kvStore
		kv.curConfig = curConfig
		kv.prevConfig = prevConfig
		kv.inShards = inShards
		kv.outShards = outShards
	}
}

func (kv *ShardKV) processor() {
	basicInfo := kv.BasicInfo("processor")

	lastProcessed := 0
	for kv.killed() == false {
		m := <-kv.applyCh
		if m.SnapshotValid {
			kv.DPrintf("[%v] Receive the snapshot to be processed | index: %v | lastProcessed: %v | term: %v\n",
				basicInfo, m.SnapshotIndex, lastProcessed, m.SnapshotTerm)

			kv.mu.Lock()
			if kv.rf.CondInstallSnapshot(m.SnapshotTerm, m.SnapshotIndex, m.Snapshot) {
				kv.readSnapshot(m.Snapshot)
				lastProcessed = m.SnapshotIndex
			}
			kv.mu.Unlock()
		} else if m.CommandValid && m.CommandIndex > lastProcessed {
			switch m.Command.(type) {
			case Op:
				op := m.Command.(Op)
				kv.DPrintf("[%v] Receive the op to be processed %v | index: %v | lastProcessed: %v\n",
					basicInfo, &op, m.CommandIndex, lastProcessed)
				kv.processOpCommand(&op, m.CommandIndex)
			case shardctrler.Config:
				newConfig := m.Command.(shardctrler.Config)
				kv.DPrintf("[%v] Receive the config to be processed %v | index: %v | lastProcessed: %v\n",
					basicInfo, &newConfig, m.CommandIndex, lastProcessed)
				kv.processConfigCommand(&newConfig, m.CommandIndex)
			case MergeReq:
				mReq := m.Command.(MergeReq)
				kv.DPrintf("[%v] Receive the merge request to be processed %v | index: %v | lastProcessed: %v\n",
					basicInfo, &mReq, m.CommandIndex, lastProcessed)
				kv.processMergeReqCommand(&mReq, m.CommandIndex)
			case DeleteReq:
				dReq := m.Command.(DeleteReq)
				kv.DPrintf("[%v] Receive the delete request to be processed %v | index: %v | lastProcessed: %v\n",
					basicInfo, &dReq, m.CommandIndex, lastProcessed)
				kv.processDeleteReqCommand(&dReq, m.CommandIndex)
			}

			raftStateSize := kv.rf.GetPersister().RaftStateSize()
			if kv.maxraftstate > 0 && raftStateSize > kv.maxraftstate {
				kv.mu.Lock()
				kv.DPrintf("[%v] Prepare snapshot data | index: %v | raftStateSize: %v > maxraftstate: %v > 0\n",
					basicInfo, m.CommandIndex, raftStateSize, kv.maxraftstate)
				snapshotData := kv.snapshotData()
				kv.mu.Unlock()
				kv.rf.Snapshot(m.CommandIndex, snapshotData)
			}

			lastProcessed = m.CommandIndex
		}
	}
}

func (kv *ShardKV) processOpCommand(op *Op, index int) {
	basicInfo := kv.BasicInfo("processOpCommand")

	opResult := ProcessResult{
		Type:     OpCmd,
		Id:       op.Id,
		ClientId: op.ClientId,
		Value:    op.Value,
		Err:      OK,
	}

	kv.mu.Lock()
	defer func() {
		kv.mu.Unlock()
		kv.NotifyProcessedResultCh(index, &opResult)
		kv.DPrintf("[%v] Finish processing the op %v\n",
			basicInfo, op)
	}()

	oriExecutedOpId, ok := kv.clientId2executedOpId[op.ClientId]
	if !ok {
		kv.DPrintf("[%v] Haven't executed any ops of C%v\n",
			basicInfo, op.ClientId)
	} else {
		kv.DPrintf("[%v] The max executed op.Id of C%v is %v\n",
			basicInfo, op.ClientId, oriExecutedOpId)
	}

	shard := key2shard(op.Key)
	shardGID := kv.curConfig.Shards[shard]
	serverGID := kv.gid
	if shardGID != serverGID {
		kv.DPrintf("[%v] Refuse to execute the op %v (don't match GID : %v(shard %v) VS %v(server)) | "+
			"curConfig.Num: %v\n",
			basicInfo, op, shardGID, shard, serverGID, kv.curConfig.Num)
		opResult.Err = ErrWrongGroup
		return
	}

	if _, inShardsOk := kv.inShards[shard]; inShardsOk {
		kv.DPrintf("[%v] Refuse to execute the op %v (shard %v not arrived) | curConfig.Num: %v\n",
			basicInfo, op, shard, kv.curConfig.Num)
		opResult.Err = ErrNotArrivedShard
		return
	}

	if op.Type == GetV {
		opResult.Value = kv.kvStore.Get(op.Key)
		kv.clientId2executedOpId[op.ClientId] = op.Id
		kv.DPrintf("[%v] Execute the op %v\n",
			basicInfo, op)
	} else if op.Type == PutKV || op.Type == AppendKV {
		opBeforeExecuted := kv.OpExecuted(op.ClientId, op.Id)
		if !opBeforeExecuted {
			kv.kvStore.PutAppend(op.Key, op.Value, op.Type)
			kv.clientId2executedOpId[op.ClientId] = op.Id
			kv.DPrintf("[%v] Execute the op %v | stored value: %v\n",
				basicInfo, op, kv.kvStore.Get(op.Key))
		} else {
			kv.DPrintf("[%v] Refuse to execute the duplicated op %v | stored value: %v\n",
				basicInfo, op, kv.kvStore.Get(op.Key))
		}
	}
}

func (kv *ShardKV) processConfigCommand(newConfig *shardctrler.Config, index int) {
	basicInfo := kv.BasicInfo("processConfigCommand")

	kv.mu.Lock()
	defer func() {
		kv.mu.Unlock()
		if len(kv.newConfig) != cap(kv.newConfig) {
			kv.newConfig <- true
		}
	}()

	if newConfig.Num <= kv.curConfig.Num {
		kv.DPrintf("[%v] Refuse to install the config %v (%v(newConfig.Num) <= %v(curConfig.Num))\n",
			basicInfo, newConfig, newConfig.Num, kv.curConfig.Num)
		return
	}

	//	install newConfig
	kv.prevConfig = kv.curConfig
	kv.curConfig = *newConfig

	if kv.curConfig.Num == 1 {
		kv.ValidateGroupShardDatas()
	} else {
		kv.UpdateInAndOutShards()
	}
	for shard := 0; shard < shardctrler.NShards; shard++ {
		if _, inShardsOk := kv.inShards[shard]; inShardsOk {
			continue
		}
		if _, outShardsOk := kv.outShards[shard]; outShardsOk {
			continue
		}
		kv.kvStore.UpdateShardConfigNum(shard, kv.curConfig.Num)
	}

	kv.DPrintf("[%v] Finish to install the config %v | inShards: %v | outShards: %v\n",
		basicInfo, newConfig, kv.inShards, kv.outShards)
}

func (kv *ShardKV) processMergeReqCommand(mReq *MergeReq, index int) {
	basicInfo := kv.BasicInfo("processMergeReqCommand")

	mReqResult := ProcessResult{
		Type:      MergeReqCmd,
		GID:       mReq.GID,
		ConfigNum: mReq.ConfigNum,
		Shard:     mReq.Shard,
		Err:       OK,
	}

	kv.mu.Lock()
	defer func() {
		kv.mu.Unlock()
		kv.NotifyProcessedResultCh(index, &mReqResult)
		kv.DPrintf("[%v] Finish processing the merge request %v\n",
			basicInfo, mReq)
	}()

	if mReq.ConfigNum > kv.curConfig.Num {
		kv.DPrintf("[%v] Refuse to merge shard %v from G%v (shard ahead: %v(mReq.ConfigNum) > %v(curConfig.Num))\n",
			basicInfo, mReq.Shard, mReq.GID, mReq.ConfigNum, kv.curConfig.Num)
		mReqResult.Err = ErrAheadShard
		return
	}

	if mReq.ConfigNum < kv.curConfig.Num {
		kv.DPrintf("[%v] Refuse to merge shard %v from G%v (shard outdated: %v(mReq.ConfigNum) < %v(curConfig.Num))\n",
			basicInfo, mReq.Shard, mReq.GID, mReq.ConfigNum, kv.curConfig.Num)
		mReqResult.Err = ErrOutdatedShard
		return
	}

	fromGID, inShardsOk := kv.inShards[mReq.Shard]
	if !inShardsOk {
		kv.DPrintf("[%v] Refuse to merge shard %v from G%v (shard not in inShards of CF%v) | inShards: %v\n",
			basicInfo, mReq.Shard, mReq.GID, kv.curConfig.Num, kv.inShards)
		mReqResult.Err = ErrRepeatedShard
		return
	}
	if mReq.GID != fromGID {
		errorMsg := fmt.Sprintf("[%v] shard %v(CF%v): mReq.GID %v != fromGID %v\n",
			basicInfo, mReq.Shard, kv.curConfig.Num, mReq.GID, fromGID)
		panic(errorMsg)
	}

	kv.kvStore.MergeShardData(mReq.Shard, mReq.ShardData)
	kv.kvStore.UpdateShardConfigNum(mReq.Shard, mReq.ConfigNum)

	for clientId, executedOpId := range mReq.ClientId2ExecutedOpId {
		if oriExecutedOpId, ok := kv.clientId2executedOpId[clientId]; !ok || oriExecutedOpId < executedOpId {
			kv.clientId2executedOpId[clientId] = executedOpId
		}
	}

	delete(kv.inShards, mReq.Shard)
}

func (kv *ShardKV) processDeleteReqCommand(dReq *DeleteReq, index int) {
	basicInfo := kv.BasicInfo("processDeleteReq")

	dReqResult := ProcessResult{
		Type:      DeleteReqCmd,
		ConfigNum: dReq.ConfigNum,
		Shard:     dReq.Shard,
		Err:       OK,
	}

	kv.mu.Lock()
	defer func() {
		kv.mu.Unlock()
		kv.NotifyProcessedResultCh(index, &dReqResult)
		kv.DPrintf("[%v] Finish processing the delete request %v\n",
			basicInfo, dReq)
	}()

	if dReq.ConfigNum > kv.curConfig.Num {
		errorMsg := fmt.Sprintf("[%v] shard %v ahead: %v(dReq.ConfigNum) > %v(curConfig.Num)\n",
			basicInfo, dReq.Shard, dReq.ConfigNum, kv.curConfig.Num)
		panic(errorMsg)
	}

	if dReq.ConfigNum < kv.curConfig.Num {
		kv.DPrintf("[%v] Refuse to delete shard %v (shard outdated: %v(dReq.ConfigNum) < %v(curConfig.Num))\n",
			basicInfo, dReq.Shard, dReq.ConfigNum, kv.curConfig.Num)
		dReqResult.Err = ErrOutdatedShard
		return
	}

	_, outShardsOk := kv.outShards[dReq.Shard]
	if !outShardsOk {
		kv.DPrintf("[%v] Refuse to delete shard %v (shard not in outShards of CF%v) | outShards: %v\n",
			basicInfo, dReq.Shard, kv.curConfig.Num, kv.outShards)
		dReqResult.Err = ErrRepeatedShard
		return
	}

	kv.kvStore.InvalidateShardData(dReq.Shard)
	kv.kvStore.UpdateShardConfigNum(dReq.Shard, dReq.ConfigNum)

	delete(kv.outShards, dReq.Shard)
}

func (kv *ShardKV) configPoller() {
	basicInfo := kv.BasicInfo("configPoller")

	for kv.killed() == false {
		time.Sleep(ConfigPollPeriod * time.Millisecond)

		if _, isLeader := kv.rf.GetState(); !isLeader {
			continue
		}

		kv.mu.Lock()
		curConfigNum := kv.curConfig.Num
		kv.mu.Unlock()

		pollConfigNum := curConfigNum + 1
		kv.DPrintf("[%v] Prepare to poll CF%v\n",
			basicInfo, pollConfigNum)
		newConfig := kv.mck.Query(pollConfigNum)
		if newConfig.Num != pollConfigNum {
			kv.DPrintf("[%v] Fail to poll CF%v | newConfig.Num: %v\n",
				basicInfo, pollConfigNum, newConfig.Num)
			continue
		}

		kv.mu.Lock()
		if len(kv.inShards) > 0 {
			kv.DPrintf("[%v] Refuse to start CF%v (inShards not empty)\n",
				basicInfo, newConfig.Num)
			kv.mu.Unlock()
			continue
		}

		if newConfig.Num <= kv.curConfig.Num {
			kv.DPrintf("[%v] Refuse to start CF%v (config installed)\n",
				basicInfo, newConfig.Num)
			kv.mu.Unlock()
			continue
		}
		kv.mu.Unlock()

		kv.DPrintf("[%v] Prepare to start CF%v\n",
			basicInfo, newConfig.Num)
		kv.rf.Start(newConfig)
	}
}

func (kv *ShardKV) shardMigrant() {
	basicInfo := kv.BasicInfo("shardMigrant")

	for kv.killed() == false {
		timer := time.NewTimer(ShardMigratePeriod * time.Millisecond)
		select {
		case <-kv.newConfig:
		case <-timer.C:
		}
		timer.Stop()

		if _, isLeader := kv.rf.GetState(); !isLeader {
			continue
		}

		kv.mu.Lock()
		if len(kv.outShards) == 0 {
			kv.mu.Unlock()
			continue
		}

		kv.DPrintf("[%v] Prepare to migrate shards | curConfig.Num: %v | outShards: %v\n",
			basicInfo, kv.curConfig.Num, kv.outShards)
		for shard, toGID := range kv.outShards {
			kv.migrateShard(shard, toGID)
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) migrateShard(shard int, toGID int) {
	basicInfo := kv.BasicInfo("migrateShard")

	clientId2executedOpIdCopy := make(map[Int64Id]int)
	for clientId, executedOpId := range kv.clientId2executedOpId {
		clientId2executedOpIdCopy[clientId] = executedOpId
	}

	shardDataCopy := make(ShardData)
	for key, value := range kv.kvStore.ShardDatas[shard] {
		shardDataCopy[key] = value
	}

	mArgs := MergeShardDatasArgs{
		GID:                   kv.gid,
		ConfigNum:             kv.curConfig.Num,
		Shard:                 shard,
		ShardData:             shardDataCopy,
		ClientId2ExecutedOpId: clientId2executedOpIdCopy,
	}

	servers := kv.curConfig.Groups[toGID]
	go func(servers []string, args *MergeShardDatasArgs) {
		flag := false
		ok := false
		targetLeader := kv.getTargetLeader(toGID, len(servers))
		startTime := time.Now()
		for !ok {
			if time.Since(startTime) > ShardMigrateTimeout*time.Millisecond {
				break
			}

			srv := kv.make_end(servers[targetLeader])
			reply := &MergeShardDatasReply{}
			kv.DPrintf("[%v] Send MergeShardDatas RPC to S%v-%v | gid: %v | configNum: %v | shard: %v\n",
				basicInfo, toGID, targetLeader, args.GID, args.ConfigNum, args.Shard)

			ok = srv.Call("ShardKV.MergeShardDatas", args, reply)

			if ok {
				kv.DPrintf("[%v] Receive MergeShardDatas ACK from S%v-%v | err: %v | "+
					"gid: %v | configNum: %v | shard: %v\n",
					basicInfo, toGID, targetLeader, reply.Err, args.GID, args.ConfigNum, args.Shard)
				switch reply.Err {
				case OK:
					flag = true
					break
				case ErrWrongLeader:
					ok = false
					targetLeader = kv.UpdateTargetLeader(toGID, len(servers))
				case ErrAheadShard:
					ok = false
				case ErrOutdatedShard:
					flag = true
					break
				case ErrRepeatedShard:
					flag = true
					break
				case ErrOvertime:
					ok = false
					targetLeader = kv.UpdateTargetLeader(toGID, len(servers))
				}
			} else {
				kv.DPrintf("[%v] Fail to receive MergeShardDatas ACK from S%v-%v | "+
					"gid: %v | configNum: %v | shard: %v\n",
					basicInfo, toGID, targetLeader, args.GID, args.ConfigNum, args.Shard)
				targetLeader = kv.UpdateTargetLeader(toGID, len(servers))
			}
		}

		if !flag {
			return
		}

		dArgs := DeleteShardDatasArgs{
			ConfigNum: mArgs.ConfigNum,
			Shard:     mArgs.Shard,
		}
		dReply := DeleteShardDatasReply{}
		kv.DeleteShardDatas(&dArgs, &dReply)
		if dReply.Err != OK {
			kv.DPrintf("[%v] Finish to delete shard %v on S%v-%v\n",
				basicInfo, dArgs.Shard, kv.gid, kv.me)
		} else {
			kv.DPrintf("[%v] Fail to delete shard %v on S%v-%v | err: %v\n",
				basicInfo, dArgs.Shard, kv.gid, kv.me, dReply.Err)
		}
	}(servers, &mArgs)
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(MergeReq{})
	labgob.Register(DeleteReq{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// Your initialization code here.
	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.clientId2executedOpId = make(map[Int64Id]int)
	kv.index2processedResultCh = make(map[int]chan ProcessResult)
	kv.kvStore.Init()

	kv.curConfig.Groups = make(map[int][]string)
	kv.prevConfig.Groups = make(map[int][]string)
	kv.inShards = make(map[int]int)
	kv.outShards = make(map[int]int)
	kv.newConfig = make(chan bool, 1)

	kv.mck.SetHostInfo(fmt.Sprintf("S%v-%v", kv.gid, kv.me))
	kv.DPrintf("[%v] Start new shard KV server | maxraftstate: %v | mck.clientId: %v\n",
		kv.BasicInfo(""), kv.maxraftstate, kv.mck.GetClientId())

	go kv.processor()
	go kv.configPoller()
	go kv.shardMigrant()

	return kv
}
