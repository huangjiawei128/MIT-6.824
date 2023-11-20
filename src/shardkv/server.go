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
	maxraftstate int   // snapshot if log grows this big
	dead         int32 // set by Kill()

	// Your definitions here.
	mck                     *shardctrler.Clerk
	clientId2executedOpId   map[Int64Id]int
	index2processedResultCh map[int]chan ProcessResult
	kvStore                 KVStore
	gid2targetLeader        sync.Map

	curConfig          shardctrler.Config
	checkCurConfigTime time.Time
	inShards           map[int]InShardInfo  //	shard -> inShardInfo
	outShards          map[int]OutShardInfo // shard -> outShardInfo
	newConfig          chan bool
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	basicInfo := kv.BasicInfo("Kill")

	kv.DPrintf("[%v] Be killed\n", basicInfo)
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
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
	var inShards map[int]InShardInfo
	var outShards map[int]OutShardInfo
	if d.Decode(&clientId2executedOpId) != nil ||
		d.Decode(&kvStore) != nil ||
		d.Decode(&curConfig) != nil ||
		d.Decode(&inShards) != nil ||
		d.Decode(&outShards) != nil {
		errorMsg := fmt.Sprintf("[%v] Decode error\n", basicInfo)
		panic(errorMsg)
	} else {
		kv.clientId2executedOpId = clientId2executedOpId
		kv.kvStore = kvStore
		kv.curConfig = curConfig
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
			kv.DPrintf("[%v] Receive the snapshot at I%v to be processed | term: %v | lastProcessed: %v\n",
				basicInfo, m.SnapshotIndex, m.SnapshotTerm, lastProcessed)

			if kv.rf.CondInstallSnapshot(m.SnapshotTerm, m.SnapshotIndex, m.Snapshot) {
				kv.processSnapshot(m.Snapshot, m.SnapshotIndex)
			}
			lastProcessed = m.SnapshotIndex
		} else if m.CommandValid && m.CommandIndex > lastProcessed {
			switch m.Command.(type) {
			case Nop:
				kv.DPrintf("[%v] Receive the nop at I%v to be processed | lastProcessed: %v\n",
					basicInfo, m.CommandIndex, lastProcessed)
			case Op:
				op := m.Command.(Op)
				kv.DPrintf("[%v] Receive the op at I%v to be processed %v | lastProcessed: %v\n",
					basicInfo, m.CommandIndex, &op, lastProcessed)
				kv.processOpCommand(&op, m.CommandIndex)
			case shardctrler.Config:
				newConfig := m.Command.(shardctrler.Config)
				kv.DPrintf("[%v] Receive the config at I%v to be processed %v | lastProcessed: %v\n",
					basicInfo, m.CommandIndex, &newConfig, lastProcessed)
				kv.processConfigCommand(&newConfig, m.CommandIndex)
			case MergeReq:
				mReq := m.Command.(MergeReq)
				kv.DPrintf("[%v] Receive the merge request at I%v to be processed %v | lastProcessed: %v\n",
					basicInfo, m.CommandIndex, &mReq, lastProcessed)
				kv.processMergeReqCommand(&mReq, m.CommandIndex)
			case DeleteReq:
				dReq := m.Command.(DeleteReq)
				kv.DPrintf("[%v] Receive the delete request at I%v to be processed %v | lastProcessed: %v\n",
					basicInfo, m.CommandIndex, &dReq, lastProcessed)
				kv.processDeleteReqCommand(&dReq, m.CommandIndex)
			}

			raftStateSize := kv.rf.GetPersister().RaftStateSize()
			if kv.maxraftstate > 0 && raftStateSize > kv.maxraftstate {
				kv.mu.Lock()
				kv.DPrintf("[%v] Prepare to snapshot data at I%v | raftStateSize: %v > maxraftstate: %v > 0 | "+
					"curConfig.Num: %v | inShards: %v | outShards: %v\n",
					basicInfo, m.CommandIndex, raftStateSize, kv.maxraftstate, kv.curConfig.Num, kv.inShards, kv.outShards)
				snapshotData := kv.snapshotData()
				kv.mu.Unlock()
				kv.rf.Snapshot(m.CommandIndex, snapshotData)
			}

			lastProcessed = m.CommandIndex
		}
	}
}

func (kv *ShardKV) processSnapshot(snapShot []byte, index int) {
	basicInfo := kv.BasicInfo("processSnapshot")

	installNewConfig := false
	kv.mu.Lock()
	defer func() {
		kv.mu.Unlock()
		if installNewConfig && len(kv.newConfig) != cap(kv.newConfig) {
			kv.newConfig <- true
		}
	}()

	oriConfigNum := kv.curConfig.Num
	kv.readSnapshot(snapShot)
	kv.DPrintf("[%v] Finish reading the snapshot at I%v | "+
		"curConfig.Num: %v | inShards: %v | outShards: %v\n",
		basicInfo, index, kv.curConfig.Num, kv.inShards, kv.outShards)
	if kv.curConfig.Num != oriConfigNum {
		installNewConfig = true
		kv.checkCurConfigTime = time.Now()
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
		kv.DPrintf("[%v] Finish executing the op %v\n",
			basicInfo, op)
	} else if op.Type == PutKV || op.Type == AppendKV {
		opBeforeExecuted := kv.OpExecuted(op.ClientId, op.Id)
		if !opBeforeExecuted {
			kv.kvStore.PutAppend(op.Key, op.Value, op.Type)
			kv.clientId2executedOpId[op.ClientId] = op.Id
			kv.DPrintf("[%v] Finish executing the op %v | stored value: %v\n",
				basicInfo, op, kv.kvStore.Get(op.Key))
		} else {
			kv.DPrintf("[%v] Refuse to execute the duplicated op %v | stored value: %v\n",
				basicInfo, op, kv.kvStore.Get(op.Key))
		}
	}
}

func (kv *ShardKV) processConfigCommand(newConfig *shardctrler.Config, index int) {
	basicInfo := kv.BasicInfo("processConfigCommand")

	installNewConfig := false
	kv.mu.Lock()
	defer func() {
		kv.mu.Unlock()
		if installNewConfig && len(kv.newConfig) != cap(kv.newConfig) {
			kv.newConfig <- true
		}
	}()

	if newConfig.Num <= kv.curConfig.Num {
		kv.DPrintf("[%v] Refuse to install the config %v (%v(newConfig.Num) <= %v(curConfig.Num))\n",
			basicInfo, newConfig, newConfig.Num, kv.curConfig.Num)
		return
	}

	if len(kv.inShards) > 0 {
		errorMsg := fmt.Sprintf("[%v] Install CF%v error: inShards not empty)\n",
			basicInfo, newConfig.Num)
		kv.mu.Unlock()
		panic(errorMsg)
	}

	//	install newConfig
	prevConfig := kv.curConfig
	kv.curConfig = *newConfig

	if kv.curConfig.Num == 1 {
		kv.ValidateShardsInGroup()
	} else {
		kv.UpdateInAndOutShards(&prevConfig)
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

	kv.DPrintf("[%v] Finish installing the config %v | inShards: %v | outShards: %v\n",
		basicInfo, newConfig, kv.inShards, kv.outShards)
	installNewConfig = true
	kv.checkCurConfigTime = time.Now()
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

	if mReq.ConfigNum < kv.curConfig.Num {
		kv.DPrintf("[%v] Refuse to merge shard %v from G%v (shard outdated: %v(mReq.ConfigNum) < %v(curConfig.Num))\n",
			basicInfo, mReq.Shard, mReq.GID, mReq.ConfigNum, kv.curConfig.Num)
		mReqResult.Err = ErrOutdatedShard
		return
	}

	shardConfigNum := kv.kvStore.GetShardConfigNum(mReq.Shard)
	if mReq.ConfigNum <= shardConfigNum {
		kv.DPrintf("[%v] Refuse to merge shard %v from G%v (shard ahead: %v(mReq.ConfigNum) <= %v(shardConfigNum))\n",
			basicInfo, mReq.Shard, mReq.GID, mReq.ConfigNum, shardConfigNum)
		mReqResult.Err = ErrRepeatedShard
		return
	}

	kv.kvStore.MergeShardData(mReq.Shard, mReq.ShardData)
	kv.kvStore.UpdateShardConfigNum(mReq.Shard, mReq.ConfigNum)

	for clientId, executedOpId := range mReq.ClientId2ExecutedOpId {
		if oriExecutedOpId, ok := kv.clientId2executedOpId[clientId]; !ok || oriExecutedOpId < executedOpId {
			kv.clientId2executedOpId[clientId] = executedOpId
		}
	}

	delete(kv.inShards, mReq.Shard)
	kv.DPrintf("[%v] Finish merging shard %v from G%v | curConfig: %v | inShards: %v | validShards: %v\n",
		basicInfo, mReq.Shard, mReq.GID, kv.curConfig, kv.inShards, kv.kvStore.GetValidShards())
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
		kv.mu.Unlock()
		panic(errorMsg)
	}

	shardConfigNum := kv.kvStore.GetShardConfigNum(dReq.Shard)
	if dReq.ConfigNum <= shardConfigNum {
		kv.DPrintf("[%v] Refuse to delete shard %v (shard ahead: %v(dReq.ConfigNum) <= %v(shardConfigNum))\n",
			basicInfo, dReq.Shard, dReq.ConfigNum, shardConfigNum)
		dReqResult.Err = ErrRepeatedShard
		return
	}

	kv.kvStore.InvalidateShard(dReq.Shard)
	kv.kvStore.UpdateShardConfigNum(dReq.Shard, dReq.ConfigNum)

	delete(kv.outShards, dReq.Shard)
	kv.DPrintf("[%v] Finish deleting shard %v | curConfig: %v | outShards: %v | validShards: %v\n",
		basicInfo, dReq.Shard, kv.curConfig, kv.outShards, kv.kvStore.GetValidShards())
}

func (kv *ShardKV) configPoller() {
	basicInfo := kv.BasicInfo("configPoller")

	for kv.killed() == false {
		time.Sleep(ConfigPollPeriod * time.Millisecond)

		if _, isLeader := kv.rf.GetState(); !isLeader {
			kv.DPrintf("[%v] Isn't leader\n", basicInfo)
			continue
		}

		kv.mu.Lock()
		curConfigNum := kv.curConfig.Num
		startNop := false
		if len(kv.inShards) > 0 && time.Since(kv.checkCurConfigTime) > StartNopTimeout*time.Millisecond {
			kv.checkCurConfigTime = time.Now()
			startNop = true
		}
		kv.mu.Unlock()

		if startNop {
			kv.DPrintf("[%v] Prepare to start Nop\n", basicInfo)
			kv.rf.Start(Nop{})
		}

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
		if newConfig.Num <= kv.curConfig.Num {
			kv.DPrintf("[%v] Refuse to start CF%v (config installed) | curConfig.Num: %v\n",
				basicInfo, newConfig.Num, kv.curConfig.Num)
			kv.mu.Unlock()
			continue
		}

		if len(kv.inShards) > 0 {
			kv.DPrintf("[%v] Refuse to start CF%v (inShards not empty) | inShards: %v\n",
				basicInfo, newConfig.Num, kv.inShards)
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
			kv.DPrintf("[%v] Isn't leader\n", basicInfo)
			continue
		}

		kv.mu.Lock()
		if len(kv.outShards) == 0 {
			kv.DPrintf("[%v] Refuse to migrate shards (outShards empty) | curConfig: %v | "+
				"outShards: %v | validShards: %v\n",
				basicInfo, kv.curConfig, kv.outShards, kv.kvStore.GetValidShards())
			kv.mu.Unlock()
			continue
		}

		kv.DPrintf("[%v] Prepare to migrate shards | curConfig.Num: %v | outShards: %v\n",
			basicInfo, kv.curConfig.Num, kv.outShards)
		for shard, info := range kv.outShards {
			kv.migrateShard(shard, info)
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) migrateShard(shard int, info OutShardInfo) {
	basicInfo := kv.BasicInfo("migrateShard")

	shardDataCopy := make(ShardData)
	for key, value := range kv.kvStore.ShardDatas[shard] {
		shardDataCopy[key] = value
	}

	clientId2executedOpIdCopy := make(map[Int64Id]int)
	for clientId, executedOpId := range kv.clientId2executedOpId {
		clientId2executedOpIdCopy[clientId] = executedOpId
	}

	mArgs := MergeShardDatasArgs{
		GID:                   kv.gid,
		ConfigNum:             info.ConfigNum,
		Shard:                 shard,
		ShardData:             shardDataCopy,
		ClientId2ExecutedOpId: clientId2executedOpIdCopy,
	}

	servers := info.Servers
	go func(servers []string, args *MergeShardDatasArgs) {
		flag := false
		ok := false
		targetLeader := kv.GetTargetLeader(info.ToGID, len(servers))
		for !ok {
			srv := kv.make_end(servers[targetLeader])
			reply := &MergeShardDatasReply{}
			kv.DPrintf("[%v] Send MergeShardDatas RPC to S%v-%v | gid: %v | configNum: %v | shard: %v\n",
				basicInfo, info.ToGID, targetLeader, args.GID, args.ConfigNum, args.Shard)

			ok = srv.Call("ShardKV.MergeShardDatas", args, reply)

			if ok {
				kv.DPrintf("[%v] Receive MergeShardDatas ACK from S%v-%v | err: %v | "+
					"gid: %v | configNum: %v | shard: %v\n",
					basicInfo, info.ToGID, targetLeader, reply.Err, args.GID, args.ConfigNum, args.Shard)
				switch reply.Err {
				case OK:
					flag = true
					break
				case ErrWrongLeader:
					ok = false
					targetLeader = kv.UpdateTargetLeader(info.ToGID, len(servers))
				case ErrOutdatedShard:
					flag = true
					break
				case ErrRepeatedShard:
					flag = true
					break
				case ErrOvertime:
					ok = false
					targetLeader = kv.UpdateTargetLeader(info.ToGID, len(servers))
				}
			} else {
				kv.DPrintf("[%v] Fail to receive MergeShardDatas ACK from S%v-%v | "+
					"gid: %v | configNum: %v | shard: %v\n",
					basicInfo, info.ToGID, targetLeader, args.GID, args.ConfigNum, args.Shard)
				targetLeader = kv.UpdateTargetLeader(info.ToGID, len(servers))
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
			kv.DPrintf("[%v] Finish deleting shard %v on S%v-%v\n",
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
	labgob.Register(Nop{})
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
	kv.rf = raft.MakeWithGID(servers, me, persister, kv.applyCh, kv.gid)

	// Your initialization code here.
	// Use something like this to talk to the shardctrler:
	hostInfo := fmt.Sprintf("S%v-%v", kv.gid, kv.me)
	kv.mck = shardctrler.MakeClerkWithHostInfo(kv.ctrlers, hostInfo)
	kv.clientId2executedOpId = make(map[Int64Id]int)
	kv.index2processedResultCh = make(map[int]chan ProcessResult)
	kv.kvStore.Init()

	kv.curConfig.Groups = make(map[int][]string)
	kv.inShards = make(map[int]InShardInfo)
	kv.outShards = make(map[int]OutShardInfo)
	kv.newConfig = make(chan bool, 1)
	kv.checkCurConfigTime = time.Now()
	kv.DPrintf("[%v] Start new shard KV server | maxraftstate: %v | mck.clientId: %v\n",
		kv.BasicInfo(""), kv.maxraftstate, kv.mck.GetClientId())

	go kv.processor()
	go kv.configPoller()
	go kv.shardMigrant()

	return kv
}
