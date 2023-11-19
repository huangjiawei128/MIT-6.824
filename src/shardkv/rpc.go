package shardkv

import "time"

//	==============================
//	Process Prepare
//	==============================
func (kv *ShardKV) prepareForOpProcess(op Op) (Err, int) {
	basicInfo := kv.BasicInfo("prepareForOpProcess")

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		if op.Type == GetV {
			kv.DPrintf("[%v(C%v-%v)] Refuse to %v V of K(%v) for C%v (isn't the leader)\n",
				basicInfo, op.ClientId, op.Id, op.Type, Key2Str(op.Key), op.ClientId)
		} else if op.Type == PutKV || op.Type == AppendKV {
			kv.DPrintf("[%v(C%v-%v)] Refuse to %v V(%v) of K(%v) for C%v (isn't the leader)\n",
				basicInfo, op.ClientId, op.Id, op.Type, Value2Str(op.Value), Key2Str(op.Key), op.ClientId)
		}
		return ErrWrongLeader, -1
	}

	return OK, index
}

func (kv *ShardKV) prepareForMergeReqProcess(mReq MergeReq) (Err, int) {
	basicInfo := kv.BasicInfo("prepareForMergeReqProcess")

	kv.mu.Lock()
	if mReq.ConfigNum < kv.curConfig.Num {
		kv.DPrintf("[%v(G%v-CF%v)] Refuse to merge shard %v from G%v "+
			"(shard outdated: %v(mReq.ConfigNum) < %v(curConfig.Num))\n",
			basicInfo, mReq.GID, mReq.ConfigNum, mReq.Shard, mReq.GID, mReq.ConfigNum, kv.curConfig.Num)
		kv.mu.Unlock()
		return ErrOutdatedShard, -1
	}

	shardConfigNum := kv.kvStore.GetShardConfigNum(mReq.Shard)
	if mReq.ConfigNum <= shardConfigNum {
		kv.DPrintf("[%v(G%v-CF%v)] Refuse to merge shard %v from G%v "+
			"(shard ahead: %v(mReq.ConfigNum) <= %v(shardConfigNum))\n",
			basicInfo, mReq.GID, mReq.ConfigNum, mReq.Shard, mReq.GID, mReq.ConfigNum, shardConfigNum)
		kv.mu.Unlock()
		return ErrRepeatedShard, -1
	}
	kv.mu.Unlock()

	index, _, isLeader := kv.rf.Start(mReq)
	if !isLeader {
		kv.DPrintf("[%v(G%v-CF%v)] Refuse to merge shard %v from G%v (isn't the leader)\n",
			basicInfo, mReq.GID, mReq.ConfigNum, mReq.Shard, mReq.GID)
		return ErrWrongLeader, -1
	}

	return OK, index
}

func (kv *ShardKV) prepareForDeleteReqProcess(dReq DeleteReq) (Err, int) {
	basicInfo := kv.BasicInfo("prepareForDeleteReqProcess")

	kv.mu.Lock()
	shardConfigNum := kv.kvStore.GetShardConfigNum(dReq.Shard)
	if dReq.ConfigNum <= shardConfigNum {
		kv.DPrintf("[%v(CF%v)] Refuse to delete shard %v "+
			"(shard ahead: %v(dReq.ConfigNum) <= %v(shardConfigNum))\n",
			basicInfo, dReq.ConfigNum, dReq.Shard, dReq.ConfigNum, kv.curConfig.Num)
		kv.mu.Unlock()
		return ErrRepeatedShard, -1
	}
	kv.mu.Unlock()

	index, _, isLeader := kv.rf.Start(dReq)
	if !isLeader {
		kv.DPrintf("[%v(CF%v)] Refuse to delete shard %v (isn't the leader)\n",
			basicInfo, dReq.ConfigNum, dReq.Shard)
		return ErrWrongLeader, -1
	}

	return OK, index
}

//	==============================
//	Process Wait
//	==============================
func (kv *ShardKV) waitForOpProcess(op Op, index int) (Err, string) {
	basicInfo := kv.BasicInfo("waitForOpProcess")

	kv.mu.Lock()
	ch := kv.GetProcessedResultCh(index, true)
	kv.mu.Unlock()

	var (
		err   Err
		value string
	)
	timer := time.NewTimer(ProcessWaitTimeout * time.Millisecond)
	select {
	case processedResult := <-ch:
		if processedResult.Type != OpCmd {
			err = ErrWrongLeader
			if op.Type == GetV {
				kv.DPrintf("[%v(C%v-%v)] Refuse to %v V of K(%v) for C%v "+
					"(isn't Op, but %v at I%v)\n",
					basicInfo, op.ClientId, op.Id, op.Type, Key2Str(op.Key), op.ClientId,
					processedResult.Type, index)
			} else if op.Type == PutKV || op.Type == AppendKV {
				kv.DPrintf("[%v(C%v-%v)] Refuse to %v V(%v) of K(%v) for C%v "+
					"(isn't Op, but %v at I%v)\n",
					basicInfo, op.ClientId, op.Id, op.Type, Value2Str(op.Value), Key2Str(op.Key), op.ClientId,
					processedResult.Type, index)
			}
		} else if processedResult.ClientId != op.ClientId || processedResult.Id != op.Id {
			err = ErrWrongLeader
			if op.Type == GetV {
				kv.DPrintf("[%v(C%v-%v)] Refuse to %v V of K(%v) for C%v "+
					"(don't match op identifier at I%v: (%v,%v) VS (%v,%v))\n",
					basicInfo, op.ClientId, op.Id, op.Type, Key2Str(op.Key), op.ClientId,
					index, processedResult.ClientId, processedResult.Id, op.ClientId, op.Id)
			} else if op.Type == PutKV || op.Type == AppendKV {
				kv.DPrintf("[%v(C%v-%v)] Refuse to %v V(%v) of K(%v) for C%v "+
					"(don't match op identifier at I%v: (%v,%v) VS (%v,%v))\n",
					basicInfo, op.ClientId, op.Id, op.Type, Value2Str(op.Value), Key2Str(op.Key), op.ClientId,
					index, processedResult.ClientId, processedResult.Id, op.ClientId, op.Id)
			}
		} else {
			err = processedResult.Err
			switch err {
			case ErrWrongGroup:
				if op.Type == GetV {
					kv.DPrintf("[%v(C%v-%v)] Refuse to %v V of K(%v) for C%v "+
						"(don't match GID)\n",
						basicInfo, op.ClientId, op.Id, op.Type, Key2Str(op.Key), op.ClientId)
				} else if op.Type == PutKV || op.Type == AppendKV {
					kv.DPrintf("[%v(C%v-%v)] Refuse to %v V(%v) of K(%v) for C%v "+
						"(don't match GID)\n",
						basicInfo, op.ClientId, op.Id, op.Type, Value2Str(op.Value), Key2Str(op.Key), op.ClientId)
				}
			case ErrNotArrivedShard:
				if op.Type == GetV {
					kv.DPrintf("[%v(C%v-%v)] Refuse to %v V of K(%v) for C%v "+
						"(shard %v not arrived)\n",
						basicInfo, op.ClientId, op.Id, op.Type, Key2Str(op.Key), op.ClientId,
						key2shard(op.Key))
				} else if op.Type == PutKV || op.Type == AppendKV {
					kv.DPrintf("[%v(C%v-%v)] Refuse to %v V(%v) of K(%v) for C%v "+
						"(shard %v not arrived)\n",
						basicInfo, op.ClientId, op.Id, op.Type, Value2Str(op.Value), Key2Str(op.Key), op.ClientId,
						key2shard(op.Key))
				}
			case OK:
				if op.Type == GetV {
					value = processedResult.Value
					kv.DPrintf("[%v(C%v-%v)] %v V(%v) of K(%v) for C%v\n",
						basicInfo, op.ClientId, op.Id, op.Type, Value2Str(value), Key2Str(op.Key), op.ClientId)
				} else if op.Type == PutKV || op.Type == AppendKV {
					kv.DPrintf("[%v(C%v-%v)] %v V(%v) of K(%v) for C%v\n",
						basicInfo, op.ClientId, op.Id, op.Type, Value2Str(op.Value), Key2Str(op.Key), op.ClientId)
				}
			}
		}
	case <-timer.C:
		err = ErrOvertime
		if op.Type == GetV {
			kv.DPrintf("[%v(C%v-%v)] Refuse to %v V of K(%v) for C%v (rpc overtime)\n",
				basicInfo, op.ClientId, op.Id, op.Type, Key2Str(op.Key), op.ClientId)
		} else if op.Type == PutKV || op.Type == AppendKV {
			kv.DPrintf("[%v(C%v-%v)] Refuse to %v V(%v) of K(%v) for C%v (rpc overtime)\n",
				basicInfo, op.ClientId, op.Id, op.Type, Value2Str(op.Value), Key2Str(op.Key), op.ClientId)
		}
	}
	timer.Stop()

	kv.mu.Lock()
	kv.DeleteProcessedResultCh(index)
	kv.mu.Unlock()

	return err, value
}

func (kv *ShardKV) waitForMergeReqProcess(mReq MergeReq, index int) Err {
	basicInfo := kv.BasicInfo("waitForMergeReqProcess")

	kv.mu.Lock()
	ch := kv.GetProcessedResultCh(index, true)
	kv.mu.Unlock()

	var err Err
	timer := time.NewTimer(ProcessWaitTimeout * time.Millisecond)
	select {
	case processedResult := <-ch:
		if processedResult.Type != MergeReqCmd {
			err = ErrWrongLeader
			kv.DPrintf("[%v(G%v-CF%v)] Refuse to merge shard %v from G%v "+
				"(isn't MergeReq, but %v at I%v)\n",
				basicInfo, mReq.GID, mReq.ConfigNum, mReq.Shard, mReq.GID,
				processedResult.Type, index)
		} else if processedResult.GID != mReq.GID || processedResult.ConfigNum != mReq.ConfigNum ||
			processedResult.Shard != mReq.Shard {
			err = ErrWrongLeader
			kv.DPrintf("[%v(G%v-CF%v)] Refuse to merge shard %v from G%v "+
				"(don't match mReq identifier at I%v: (%v,%v,%v) VS (%v,%v,%v))\n",
				basicInfo, mReq.GID, mReq.ConfigNum, mReq.Shard, mReq.GID,
				index, processedResult.GID, processedResult.ConfigNum, processedResult.Shard,
				mReq.GID, mReq.ConfigNum, mReq.Shard)
		} else {
			err = processedResult.Err
			switch err {
			case ErrOutdatedShard:
				kv.DPrintf("[%v(G%v-CF%v)] Refuse to merge shard %v from G%v (shard outdated)\n",
					basicInfo, mReq.GID, mReq.ConfigNum, mReq.Shard, mReq.GID)
			case ErrRepeatedShard:
				kv.DPrintf("[%v(G%v-CF%v)] Refuse to merge shard %v from G%v (shard has been merged)\n",
					basicInfo, mReq.GID, mReq.ConfigNum, mReq.Shard, mReq.GID)
			case OK:
				kv.DPrintf("[%v(G%v-CF%v)] Merge shard %v from G%v\n",
					basicInfo, mReq.GID, mReq.ConfigNum, mReq.Shard, mReq.GID)
			}
		}
	case <-timer.C:
		err = ErrOvertime
		kv.DPrintf("[%v(G%v-CF%v)] Refuse to merge shard %v from G%v (rpc overtime)\n",
			basicInfo, mReq.GID, mReq.ConfigNum, mReq.Shard, mReq.GID)
	}
	timer.Stop()

	kv.mu.Lock()
	kv.DeleteProcessedResultCh(index)
	kv.mu.Unlock()

	return err
}

func (kv *ShardKV) waitForDeleteReqProcess(dReq DeleteReq, index int) Err {
	basicInfo := kv.BasicInfo("waitForDeleteReqProcess")

	kv.mu.Lock()
	ch := kv.GetProcessedResultCh(index, true)
	kv.mu.Unlock()

	var err Err
	timer := time.NewTimer(ProcessWaitTimeout * time.Millisecond)
	select {
	case processedResult := <-ch:
		if processedResult.Type != DeleteReqCmd {
			err = ErrWrongLeader
			kv.DPrintf("[%v(CF%v)] Refuse to delete shard %v "+
				"(isn't DeleteReq, but %v at I%v)\n",
				basicInfo, dReq.ConfigNum, dReq.Shard,
				processedResult.Type, index)
		} else if processedResult.ConfigNum != dReq.ConfigNum || processedResult.Shard != dReq.Shard {
			err = ErrWrongLeader
			kv.DPrintf("[%v(CF%v)] Refuse to delete shard %v "+
				"(don't match mReq identifier at I%v: (%v,%v) VS (%v,%v))\n",
				basicInfo, dReq.ConfigNum, dReq.Shard,
				index, processedResult.ConfigNum, processedResult.Shard, dReq.ConfigNum, dReq.Shard)
		} else {
			err = processedResult.Err
			switch err {
			case ErrRepeatedShard:
				kv.DPrintf("[%v(CF%v)] Delete shard %v (shard has been deleted)\n",
					basicInfo, dReq.ConfigNum, dReq.Shard)
			case OK:
				kv.DPrintf("[%v(CF%v)] Delete shard %v\n",
					basicInfo, dReq.ConfigNum, dReq.Shard)
			}
		}
	case <-timer.C:
		err = ErrOvertime
		kv.DPrintf("[%v(CF%v)] Refuse to delete shard %v (rpc overtime)\n",
			basicInfo, dReq.ConfigNum, dReq.Shard)
	}
	timer.Stop()

	kv.mu.Lock()
	kv.DeleteProcessedResultCh(index)
	kv.mu.Unlock()

	return err
}

//	==============================
//	Get RPC
//	==============================
type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId Int64Id
	OpId     int
}

type GetReply struct {
	Err   Err
	Value string
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	opType := OpType(GetV)
	op := Op{
		Id:       args.OpId,
		ClientId: args.ClientId,
		Type:     opType,
		Key:      args.Key,
	}

	prepareErr, index := kv.prepareForOpProcess(op)
	if prepareErr != OK {
		reply.Err = prepareErr
		return
	}

	reply.Err, reply.Value = kv.waitForOpProcess(op, index)
}

//	==============================
//	PutAppend RPC
//	==============================
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    OpType // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId Int64Id
	OpId     int
}

type PutAppendReply struct {
	Err Err
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	opType := args.Op
	op := Op{
		Id:       args.OpId,
		ClientId: args.ClientId,
		Type:     opType,
		Key:      args.Key,
		Value:    args.Value,
	}

	prepareErr, index := kv.prepareForOpProcess(op)
	if prepareErr != OK {
		reply.Err = prepareErr
		return
	}

	reply.Err, _ = kv.waitForOpProcess(op, index)
}

//	==============================
//	MergeShardDatas RPC
//	==============================
type MergeShardDatasArgs struct {
	GID                   int
	ConfigNum             int
	Shard                 int
	ShardData             ShardData
	ClientId2ExecutedOpId map[Int64Id]int
}

type MergeShardDatasReply struct {
	Err Err
}

func (kv *ShardKV) MergeShardDatas(args *MergeShardDatasArgs, reply *MergeShardDatasReply) {
	// Your code here.
	mReq := MergeReq{
		GID:                   args.GID,
		ConfigNum:             args.ConfigNum,
		Shard:                 args.Shard,
		ShardData:             args.ShardData,
		ClientId2ExecutedOpId: args.ClientId2ExecutedOpId,
	}

	prepareErr, index := kv.prepareForMergeReqProcess(mReq)
	if prepareErr != OK {
		reply.Err = prepareErr
		return
	}

	reply.Err = kv.waitForMergeReqProcess(mReq, index)
}

//	==============================
//	DeleteShardDatas Process
//	==============================
type DeleteShardDatasArgs struct {
	ConfigNum int
	Shard     int
}

type DeleteShardDatasReply struct {
	Err Err
}

func (kv *ShardKV) DeleteShardDatas(args *DeleteShardDatasArgs, reply *DeleteShardDatasReply) {
	// Your code here.
	dReq := DeleteReq{
		ConfigNum: args.ConfigNum,
		Shard:     args.Shard,
	}

	prepareErr, index := kv.prepareForDeleteReqProcess(dReq)
	if prepareErr != OK {
		reply.Err = prepareErr
		return
	}

	reply.Err = kv.waitForDeleteReqProcess(dReq, index)
}
