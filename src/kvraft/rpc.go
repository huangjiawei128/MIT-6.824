package kvraft

import "time"

//	==============================
//	Process Prepare
//	==============================
func (kv *KVServer) prepareForProcess(op Op) (Err, int) {
	basicInfo := kv.BasicInfo("prepareForProcess")

	index, _, isLeader := kv.rf.Start(op)
	var err Err = OK
	if !isLeader {
		err = ErrWrongLeader
		if op.Type == GetV {
			kv.DPrintf("[%v(C%v-%v)] Refuse to %v V of K(%v) for C%v (isn't the leader)\n",
				basicInfo, op.ClientId, op.Id, op.Type, Key2Str(op.Key), op.ClientId)
		} else if op.Type == PutKV || op.Type == AppendKV {
			kv.DPrintf("[%v(C%v-%v)] Refuse to %v V(%v) of K(%v) for C%v (isn't the leader)\n",
				basicInfo, op.ClientId, op.Id, op.Type, Value2Str(op.Value), Key2Str(op.Key), op.ClientId)
		}
	}

	return err, index
}

//	==============================
//	Process Wait
//	==============================
func (kv *KVServer) waitForProcess(op Op, index int) (Err, string) {
	basicInfo := kv.BasicInfo("waitForProcess")

	kv.mu.Lock()
	ch := kv.GetProcessedOpResultCh(index, true)
	kv.mu.Unlock()

	var (
		err   Err
		value string
	)
	timer := time.NewTimer(ProcessWaitTimeout * time.Millisecond)
	select {
	case processedOpResult := <-ch:
		if processedOpResult.ClientId != op.ClientId || processedOpResult.Id != op.Id {
			err = ErrWrongLeader
			if op.Type == GetV {
				kv.DPrintf("[%v(C%v-%v)] Refuse to %v V of K(%v) for C%v "+
					"(don't match op identifier at I%v: (%v,%v) VS (%v,%v))\n",
					basicInfo, op.ClientId, op.Id, op.Type, Key2Str(op.Key), op.ClientId,
					index, processedOpResult.ClientId, processedOpResult.Id, op.ClientId, op.Id)
			} else if op.Type == PutKV || op.Type == AppendKV {
				kv.DPrintf("[%v(C%v-%v)] Refuse to %v V(%v) of K(%v) for C%v "+
					"(don't match op identifier at I%v: (%v,%v) VS (%v,%v))\n",
					basicInfo, op.ClientId, op.Id, op.Type, Value2Str(op.Value), Key2Str(op.Key), op.ClientId,
					index, processedOpResult.ClientId, processedOpResult.Id, op.ClientId, op.Id)
			}
		} else {
			err = OK
			if op.Type == GetV {
				value = processedOpResult.Value
				kv.DPrintf("[%v(C%v-%v)] %v V(%v) of K(%v) for C%v\n",
					basicInfo, op.ClientId, op.Id, op.Type, Value2Str(value), Key2Str(op.Key), op.ClientId)
			} else if op.Type == PutKV || op.Type == AppendKV {
				kv.DPrintf("[%v(C%v-%v)] %v V(%v) of K(%v) for C%v\n",
					basicInfo, op.ClientId, op.Id, op.Type, Value2Str(op.Value), Key2Str(op.Key), op.ClientId)
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
	kv.DeleteProcessedOpResultCh(index)
	kv.mu.Unlock()

	return err, value
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

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	opType := OpType(GetV)
	op := Op{
		Id:       args.OpId,
		ClientId: args.ClientId,
		Type:     opType,
		Key:      args.Key,
	}

	prepareErr, index := kv.prepareForProcess(op)
	if prepareErr != OK {
		reply.Err = prepareErr
		return
	}

	reply.Err, reply.Value = kv.waitForProcess(op, index)
}

//	==============================
//	PutAppend RPC
//	==============================
type PutAppendArgs struct {
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

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	opType := args.Op
	op := Op{
		Id:       args.OpId,
		ClientId: args.ClientId,
		Type:     opType,
		Key:      args.Key,
		Value:    args.Value,
	}

	prepareErr, index := kv.prepareForProcess(op)
	if prepareErr != OK {
		reply.Err = prepareErr
		return
	}

	reply.Err, _ = kv.waitForProcess(op, index)
}
