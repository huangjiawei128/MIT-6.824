package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id       int
	ClientId Int64Id
	Type     string
	Key      string
	Value    string
}

func (op Op) String() string {
	ret := ""
	ret = fmt.Sprintf("Id: %v | ClientId: %v | Type: %v | Key: %v | Value: %v",
		op.Id, op.ClientId, op.Type, Key2Str(op.Key), Value2Str(op.Value))
	return ret
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	clientId2executedOpId map[Int64Id]int
	index2processedOpCh   map[int]chan Op
	kvStore               map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	opType := "Get"
	startOp := Op{
		Id:       args.OpId,
		ClientId: args.ClientId,
		Type:     opType,
		Key:      args.Key,
	}
	index, _, isLeader := kv.rf.Start(startOp)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.DPrintf("[S%v KVServer.Get(C%v-%v)] Refuse to %v V of K(%v) for C%v (isn't the leader)\n",
			kv.me, args.ClientId, args.OpId, opType, Key2Str(args.Key), args.ClientId)
		return
	}
	kv.mu.Lock()
	ch := kv.GetProcessedOpCh(index)
	kv.mu.Unlock()

	select {
	case executedOp := <-ch:
		if executedOp.ClientId != startOp.ClientId || executedOp.Id != startOp.Id {
			reply.Err = ErrWrongLeader
			kv.DPrintf("[S%v KVServer.Get(C%v-%v)] Refuse to %v V of K(%v) for C%v "+
				"(don't match op identifier at I%v: (%v,%v) VS (%v,%v))\n",
				kv.me, args.ClientId, args.OpId, opType, Key2Str(args.Key), args.ClientId,
				index, executedOp.ClientId, executedOp.Id, startOp.ClientId, startOp.Id)
		} else {
			reply.Err = OK
			reply.Value = executedOp.Value
			kv.DPrintf("[S%v KVServer.Get(C%v-%v)] %v V(%v) of K(%v) for C%v\n",
				kv.me, args.ClientId, args.OpId, opType, Value2Str(reply.Value), Key2Str(args.Key), args.ClientId)
		}
	case <-time.After(RpcTimeout * time.Millisecond):
		reply.Err = ErrWrongLeader
		kv.DPrintf("[S%v KVServer.Get(C%v-%v)] Refuse to %v V of K(%v) for C%v (rpc timeout)\n",
			kv.me, args.ClientId, args.OpId, opType, Key2Str(args.Key), args.ClientId)
	}

	kv.mu.Lock()
	kv.DeleteProcessedOpCh(index)
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	opType := args.Op
	startOp := Op{
		Id:       args.OpId,
		ClientId: args.ClientId,
		Type:     opType,
		Key:      args.Key,
		Value:    args.Value,
	}
	index, _, isLeader := kv.rf.Start(startOp)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.DPrintf("[S%v KVServer.PutAppend(C%v-%v)] Refuse to %v V(%v) of K(%v) for C%v (isn't the leader)\n",
			kv.me, args.ClientId, args.OpId, opType, Value2Str(args.Value), Key2Str(args.Key), args.ClientId)
		return
	}
	kv.mu.Lock()
	ch := kv.GetProcessedOpCh(index)
	kv.mu.Unlock()

	select {
	case executedOp := <-ch:
		if executedOp.ClientId != startOp.ClientId || executedOp.Id != startOp.Id {
			reply.Err = ErrWrongLeader
			kv.DPrintf("[S%v KVServer.PutAppend(C%v-%v)] Refuse to %v V(%v) of K(%v) for C%v "+
				"(don't match op identifier at I%v: (%v,%v) VS (%v,%v))\n",
				kv.me, args.ClientId, args.OpId, opType, Value2Str(args.Value), Key2Str(args.Key), args.ClientId,
				index, executedOp.ClientId, executedOp.Id, startOp.ClientId, startOp.Id)
		} else {
			reply.Err = OK
			kv.DPrintf("[S%v KVServer.PutAppend(C%v-%v)] %v V(%v) of K(%v) for C%v\n",
				kv.me, args.ClientId, args.OpId, opType, Value2Str(args.Value), Key2Str(args.Key), args.ClientId)
		}
	case <-time.After(RpcTimeout * time.Millisecond):
		reply.Err = ErrWrongLeader
		kv.DPrintf("[S%v KVServer.PutAppend(C%v-%v)] Refuse to %v V(%v) of K(%v) for C%v (rpc timeout)\n",
			kv.me, args.ClientId, args.OpId, opType, Value2Str(args.Value), Key2Str(args.Key), args.ClientId)
	}

	kv.mu.Lock()
	kv.DeleteProcessedOpCh(index)
	kv.mu.Unlock()
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	kv.DPrintf("[S%v] Be killed\n", kv.me)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) processor() {
	lastProcessed := 0
	for kv.killed() == false {
		m := <-kv.applyCh
		if m.SnapshotValid {
			// TODO
		} else if m.CommandValid && m.CommandIndex > lastProcessed {
			op := m.Command.(Op)
			kv.DPrintf("[S%v KVServer.applier] Receive the op to be processed \"%v\"\n",
				kv.me, op)
			kv.mu.Lock()
			if op.Type == "Get" {
				op.Value = kv.kvStore[op.Key]
				kv.clientId2executedOpId[op.ClientId] = op.Id
				kv.DPrintf("[S%v KVServer.applier] Execute the op \"%v\"\n",
					kv.me, op)
			} else {
				opBeforeExecuted := kv.OpExecuted(op.ClientId, op.Id)
				if !opBeforeExecuted {
					switch op.Type {
					case "Put":
						kv.kvStore[op.Key] = op.Value
					case "Append":
						kv.kvStore[op.Key] += op.Value
					}
					kv.clientId2executedOpId[op.ClientId] = op.Id
					kv.DPrintf("[S%v KVServer.applier] Execute the op \"%v\"\n",
						kv.me, op)
				} else {
					kv.DPrintf("[S%v KVServer.applier] Refuse to execute the duplicated op \"%v\"\n",
						kv.me, op)
				}
			}
			ch := kv.GetProcessedOpCh(m.CommandIndex)
			kv.mu.Unlock()

			kv.DPrintf("[S%v KVServer.applier] Before return the processed op \"%v\"\n",
				kv.me, op)
			ch <- op
			kv.DPrintf("[S%v KVServer.applier] After return the processed op \"%v\"\n",
				kv.me, op)
			lastProcessed = m.CommandIndex
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.clientId2executedOpId = make(map[Int64Id]int)
	kv.index2processedOpCh = make(map[int]chan Op)
	kv.kvStore = make(map[string]string)
	kv.DPrintf("[S%v] Start new KV server | maxraftstate: %v\n",
		kv.me, kv.maxraftstate)

	go kv.processor()

	return kv
}
