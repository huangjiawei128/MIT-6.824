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

type OpType int

const (
	GetV = iota
	PutKV
	AppendKV
)

func (opType OpType) String() string {
	var ret string
	switch opType {
	case GetV:
		ret = "Get"
	case PutKV:
		ret = "Put"
	case AppendKV:
		ret = "Append"
	}
	return ret
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id       int
	ClientId Int64Id
	Type     OpType
	Key      string
	Value    string
}

func (op Op) String() string {
	ret := ""
	ret = fmt.Sprintf("Id: %v | ClientId: %v | Type: %v | Key: %v | Value: %v",
		op.Id, op.ClientId, op.Type, Key2Str(op.Key), Value2Str(op.Value))
	return ret
}

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

	mck                   *shardctrler.Clerk
	clientId2executedOpId map[Int64Id]int
	index2processedOpCh   map[int]chan Op
	kvStore               KVStore
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	basicInfo := kv.BasicInfo("Get")

	// Your code here.
	opType := OpType(GetV)
	startOp := Op{
		Id:       args.OpId,
		ClientId: args.ClientId,
		Type:     opType,
		Key:      args.Key,
	}
	index, _, isLeader := kv.rf.Start(startOp)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.DPrintf("[%v(C%v-%v)] Refuse to %v V of K(%v) for C%v (isn't the leader)\n",
			basicInfo, args.ClientId, args.OpId, opType, Key2Str(args.Key), args.ClientId)
		return
	}
	kv.mu.Lock()
	ch := kv.GetProcessedOpCh(index)
	kv.mu.Unlock()

	timer := time.NewTimer(RpcTimeout * time.Millisecond)
	select {
	case executedOp := <-ch:
		if executedOp.ClientId != startOp.ClientId || executedOp.Id != startOp.Id {
			reply.Err = ErrWrongLeader
			kv.DPrintf("[%v(C%v-%v)] Refuse to %v V of K(%v) for C%v "+
				"(don't match op identifier at I%v: (%v,%v) VS (%v,%v))\n",
				basicInfo, args.ClientId, args.OpId, opType, Key2Str(args.Key), args.ClientId,
				index, executedOp.ClientId, executedOp.Id, startOp.ClientId, startOp.Id)
		} else {
			reply.Err = OK
			reply.Value = executedOp.Value
			kv.DPrintf("[%v(C%v-%v)] %v V(%v) of K(%v) for C%v\n",
				basicInfo, args.ClientId, args.OpId, opType, Value2Str(reply.Value), Key2Str(args.Key), args.ClientId)
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
		kv.DPrintf("[%v(C%v-%v)] Refuse to %v V of K(%v) for C%v (rpc timeout)\n",
			basicInfo, args.ClientId, args.OpId, opType, Key2Str(args.Key), args.ClientId)
	}
	timer.Stop()

	kv.mu.Lock()
	kv.DeleteProcessedOpCh(index)
	kv.mu.Unlock()
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	basicInfo := kv.BasicInfo("PutAppend")

	// Your code here.
	opType := args.Op
	startOp := Op{
		Id:       args.OpId,
		ClientId: args.ClientId,
		Type:     args.Op,
		Key:      args.Key,
		Value:    args.Value,
	}
	index, _, isLeader := kv.rf.Start(startOp)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.DPrintf("[%v(C%v-%v)] Refuse to %v V(%v) of K(%v) for C%v (isn't the leader)\n",
			basicInfo, args.ClientId, args.OpId, opType, Value2Str(args.Value), Key2Str(args.Key), args.ClientId)
		return
	}
	kv.mu.Lock()
	ch := kv.GetProcessedOpCh(index)
	kv.mu.Unlock()

	timer := time.NewTimer(RpcTimeout * time.Millisecond)
	select {
	case executedOp := <-ch:
		if executedOp.ClientId != startOp.ClientId || executedOp.Id != startOp.Id {
			reply.Err = ErrWrongLeader
			kv.DPrintf("[%v(C%v-%v)] Refuse to %v V(%v) of K(%v) for C%v "+
				"(don't match op identifier at I%v: (%v,%v) VS (%v,%v))\n",
				basicInfo, args.ClientId, args.OpId, opType, Value2Str(args.Value), Key2Str(args.Key), args.ClientId,
				index, executedOp.ClientId, executedOp.Id, startOp.ClientId, startOp.Id)
		} else {
			reply.Err = OK
			kv.DPrintf("[%v(C%v-%v)] %v V(%v) of K(%v) for C%v\n",
				basicInfo, args.ClientId, args.OpId, opType, Value2Str(args.Value), Key2Str(args.Key), args.ClientId)
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
		kv.DPrintf("[%v(C%v-%v)] Refuse to %v V(%v) of K(%v) for C%v (rpc timeout)\n",
			basicInfo, args.ClientId, args.OpId, opType, Value2Str(args.Value), Key2Str(args.Key), args.ClientId)
	}
	timer.Stop()

	kv.mu.Lock()
	kv.DeleteProcessedOpCh(index)
	kv.mu.Unlock()
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
	return w.Bytes()
}

func (kv *ShardKV) readSnapshot(data []byte) {
	basicInfo := kv.BasicInfo("readSnapshot")

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var clientId2executedOpId map[Int64Id]int
	var kvStore KVStore
	if d.Decode(&clientId2executedOpId) != nil ||
		d.Decode(&kvStore) != nil {
		errorMsg := fmt.Sprintf("[%v] Decode error\n", basicInfo)
		panic(errorMsg)
	} else {
		kv.clientId2executedOpId = clientId2executedOpId
		kv.kvStore = kvStore
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
			op := m.Command.(Op)
			kv.DPrintf("[%v] Receive the op to be processed \"%v\" | index: %v | lastProcessed: %v\n",
				basicInfo, op, m.CommandIndex, lastProcessed)

			kv.mu.Lock()
			oriExecutedOpId, ok := kv.clientId2executedOpId[op.ClientId]
			if !ok {
				kv.DPrintf("[%v] Haven't executed any ops of C%v\n",
					basicInfo, op.ClientId)
			} else {
				kv.DPrintf("[%v] The max executed op.Id of C%v is %v\n",
					basicInfo, op.ClientId, oriExecutedOpId)
			}

			if op.Type == GetV {
				op.Value = kv.kvStore.Get(op.Key)
				kv.clientId2executedOpId[op.ClientId] = op.Id
				kv.DPrintf("[%v] Execute the op \"%v\"\n",
					basicInfo, op)
			} else {
				opBeforeExecuted := kv.OpExecuted(op.ClientId, op.Id)
				if !opBeforeExecuted {
					kv.kvStore.PutAppend(op.Key, op.Value, op.Type)
					kv.clientId2executedOpId[op.ClientId] = op.Id
					kv.DPrintf("[%v] Execute the op \"%v\" | stored value: %v\n",
						basicInfo, op, kv.kvStore.Get(op.Key))
				} else {
					kv.DPrintf("[%v] Refuse to execute the duplicated op \"%v\" | stored value: %v\n",
						basicInfo, op, kv.kvStore.Get(op.Key))
				}
			}
			ch := kv.GetProcessedOpCh(m.CommandIndex)
			kv.mu.Unlock()

			raftStateSize := kv.rf.GetPersister().RaftStateSize()
			if kv.maxraftstate > 0 && raftStateSize > kv.maxraftstate {
				kv.DPrintf("[%v] Prepare snapshot data | index: %v | raftStateSize: %v > maxraftstate: %v > 0\n",
					basicInfo, m.CommandIndex, raftStateSize, kv.maxraftstate)
				kv.mu.Lock()
				snapshotData := kv.snapshotData()
				kv.mu.Unlock()
				kv.rf.Snapshot(m.CommandIndex, snapshotData)
			}

			ch <- op
			kv.DPrintf("[%v] After return the processed op \"%v\"\n",
				basicInfo, op)
			lastProcessed = m.CommandIndex
		}
	}
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
	kv.index2processedOpCh = make(map[int]chan Op)
	kv.kvStore.KVMap = make(map[string]string)
	kv.DPrintf("[%v] Start new shard KV server | maxraftstate: %v\n",
		kv.BasicInfo(""), kv.maxraftstate)

	go kv.processor()

	return kv
}
