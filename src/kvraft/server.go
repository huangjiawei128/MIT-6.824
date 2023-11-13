package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
)

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	clientId2executedOpId     map[Int64Id]int
	index2processedOpResultCh map[int]chan OpResult
	kvStore                   KVStore
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
	basicInfo := kv.BasicInfo("Kill")

	kv.DPrintf("[%v] Be killed\n", basicInfo)
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) snapshotData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.clientId2executedOpId)
	e.Encode(kv.kvStore)
	return w.Bytes()
}

func (kv *KVServer) readSnapshot(data []byte) {
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

func (kv *KVServer) processor() {
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
			op := m.Command.(Op)
			kv.DPrintf("[%v] Receive the op at I%v to be processed %v | lastProcessed: %v\n",
				basicInfo, m.CommandIndex, &op, lastProcessed)
			kv.processOpCommand(&op, m.CommandIndex)

			raftStateSize := kv.rf.GetPersister().RaftStateSize()
			if kv.maxraftstate > 0 && raftStateSize > kv.maxraftstate {
				kv.mu.Lock()
				kv.DPrintf("[%v] Prepare to snapshot data at I%v | raftStateSize: %v > maxraftstate: %v > 0\n",
					basicInfo, m.CommandIndex, raftStateSize, kv.maxraftstate)
				snapshotData := kv.snapshotData()
				kv.mu.Unlock()
				kv.rf.Snapshot(m.CommandIndex, snapshotData)
			}

			lastProcessed = m.CommandIndex
		}
	}
}

func (kv *KVServer) processSnapshot(snapShot []byte, index int) {
	basicInfo := kv.BasicInfo("processSnapshot")

	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.readSnapshot(snapShot)
	kv.DPrintf("[%v] Finish reading the snapshot at I%v\n",
		basicInfo, index)
}

func (kv *KVServer) processOpCommand(op *Op, index int) {
	basicInfo := kv.BasicInfo("processOpCommand")

	opResult := OpResult{
		Id:       op.Id,
		ClientId: op.ClientId,
		Value:    op.Value,
	}

	kv.mu.Lock()
	defer func() {
		kv.mu.Unlock()
		kv.NotifyProcessedOpResultCh(index, &opResult)
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

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.clientId2executedOpId = make(map[Int64Id]int)
	kv.index2processedOpResultCh = make(map[int]chan OpResult)
	kv.kvStore.KVMap = make(map[string]string)
	kv.DPrintf("[%v] Start new KV server | maxraftstate: %v\n",
		kv.BasicInfo(""), kv.maxraftstate)

	go kv.processor()

	return kv
}
