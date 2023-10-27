package shardkv

import (
	"fmt"
	"log"
	mathRand "math/rand"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	RpcTimeout = 250
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

func (err Err) String() string {
	var ret string
	switch err {
	case OK:
		ret = "OK"
	case ErrNoKey:
		ret = "ErrNoKey"
	case ErrWrongGroup:
		ret = "ErrWrongGroup"
	case ErrWrongLeader:
		ret = "ErrWrongLeader"
	}
	return ret
}

func Key2Str(key interface{}) string {
	ret := fmt.Sprintf("%v", key)
	maxLen := 50
	if len(ret) > maxLen {
		ret = ret[0:maxLen] + "..."
	}
	return ret
}

func Value2Str(value interface{}) string {
	ret := fmt.Sprintf("%v", value)
	maxLen := 1000
	if len(ret) > maxLen {
		ret = ret[0:maxLen] + "..."
	}
	return ret
}

// Put or Append
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

//	==============================
//	Clerk
//	==============================
func (ck *Clerk) DPrintf(format string, a ...interface{}) (n int, err error) {
	DPrintf(format, a...)
	return
}

func (ck *Clerk) BasicInfo(methodName string) string {
	if methodName == "" {
		return fmt.Sprintf("KV-C%v Clerk", ck.clientId)
	}
	return fmt.Sprintf("KV-C%v Clerk.%v", ck.clientId, methodName)
}

func (ck *Clerk) getTargetLeader(gid int, serverNum int) int {
	ret, ok := ck.gid2targetLeader[gid]
	if !ok {
		ret = mathRand.Intn(serverNum)
		ck.gid2targetLeader[gid] = ret
	}
	return ret
}

func (ck *Clerk) UpdateTargetLeader(gid int, serverNum int) int {
	oriTargetLeader := ck.getTargetLeader(gid, serverNum)
	newTargetLeader := (oriTargetLeader + 1) % serverNum
	ck.gid2targetLeader[gid] = newTargetLeader
	return newTargetLeader
}

//	==============================
//	KVStore
//	==============================
type KVStore struct {
	KVMap map[string]string
}

func (kvStore *KVStore) Get(key string) string {
	return kvStore.KVMap[key]
}

func (kvStore *KVStore) Put(key string, value string) {
	kvStore.KVMap[key] = value
}

func (kvStore *KVStore) Append(key string, value string) {
	kvStore.KVMap[key] += value
}

func (kvStore *KVStore) PutAppend(key string, value string, opType OpType) {
	switch opType {
	case PutKV:
		kvStore.Put(key, value)
	case AppendKV:
		kvStore.Append(key, value)
	}
}

//	==============================
//	ShardKV
//	==============================
func (kv *ShardKV) DPrintf(format string, a ...interface{}) (n int, err error) {
	if !kv.killed() {
		DPrintf(format, a...)
	}
	return
}

func (kv *ShardKV) BasicInfo(methodName string) string {
	if methodName == "" {
		return fmt.Sprintf("S%v-%v ShardKV", kv.gid, kv.me)
	}
	return fmt.Sprintf("S%v-%v ShardKV.%v", kv.gid, kv.me, methodName)
}

func (kv *ShardKV) GetProcessedOpCh(index int, create bool) chan Op {
	ch, ok := kv.index2processedOpCh[index]
	if !ok && create {
		ch = make(chan Op, 1)
		kv.index2processedOpCh[index] = ch
	}
	return ch
}

func (kv *ShardKV) DeleteProcessedOpCh(index int) {
	delete(kv.index2processedOpCh, index)
}

func (kv *ShardKV) OpExecuted(clientId Int64Id, opId int) bool {
	executedOpId, ok := kv.clientId2executedOpId[clientId]
	if !ok {
		return false
	}
	return opId <= executedOpId
}
