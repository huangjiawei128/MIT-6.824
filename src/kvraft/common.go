package kvraft

import (
	"fmt"
	"log"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	ProcessWaitTimeout = 250
)

const (
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
	ErrOvertime    = "ErrOvertime"
)

type Err string

func (err Err) String() string {
	var ret string
	switch err {
	case OK:
		ret = "OK"
	case ErrWrongLeader:
		ret = "ErrWrongLeader"
	case ErrOvertime:
		ret = "ErrOvertime"
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

//	==============================
//	Op
//	==============================
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

	Type  OpType
	Key   string
	Value string
}

func (op Op) String() string {
	var ret string
	ret = fmt.Sprintf("{Id: %v | ClientId: %v | Type: %v | Key: %v | Value: %v}",
		op.Id, op.ClientId, op.Type, Key2Str(op.Key), Value2Str(op.Value))
	return ret
}

type OpResult struct {
	Id       int
	ClientId Int64Id
	Value    string
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
		return fmt.Sprintf("C%v Clerk", ck.clientId)
	}
	return fmt.Sprintf("C%v Clerk.%v", ck.clientId, methodName)
}

func (ck *Clerk) UpdateTargetLeader() {
	ck.targetLeader = (ck.targetLeader + 1) % len(ck.servers)
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
//	KVServer
//	==============================
func (kv *KVServer) DPrintf(format string, a ...interface{}) (n int, err error) {
	if !kv.killed() {
		DPrintf(format, a...)
	}
	return
}

func (kv *KVServer) BasicInfo(methodName string) string {
	if methodName == "" {
		return fmt.Sprintf("S%v KVServer", kv.me)
	}
	return fmt.Sprintf("S%v KVServer.%v", kv.me, methodName)
}

func (kv *KVServer) GetProcessedOpResultCh(index int, create bool) chan OpResult {
	ch, ok := kv.index2processedOpResultCh[index]
	if !ok && create {
		ch = make(chan OpResult, 1)
		kv.index2processedOpResultCh[index] = ch
	}
	return ch
}

func (kv *KVServer) DeleteProcessedOpResultCh(index int) {
	delete(kv.index2processedOpResultCh, index)
}

func (kv *KVServer) NotifyProcessedOpResultCh(index int, opResult *OpResult) {
	kv.mu.Lock()
	ch := kv.GetProcessedOpResultCh(index, false)
	kv.mu.Unlock()
	if ch != nil {
		ch <- *opResult
	}
}

func (kv *KVServer) OpExecuted(clientId Int64Id, opId int) bool {
	executedOpId, ok := kv.clientId2executedOpId[clientId]
	if !ok {
		return false
	}
	return opId <= executedOpId
}
