package shardkv

import (
	"6.824/shardctrler"
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
	//	client
	ConfigQueryInterval = 100

	//	server
	ProcessWaitTimeout  = 250
	ShardMigrateTimeout = 1000

	ConfigPollPeriod   = 100
	ShardMigratePeriod = 250
)

const (
	OK                 = "OK"
	ErrWrongGroup      = "ErrWrongGroup"
	ErrNotArrivedShard = "ErrNotArrivedShard"
	ErrWrongLeader     = "ErrWrongLeader"
	ErrOvertime        = "ErrOvertime"

	ErrAheadShard    = "ErrAheadShard"
	ErrOutdatedShard = "ErrOutdatedShard"
	ErrRepeatedShard = "ErrRepeatedShard"
)

type Err string

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
//	Command
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

	Type      OpType
	ConfigNum int
	Key       string
	Value     string
}

func (op Op) String() string {
	ret := fmt.Sprintf("{Id: %v | ClientId: %v | Type: %v | ConfigNum: %v | Key: %v | Value: %v}",
		op.Id, op.ClientId, op.Type, op.ConfigNum, Key2Str(op.Key), Value2Str(op.Value))
	return ret
}

type MergeReq struct {
	GID       int
	ConfigNum int
	Shard     int

	ShardData             ShardData
	ClientId2ExecutedOpId map[Int64Id]int
}

func (mReq MergeReq) String() string {
	ret := fmt.Sprintf("{GID: %v | ConfigNum: %v | Shard: %v | ClientId2ExecutedOpId: %v}",
		mReq.GID, mReq.ConfigNum, mReq.Shard, mReq.ClientId2ExecutedOpId)
	return ret
}

type DeleteReq struct {
	ConfigNum int
	Shard     int
}

func (dReq DeleteReq) String() string {
	ret := fmt.Sprintf("{ConfigNum: %v | Shard: %v}",
		dReq.ConfigNum, dReq.Shard)
	return ret
}

type CommandType int

const (
	OpCmd = iota
	MergeReqCmd
	DeleteReqCmd
	ConfigCmd
)

func (commandType CommandType) String() string {
	var ret string
	switch commandType {
	case OpCmd:
		ret = "Op"
	case MergeReqCmd:
		ret = "MergeReq"
	case DeleteReqCmd:
		ret = "DeleteReq"
	case ConfigCmd:
		ret = "Config"
	}
	return ret
}

type ProcessResult struct {
	Type CommandType

	Id       int
	ClientId Int64Id
	Value    string

	GID       int
	ConfigNum int
	Shard     int

	Err Err
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
type ShardData map[string]string

type KVStore struct {
	ShardDatas [shardctrler.NShards]ShardData //	shard -> shardData(K-Vs)
	ConfigNums [shardctrler.NShards]int       //	shard -> config number
}

func (kvStore *KVStore) Init() {
	for shard := 0; shard < shardctrler.NShards; shard++ {
		kvStore.ShardDatas[shard] = nil
		kvStore.ConfigNums[shard] = 0
	}
}

func (kvStore *KVStore) Get(key string) string {
	ret := ""
	if shardData := kvStore.ShardDatas[key2shard(key)]; shardData != nil {
		ret = shardData[key]
	}
	return ret
}

func (kvStore *KVStore) Put(key string, value string) {
	if shardData := kvStore.ShardDatas[key2shard(key)]; shardData != nil {
		shardData[key] = value
	}
}

func (kvStore *KVStore) Append(key string, value string) {
	if shardData := kvStore.ShardDatas[key2shard(key)]; shardData != nil {
		shardData[key] += value
	}
}

func (kvStore *KVStore) PutAppend(key string, value string, opType OpType) {
	switch opType {
	case PutKV:
		kvStore.Put(key, value)
	case AppendKV:
		kvStore.Append(key, value)
	}
}

func (kvStore *KVStore) ValidateShardData(shard int) {
	if shardData := kvStore.ShardDatas[shard]; shardData == nil {
		kvStore.ShardDatas[shard] = make(ShardData)
	}
}

func (kvStore *KVStore) InvalidateShardData(shard int) {
	kvStore.ShardDatas[shard] = nil
}

func (kvStore *KVStore) ShardDataValid(shard int) bool {
	return kvStore.ShardDatas[shard] != nil
}

func (kvStore *KVStore) MergeShardData(shard int, shardData ShardData) {
	if shardData == nil {
		kvStore.ShardDatas[shard] = nil
		return
	}
	kvStore.ShardDatas[shard] = make(ShardData)
	for key, value := range shardData {
		kvStore.ShardDatas[shard][key] = value
	}
}

func (kvStore *KVStore) GetShardConfigNum(shard int) int {
	return kvStore.ConfigNums[shard]
}

func (kvStore *KVStore) UpdateShardConfigNum(shard int, newConfigNum int) {
	if newConfigNum > kvStore.ConfigNums[shard] {
		kvStore.ConfigNums[shard] = newConfigNum
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

func (kv *ShardKV) GetProcessedResultCh(index int, create bool) chan ProcessResult {
	ch, ok := kv.index2processedResultCh[index]
	if !ok && create {
		ch = make(chan ProcessResult, 1)
		kv.index2processedResultCh[index] = ch
	}
	return ch
}

func (kv *ShardKV) DeleteProcessedResultCh(index int) {
	delete(kv.index2processedResultCh, index)
}

func (kv *ShardKV) NotifyProcessedResultCh(index int, result *ProcessResult) {
	kv.mu.Lock()
	ch := kv.GetProcessedResultCh(index, false)
	kv.mu.Unlock()
	if ch != nil {
		ch <- *result
	}
}

func (kv *ShardKV) OpExecuted(clientId Int64Id, opId int) bool {
	executedOpId, ok := kv.clientId2executedOpId[clientId]
	if !ok {
		return false
	}
	return opId <= executedOpId
}

func (kv *ShardKV) ValidateGroupShardDatas() {
	for shard := 0; shard < shardctrler.NShards; shard++ {
		if kv.curConfig.Shards[shard] == kv.gid {
			kv.kvStore.ValidateShardData(shard)
		}
	}
}

func (kv *ShardKV) UpdateInAndOutShards() {
	kv.inShards = make(map[int]int)
	kv.outShards = make(map[int]int)

	for shard := 0; shard < shardctrler.NShards; shard++ {
		if kv.curConfig.Shards[shard] == kv.gid && kv.prevConfig.Shards[shard] != kv.gid {
			kv.inShards[shard] = kv.prevConfig.Shards[shard]
		}

		if kv.curConfig.Shards[shard] != kv.gid && kv.prevConfig.Shards[shard] == kv.gid {
			kv.outShards[shard] = kv.curConfig.Shards[shard]
		}
	}
}

func (kv *ShardKV) getTargetLeader(gid int, serverNum int) int {
	ret, ok := kv.gid2targetLeader.Load(gid)
	if !ok {
		ret = mathRand.Intn(serverNum)
		kv.gid2targetLeader.Store(gid, ret)
	}
	return ret.(int)
}

func (kv *ShardKV) UpdateTargetLeader(gid int, serverNum int) int {
	oriTargetLeader := kv.getTargetLeader(gid, serverNum)
	newTargetLeader := (oriTargetLeader + 1) % serverNum
	kv.gid2targetLeader.Store(gid, newTargetLeader)
	return newTargetLeader
}
