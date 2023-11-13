package shardctrler

import (
	"fmt"
	"log"
	"sort"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	ProcessWaitTimeout = 250
)

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func (config Config) String() string {
	ret := fmt.Sprintf("{Num: %v | Shards: %v | Groups: %v}",
		config.Num, config.Shards, config.Groups)
	return ret
}

type RGInfo struct {
	GID      int
	ShardNum int
}

const (
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
	ErrOvertime    = "ErrOverTime"
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

//	==============================
//	Op
//	==============================
type OpType int

const (
	JoinRG = iota
	LeaveRG
	MoveSD
	QueryCF
)

func (opType OpType) String() string {
	var ret string
	switch opType {
	case JoinRG:
		ret = "Join"
	case LeaveRG:
		ret = "Leave"
	case MoveSD:
		ret = "Move"
	case QueryCF:
		ret = "Query"
	}
	return ret
}

type Op struct {
	// Your data here.
	Id       int
	ClientId Int64Id

	Type      OpType
	Servers   map[int][]string
	GIDs      []int
	Shard     int
	ConfigNum int
}

type OpResult struct {
	Id       int
	ClientId Int64Id
	Config   *Config
}

func (op Op) String() string {
	var ret string
	ret = fmt.Sprintf("Op{Id: %v | ClientId: %v | Type: %v | ",
		op.Id, op.ClientId, op.Type)
	switch op.Type {
	case QueryCF:
		ret += fmt.Sprintf("ConfigNum: %v", op.ConfigNum)
	case JoinRG:
		ret += fmt.Sprintf("Servers: %v", op.Servers)
	case LeaveRG:
		ret += fmt.Sprintf("GIDs: %v", op.GIDs)
	case MoveSD:
		ret += fmt.Sprintf("Shard: %v | GID: %v", op.Shard, op.GIDs[0])
	}
	ret += "}"
	return ret
}

//	==============================
//	Clerk
//	==============================
func (ck *Clerk) DPrintf(format string, a ...interface{}) (n int, err error) {
	DPrintf(format, a...)
	return
}

func (ck *Clerk) BasicInfo(methodName string) string {
	var ret string
	if ck.hostInfo == "" {
		ret = fmt.Sprintf("Ctrler-C%v Clerk", ck.clientId)
	} else {
		ret = fmt.Sprintf("Ctrler-C%v %v Clerk", ck.clientId, ck.hostInfo)
	}
	if methodName != "" {
		ret += fmt.Sprintf(".%v", methodName)
	}
	return ret
}

func (ck *Clerk) UpdateTargetLeader() {
	ck.targetLeader = (ck.targetLeader + 1) % len(ck.servers)
}

func (ck *Clerk) GetClientId() Int64Id {
	return ck.clientId
}

//	==============================
//	Config
//	==============================
func (config *Config) DPrintf(format string, a ...interface{}) (n int, err error) {
	DPrintf(format, a...)
	return
}

func (config *Config) BasicInfo(methodName string) string {
	if methodName == "" {
		return fmt.Sprintf("CF%v Config", config.Num)
	}
	return fmt.Sprintf("CF%v Config.%v", config.Num, methodName)
}

func (config *Config) DeepCopyConfig(configCopy *Config) {
	configCopy.Num = config.Num
	configCopy.Shards = config.Shards
	configCopy.Groups = make(map[int][]string)
	for gid, serverList := range config.Groups {
		configCopy.Groups[gid] = serverList
	}
}

func getRGInfos(gid2Shards map[int][]int) []RGInfo {
	rgInfos := make([]RGInfo, 0)
	for gid, shards := range gid2Shards {
		rgInfos = append(rgInfos, RGInfo{GID: gid, ShardNum: len(shards)})
	}

	sort.Slice(rgInfos, func(i, j int) bool {
		if rgInfos[i].ShardNum == rgInfos[j].ShardNum {
			return rgInfos[i].GID < rgInfos[j].GID
		}
		return rgInfos[i].ShardNum < rgInfos[j].ShardNum
	})
	return rgInfos
}

func getGoalShardNum(shardNum int, rgNum int) []int {
	goalShardNum := make([]int, rgNum)
	minNum, leftNum := shardNum/rgNum, shardNum%rgNum
	for i := 0; i < rgNum; i++ {
		goalShardNum[i] = minNum
		if i+leftNum >= rgNum {
			goalShardNum[i]++
		}
	}
	return goalShardNum
}

func (config *Config) Rebalance(gid2Shards map[int][]int, leftShards []int) {
	basicInfo := config.BasicInfo("Rebalance")

	rgNum := len(config.Groups)
	if rgNum != len(gid2Shards) {
		errMsg := fmt.Sprintf("[%v] rgNum %v != len(gid2Shards) %v\n",
			basicInfo, rgNum, len(gid2Shards))
		panic(errMsg)
	}

	if rgNum == 0 {
		for i := 0; i < NShards; i++ {
			config.Shards[i] = 0
		}
		return
	}

	rgInfos := getRGInfos(gid2Shards)
	goalShardNum := getGoalShardNum(NShards, rgNum)
	config.DPrintf("[%v] rgInfos: %v | goalShardNum: %v\n",
		basicInfo, rgInfos, goalShardNum)

	for i := rgNum - 1; i >= 0; i-- {
		newLeftShardNum := rgInfos[i].ShardNum - goalShardNum[i]
		gid := rgInfos[i].GID
		if newLeftShardNum <= 0 {
			continue
		}

		leftShards = append(leftShards, gid2Shards[gid][:newLeftShardNum]...)
		rgInfos[i].ShardNum = goalShardNum[i]
	}
	config.DPrintf("[%v] leftShards after adjusting: %v\n",
		basicInfo, leftShards)

	leftShardsIndex := 0
	for i := 0; i < rgNum; i++ {
		fillShardNum := goalShardNum[i] - rgInfos[i].ShardNum
		if fillShardNum < 0 {
			errMsg := fmt.Sprintf("[%v] After adjusting leftShards: goalShardNum[%v] %v < rgInfos[%v].ShardNum %v\n",
				basicInfo, i, goalShardNum[i], i, rgInfos[i].ShardNum)
			panic(errMsg)
		}
		if fillShardNum == 0 {
			continue
		}

		gid := rgInfos[i].GID
		for j := 0; j < fillShardNum; j++ {
			config.Shards[leftShards[leftShardsIndex]] = gid
			leftShardsIndex++
		}
		rgInfos[i].ShardNum = goalShardNum[i]
	}
}

//	==============================
//	ShardCtrler
//	==============================
func (sc *ShardCtrler) DPrintf(format string, a ...interface{}) (n int, err error) {
	if !sc.killed() {
		DPrintf(format, a...)
	}
	return
}

func (sc *ShardCtrler) BasicInfo(methodName string) string {
	if methodName == "" {
		return fmt.Sprintf("S%v ShardCtrler", sc.me)
	}
	return fmt.Sprintf("S%v ShardCtrler.%v", sc.me, methodName)
}

func (sc *ShardCtrler) GetProcessedOpResultCh(index int, create bool) chan OpResult {
	ch, ok := sc.index2processedOpResultCh[index]
	if !ok && create {
		ch = make(chan OpResult, 1)
		sc.index2processedOpResultCh[index] = ch
	}
	return ch
}

func (sc *ShardCtrler) DeleteProcessedOpResultCh(index int) {
	delete(sc.index2processedOpResultCh, index)
}

func (sc *ShardCtrler) NotifyProcessedOpResultCh(index int, opResult *OpResult) {
	sc.mu.Lock()
	ch := sc.GetProcessedOpResultCh(index, false)
	sc.mu.Unlock()
	if ch != nil {
		ch <- *opResult
	}
}

func (sc *ShardCtrler) OpExecuted(clientId Int64Id, opId int) bool {
	executedOpId, ok := sc.clientId2executedOpId[clientId]
	if !ok {
		return false
	}
	return opId <= executedOpId
}

func (sc *ShardCtrler) GetValidConfigNum(configNum int) int {
	maxConfigNum := len(sc.configs) - 1
	if configNum > maxConfigNum {
		configNum = maxConfigNum
	}
	if configNum < 0 {
		configNum = maxConfigNum + 1 + configNum
		if configNum < 0 {
			configNum = 0
		}
	}
	return configNum
}
