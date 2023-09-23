package shardctrler

import (
	"fmt"
	"log"
	"sort"
)

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

type RGInfo struct {
	GID      int
	ShardNum int
}

const (
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

func (err Err) String() string {
	var ret string
	switch err {
	case OK:
		ret = "OK"
	case ErrWrongLeader:
		ret = "ErrWrongLeader"
	}
	return ret
}

type JoinArgs struct {
	Servers  map[int][]string // new GID -> servers mappings
	ClientId Int64Id
	OpId     int
}

type JoinReply struct {
	Err Err
}

type LeaveArgs struct {
	GIDs     []int
	ClientId Int64Id
	OpId     int
}

type LeaveReply struct {
	Err Err
}

type MoveArgs struct {
	Shard    int
	GID      int
	ClientId Int64Id
	OpId     int
}

type MoveReply struct {
	Err Err
}

type QueryArgs struct {
	Num      int // desired config number
	ClientId Int64Id
	OpId     int
}

type QueryReply struct {
	Err    Err
	Config Config
}

//	==============================
//	Clerk
//	==============================
func (ck *Clerk) DPrintf(format string, a ...interface{}) (n int, err error) {
	DPrintf(format, a...)
	return
}

func (ck *Clerk) UpdateTargetLeader() {
	ck.targetLeader = (ck.targetLeader + 1) % len(ck.servers)
}

//	==============================
//	Config
//	==============================
func getRGInfos(gid2Shards map[int][]int) []RGInfo {
	rgInfos := make([]RGInfo, 0)
	for gid, shards := range gid2Shards {
		rgInfos = append(rgInfos, RGInfo{GID: gid, ShardNum: len(shards)})
	}

	sort.Slice(rgInfos, func(i, j int) bool {
		return rgInfos[i].ShardNum < rgInfos[j].ShardNum
	})
	return rgInfos
}

func getGoalShardNum(shardsNum int, rgNum int) []int {
	goalShardNum := make([]int, rgNum)
	minNum, leftNum := shardsNum/rgNum, shardsNum%rgNum
	for i := 0; i < rgNum; i++ {
		goalShardNum[i] = minNum
		if i+leftNum >= rgNum {
			goalShardNum[i]++
		}
	}
	return goalShardNum
}

func (config *Config) Rebalance(gid2Shards map[int][]int, leftShards []int) {
	rgNum := len(config.Groups) - 1
	if rgNum != len(gid2Shards) {
		errMsg := fmt.Sprintf("[CF%v Config.Rebalance] rgNum != len(gid2Shards)\n", config.Num)
		panic(errMsg)
	}

	rgInfos := getRGInfos(gid2Shards)
	goalShardNum := getGoalShardNum(NShards, rgNum)

	toBalanceMin, toBalanceMax := 0, rgNum-1
	for ; toBalanceMax >= 0; toBalanceMax-- {
		newLeftShardNum := rgInfos[toBalanceMax].ShardNum - goalShardNum[toBalanceMax]
		if newLeftShardNum <= 0 {
			break
		}

		for i := 0; i < newLeftShardNum; i++ {
			leftShards = append(leftShards, gid2Shards[toBalanceMax][i])
		}
	}

	leftShardsIndex := 0
	for ; toBalanceMin <= toBalanceMax; toBalanceMin++ {
		fillShardNum := goalShardNum[toBalanceMin] - rgInfos[toBalanceMin].ShardNum
		if fillShardNum < 0 {
			errMsg := fmt.Sprintf("[CF%v Config.Rebalance] "+
				"goalShardNum[toBalanceMin] < rgInfos[toBalanceMin].ShardNum\n", config.Num)
			panic(errMsg)
		}
		if fillShardNum == 0 {
			break
		}

		if rgInfos[toBalanceMin].ShardNum == goalShardNum[toBalanceMin] {
			toBalanceMin++
			continue
		}

		for i := 0; i < fillShardNum; i++ {
			shard, gid := leftShards[leftShardsIndex], toBalanceMin
			config.Shards[shard] = gid
			leftShardsIndex++
		}
	}
}

//	==============================
//	ShardCtrler
//	==============================
func (sc *ShardCtrler) DPrintf(format string, a ...interface{}) (n int, err error) {
	DPrintf(format, a...)
	return
}

func (sc *ShardCtrler) GetProcessedOpCh(index int) chan Op {
	ch, ok := sc.index2processedOpCh[index]
	if !ok {
		ch = make(chan Op, 1)
		sc.index2processedOpCh[index] = ch
	}
	return ch
}

func (sc *ShardCtrler) DeleteProcessedOpCh(index int) {
	delete(sc.index2processedOpCh, index)
}

func (sc *ShardCtrler) OpExecuted(clientId Int64Id, opId int) bool {
	executedOpId, ok := sc.clientId2executedOpId[clientId]
	if !ok {
		return false
	}
	return opId <= executedOpId
}

func (sc *ShardCtrler) DeepCopyConfig(dst *Config, src *Config) {
	dst.Num = src.Num
	dst.Shards = src.Shards
	for gid, serverList := range src.Groups {
		dst.Groups[gid] = make([]string, 0)
		copy(dst.Groups[gid], serverList)
	}
}
