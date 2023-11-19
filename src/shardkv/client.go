package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"crypto/rand"
	"fmt"
	"math/big"
	mathRand "math/rand"
	"time"
)

type Int64Id int64

func (id Int64Id) String() string {
	ret := fmt.Sprintf("%v", int64(id))
	maxLen := 6
	return ret[0:maxLen]
}

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	clientId         Int64Id
	nextOpId         int
	gid2targetLeader map[int]int
}

//
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	hostInfo := fmt.Sprintf("C%v", ck.clientId)
	ck.sm = shardctrler.MakeClerkWithHostInfo(ctrlers, hostInfo)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.config = ck.sm.Query(-1)
	ck.clientId = Int64Id(nrand())
	mathRand.Seed(time.Now().Unix() + int64(ck.clientId))
	ck.nextOpId = 0
	ck.gid2targetLeader = make(map[int]int)
	ck.DPrintf("[%v] Make new shard KV clerk | sm.clientId: %v\n",
		ck.BasicInfo(""), ck.sm.GetClientId())
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	basicInfo := ck.BasicInfo("Get")

	args := GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		OpId:     ck.nextOpId,
	}
	shard := key2shard(key)

	ret := ""
	for {
		gid := ck.config.Shards[shard]
		flag := false
		if servers, _ok := ck.config.Groups[gid]; _ok {
			// try each server for the shard.
			ok := false
			targetLeader := ck.GetTargetLeader(gid, len(servers))
			for !ok {
				srv := ck.make_end(servers[targetLeader])
				reply := GetReply{}
				ck.DPrintf("[%v(%v)] Send Get RPC to S%v-%v | key: %v | shard: %v\n",
					basicInfo, args.OpId, gid, targetLeader, key, shard)

				ok = srv.Call("ShardKV.Get", &args, &reply)

				if ok {
					ck.DPrintf("[%v(%v)] Receive Get ACK from S%v-%v | err: %v | key: %v | shard: %v | value: %v\n",
						basicInfo, args.OpId, gid, targetLeader, reply.Err, key, shard, reply.Value)
					switch reply.Err {
					case OK:
						ret = reply.Value
						flag = true
						break
					case ErrWrongGroup:
						break
					case ErrNotArrivedShard:
						ok = false
					case ErrWrongLeader:
						ok = false
						targetLeader = ck.UpdateTargetLeader(gid, len(servers))
					case ErrOvertime:
						ok = false
						targetLeader = ck.UpdateTargetLeader(gid, len(servers))
					}
				} else {
					ck.DPrintf("[%v(%v)] Fail to receive Get ACK from S%v-%v | key: %v | shard: %v\n",
						basicInfo, args.OpId, gid, targetLeader, key, shard)
					targetLeader = ck.UpdateTargetLeader(gid, len(servers))
				}
			}
		}

		if flag {
			break
		}
		time.Sleep(ConfigQueryInterval * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
	ck.nextOpId++
	return ret
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	basicInfo := ck.BasicInfo("PutAppend")

	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		ClientId: ck.clientId,
		OpId:     ck.nextOpId,
	}
	switch op {
	case "Put":
		args.Op = PutKV
	case "Append":
		args.Op = AppendKV
	}
	shard := key2shard(key)

	for {
		gid := ck.config.Shards[shard]
		flag := false
		if servers, _ok := ck.config.Groups[gid]; _ok {
			// try each server for the shard.
			ok := false
			targetLeader := ck.GetTargetLeader(gid, len(servers))
			for !ok {
				srv := ck.make_end(servers[targetLeader])
				reply := PutAppendReply{}
				ck.DPrintf("[%v(%v)] Send PutAppend RPC to S%v-%v | op: %v | key: %v | shard: %v | value: %v\n",
					basicInfo, args.OpId, gid, targetLeader, args.Op, key, shard, value)

				ok = srv.Call("ShardKV.PutAppend", &args, &reply)

				if ok {
					ck.DPrintf("[%v(%v)] Receive PutAppend ACK from S%v-%v | err: %v | "+
						"key: %v | shard: %v | value: %v\n",
						basicInfo, args.OpId, gid, targetLeader, reply.Err, key, shard, value)
					switch reply.Err {
					case OK:
						flag = true
						break
					case ErrWrongGroup:
						break
					case ErrNotArrivedShard:
						ok = false
					case ErrWrongLeader:
						ok = false
						targetLeader = ck.UpdateTargetLeader(gid, len(servers))
					case ErrOvertime:
						ok = false
						targetLeader = ck.UpdateTargetLeader(gid, len(servers))
					}
				} else {
					ck.DPrintf("[%v(%v)] Fail to receive PutAppend ACK from S%v-%v | "+
						"key: %v | shard: %v | value: %v\n",
						basicInfo, args.OpId, gid, targetLeader, key, shard, value)
					targetLeader = ck.UpdateTargetLeader(gid, len(servers))
				}
			}
		}

		if flag {
			break
		}
		time.Sleep(ConfigQueryInterval * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
	ck.nextOpId++
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
