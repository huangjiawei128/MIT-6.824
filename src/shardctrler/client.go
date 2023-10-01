package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.824/labrpc"
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

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientId     Int64Id
	nextOpId     int
	targetLeader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.clientId = Int64Id(nrand())
	mathRand.Seed(time.Now().Unix() + int64(ck.clientId))
	ck.nextOpId = 0
	ck.targetLeader = mathRand.Intn(len(servers))
	ck.DPrintf("[%v] Make new ShardCtrler clerk | targetLeader: %v\n",
		ck.BasicInfo(""), ck.targetLeader)
	return ck
}

func (ck *Clerk) Query(num int) Config {
	basicInfo := ck.BasicInfo("Query")

	args := QueryArgs{
		Num:      num,
		ClientId: ck.clientId,
		OpId:     ck.nextOpId,
	}
	ok := false
	var ret Config
	for !ok {
		reply := QueryReply{}
		ck.DPrintf("[%v(%v)] Send Query RPC to S%v | num: %v\n",
			basicInfo, args.OpId, ck.targetLeader, num)

		ok = ck.servers[ck.targetLeader].Call("ShardCtrler.Query", &args, &reply)

		if ok {
			ck.DPrintf("[%v(%v)] Receive Query ACK from S%v | err: %v | num: %v | config: %v\n",
				basicInfo, args.OpId, ck.targetLeader, reply.Err, num, reply.Config)
			switch reply.Err {
			case OK:
				ret = reply.Config
				break
			case ErrWrongLeader:
				ok = false
				ck.UpdateTargetLeader()
			}
		} else {
			ck.DPrintf("[%v(%v)] Fail to receive Query ACK from S%v | num: %v\n",
				basicInfo, args.OpId, ck.targetLeader, num)
			ck.UpdateTargetLeader()
		}
	}
	ck.nextOpId++
	return ret
}

func (ck *Clerk) Join(servers map[int][]string) {
	basicInfo := ck.BasicInfo("Join")

	args := JoinArgs{
		Servers:  servers,
		ClientId: ck.clientId,
		OpId:     ck.nextOpId,
	}
	ok := false
	for !ok {
		reply := JoinReply{}
		ck.DPrintf("[%v(%v)] Send Join RPC to S%v | servers: %v\n",
			basicInfo, args.OpId, ck.targetLeader, servers)

		ok = ck.servers[ck.targetLeader].Call("ShardCtrler.Join", &args, &reply)

		if ok {
			ck.DPrintf("[%v(%v)] Receive Join ACK from S%v | err: %v | servers: %v\n",
				basicInfo, args.OpId, ck.targetLeader, reply.Err, servers)
			switch reply.Err {
			case OK:
				break
			case ErrWrongLeader:
				ok = false
				ck.UpdateTargetLeader()
			}
		} else {
			ck.DPrintf("[%v(%v)] Fail to receive Join ACK from S%v | servers: %v\n",
				basicInfo, args.OpId, ck.targetLeader, servers)
			ck.UpdateTargetLeader()
		}
	}
	ck.nextOpId++
}

func (ck *Clerk) Leave(gids []int) {
	basicInfo := ck.BasicInfo("Leave")

	args := LeaveArgs{
		GIDs:     gids,
		ClientId: ck.clientId,
		OpId:     ck.nextOpId,
	}
	ok := false
	for !ok {
		reply := LeaveReply{}
		ck.DPrintf("[%v(%v)] Send Leave RPC to S%v | gids: %v\n",
			basicInfo, args.OpId, ck.targetLeader, gids)

		ok = ck.servers[ck.targetLeader].Call("ShardCtrler.Leave", &args, &reply)

		if ok {
			ck.DPrintf("[%v(%v)] Receive Leave ACK from S%v | err: %v | gids: %v\n",
				basicInfo, args.OpId, ck.targetLeader, reply.Err, gids)
			switch reply.Err {
			case OK:
				break
			case ErrWrongLeader:
				ok = false
				ck.UpdateTargetLeader()
			}
		} else {
			ck.DPrintf("[%v(%v)] Fail to receive Leave ACK from S%v | gids: %v\n",
				basicInfo, args.OpId, ck.targetLeader, gids)
			ck.UpdateTargetLeader()
		}
	}
	ck.nextOpId++
}

func (ck *Clerk) Move(shard int, gid int) {
	basicInfo := ck.BasicInfo("Move")

	args := MoveArgs{
		Shard:    shard,
		GID:      gid,
		ClientId: ck.clientId,
		OpId:     ck.nextOpId,
	}
	ok := false
	for !ok {
		reply := MoveReply{}
		ck.DPrintf("[%v(%v)] Send Move RPC to S%v | shard: %v | gid: %v\n",
			basicInfo, args.OpId, ck.targetLeader, shard, gid)

		ok = ck.servers[ck.targetLeader].Call("ShardCtrler.Move", &args, &reply)

		if ok {
			ck.DPrintf("[%v(%v)] Receive Move ACK from S%v | err: %v | shard: %v | gid: %v\n",
				basicInfo, args.OpId, ck.targetLeader, reply.Err, shard, gid)
			switch reply.Err {
			case OK:
				break
			case ErrWrongLeader:
				ok = false
				ck.UpdateTargetLeader()
			}
		} else {
			ck.DPrintf("[%v(%v)] Fail to receive Move ACK from S%v | shard: %v | gid: %v\n",
				basicInfo, args.OpId, ck.targetLeader, shard, gid)
			ck.UpdateTargetLeader()
		}
	}
	ck.nextOpId++
}
