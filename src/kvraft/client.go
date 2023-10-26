package kvraft

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
	// You will have to modify this struct.
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
	// You'll have to add code here.
	ck.clientId = Int64Id(nrand())
	mathRand.Seed(time.Now().Unix() + int64(ck.clientId))
	ck.nextOpId = 0
	ck.targetLeader = mathRand.Intn(len(servers))
	ck.DPrintf("[%v] Make new KV clerk | targetLeader: %v\n",
		ck.BasicInfo(""), ck.targetLeader)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	basicInfo := ck.BasicInfo("Get")

	// You will have to modify this function.
	args := GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		OpId:     ck.nextOpId,
	}
	ok := false
	ret := ""
	for !ok {
		reply := GetReply{}
		ck.DPrintf("[%v(%v)] Send Get RPC to S%v | key: %v\n",
			basicInfo, args.OpId, ck.targetLeader, key)

		ok = ck.servers[ck.targetLeader].Call("KVServer.Get", &args, &reply)

		if ok {
			ck.DPrintf("[%v(%v)] Receive Get ACK from S%v | err: %v | key: %v | value: %v\n",
				basicInfo, args.OpId, ck.targetLeader, reply.Err, key, reply.Value)
			switch reply.Err {
			case OK:
				ret = reply.Value
				break
			case ErrWrongLeader:
				ok = false
				ck.UpdateTargetLeader()
			}
		} else {
			ck.DPrintf("[%v(%v)] Fail to receive Get ACK from S%v | key: %v\n",
				basicInfo, args.OpId, ck.targetLeader, key)
			ck.UpdateTargetLeader()
		}
	}
	ck.nextOpId++
	return ret
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	basicInfo := ck.BasicInfo("PutAppend")

	// You will have to modify this function.
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
	default:
		errorMsg := fmt.Sprintf("[%v] invalid op %v: (should be \"Put\" or \"Append\")\n", basicInfo, op)
		panic(errorMsg)
	}
	ok := false
	for !ok {
		reply := PutAppendReply{}
		ck.DPrintf("[%v(%v)] Send PutAppend RPC to S%v | op: %v | key: %v | value: %v\n",
			basicInfo, args.OpId, ck.targetLeader, args.Op, key, value)

		ok = ck.servers[ck.targetLeader].Call("KVServer.PutAppend", &args, &reply)

		if ok {
			ck.DPrintf("[%v(%v)] Receive PutAppend ACK from S%v | err: %v | key: %v | value: %v\n",
				basicInfo, args.OpId, ck.targetLeader, reply.Err, key, value)
			switch reply.Err {
			case OK:
				break
			case ErrWrongLeader:
				ok = false
				ck.UpdateTargetLeader()
			}
		} else {
			ck.DPrintf("[%v(%v)] Fail to receive PutAppend ACK from S%v | key: %v | value: %v\n",
				basicInfo, args.OpId, ck.targetLeader, key, value)
			ck.UpdateTargetLeader()
		}
	}
	ck.nextOpId++
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
