package shardctrler

import "time"

//	==============================
//	Process Prepare
//	==============================
func (sc *ShardCtrler) prepareForProcess(op Op) (Err, int) {
	basicInfo := sc.BasicInfo("prepareForProcess")

	index, _, isLeader := sc.rf.Start(op)
	var err Err = OK
	if !isLeader {
		err = ErrWrongLeader
		switch op.Type {
		case JoinRG:
			sc.DPrintf("[%v(C%v-%v)] Refuse to %v RG(servers: %v) for C%v (isn't the leader)\n",
				basicInfo, op.ClientId, op.Id, op.Type, op.Servers, op.ClientId)
		case LeaveRG:
			sc.DPrintf("[%v(C%v-%v)] Refuse to %v RG(gids: %v) for C%v (isn't the leader)\n",
				basicInfo, op.ClientId, op.Id, op.Type, op.GIDs, op.ClientId)
		case MoveSD:
			sc.DPrintf("[%v(C%v-%v)] Refuse to %v SD%v to RG%v for C%v (isn't the leader)\n",
				basicInfo, op.ClientId, op.Id, op.Type, op.Shard, op.GIDs[0], op.ClientId)
		case QueryCF:
			sc.DPrintf("[%v(C%v-%v)] Refuse to %v CF%v for C%v (isn't the leader)\n",
				basicInfo, op.ClientId, op.Id, op.Type, op.ConfigNum, op.ClientId)
		}
	}

	return err, index
}

//	==============================
//	Process Wait
//	==============================
func (sc *ShardCtrler) waitForProcess(op Op, index int) (Err, *Config) {
	basicInfo := sc.BasicInfo("waitForProcess")

	sc.mu.Lock()
	ch := sc.GetProcessedOpResultCh(index, true)
	sc.mu.Unlock()

	var (
		err       Err
		configRet *Config
	)
	timer := time.NewTimer(ProcessWaitTimeout * time.Millisecond)
	select {
	case processedOpResult := <-ch:
		if processedOpResult.ClientId != op.ClientId || processedOpResult.Id != op.Id {
			err = ErrWrongLeader
			switch op.Type {
			case JoinRG:
				sc.DPrintf("[%v(C%v-%v)] Refuse to %v RG(servers: %v) for C%v "+
					"(don't match op identifier at I%v: (%v,%v) VS (%v,%v))\n",
					basicInfo, op.ClientId, op.Id, op.Type, op.Servers, op.ClientId,
					index, processedOpResult.ClientId, processedOpResult.Id, op.ClientId, op.Id)
			case LeaveRG:
				sc.DPrintf("[%v(C%v-%v)] Refuse to %v RG(gids: %v) for C%v "+
					"(don't match op identifier at I%v: (%v,%v) VS (%v,%v))\n",
					basicInfo, op.ClientId, op.Id, op.Type, op.GIDs, op.ClientId,
					index, processedOpResult.ClientId, processedOpResult.Id, op.ClientId, op.Id)
			case MoveSD:
				sc.DPrintf("[%v(C%v-%v)] Refuse to %v SD%v to RG%v for C%v "+
					"(don't match op identifier at I%v: (%v,%v) VS (%v,%v))\n",
					basicInfo, op.ClientId, op.Id, op.Type, op.Shard, op.GIDs[0], op.ClientId,
					index, processedOpResult.ClientId, processedOpResult.Id, op.ClientId, op.Id)
			case QueryCF:
				sc.DPrintf("[%v(C%v-%v)] Refuse to %v CF%v for C%v "+
					"(don't match op identifier at I%v: (%v,%v) VS (%v,%v))\n",
					basicInfo, op.ClientId, op.Id, op.Type, op.ConfigNum, op.ClientId,
					index, processedOpResult.ClientId, processedOpResult.Id, op.ClientId, op.Id)
			}
		} else {
			err = OK
			switch op.Type {
			case JoinRG:
				sc.DPrintf("[%v(C%v-%v)] %v RG(servers: %v) for C%v\n",
					basicInfo, op.ClientId, op.Id, op.Type, op.Servers, op.ClientId)
			case LeaveRG:
				sc.DPrintf("[%v(C%v-%v)] %v RG(gids: %v) for C%v\n",
					basicInfo, op.ClientId, op.Id, op.Type, op.GIDs, op.ClientId)
			case MoveSD:
				sc.DPrintf("[%v(C%v-%v)] %v SD%v to RG%v for C%v\n",
					basicInfo, op.ClientId, op.Id, op.Type, op.Shard, op.GIDs[0], op.ClientId)
			case QueryCF:
				configRet = processedOpResult.Config
				sc.DPrintf("[%v(C%v-%v)] %v CF%v(%v) for C%v\n",
					basicInfo, op.ClientId, op.Id, op.Type, op.ConfigNum, configRet, op.ClientId)
			}
		}
	case <-timer.C:
		err = ErrOvertime
		switch op.Type {
		case JoinRG:
			sc.DPrintf("[%v(C%v-%v)] Refuse to %v RG(servers: %v) for C%v (rpc overtime)\n",
				basicInfo, op.ClientId, op.Id, op.Type, op.Servers, op.ClientId)
		case LeaveRG:
			sc.DPrintf("[%v(C%v-%v)] Refuse to %v RG(gids: %v) for C%v (rpc overtime)\n",
				basicInfo, op.ClientId, op.Id, op.Type, op.GIDs, op.ClientId)
		case MoveSD:
			sc.DPrintf("[%v(C%v-%v)] Refuse to %v SD%v to RG%v for C%v (rpc overtime)\n",
				basicInfo, op.ClientId, op.Id, op.Type, op.Shard, op.GIDs[0], op.ClientId)
		case QueryCF:
			sc.DPrintf("[%v(C%v-%v)] Refuse to %v CF%v for C%v (rpc overtime)\n",
				basicInfo, op.ClientId, op.Id, op.Type, op.ConfigNum, op.ClientId)
		}
	}
	timer.Stop()

	sc.mu.Lock()
	sc.DeleteProcessedOpResultCh(index)
	sc.mu.Unlock()

	return err, configRet
}

//	==============================
//	Join RPC
//	==============================
type JoinArgs struct {
	Servers  map[int][]string // new GID -> servers mappings
	ClientId Int64Id
	OpId     int
}

type JoinReply struct {
	Err Err
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	opType := OpType(JoinRG)
	op := Op{
		Id:       args.OpId,
		ClientId: args.ClientId,
		Type:     opType,
		Servers:  args.Servers,
	}

	prepareErr, index := sc.prepareForProcess(op)
	if prepareErr != OK {
		reply.Err = prepareErr
		return
	}

	waitErr, _ := sc.waitForProcess(op, index)
	reply.Err = waitErr
}

//	==============================
//	Leave RPC
//	==============================
type LeaveArgs struct {
	GIDs     []int
	ClientId Int64Id
	OpId     int
}

type LeaveReply struct {
	Err Err
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	opType := OpType(LeaveRG)
	op := Op{
		Id:       args.OpId,
		ClientId: args.ClientId,
		Type:     opType,
		GIDs:     args.GIDs,
	}

	prepareErr, index := sc.prepareForProcess(op)
	if prepareErr != OK {
		reply.Err = prepareErr
		return
	}

	waitErr, _ := sc.waitForProcess(op, index)
	reply.Err = waitErr
}

//	==============================
//	Move RPC
//	==============================
type MoveArgs struct {
	Shard    int
	GID      int
	ClientId Int64Id
	OpId     int
}

type MoveReply struct {
	Err Err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	opType := OpType(MoveSD)
	op := Op{
		Id:       args.OpId,
		ClientId: args.ClientId,
		Type:     opType,
		Shard:    args.Shard,
		GIDs:     make([]int, 1),
	}
	op.GIDs[0] = args.GID

	prepareErr, index := sc.prepareForProcess(op)
	if prepareErr != OK {
		reply.Err = prepareErr
		return
	}

	waitErr, _ := sc.waitForProcess(op, index)
	reply.Err = waitErr
}

//	==============================
//	Query RPC
//	==============================
type QueryArgs struct {
	Num      int // desired config number
	ClientId Int64Id
	OpId     int
}

type QueryReply struct {
	Err    Err
	Config Config
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	opType := OpType(QueryCF)
	op := Op{
		Id:        args.OpId,
		ClientId:  args.ClientId,
		Type:      opType,
		ConfigNum: args.Num,
	}

	prepareErr, index := sc.prepareForProcess(op)
	if prepareErr != OK {
		reply.Err = prepareErr
		return
	}

	waitErr, configRet := sc.waitForProcess(op, index)
	reply.Err = waitErr
	if configRet != nil {
		reply.Config = *configRet
	}
}
