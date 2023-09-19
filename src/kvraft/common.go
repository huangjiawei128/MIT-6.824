package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
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
	case ErrWrongLeader:
		ret = "ErrWrongLeader"
	}
	return ret
}

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
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
