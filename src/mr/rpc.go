package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type TaskKind int

const (
	MapTask = iota
	ReduceTask
	EndTask
)

func (taskKind TaskKind) String() string {
	var ret string
	switch taskKind {
	case MapTask:
		ret = "MapTask"
	case ReduceTask:
		ret = "ReduceTask"
	case EndTask:
		ret = "EndTask"
	}
	return ret
}

type Task struct {
	TaskId     int
	TaskKind   TaskKind
	ReducerNum int
	Filenames  []string
}

type DistributeTaskArgs struct {
}

type DistributeTaskReply struct {
	Task Task
}

type DealTaskDoneArgs struct {
	TaskId int
}

type DealTaskDoneReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
