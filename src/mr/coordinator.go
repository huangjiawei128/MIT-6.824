package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const MAX_WAIT_SECOND = 5

type Phase int

const (
	MapPhase = iota
	ReducePhase
	EndPhase
)

func (phase Phase) String() string {
	var ret string
	switch phase {
	case MapPhase:
		ret = "MapPhase"
	case ReducePhase:
		ret = "ReducePhase"
	case EndPhase:
		ret = "EndPhase"
	}
	return ret
}

type TaskState int

const (
	Idle = iota
	Doing
	Done
)

func (taskState TaskState) String() string {
	var ret string
	switch taskState {
	case Idle:
		ret = "Idle"
	case Doing:
		ret = "Doing"
	case Done:
		ret = "Done"
	}
	return ret
}

type TaskInfo struct {
	State     TaskState
	StartTime time.Time
	Ptr       *Task
}

type Coordinator struct {
	// Your definitions here.
	mu   sync.Mutex
	cond *sync.Cond

	Phase      Phase
	MapperNum  int
	ReducerNum int
	Filenames  []string

	MapChan    chan *Task
	ReduceChan chan *Task

	NextTaskId        int
	MapTaskInfoMap    map[int]*TaskInfo
	ReduceTaskInfoMap map[int]*TaskInfo
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetNextTaskId() int {
	ret := c.NextTaskId
	c.NextTaskId++
	return ret
}

func (c *Coordinator) GetTaskInfoMapPtr(taskKind TaskKind) *map[int]*TaskInfo {
	var taskInfoMapPtr *map[int]*TaskInfo
	switch taskKind {
	case MapTask:
		taskInfoMapPtr = &c.MapTaskInfoMap
	case ReduceTask:
		taskInfoMapPtr = &c.ReduceTaskInfoMap
	}
	return taskInfoMapPtr
}

func (c *Coordinator) GetTaskPtrChan(taskKind TaskKind) chan *Task {
	var taskPtrChan chan *Task
	switch taskKind {
	case MapTask:
		taskPtrChan = c.MapChan
	case ReduceTask:
		taskPtrChan = c.ReduceChan
	}
	return taskPtrChan
}

func (c *Coordinator) StartTask(taskId int, taskKind TaskKind) bool {
	taskInfoMapPtr := c.GetTaskInfoMapPtr(taskKind)
	taskInfo, ok := (*taskInfoMapPtr)[taskId]
	if ok && taskInfo.State == Idle {
		(*taskInfoMapPtr)[taskId].State = Doing
		(*taskInfoMapPtr)[taskId].StartTime = time.Now()
		return true
	}
	return false
}

func (c *Coordinator) EndTask(taskId int, taskKind TaskKind) bool {
	taskInfoMapPtr := c.GetTaskInfoMapPtr(taskKind)
	taskInfo, ok := (*taskInfoMapPtr)[taskId]
	if ok && taskInfo.State != Done {
		(*taskInfoMapPtr)[taskId].State = Done
		return true
	}
	return false
}

func (c *Coordinator) AllMapTasksDone() bool {
	for _, taskInfo := range c.MapTaskInfoMap {
		if taskInfo.Ptr.TaskKind == MapTask && taskInfo.State != Done {
			return false
		}
	}
	return true
}

func (c *Coordinator) AllReduceTasksDone() bool {
	for _, taskInfo := range c.ReduceTaskInfoMap {
		if taskInfo.Ptr.TaskKind == ReduceTask && taskInfo.State != Done {
			return false
		}
	}
	return true
}

func (c *Coordinator) ProduceMapTasks() {
	for _, filename := range c.Filenames {
		task := Task{
			TaskId:     c.GetNextTaskId(),
			TaskKind:   MapTask,
			ReducerNum: c.ReducerNum,
			Filenames:  []string{filename},
		}

		taskInfo := TaskInfo{
			State: Idle,
			Ptr:   &task,
		}

		c.MapTaskInfoMap[task.TaskId] = &taskInfo
		fmt.Println("[Coordinator] Produce a new map task:", task)
		c.MapChan <- &task
	}

	fmt.Println("[Coordinator] Finish producing map tasks")
}

func (c *Coordinator) ProduceReduceTasks() {
	for i := 0; i < c.ReducerNum; i++ {
		var filenames []string
		for j := 0; j < c.MapperNum; j++ {
			filename := fmt.Sprintf("mr-%v-%v", j, i)
			filenames = append(filenames, filename)
		}

		task := Task{
			TaskId:     c.GetNextTaskId(),
			TaskKind:   ReduceTask,
			ReducerNum: c.ReducerNum,
			Filenames:  filenames,
		}

		taskInfo := TaskInfo{
			State: Idle,
			Ptr:   &task,
		}

		c.ReduceTaskInfoMap[task.TaskId] = &taskInfo
		fmt.Println("[Coordinator] Produce a new reduce task:", task)
		c.ReduceChan <- &task
	}

	fmt.Println("[Coordinator] Finish producing reduce tasks")
}

func (c *Coordinator) DistributeTask(args *DistributeTaskArgs, reply *DistributeTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	flag := false
	for !flag {
		switch c.Phase {
		case MapPhase:
			var tempTask *Task
			if len(c.MapChan) > 0 {
				tempTask = <-c.MapChan
				flag = true
			}
			if flag {
				reply.Task = *tempTask
				if !c.StartTask(reply.Task.TaskId, reply.Task.TaskKind) {
					fmt.Printf("[Coordinator] Start map task %v repeatedly\n", reply.Task.TaskId)
				}
			} else {
				for len(c.MapChan) == 0 && !c.AllMapTasksDone() {
					c.cond.Wait()
				}
				if len(c.MapChan) == 0 && c.Phase == MapPhase {
					c.ProduceReduceTasks()
					c.Phase = ReducePhase
					fmt.Println("[Coordinator] Turn to", c.Phase)
				}
			}
		case ReducePhase:
			var tempTask *Task
			if len(c.ReduceChan) > 0 {
				tempTask = <-c.ReduceChan
				flag = true
			}
			if flag {
				reply.Task = *tempTask
				if !c.StartTask(reply.Task.TaskId, reply.Task.TaskKind) {
					fmt.Printf("[Coordinator] Start reduce task %v repeatedly\n", reply.Task.TaskId)
				}
			} else {
				for len(c.ReduceChan) == 0 && !c.AllReduceTasksDone() {
					c.cond.Wait()
				}
				if len(c.ReduceChan) == 0 && c.Phase == ReducePhase {
					c.Phase = EndPhase
					fmt.Println("[Coordinator] Turn to", c.Phase)
				}
			}
		case EndPhase:
			flag = true
			reply.Task.TaskKind = EndTask
		}
	}

	return nil
}

func (c *Coordinator) DealTaskDone(args *DealTaskDoneArgs, reply *DealTaskDoneReply) error {
	//	fmt.Println("[Coordinator] TaskDone dealer tries to get lock")
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.EndTask(args.TaskId, args.TaskKind) {
		fmt.Printf("[Coordinator] End task %v repeatedly\n", args.TaskId)
	} else {
		fmt.Println("[Coordinator] Notify all wait routines when task has been done")
		c.cond.Broadcast()
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) DetectCrash() {
	defer c.mu.Unlock()
	for {
		//	fmt.Println("[Coordinator] Crash detector tries to get lock")
		c.mu.Lock()
		if c.Phase == EndPhase {
			break
		}
		curTime := time.Now()
		crash := false

		var taskKind TaskKind
		switch c.Phase {
		case MapPhase:
			taskKind = MapTask
		case ReduceTask:
			taskKind = ReduceTask
		}
		taskInfoMapPtr := c.GetTaskInfoMapPtr(taskKind)
		taskPtrChan := c.GetTaskPtrChan(taskKind)

		for _, taskInfo := range *taskInfoMapPtr {
			if taskInfo.State != Doing {
				continue
			}
			if curTime.Sub(taskInfo.StartTime) > time.Second*MAX_WAIT_SECOND {
				taskInfo.State = Idle
				taskPtrChan <- taskInfo.Ptr
				crash = true
			}
		}

		if crash {
			fmt.Println("[Coordinator] Notify all wait routines when crash happens")
			c.cond.Broadcast()
		}
		c.mu.Unlock()
		time.Sleep(time.Second)
	}
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.Phase == EndPhase
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Phase:      MapPhase,
		MapperNum:  len(files),
		ReducerNum: nReduce,
		Filenames:  files,

		MapChan:    make(chan *Task, len(files)),
		ReduceChan: make(chan *Task, nReduce),

		NextTaskId:        0,
		MapTaskInfoMap:    make(map[int]*TaskInfo),
		ReduceTaskInfoMap: make(map[int]*TaskInfo),
	}
	c.cond = sync.NewCond(&c.mu)

	// Your code here.

	c.ProduceMapTasks()

	c.server()

	go c.DetectCrash()
	return &c
}
