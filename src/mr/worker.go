package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	alive := true

	// Your worker implementation here.
	for alive {
		task, ok := AskForTask()
		if ok {
			switch task.TaskKind {
			case MapTask:
				fmt.Println("[Worker] Do map task: ", task.TaskId)
				DoMapTask(mapf, task)
				for !InformTaskDone(task) {
					continue
				}
			case ReduceTask:
				fmt.Println("[Worker] Do reduce task: ", task.TaskId)
				DoReduceTask(reducef, task)
				for !InformTaskDone(task) {
					continue
				}
			case WaitTask:
				fmt.Println("[Worker] Wait for a moment")
			case EndTask:
				fmt.Println("[Worker] Stop working")
				alive = false
			}
		}

		if !ok || task.TaskId != EndTask {
			time.Sleep(time.Second)
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func AskForTask() (*Task, bool) {
	args := DistributeTaskArgs{}
	reply := DistributeTaskReply{}

	ok := call("Coordinator.DistributeTask", &args, &reply)

	if ok {
		fmt.Println("[Worker] Succeed to ask for a task: ", reply.Task)
	} else {
		fmt.Println("[Worker] Fail to ask for a task")
	}

	return &reply.Task, ok
}

func InformTaskDone(task *Task) bool {
	args := DealTaskDoneArgs{
		TaskId: task.TaskId,
	}
	reply := DealTaskDoneReply{}

	ok := call("Coordinator.DealTaskDone", &args, &reply)

	if ok {
		fmt.Printf("[Worker] Succeed to inform coordinator that task %v is done\n", task.TaskId)
	} else {
		fmt.Printf("[Worker] Fail to inform coordinator that task %v is done\n", task.TaskId)
	}

	return ok
}

func DoMapTask(mapf func(string, string) []KeyValue, task *Task) {
	intermediate := []KeyValue{}
	filename := task.Filenames[0]

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	reducerNum := task.ReducerNum
	intermediateGroups := make([][]KeyValue, reducerNum)
	for _, kv := range intermediate {
		reducerIdx := ihash(kv.Key) % reducerNum
		intermediateGroups[reducerIdx] = append(intermediateGroups[reducerIdx], kv)
	}

	for i := 0; i < reducerNum; i++ {
		intermediateFilename := fmt.Sprintf("mr-%v-%v", task.TaskId, i)
		intermediateFile, err := os.Create(intermediateFilename)
		if err != nil {
			log.Fatalf("cannot create %v", intermediateFilename)
		}
		enc := json.NewEncoder(intermediateFile)
		for _, kv := range intermediateGroups[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot encode %v", kv)
			}
		}
		intermediateFile.Close()
	}
}

func DoReduceTask(reducef func(string, []string) string, task *Task) {
	var intermediate []KeyValue
	for _, filename := range task.Filenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))

	dir, _ := os.Getwd()
	mapperNum := len(task.Filenames)
	reduceTaskIdx := task.TaskId - mapperNum
	tname := fmt.Sprintf("mr-tmp-%v", reduceTaskIdx)
	tfile, err := ioutil.TempFile(dir, tname)
	if err != nil {
		log.Fatalf("cannot create %v", tname)
	}

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tfile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	tfile.Close()
	oname := fmt.Sprintf("mr-out-%v", reduceTaskIdx)
	os.Rename(tfile.Name(), oname)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
