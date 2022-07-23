package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
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

	// Your worker implementation here.
	workerId, nReduce := callWorkerId()
	keepAlive(workerId)

	for {
		task := callAskTask(workerId, nReduce, mapf, reducef)
		time.Sleep(2 * time.Second)
		switch task.TaskType {
		case MAP_TASK:
			processMapTask(mapf, task, nReduce)
		case REDUCE_TASK:
			processReduceTask(reducef, task)
		case WAITING_TASK:
			fmt.Printf("worker %v got a waiting task\n", task.TaskId)
			time.Sleep(3 * time.Second)
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func keepAlive(workerId int) {
	go func() {
		for {
			callKeepAlive(workerId)
			fmt.Printf("worker %v keep alive\n", workerId)
			time.Sleep(ALIVE_TIME >> 1)
		}
	}()
}

func workerExit() {
	fmt.Printf("worker exit")
	os.Exit(0)
}

func processMapTask(mapf func(string, string) []KeyValue,
	task *TaskStruct, nReduce int) {
	fmt.Printf("get a Map task:%v, files:%v\n", task.TaskId, task.FileLocs)

	var intermediate []KeyValue
	for _, filename := range task.FileLocs {
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
	}
	interBuckets := make([][]KeyValue, nReduce)
	for _, kv := range intermediate {
		i := ihash(kv.Key) % nReduce
		interBuckets[i] = append(interBuckets[i], kv)
	}
	interLocs := make([]string, 0, nReduce)
	for i := 0; i < nReduce; i++ {
		tempFile, err := ioutil.TempFile(".", "mr-t-")
		if err != nil {
			log.Fatalf("cannot create temp file")
		}
		enc := json.NewEncoder(tempFile)
		if len(interBuckets[i]) == 0 {
			continue
		}

		for _, kv := range interBuckets[i] {
			if err := enc.Encode(&kv); err != nil {
				log.Fatalf("cannot encode %v to json\n", kv)
			}
		}
		tempFile.Close()
		interFilename := "mr-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
		if os.Rename(tempFile.Name(), interFilename) != nil {
			log.Fatalf("cannot rename temp file %v to %v\n", tempFile.Name(), interFilename)
		}
		interLocs = append(interLocs, interFilename)
	}
	fmt.Printf("create inter files: %v\n", interLocs)

	callFinMap(task.TaskId, interLocs)
}

func processReduceTask(reducef func(string, []string) string, task *TaskStruct) {
	fmt.Printf("get a Reduce task:%v, files:%v\n", task.TaskId, task.FileLocs)
	var intermediate []KeyValue
	for _, filename := range task.FileLocs {
		file, err := os.Open(filename)
		if err != nil {
			fmt.Printf("cannot open file %v\n", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + strconv.Itoa(task.TaskId)
	ofile, _ := os.Create(oname)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		// 将同一键名的值放入列表values[]
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		// 执行reduce函数
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	callFinReduce(task.TaskId)
}

func callWorkerId() (int, int) {
	args := AskWorkerIdArgs{}
	reply := AskWorkerIdReply{}
	if ok := call("Coordinator.AskWorkerId", &args, &reply); !ok {
		workerExit()
	}
	fmt.Printf("Get a workerId: %v, nReduce: %v\n", reply.WorkerId, reply.NReduce)
	return reply.WorkerId, reply.NReduce
}

func callAskTask(workerId int, nReduce int, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) *TaskStruct {
	args := AskTaskArgs{
		WorkerId: workerId,
	}
	reply := AskTaskReply{}

	if ok := call("Coordinator.AskTask", &args, &reply); !ok {
		workerExit()
	}
	fmt.Printf("worker %v ask for a task\n", workerId)
	return reply.Task
}

func callFinMap(taskId int, interLocs []string) {
	args := FinMapArgs{
		TaskId:    taskId,
		InterLocs: interLocs,
	}
	reply := FinMapReply{}
	if ok := call("Coordinator.FinMap", &args, &reply); !ok {
		workerExit()
	}
	fmt.Printf("fin map\n")
}

func callFinReduce(taskId int) {
	args := FinReduceArgs{
		TaskId: taskId,
	}
	reply := FinReduceReply{}
	if ok := call("Coordinator.FinReduce", &args, &reply); !ok {
		workerExit()
	}
	fmt.Printf("fin reduce")
}

func callKeepAlive(workerId int) {
	args := KeepAliveArgs{
		WorkerId: workerId,
	}
	reply := KeepAliveReply{}
	if ok := call("Coordinator.KeepAlive", &args, &reply); !ok {
		workerExit()
	}
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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
