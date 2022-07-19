package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
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
	for {

		task := callAskTask(workerId, nReduce, mapf, reducef)

		switch task.TaskType {
		case MAP_TASK:
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
				interFilename := "mr-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
				if os.Rename(tempFile.Name(), interFilename) != nil {
					log.Fatalf("cannot rename temp file %v to %v\n", tempFile.Name(), interFilename)
				}
				interLocs = append(interLocs, interFilename)
			}
			fmt.Printf("create inter files: %v\n", interLocs)
			callPushInterLocs(task.TaskId, interLocs)

		case REDUCE_TASK:

		case WAITING_TASK:
			fmt.Printf("worker %v got a waiting task", task.TaskId)
			return
		}
	}
	return
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func callWorkerId() (int, int) {
	args := AskWorkerIdArgs{}
	reply := AskWorkerIdReply{}
	if ok := call("Coordinator.AskWorkerId", &args, &reply); !ok {
		log.Fatal("AskWorkerId error")
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
		return nil
	}
	fmt.Printf("worker %v ask for a task\n", workerId)
	return reply.Task
}

func callPushInterLocs(taskId int, interLocs []string) {
	args := PushInterLocsArgs{
		TaskId:    taskId,
		InterLocs: interLocs,
	}
	reply := PushInterLocsReply{}
	_ = call("Coordinator.PushInterLocs", &args, &reply)
	fmt.Printf("pushInstrLocs\n")
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
