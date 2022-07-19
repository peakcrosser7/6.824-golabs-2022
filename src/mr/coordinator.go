package mr

import (
	"errors"
	"fmt"
	"log"
	"sync"
)

import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskStateType uint8

const (
	TASK_IDLE TaskStateType = iota
	TASK_INPROGRESS
	TASK_COMPLETED
)

type TaskType uint8

const (
	MAP_TASK TaskType = iota
	REDUCE_TASK
	WAITING_TASK
	NO_TASK
)

type TaskStruct struct {
	TaskType TaskType
	TaskId   int
	FileLocs []string
}

type WorkerType uint8

const (
	NEW_WORKER WorkerType = iota
	MAP_WORKER
	REDUCE_WORKER
	CLOSED_WORKER
)

type Coordinator struct {
	// Your definitions here.
	inputFiles       []string
	taskMu           sync.Mutex
	mapTaskStates    []TaskStateType
	reduceTaskStates []TaskStateType

	idMu         sync.Mutex
	workerId     int
	workersTypes []WorkerType

	interMu   sync.Mutex
	interLocs [][]string

	taskLeft int
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) AskWorkerId(args *AskWorkerIdArgs, reply *AskWorkerIdReply) error {
	c.idMu.Lock()
	defer c.idMu.Unlock()
	reply.WorkerId = c.workerId
	reply.NReduce = len(c.reduceTaskStates)

	c.workersTypes = append(c.workersTypes, NEW_WORKER)
	fmt.Printf("send a workerId %v and nReduce %v\n", c.workerId, reply.NReduce)
	c.workerId++
	return nil
}

func (c *Coordinator) AskTask(args *AskTaskArgs, reply *AskTaskReply) error {
	if args.WorkerId == -1 {
		return errors.New("an error worker machine")
	}

	c.taskMu.Lock()
	allCompleted := true
	for i := 0; i < len(c.mapTaskStates); i++ {
		if c.mapTaskStates[i] == TASK_IDLE {
			c.workersTypes[args.WorkerId] = MAP_WORKER
			c.mapTaskStates[i] = TASK_INPROGRESS
			reply.Task = &TaskStruct{
				TaskType: MAP_TASK,
				TaskId:   i,
				FileLocs: c.inputFiles[i : i+1],
			}
			fmt.Printf("send a Map task %v to worker %v\n", i, args.WorkerId)
			c.taskMu.Unlock()
			return nil
		} else if c.mapTaskStates[i] == TASK_INPROGRESS {
			allCompleted = false
		}
	}
	c.taskMu.Unlock()

	reply.Task = &TaskStruct{
		TaskType: WAITING_TASK,
	}
	c.taskMu.Lock()
	c.taskLeft = 0
	c.taskMu.Unlock()
	fmt.Printf("send a WAITING to worker %v\n", args.WorkerId)
	return nil

	if !allCompleted {
		reply.Task = &TaskStruct{
			TaskType: WAITING_TASK,
		}
		c.taskLeft--
		fmt.Printf("send a WAITING to worker %v\n", args.WorkerId)
		return nil
	}

	c.taskMu.Lock()
	for i := 0; i < len(c.reduceTaskStates); i++ {
		if c.reduceTaskStates[i] == TASK_IDLE {
			c.workersTypes[args.WorkerId] = REDUCE_WORKER
			c.reduceTaskStates[i] = TASK_INPROGRESS
			reply.Task = &TaskStruct{
				TaskType: REDUCE_TASK,
				TaskId:   i,
				FileLocs: c.interLocs[i],
			}
			fmt.Printf("send a Reduce task %v to worker %v\n", i, args.WorkerId)
			return nil
		}
	}
	return nil
}

func (c *Coordinator) PushInterLocs(args *PushInterLocsArgs, reply *PushInterLocsReply) error {
	c.interMu.Lock()
	for i, locs := range args.InterLocs {
		c.interLocs[i] = append(c.interLocs[i], locs)
	}
	c.interMu.Unlock()

	c.taskMu.Lock()
	c.mapTaskStates[args.TaskId] = TASK_COMPLETED
	c.taskMu.Unlock()
	fmt.Printf("task %v is completed\n", args.TaskId)
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.taskMu.Lock()
	if c.taskLeft == 0 {
		ret = true
	}
	c.taskMu.Unlock()

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.inputFiles = files
	c.mapTaskStates = make([]TaskStateType, len(files))
	fmt.Printf("Map task total: %v\n", len(c.mapTaskStates))
	for i := 0; i < len(c.mapTaskStates); i++ {
		c.mapTaskStates[i] = TASK_IDLE
	}
	c.reduceTaskStates = make([]TaskStateType, nReduce)
	for i := 0; i < nReduce; i++ {
		c.reduceTaskStates[i] = TASK_IDLE
		c.interLocs = make([][]string, nReduce)
	}
	c.workerId = 0
	c.taskLeft = len(files)

	fmt.Printf("coordinator init\n")
	c.server()
	return &c
}
