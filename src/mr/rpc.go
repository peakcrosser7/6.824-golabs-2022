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

type AskWorkerIdArgs struct {
}

type AskWorkerIdReply struct {
	WorkerId int
	NReduce  int
}

type AskTaskArgs struct {
	WorkerId int
}

type AskTaskReply struct {
	Task *TaskStruct
}

type FinMapArgs struct {
	TaskId    int
	InterLocs []string
}

type FinMapReply struct {
}

type FinReduceArgs struct {
	TaskId int
}

type FinReduceReply struct {
}

type KeepAliveArgs struct {
	WorkerId int
}

type KeepAliveReply struct {
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
