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

type MapRequestArgs struct {
	FileId int
}

type ReduceRequestArgs struct {
	Id int
}

type MapReplyArgs struct {
	Filename     string
	FileId       int
	NReduce      int
	MapPhaseDone bool
}

type ReduceReplyArgs struct {
	Id              int
	ReducePhaseDone bool
}

type MapTaskDone struct {
	Id int
}

type MapTaskDoneReply struct {
}

type ReduceTaskDone struct {
}

type ReduceDoneReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
