package mr

import (
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type MapTask struct {
	Status   int // 0 not start 1 start 2 done
	FileName string
}

type ReduceTask struct {
	Status int
	Id     int
}

type Coordinator struct {
	// Your definitions here.
	files            []string
	MapTaskStatus    []int
	ReduceTaskStatus []int
	NReduce          int
	TaskDone         bool
	lastRequestTime  time.Time
}

func (c *Coordinator) MapRequest(args *MapRequestArgs, reply *MapReplyArgs) error {
	args.FileId = -1
	for idx, status := range c.MapTaskStatus {
		if status == 0 {
			args.FileId = idx
			break
		}
	}
	if args.FileId == -1 {
		reply.MapPhaseDone = true
		return nil
	}
	reply.Filename = c.files[args.FileId]
	reply.FileId = args.FileId
	reply.NReduce = c.NReduce
	c.MapTaskStatus[args.FileId] = 1
	c.lastRequestTime = time.Now()
	return nil
}

func (c *Coordinator) MapDone(args *MapTaskDone, reply *MapTaskDoneReply) error {
	c.MapTaskStatus[args.Id] = 2
	return nil
}

func (c *Coordinator) ReduceRequest(args *ReduceRequestArgs, reply *ReduceReplyArgs) error {
	args.Id = -1
	for idx, status := range c.ReduceTaskStatus {
		if status == 0 {
			args.Id = idx
			break
		}
	}
	if args.Id == -1 {
		reply.ReducePhaseDone = true
		c.TaskDone = true
		return nil
	}
	reply.Id = args.Id
	c.lastRequestTime = time.Now()
	return nil
}

func (c *Coordinator) ReduceDone(args *ReduceTaskDone, reply *ReduceDoneReply) error {
	c.ReduceTaskStatus[args.Id] = 2
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := c.TaskDone
	//d := time.Now().Sub(c.lastRequestTime)
	//if d.Seconds() > 10 {
	//	ret = true
	//}
	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.TaskDone = false
	c.files = files
	c.NReduce = nReduce
	c.MapTaskStatus = make([]int, len(files), len(files))
	c.ReduceTaskStatus = make([]int, nReduce, nReduce)
	c.server()
	return &c
}
