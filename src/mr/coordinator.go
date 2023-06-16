package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

type MapTask struct {
	Status   int // 0 not start 1 start 2 done
	FileName string
}
type Coordinator struct {
	// Your definitions here.
	files            []string
	MapTaskStatus    []int
	ReduceTaskStatus []int
	NReduce          int
}

func (c *Coordinator) KeyRequest(args *KeyRequestArgs, reply *KeyReplyArgs) error {
	reply.Filename = c.files[args.FileId]
	reply.NReduce = c.NReduce
	c.MapTaskStatus[args.FileId] = 1
	return nil
}

func (c *Coordinator) MapDone(args *MapTaskDone, reply *MapTaskDoneReply) error {
	c.MapTaskStatus[args.Id] = 2
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
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.files = files
	c.NReduce = nReduce
	c.MapTaskStatus = make([]int, len(files), len(files))
	c.ReduceTaskStatus = make([]int, nReduce, nReduce)
	c.server()
	return &c
}
