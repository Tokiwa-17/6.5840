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

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	args := KeyRequestArgs{}
	keyReply := KeyReplyArgs{}
	CallKeyRequest(0, &args, &keyReply)
	intermediate := []KeyValue{}
	file, err := os.Open("/Users/ylf/Desktop/23spring/6.5840/src/main/" + keyReply.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", keyReply.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", keyReply.Filename)
	}
	file.Close()
	kva := mapf(keyReply.Filename, string(content))
	intermediate = append(intermediate, kva...)
	nReduce := keyReply.NReduce
	outfiles := make([]*os.File, nReduce)
	encfiles := make([]*json.Encoder, nReduce)
	for i := 0; i < nReduce; i++ {
		outfiles[i], _ = ioutil.TempFile("/Users/ylf/Desktop/23spring/6.5840/src/mr", "mr-tmp-*")
		encfiles[i] = json.NewEncoder(outfiles[i])
	}
	for _, kv := range intermediate {
		outIdx := ihash(kv.Key) % nReduce
		err := encfiles[outIdx].Encode(&kv)
		if err != nil {
			log.Fatalf("write intermediate file failed")
		}
	}
	prefix := "/Users/ylf/Desktop/23spring/6.5840/src/mr/mr-"
	for idx, file := range outfiles {
		oldname := (*file).Name()
		newname := prefix + strconv.Itoa(args.FileId) + "-" + strconv.Itoa(idx)
		os.Rename(oldname, newname)
	}
	CallMapDoneRequest(args.FileId)
}

func CallMapDoneRequest(Id int) {
	args := MapTaskDone{}
	args.Id = Id
	reply := MapTaskDoneReply{}
	ok := call("Coordinator.MapDone", &args, &reply)
	if ok {
		fmt.Printf("MapTask %v done", Id)
	} else {
		fmt.Printf("MapTask %v error")
	}
}

func CallKeyRequest(Id int, args *KeyRequestArgs, keyReply *KeyReplyArgs) {

	args.FileId = Id

	ok := call("Coordinator.KeyRequest", &args, &keyReply)

	if ok {
		fmt.Printf("reply.filename %v\n", keyReply.Filename)

	} else {
		log.Fatalf("KeyRequest call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
