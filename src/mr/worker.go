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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	for {
		//Fixme： add an askForTask call
		args := MapRequestArgs{}
		keyReply := MapReplyArgs{}
		CallMapRequest(&args, &keyReply)
		if !keyReply.MapPhaseDone {
			intermediate := []KeyValue{}
			//file, err := os.Open("main/" + keyReply.Filename)
			file, err := os.Open(keyReply.Filename)
			if err != nil {
				time.Sleep(time.Duration(time.Second * 2))
				continue
				//log.Fatalf("cannot open %v", keyReply.Filename)
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
				//outfiles[i], _ = ioutil.TempFile("main", "mr-tmp-*")
				outfiles[i], err = ioutil.TempFile(".", "mr-tmp-*")
				if err != nil {
					log.Fatalf(("create temp file failed.\n"))
				}
				encfiles[i] = json.NewEncoder(outfiles[i])
			}
			for _, kv := range intermediate {
				outIdx := ihash(kv.Key) % nReduce
				err := encfiles[outIdx].Encode(&kv)
				if err != nil {
					log.Fatalf("write intermediate file failed.\n")
				}
			}
			//prefix := "main/mr-"
			prefix := "./mr-"
			for idx, file := range outfiles {
				oldname := (*file).Name()
				newname := prefix + strconv.Itoa(keyReply.FileId) + "-" + strconv.Itoa(idx)
				os.Rename(oldname, newname)
			}
			CallMapDoneRequest(keyReply.FileId)

		} else {
			reduceArgs := ReduceRequestArgs{}
			reduceReply := ReduceReplyArgs{}
			CallReduceRequest(&reduceArgs, &reduceReply)
			if reduceReply.ReducePhaseDone {
				return
			} else {
				if reduceReply.Id == -1 {
					time.Sleep(time.Second * 3)
					return
				}
			}
			kva := []KeyValue{}
			for i := 0; i < 8; i++ {
				filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reduceReply.Id)
				file, err := os.Open(filename)
				if err != nil {
					continue
					//log.Fatalf("cannot read intermediate file %v\n", filename)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
			}

			sort.Sort(ByKey(kva))

			oname := "mr-out-" + strconv.Itoa(reduceReply.Id)
			ofile, _ := os.Create(oname)

			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

				i = j
			}

			ofile.Close()
			CallReduceDoneRequest(reduceReply.Id)
		}
	}
}

func CallMapRequest(args *MapRequestArgs, mapReply *MapReplyArgs) {

	ok := call("Coordinator.MapRequest", &args, &mapReply)

	if ok {
		//fmt.Printf("Map key %v\n", mapReply.Filename)
	} else {
		log.Fatalf("MapRequest call failed!\n")
	}
}

func CallReduceRequest(args *ReduceRequestArgs, reduceReply *ReduceReplyArgs) {
	ok := call("Coordinator.ReduceRequest", &args, &reduceReply)

	if ok {
		//fmt.Printf("Reduce Id %v\n", reduceReply.Id)
	} else {
		log.Fatalf("ReduceRequest call failed!\n")
	}
}

func CallMapDoneRequest(Id int) {
	args := MapTaskDone{}
	args.Id = Id
	reply := MapTaskDoneReply{}
	ok := call("Coordinator.MapDone", &args, &reply)
	if ok {
		//fmt.Printf("MapTask %v done\n", Id)
	} else {
		log.Fatalf("MapTask %v error\n")
	}
}

func CallReduceDoneRequest(Id int) {
	args := ReduceTaskDone{}
	args.Id = Id
	reply := ReduceDoneReply{}
	ok := call("Coordinator.ReduceDone", &args, &reply)
	if ok {
		//fmt.Printf("ReduceTask %v done\n", Id)
	} else {
		log.Fatalf("ReduceTask %v error\n")
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
