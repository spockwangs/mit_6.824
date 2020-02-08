package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "encoding/json"

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

func makeIntermediateFilename(input string, reduce_idx int) string {
	return fmt.Sprintf("%s-intermediate-%d", input, reduce_idx)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	task_req := TaskReq{}
	task_req.Complete_task.Task_type = 0
	for {
		task_resp, ok := GetTask(task_req)
		if !ok {
			break
		}
		if task_resp.Done == true {
			break
		}
		task := &task_resp.New_task
		switch task.Task_type {
		case 1:
			DoMap(mapf, task.Map_task)
			task_req.Complete_task = *task
		case 2:
			DoReduce(reducef, task.Reduce_task)
			task_req.Complete_task = *task
		default:
			log.Fatal("unknown task type: %d", task.Task_type)
		}
	}

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

func DoMap(mapf func(string, string) []KeyValue,
	map_task MapTask) {
	file, err := os.Open(map_task.Filename)
	if err != nil {
		log.Fatal("cannot open %v", map_task.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatal("cannot read %v", map_task.Filename)
	}
	file.Close()
	kva := mapf(map_task.Filename, string(content))

	type InterFile struct {
		enc *json.Encoder
		file *os.File
	}
	intermediate_files := make([]InterFile, map_task.Num_reduce)
	for i := 0; i < map_task.Num_reduce; i++ {
		ofile, _ := ioutil.TempFile("", "mr-inter-")
		intermediate_files[i] = InterFile {
			enc: json.NewEncoder(ofile),
				file: ofile,
			}
	}

	for _, kv := range kva {
		idx := ihash(kv.Key) % map_task.Num_reduce
		enc := intermediate_files[idx].enc
		enc.Encode(&kv)
	}
	for i, file := range intermediate_files {
		os.Rename(file.file.Name(), makeIntermediateFilename(map_task.Filename, i))
		file.file.Close()
	}
}
	
func DoReduce(reducef func(string, []string) string,
	reduce_task ReduceTask) {
	reduce_map := make(map[string][]string)
	for _, filename := range reduce_task.Filenames {
		intermediate_filename := makeIntermediateFilename(filename, reduce_task.Reduce_idx)
		file, err := os.Open(intermediate_filename)
		if err != nil {
			log.Fatalf("cannot open %v", intermediate_filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			val, _ := reduce_map[kv.Key]
			reduce_map[kv.Key] = append(val, kv.Value)
		}
	}

	ofile, _ := ioutil.TempFile("", "mr-out-")
	for key, value := range reduce_map {
		output := reducef(key, value)
		fmt.Fprintf(ofile, "%v %v\n", key, output)
	}
	os.Rename(ofile.Name(), fmt.Sprintf("mr-out-%d", reduce_task.Reduce_idx))
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func GetTask(task_req TaskReq) (task_resp TaskResp, ok bool) {
	ok = call("Master.GetTask", &task_req, &task_resp)
	return
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", "mr-socket")
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

