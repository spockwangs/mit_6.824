package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "bufio"
import "io"
import "strings"

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

	intermediate_files := make([]*os.File, map_task.Num_reduce)
	for i := 0; i < map_task.Num_reduce; i++ {
		ofile, _ := ioutil.TempFile("", "mr-inter-")
		intermediate_files[i] = ofile
	}

	for _, kv := range kva {
		idx := ihash(kv.Key) % map_task.Num_reduce
		ofile := intermediate_files[idx]
		fmt.Fprintf(ofile, "%s %s\n", kv.Key, kv.Value)
	}
	for i, file := range intermediate_files {
		os.Rename(file.Name(), makeIntermediateFilename(map_task.Filename, i))
		file.Close()
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
		buf := bufio.NewReader(file)
		for {
			line, err := buf.ReadString('\n')
			if err == io.EOF {
				file.Close()
				break;
			} else if err != nil {
				log.Fatalf("cannot read %v", intermediate_filename)
			}
			line = strings.TrimSpace(line)
			arr := strings.Split(line, " ")
			if len(arr) != 2 {
				log.Fatalf("bad line %v", line)
			}
			val, _ := reduce_map[arr[0]]
			reduce_map[arr[0]] = append(val, arr[1])
		}
	}

	ofile, _ := os.Create(fmt.Sprintf("mr-out-%d", reduce_task.Reduce_idx))
	for key, value := range reduce_map {
		output := reducef(key, value)
		fmt.Fprintf(ofile, "%v %v\n", key, output)
	}
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

