package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
//import "fmt"
import "sync"

type Master struct {
	// Your definitions here.
	filenames []string
	num_reduce int
	map_tasks map[string]int64
	pending_map_tasks map[string]int64
	reduce_tasks map[int]int64
	pending_reduce_tasks map[int]int64
	mutex sync.Mutex
	cond *sync.Cond
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) GetTask(task_req *TaskReq, task_resp *TaskResp) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.finishTask(task_req.Complete_task)
	for {
		var status Status
		task_resp.New_task, status = m.allocateTask()
		task_resp.Done = false
		if status == OK {
			//fmt.Printf("allocated task: %v\n", task_resp)
			return nil
		} else if status == JOB_IS_DONE {
			task_resp.Done = true
			return nil
		} else {
			m.cond.Wait()
		}
	}
	return nil
}

func (m *Master) finishTask(task Task) {
	switch task.Task_type {
	case 1:
		delete(m.pending_map_tasks, task.Map_task.Filename)
	case 2:
		delete(m.pending_reduce_tasks, task.Reduce_task.Reduce_idx)
	}
	m.cond.Signal()
}

type Status int
const (
	OK Status = 0
	NOT_AVAILABLE Status = 1
	JOB_IS_DONE Status = 2
)

func (m *Master) allocateTask() (task Task, status Status) {
	now := time.Now().Unix()
	for filename, _ := range m.map_tasks {
		task.Task_type = 1
		task.Map_task = MapTask {
			Filename: filename,
				Num_reduce: m.num_reduce,
			}
		status = OK
		delete(m.map_tasks, filename)
		m.pending_map_tasks[filename] = now
		return
	}
	if len(m.pending_map_tasks) > 0 {
		status = NOT_AVAILABLE
		return
	}

	// All map tasks are done. Init reduce tasks.
	if m.reduce_tasks == nil {
		m.reduce_tasks = make(map[int]int64)
		for i := 0; i < m.num_reduce; i++ {
			m.reduce_tasks[i] = 0
		}
	}
		
	for idx, _ := range m.reduce_tasks {
		task.Task_type = 2
		task.Reduce_task = ReduceTask {
			Filenames: m.filenames,
				Reduce_idx: idx,
			}
		status = OK

		delete(m.reduce_tasks, idx)
		m.pending_reduce_tasks[idx] = now
		return
	}
	if len(m.pending_reduce_tasks) > 0 {
		status = NOT_AVAILABLE
		return
	}

	status = JOB_IS_DONE
	return
}

func (m *Master) tick() {
	for {
		if m.Done() {
			return
		}
		
		m.mutex.Lock()

		count := 0
		now := time.Now().Unix()
		for filename, time := range m.pending_map_tasks {
			if time + 10 < now {
				m.map_tasks[filename] = 0
				delete(m.pending_map_tasks, filename)
				count++
			}
		}

		for idx, time := range m.pending_reduce_tasks {
			if time + 10 < now {
				m.reduce_tasks[idx] = 0
				delete(m.pending_reduce_tasks, idx)
				count++
			}
		}
		if count > 0 {
			m.cond.Signal()
		}

		m.mutex.Unlock()

		time.Sleep(time.Second)
	}
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	os.Remove("mr-socket")
	l, e := net.Listen("unix", "mr-socket")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	m.mutex.Lock()
	defer m.mutex.Unlock()
	
	if len(m.map_tasks) == 0 && len(m.reduce_tasks) == 0 &&
		len(m.pending_map_tasks) == 0 && len(m.pending_reduce_tasks) == 0 {
		ret = true
	}

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.filenames = files
	m.num_reduce = nReduce
	m.map_tasks = make(map[string]int64)
	for _, filename := range files {
		m.map_tasks[filename] = 0
	}
	m.pending_map_tasks = make(map[string]int64)
	m.pending_reduce_tasks = make(map[int]int64)
	m.cond = sync.NewCond(&m.mutex)
	m.server()
	go m.tick()
	return &m
}
