package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
//import "fmt"
import "sync"
import "container/list"

type Master struct {
	// Your definitions here.
	filenames []string
	num_reduce int
	// A list of filenames.
	mapTasks *list.List
	// Map filename to start time.
	pendingMapTasks map[string]int64
	// A list of reduce indices.
	reduceTasks *list.List
	// Map reduce index to start time.
	pendingReduceTasks map[int]int64
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
		delete(m.pendingMapTasks, task.Map_task.Filename)
	case 2:
		delete(m.pendingReduceTasks, task.Reduce_task.Reduce_idx)
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
	if m.mapTasks.Len() > 0 {
		e := m.mapTasks.Front()
		task.Task_type = 1
		task.Map_task = MapTask {
			Filename: e.Value.(string),
			Num_reduce: m.num_reduce,
			}
		status = OK
		m.mapTasks.Remove(e)
		m.pendingMapTasks[e.Value.(string)] = now
		return
	} else if len(m.pendingMapTasks) > 0 {
		status = NOT_AVAILABLE
		return
	}

	// All map tasks are done. Allocate reduce tasks.
	if m.reduceTasks.Len() > 0 {
		e := m.reduceTasks.Front()
		task.Task_type = 2
		task.Reduce_task = ReduceTask {
			Filenames: m.filenames,
				Reduce_idx: e.Value.(int),
			}
		status = OK
		m.reduceTasks.Remove(e)
		m.pendingReduceTasks[e.Value.(int)] = now
		return
	} else if len(m.pendingReduceTasks) > 0 {
		status = NOT_AVAILABLE
		return
	}

	status = JOB_IS_DONE
	return
}

func (m *Master) tick() {
	const (
		TIMEOUT_SECONDS int64 = 10
	)
	for {
		if m.Done() {
			return
		}
		
		m.mutex.Lock()

		count := 0
		now := time.Now().Unix()
		for filename, time := range m.pendingMapTasks {
			if time + TIMEOUT_SECONDS < now {
				m.mapTasks.PushBack(filename)
				delete(m.pendingMapTasks, filename)
				count++
			}
		}

		for idx, time := range m.pendingReduceTasks {
			if time + TIMEOUT_SECONDS < now {
				m.reduceTasks.PushBack(idx)
				delete(m.pendingReduceTasks, idx)
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
	
	if m.mapTasks.Len() == 0 && m.reduceTasks.Len() == 0 &&
		len(m.pendingMapTasks) == 0 && len(m.pendingReduceTasks) == 0 {
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
	m.mapTasks = list.New()
	for _, filename := range files {
		m.mapTasks.PushBack(filename)
	}
	m.pendingMapTasks = make(map[string]int64)
	m.reduceTasks = list.New()
	for i := 0; i < m.num_reduce; i++ {
		m.reduceTasks.PushBack(i)
	}
	m.pendingReduceTasks = make(map[int]int64)
	m.cond = sync.NewCond(&m.mutex)
	m.server()
	go m.tick()
	return &m
}
