package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

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

type MapTask struct {
	Filename string
	Num_reduce int
}

type ReduceTask struct {
	Filenames []string
	Reduce_idx int
}

type Task struct {
	// 0 for none, 1 for map task and 2 for reduce task
	Task_type int
	Map_task MapTask
	Reduce_task ReduceTask
}

type TaskReq struct {
	Complete_task Task
}

type TaskResp struct {
	New_task Task
	Done bool
}
	
