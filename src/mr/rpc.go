package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

type TaskRequest struct {
	Tybe   int // 任务类型 1: wait  2: Map  3: Reduce
	TaskId int
}

type TaskReply struct {
	Tybe      int // 任务类型 1: wait  2: Map  3: Reduce
	FileName  string
	FileNames []string
	TaskId    int
	ReduceNum int // nReduce
}

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
