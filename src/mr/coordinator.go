package mr

import (
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	MapTasks    []MapTask
	ReduceTasks []ReduceTask
	NReduce     int        // nReduce
	TaskSection int        // 1: Map阶段 2: Reduce阶段 3: done
	Locker      sync.Mutex // 锁
}

type MapTask struct {
	Id        int
	FileName  string
	Status    int       // 1: not started	2: working  3: done
	StartTime time.Time // 记录任务被分配时的时间
}

type ReduceTask struct {
	Id        int
	FileNames []string
	Status    int       // 1: not started 2: working 3: done
	StartTime time.Time // 记录任务被分配时的时间
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.Locker.Lock()
	defer c.Locker.Unlock()
	ret := false
	// Your code here.
	if c.TaskSection == 3 {
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	// 遍历每个文件 创建 Map 任务
	mapTasks := make([]MapTask, 0)
	for i, file := range files {
		mapTask := MapTask{
			Id:       i,
			FileName: file,
			Status:   1,
		}
		mapTasks = append(mapTasks, mapTask)
	}
	c.MapTasks = mapTasks
	c.NReduce = nReduce
	c.TaskSection = 1
	c.server(sockname)
	// 开启go程 判断是否所有map任务完成
	go RefreshMapTask(&c)
	// 开启go程 判断是否所有reduce任务完成 更新调度节点的阶段
	go RefreshReduceTask(&c)
	return &c
}

// RefreshMapTask 定时判断是否所有map任务完成
// 如果完成 更新coordinator的任务阶段
// 并创建reduce任务
// 同时检查处理超时任务
func RefreshMapTask(c *Coordinator) {
	for {
		// 加锁
		c.Locker.Lock()
		done := true
		for i := 0; i < len(c.MapTasks); i++ {
			if c.MapTasks[i].Status != 3 {
				done = false
				break
			}
		}
		if done {
			// 更新coordinator任务阶段
			c.TaskSection = 2
			// 创建reduce任务
			reduceTasks := make([]ReduceTask, 0)
			for i := 0; i < c.NReduce; i++ {
				reduceTask := ReduceTask{
					Id:     i,
					Status: 1,
				}
				filenames := make([]string, 0)
				for j := 0; j < len(c.MapTasks); j++ {
					filename := "mr-" + strconv.Itoa(j) + "-" + strconv.Itoa(i)
					filenames = append(filenames, filename)
				}
				reduceTask.FileNames = filenames
				reduceTasks = append(reduceTasks, reduceTask)
			}
			c.ReduceTasks = reduceTasks
			c.Locker.Unlock()
			return
		}
		// 如果任务并没有全部执行完
		for j := 0; j < len(c.MapTasks); j++ {
			// 判断任务是否超时
			if c.MapTasks[j].Status == 2 && c.MapTasks[j].StartTime.Add(12*time.Second).Before(time.Now()) {
				c.MapTasks[j].Status = 1
			}
		}
		c.Locker.Unlock()
		// 休息 500ms 然后进行下一次轮询判断
		time.Sleep(time.Second)
	}
}

// RefreshReduceTask 定时判断是否所有reduce任务完成
// 同时检查处理超时任务
func RefreshReduceTask(c *Coordinator) {
	for {
		c.Locker.Lock()
		if c.TaskSection != 2 {
			c.Locker.Unlock()
			time.Sleep(time.Second)
			continue
		}
		done := true
		for i := 0; i < len(c.ReduceTasks); i++ {
			if c.ReduceTasks[i].Status != 3 {
				done = false
				break
			}
		}
		if done {
			// 更新 coordinator 的任务阶段
			c.TaskSection = 3
			c.Locker.Unlock()
			return
		}
		// 如果任务并没有全部执行完
		for j := 0; j < len(c.ReduceTasks); j++ {
			// 判断任务是否超时
			if c.ReduceTasks[j].Status == 2 && c.ReduceTasks[j].StartTime.Add(12*time.Second).Before(time.Now()) {
				c.ReduceTasks[j].Status = 1
			}
		}
		c.Locker.Unlock()
		// 休息 500ms 进行下一次轮询判断
		time.Sleep(time.Second)
	}
}

// DispatchTask worker 节点通过 rpc 调用 请求分配任务
func (c *Coordinator) DispatchTask(args *TaskRequest, reply *TaskReply) error {
	c.Locker.Lock()
	defer c.Locker.Unlock()
	switch c.TaskSection {
	case 1:
		// Map阶段
		for i := range c.MapTasks {
			if c.MapTasks[i].Status == 1 {
				c.MapTasks[i].Status = 2
				c.MapTasks[i].StartTime = time.Now()
				reply.Tybe = 2
				reply.TaskId = c.MapTasks[i].Id
				reply.FileName = c.MapTasks[i].FileName
				reply.ReduceNum = c.NReduce
				return nil
			}
		}
	case 2:
		// Reduce阶段
		for i := 0; i < c.NReduce; i++ {
			if c.ReduceTasks[i].Status == 1 {
				c.ReduceTasks[i].Status = 2
				c.ReduceTasks[i].StartTime = time.Now()
				reply.Tybe = 3
				reply.TaskId = c.ReduceTasks[i].Id
				reply.FileNames = c.ReduceTasks[i].FileNames
				reply.ReduceNum = c.NReduce
				return nil
			}
		}
	}
	reply.Tybe = 1
	return nil
}

// UpdateTask worker 节点通过 rpc 调用 请求更新任务状态
func (c *Coordinator) UpdateTask(args *TaskRequest, reply *TaskReply) error {
	c.Locker.Lock()
	defer c.Locker.Unlock()
	taskId := args.TaskId
	tybe := args.Tybe
	// 如果是 Map 任务完成
	if tybe == 2 {
		// 更新 Map 任务的状态
		c.MapTasks[taskId].Status = 3
		return nil
	}
	// 如果是 Reduce 任务完成
	if tybe == 3 {
		// 更新 Reduce 任务的状态
		c.ReduceTasks[taskId].Status = 3
		return nil
	}
	return nil
}
