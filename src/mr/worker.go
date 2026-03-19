package mr

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"sort"
	"strconv"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"
import "os"

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

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// Your worker implementation here.
	for {
		args := TaskRequest{}
		reply := TaskReply{}
		// 用 rpc 调用调度节点 请求任务
		ok := call("Coordinator.DispatchTask", &args, &reply)
		if !ok {
			log.Fatalf("rpc 调用时出错")
		}
		switch reply.Tybe {
		case 1:
			// 没有分配到任务 等待下一次请求
			time.Sleep(time.Second)
		case 2:
			// 处理 Map 任务
			// 逻辑和 mrsequential.go 中的类似
			filename := reply.FileName
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))
			// 把 kva 中的内容分类到正确的文件中
			// 先把 kva 分类到 buckets 中
			buckets := make([][]KeyValue, reply.ReduceNum)
			for i := 0; i < len(kva); i++ {
				// 确认分类文件 mr-x-y 中的 y -> buckets[y]
				num := ihash(kva[i].Key) % reply.ReduceNum
				buckets[num] = append(buckets[num], kva[i])
			}
			// 把 buckets 中的内容分类到文件中
			for j := 0; j < len(buckets); j++ {
				oname := "mr-" + strconv.Itoa(reply.TaskId) + "-" + strconv.Itoa(j)
				ofile, err := os.Create(oname)
				if err != nil {
					log.Fatalf("cannot create %v", oname)
				}
				// 写入文件
				for k := 0; k < len(buckets[j]); k++ {
					fmt.Fprintf(ofile, "%v %v\n", buckets[j][k].Key, buckets[j][k].Value)
				}
				ofile.Close()
			}
			taskRequest := TaskRequest{
				Tybe:   2,
				TaskId: reply.TaskId,
			}
			ok := call("Coordinator.UpdateTask", &taskRequest, &TaskReply{})
			if !ok {
				log.Fatalf("rpc 调用时出错")
			}
		case 3:
			// 处理 Reduce 任务
			// 把当前 reduce 任务需要处理的 mr-x-y 文件读取到 intermediate 切片中
			intermediate := make([]KeyValue, 0)
			for _, filename := range reply.FileNames {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				scanner := bufio.NewScanner(file)
				for scanner.Scan() {
					line := scanner.Text()
					parts := strings.Split(line, " ")
					kv := KeyValue{
						Key:   parts[0],
						Value: parts[1],
					}
					intermediate = append(intermediate, kv)
				}
				file.Close()
			}
			// 后面处理逻辑和 mrsequential 中类似
			sort.Sort(ByKey(intermediate))
			oname := "mr-out-" + strconv.Itoa(reply.TaskId)
			ofile, err := os.Create(oname)
			if err != nil {
				log.Fatalf("cannot create %v", oname)
			}
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				// 目前相同key的范围是 [i, j-1]
				// values 中存的是 {"1", "1", "1"...}
				values := make([]string, 0)
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				// 当前 key 的数量
				output := reducef(intermediate[i].Key, values)
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
				i = j
			}
			ofile.Close()
			// 任务处理完 通过 rpc 向调度节点发送请求 更新任务状态
			taskRequest := TaskRequest{
				Tybe:   3,
				TaskId: reply.TaskId,
			}
			ok := call("Coordinator.UpdateTask", &taskRequest, &TaskReply{})
			if !ok {
				log.Fatalf("rpc 调用时出错")
			}
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.

// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}

// ByKey 是sort.Sort() 的接口实现
type ByKey []KeyValue

// 实现接口中的方法
func (a ByKey) Len() int {
	return len(a)
}

func (a ByKey) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a ByKey) Less(i, j int) bool {
	return a[i].Key < a[j].Key
}
