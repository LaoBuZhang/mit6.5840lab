package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

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
//哈希函数，通过输入的key返回生成的哈希值
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
// 传入的参数是两个函数，map函数和reduce函数
// mapf和reducef都是从插件传过来的
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// 接受任务并执行
	for true {
		task := GetTask()
		switch task.TaskType {
		case MapTask:
			{
				DoMapTask(mapf, &task)
				callDone()
			}
		case WaittingTask:
			{
				fmt.Println("所有任务都以分配，请等待...")
				time.Sleep(time.Second)
			}
		case ExitTask:
			{
				fmt.Println("任务" + strconv.Itoa(task.TaskId) + "已经结束...")
				break
			}
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

}

// map任务
// 把相同的单词都放到邻近的位置，并生成json作为临时的过渡文件
func DoMapTask(mapf func(string, string) []KeyValue, response *Task) {
	// 键值对数组
	var intermediate []KeyValue
	filename := response.Filename

	// 打开输入文件并获得内容
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("无法打开%v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	// 调用插件的方法获得拆分好的键值对数组
	intermediate = mapf(filename, string(content))

	rn := response.ReducerNum
	//二维切片，第一维长度为rn（即输入文件数量）
	HashedKV := make([][]KeyValue, rn)

	for _, kv := range intermediate {
		// 向HashedKV[ihash(kv.Key)%rn]行的末尾加入kv
		// key值相同的加入到同一行kv切片（数组）中
		HashedKV[ihash(kv.Key)%rn] = append(HashedKV[ihash(kv.Key)%rn], kv)
	}
	for i := 0; i < rn; i++ {
		oname := "mr-tmp-" + strconv.Itoa(response.TaskId) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile) //创建json文件
		for _, kv := range HashedKV[i] {
			enc.Encode(kv) //将kv键值对写入json文件，会有重复的key，value一般都为1
		}
		ofile.Close()
	}

}

// 完成任务
func callDone() Task {
	args := TaskArgs{}
	reply := Task{}
	ok := call("Coordinator.MarkFinished", &args, &reply)

	if ok {
		fmt.Println(reply)
	} else {
		fmt.Println("调用call失败")
	}
	return reply
}

// 获得任务
func GetTask() Task {
	reply := Task{}
	args := TaskArgs{}
	ok := call("Coordinator.PollTask", &args, &reply)
	if ok {
		fmt.Println(reply)
	} else {
		fmt.Println("调用call失败")
	}
	return reply
}

//
// example function to show how to make an RPC call to the coordinator.
// the RPC argument and reply types are defined in rpc.go.
//向协调者发送rpc的例子
func CallExample() {

	// declare an argument structure.
	// rpc.go中的参数结构
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	// rpc.go中的返回值结构
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	// 调用协调者的Example方法
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//调用协调者中的方法
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
