package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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
//向worker返回的数据类型
//任务类型
//任务id
//传入reducer的数量（即该任务需要处理的输入文件的数量）用于hash
//输入文件
type Task struct {
	TaskType   TaskType
	TaskId     int
	ReducerNum int
	Filename   string
}

//传入的参数类型，由于worker不需要向rpc传入参数，所以为空
type TaskArgs struct{}

//三种参数以及对应的枚举类型
type TaskType int
type Phase int
type State int

const (
	MapTask TaskType = iota
	ReduceTask
	WaittingTask
	ExitTask
)
const (
	MapPhase Phase = iota
	ReducePhase
	AllDone
)
const (
	Working State = iota
	Waitting
	Doen
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
