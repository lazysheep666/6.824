package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


type taskState int
const(
	UNDISTRIBUTED taskState = iota
	DISTRIBUTED
	FINISHED
)
type Task struct {
	taskType TaskType
	taskID int
	taskState taskState
}
type Coordinator struct {
	// Your definitions here.
	files []string
	mapTasks []Task
	reduceTasks []Task
	timeoutDuration time.Duration
	mapFinishedCount int
	reduceFinishedCount int
	isFinished bool
	mutex sync.RWMutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AskTask(args *AskTaskArgs, reply *AskTaskReply) error {
	c.mutex.RLock()
	if c.isFinished {
		reply.TaskType = NONE
		return nil
	}
	c.mutex.RUnlock()
	task := Task{NONE, -1, UNDISTRIBUTED}
	if c.IsFinishedMap() {
		task = c.GetTask(REDUCE)
	} else {
		task = c.GetTask(MAP)
	}
	if task.taskType == NONE {
		return nil
	}
	if task.taskType == MAP {
		reply.TaskType = MAP
		reply.FileName = c.files[task.taskID]
		reply.BucketNum = len(c.reduceTasks)
		reply.TaskID = task.taskID
		fmt.Printf("Send A Map Task %d To Worker. \n", task.taskID)
	} else {
		reply.TaskType = REDUCE
		reply.BucketNum = len(c.mapTasks)
		reply.TaskID = task.taskID
		fmt.Printf("Send A Reduce Task %d To Worker. \n", task.taskID)
	}
	go c.WaitForTask(task.taskID, task.taskType)
	return nil
}


func (c* Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error{
	c.mutex.Lock()
	if args.TaskType == MAP && c.mapTasks[args.TaskID].taskState != FINISHED {
		c.mapTasks[args.TaskID].taskState = FINISHED
		c.mapFinishedCount++
		fmt.Printf("Recieve Compelet Map Task %d \n",args.TaskID)
	} else if args.TaskType == REDUCE && c.reduceTasks[args.TaskID].taskState != FINISHED {
		c.reduceTasks[args.TaskID].taskState = FINISHED
		c.reduceFinishedCount++
		fmt.Printf("Recieve Compelet Reduce Task %d \n",args.TaskID)
		if c.reduceFinishedCount == len(c.reduceTasks) {
			c.isFinished = true
		}
	}
	c.mutex.Unlock()
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mutex.RLock()
	ret = c.isFinished
	c.mutex.RUnlock()

	return ret
}

func (c* Coordinator) IsFinishedMap() bool {
	isFinished := false
	c.mutex.RLock()
	if c.mapFinishedCount == len(c.mapTasks) {
		isFinished = true
	}
	c.mutex.RUnlock()
	return isFinished
}
func (c* Coordinator) IsFinishedReduce() bool {
	isFinished := false
	c.mutex.RLock()
	if c.reduceFinishedCount == len(c.reduceTasks) {
		isFinished = true
	}
	c.mutex.RUnlock()
	return isFinished
}

func (c* Coordinator) GetTask(taskType TaskType) Task {
	task := Task{}
	task.taskType = NONE
	c.mutex.Lock()
	if taskType == MAP {
		for i := 0; i < len(c.mapTasks); i++ {
			if c.mapTasks[i].taskState == UNDISTRIBUTED {
				c.mapTasks[i].taskState = DISTRIBUTED
				task = c.mapTasks[i]
				break
			}
		}
	} else if taskType == REDUCE {
		for i := 0; i < len(c.reduceTasks); i++ {
			if c.reduceTasks[i].taskState == UNDISTRIBUTED {
				c.reduceTasks[i].taskState = DISTRIBUTED
				task = c.reduceTasks[i]
				break
			}
		}
	}
	c.mutex.Unlock()
	return task
}

func (c* Coordinator) WaitForTask(taskID int, taskType TaskType) {
	timer := time.NewTimer(c.timeoutDuration)
	<-timer.C
	c.mutex.Lock()
	if taskType == MAP {
		if c.mapTasks[taskID].taskState != FINISHED {
			c.mapTasks[taskID].taskState = UNDISTRIBUTED
		}
	} else {
		if c.reduceTasks[taskID].taskState != FINISHED {
			c.reduceTasks[taskID].taskState = UNDISTRIBUTED
		}
	}
	c.mutex.Unlock()
}
//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.files = files
	c.timeoutDuration = 10 * time.Second
	c.isFinished = false
	c.mapFinishedCount = 0
	c.reduceFinishedCount = 0
	c.mutex = sync.RWMutex{}

	for index, _ := range files {
		task := Task{MAP,index,UNDISTRIBUTED}
		c.mapTasks = append(c.mapTasks, task)
	}

	for i := 0; i < nReduce; i++ {
		task := Task{REDUCE, i, UNDISTRIBUTED}
		c.reduceTasks = append(c.reduceTasks, task)
	}

	// Your code here.

	c.server()
	return &c
}
