package mr

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}


type worker struct {
	isRunning bool
	workerID string
	taskType TaskType
	taskID int
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


//
// generate the random id of worker
//
func genWorkerID() (uuid string) {
	// generate 32 bits timestamp
	unix32bits := uint32(time.Now().UTC().Unix())

	buff := make([]byte, 12)

	numRead, err := rand.Read(buff)

	if numRead != len(buff) || err != nil {
		panic(err)
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x-%x\n", unix32bits, buff[0:2], buff[2:4], buff[4:6], buff[6:8], buff[8:])
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	state := worker{
		isRunning: false,
		workerID:  genWorkerID(),
		taskType:  NONE,
		taskID:    -1,
	}

	for {
		req := AskTaskArgs{state.workerID}
		res := AskTaskReply{}
		success := call("Coordinator.AskTask", &req, &res)

		if !success {
			log.Fatalf("Ask Task Faild !\n")
		}

		state.taskID = res.TaskID
		state.taskType = res.TaskType

		if state.taskType == NONE {
			fmt.Printf("Do Not Get The Task\n")
			time.Sleep(time.Millisecond * 200)
		} else if state.taskType == MAP {
			// Do Map Task
			fmt.Printf("Worker %s Will Do The Map Task: %d \n", state.workerID, state.taskID)
			doMap(mapf, res.FileName, state.taskID, res.BucketNum)
			// Finish Map Task
			_ = doSubmit(&state)
		} else {
			fmt.Printf("Worker %s Will Do The Reduce Task: %d \n", state.workerID, state.taskID)
			doReduce(reducef, state.taskID, res.BucketNum)
			_ = doSubmit(&state)
		}
		state.isRunning = false
	}
}
func doReduce(reducef func(string, []string) string, taskID int, bucketNum int) {
	kvs := loadIntermediate(taskID, bucketNum)
	sort.Sort(ByKey(kvs))
	tempFile, err := ioutil.TempFile("./", "temp")
	if err != nil {
		log.Fatalf("Faile To Create Temp File \n")
	}
	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[i].Key == kvs[j].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		output := reducef(kvs[i].Key, values)
		_, err := fmt.Fprintf(tempFile, "%v %v\n", kvs[i].Key, output)
		if err != nil {
			log.Fatalf("Faile To Write To Temp File \n")
		}
		i = j
	}
	tempFileName := tempFile.Name()
	tempFile.Close()
	name := fmt.Sprintf("mr-out-%v",taskID)
	err = os.Rename(tempFileName, name)
	if err != nil {
		log.Fatalf("Faile To Rename Temp File \n")
	}
}

func doSubmit(worker *worker) bool {
	req := FinishTaskArgs{}
	res := FinishTaskReply{}
	req.TaskType = worker.taskType
	req.TaskID = worker.taskID
	req.WorkerID = worker.workerID
	success := call("Coordinator.FinishTask", &req, &res)
	return success && res.Success
}

func doMap(mapf func(string, string) []KeyValue, fileName string, taskID int, bucketNum int) {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("Can Not Open File %v \n", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("Can Not Read File %v \n", fileName)
	}
	file.Close()

	kvs := mapf(fileName, string(content))
	sort.Sort(ByKey(kvs))

	saveIntermediate(kvs, taskID, bucketNum)
}

func loadIntermediate(taskID int, bucketNum int) []KeyValue{
	intermediates := []KeyValue{}
	for mapNum := 0; mapNum < bucketNum; mapNum++ {
		filename := "mr-" + strconv.Itoa(mapNum) + "-" + strconv.Itoa(taskID)
		f, err := os.Open(filename)
		if err != nil {
			log.Fatalf("Unable To Open File: %s\n", filename)
		}
		decoder := json.NewDecoder(f)
		var kv KeyValue
		for decoder.More() {
			err := decoder.Decode(&kv)
			if err != nil {
				log.Fatalf("Json Decode Failed, %v\n", err)
			}
			intermediates = append(intermediates, kv)
		}
		f.Close()
	}
	return intermediates
}
func saveIntermediate(kvs []KeyValue, taskID int, bucketNum int) {
	tempFiles := make([]*os.File, 0)
	tempFilesName := make([]string, 0)
	tempFilesEncoder := make([]*json.Encoder, 0)
	for i := 0; i < bucketNum; i++ {
		tempFile, err := ioutil.TempFile("./", "temp")
		if err != nil {
			log.Fatalf("Faile To Create Temp File \n")
		}
		tempFilesName = append(tempFilesName, tempFile.Name())
		tempFiles = append(tempFiles, tempFile)
		tempFilesEncoder = append(tempFilesEncoder, json.NewEncoder(tempFile))
	}

	for _, kv := range kvs {
		bucketIdx := ihash(kv.Key) % bucketNum
		err := tempFilesEncoder[bucketIdx].Encode(&kv)
		if err != nil {
			log.Fatalf("Unable To Write To Temp File %s\n", tempFilesName[bucketIdx])
		}
	}
	for _, tempFile := range tempFiles {
		tempFile.Close()
	}
	for i, tempFileName := range tempFilesName {
		newName := "./" + fmt.Sprintf("mr-%v-%v",taskID, i)
		err := os.Rename(tempFileName, newName)
		if err != nil {
			log.Fatalf("Unable To Rename The Temp File %s\n", tempFileName)
		}
	}
}
//
// example function to show how to make an RPC call to the coordinator.
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
