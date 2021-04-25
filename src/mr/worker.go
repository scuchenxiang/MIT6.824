package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"
import "sync"


//
// Map functions return a slice of KeyValue.
//

const mypath= "/home/cx/MIT6.824/6.824/src/main/mr-tmp"

type KeyValue struct {
	Key   string
	Value string
}

type worker struct {
	id int
	mutex sync.Mutex
	isRunning bool
}
type arrayKV []KeyValue
//implement the interface
func (a arrayKV)  Len() int{
	return len(a)
}
func (a arrayKV) Swap(i int,j int){
	a[i],a[j]=a[j],a[i]
}
func (a arrayKV) Less(i int,j int) bool {
	return a[i].Key<a[j].Key
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
func readMapContent(key string)[]byte {
	file,err:=os.Open(key)
	if(err!=nil){
		fmt.Printf("open file %v failed",key)
	}
	content,err:=ioutil.ReadAll(file)
	if(err!=nil){
		fmt.Printf("read file %v failed",key)
	}
	file.Close()
	return content
}

func (worker1 *worker) setWorkerState(runState bool)  {
	worker1.mutex.Lock()
	worker1.isRunning=runState
	worker1.mutex.Unlock()
}
func (worker1 *worker) getWorkerState()bool  {
	worker1.mutex.Lock()
	res:=worker1.isRunning
	worker1.mutex.Unlock()
	return res
}
func (worker1 *worker)getWorkerId()int  {
	res:=-1
	worker1.mutex.Lock()
	res=worker1.id
	worker1.mutex.Unlock()
	return res
}
func (worker1 *worker)setWorkerId(id int) {
	worker1.mutex.Lock()
	worker1.id=id
	worker1.mutex.Unlock()
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	worker1:=worker{-1,sync.Mutex{},false}
	for{
		request:=GetTaskRequest{}
		response:=GetTaskResponse{}
		isSuccess:=call("Coordinator.GetTask",&request,&response)
		if isSuccess==false{
			fmt.Printf("get task failed\n")
			os.Exit(-1)
		}
		worker1.setWorkerId(response.Index)
		if(response.TaskType==NONE){
			time.Sleep(time.Millisecond * 200)
			continue
		}else if(response.TaskType==MAP){
			DoMap(mapf,response.FileName,worker1.getWorkerId())
			doSubmit(MAP,worker1.getWorkerId())
		}else if(response.TaskType==REDUCE){
			DoReduce(reducef,worker1.getWorkerId())
			doSubmit(REDUCE,worker1.getWorkerId())
		}

	}
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}
func doSubmit (taskType int,index int) bool {
	request:=SubmitTaskRequest{}
	request.Index=index
	request.TaskType=taskType
	response:=SubmitTaskResponse{}
	isSuccess:=call("Coordinator.SubmitTask",&request,&response)
	return isSuccess
}
func DoReduce(reducef func(string, []string) string, index int) {
	kva:=loadIntermediate(index)
	sort.Sort(arrayKV(kva))
	i:=0
	//TempFile 在目录 dir 中创建一个名称以前缀开头的新临时文件，打开文件进行读写操作，并返回结果*
	ofile,err:=ioutil.TempFile("./","prefix")
	if err!=nil {
		log.Fatal(err)
	}
	for i<len(kva){
		j:=i+1
		for j<len(kva)&&(kva)[j].Key==(kva)[i].Key {
			j++
		}
		values:=[]string{}
		for k:=1;k<j;k++{
			values=append(values,(kva)[k].Value)
		}
		output:=reducef(kva[i].Key,values)
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		i=j
	}
	oldname := ofile.Name()
	ofile.Close()
	name := fmt.Sprintf("mr-out-%v",index)
	os.Rename(oldname,name)
}
func DoMap(mapf func(string, string) []KeyValue, fileNames[]string, index int) {
	for _,file := range fileNames{
		content:=readMapContent(file)
		kva:=mapf(file,string(content))
		sort.Sort(arrayKV(kva))
		saveIntermediate(&kva,index)
	}
	}

func saveIntermediate(kva *[]KeyValue,index int) {
	var tempfile []string
	//for every map task, make R file to store map result
	// put the filename to tempfile
	for i:=0;i<10;i++{
		tempname:=fmt.Sprintf("mr-map%v-reduce%v",index,i)
		file,err:=ioutil.TempFile("./","prefix"+tempname)
		if(err!=nil){
			log.Fatal(err)
		}
		tempfile=append(tempfile,tempname)
		file.Close()
	}
	i:=0
	for i<len(*kva){
		j:=i+1
		for j<len(*kva)&&(*kva)[j].Key==(*kva)[i].Key{
			j++
		}
		reduceIndex:=ihash((*kva)[i].Key)%10
		file,err:=os.OpenFile(tempfile[reduceIndex],os.O_APPEND|os.O_RDWR,os.ModePerm)
		if(err!=nil){
			fmt.Printf("open tempfile failed in map task ")
		}
		encodeFile:=json.NewEncoder(file)
		for k:=i;k<j;k++ {
			err:=encodeFile.Encode((*kva)[k])
			if(err!=nil){
				fmt.Printf("encode to file failed ")
				os.Exit(-1)
			}
		}
		file.Close()
		i=j
	}
	// before this ,tempfile is temprory ,after we think write it to disk
	for reduceIndex,oldName:=range tempfile{
		newName:="./" + fmt.Sprintf("mr-%v-%v",index,reduceIndex)
		os.Rename(oldName,newName)
	}
}
func loadIntermediate(index int) []KeyValue{
	files,err:=ioutil.ReadDir(mypath)
	if err!=nil{
		fmt.Printf("Reda mypath failed  ")
		os.Exit(-1)
	}
	var kvList []KeyValue
	for _,fileInfo:=range files{
		filename:="./"+fileInfo.Name()
		mapIndex:=-1
		reduceIndex:=-1
		matchNum,err:=fmt.Sscanf(fileInfo.Name(),"mr-%v-%v",&mapIndex,&reduceIndex)
		if err!=nil &&matchNum==2&& reduceIndex==index{
			file,err:=os.Open(filename)
			if err!=nil{
				fmt.Printf("open %v failed,index is %v",filename,index)
			}
			decoder:=json.NewDecoder(file)
			for{
				var kv KeyValue
				err:=decoder.Decode(&kv)
				if(err==nil){
					kvList=append(kvList,kv)
				}else {
					fmt.Printf("decoder error")
					break
				}
			}
		}
	}
	sort.Sort(arrayKV(kvList))
	return kvList
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
