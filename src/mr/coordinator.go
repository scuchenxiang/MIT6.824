package mr

import (
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"
import "sync"

//type myType int
//
const (
	MAP int =iota
	REDUCE
	NONE
)

type Task struct
{
	taskType int//1 represent map task,-1represent reduce task
	isComplete bool
	isDistribute bool
	index int
}

type Coordinator struct {
	// Your definitions here.
	file []string//go language slice,similar to list in python
	mapTask []Task
	reduceTask []Task
	mapNum int
	reduceNum int
	isFinish bool
	mutex sync.Mutex
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

// must be SubmitTask not submitTask
func (c *Coordinator)  SubmitTask(args *SubmitTaskRequest,reply *SubmitTaskResponse) error{
	c.mutex.Lock()
	if args.TaskType==MAP&&c.mapTask[args.Index].isComplete==false{
		c.mapNum++
		c.mapTask[args.Index].isComplete=true
	} else if args.TaskType==REDUCE &&c.reduceTask[args.Index].isComplete==false{
		c.reduceNum++
		c.reduceTask[args.Index].isComplete=true
		if(c.reduceNum==(len(c.reduceTask)-1)){
			c.isFinish=true
		}
	}
	c.mutex.Unlock()
	return nil
}
func (c *Coordinator) checkTaskIsComplete(taskIndex int,taskType int){
	time:=time.NewTimer(time.Second*10)
	<-time.C
	c.mutex.Lock()

	if(taskType==MAP){
		if(c.mapTask[taskIndex].isComplete==false){
			c.mapTask[taskIndex].isDistribute=false
		}
	}else if(taskType==REDUCE){
		if(c.reduceTask[taskIndex].isComplete==false){
			c.reduceTask[taskIndex].isDistribute=false
		}
	}
	c.mutex.Unlock()
}
//must be GetTask not getTask
func (c *Coordinator)  GetTask(args *GetTaskRequest,reply *GetTaskResponse) error{
	if (!c.isFinishMap()) {
		task:=c.tryGetTask(MAP)
		if task.taskType==NONE{
			reply.TaskType=NONE
			return nil
		}
		reply.TaskType=MAP
		reply.Index=task.index
		//tart a timer task ,check the task is complete after 10s
		go c.checkTaskIsComplete(task.index,MAP)
	} else if(!c.isFinishReduce()){
		task:=c.tryGetTask(REDUCE)
		if task.taskType==NONE{
			reply.TaskType=NONE
			return nil
		}
		reply.TaskType=REDUCE
		reply.Index=task.index
		reply.FileName=append(reply.FileName,c.getFile(task.index))
		go c.checkTaskIsComplete(task.index,REDUCE)
	}else{
		reply.TaskType=NONE
		return nil
	}
	return nil
}
func (c *Coordinator)  getFile(index int) string{
	var res string
	c.mutex.Lock()

	res=c.file[index]

	c.mutex.Unlock()
	return res
}
func (c *Coordinator) tryGetTask(taskType int) Task {
	task:=Task{}
	task.taskType=NONE
	c.mutex.Lock()
	if(taskType==MAP) {
		for i:=0;i<len(c.mapTask);i++{
			if c.mapTask[i].isDistribute==false{
				c.mapTask[i].isDistribute=true
				task=c.mapTask[i]
				break
			}
		}
	} else if(taskType==REDUCE){
		for i:=0;i<len(c.reduceTask);i++ {
			if c.reduceTask[i].isDistribute==false{
				c.reduceTask[i].isDistribute=true
				task=c.reduceTask[i]
				break
			}
		}
	}
	c.mutex.Unlock()
	return task
}

func (c *Coordinator) isFinishMap() bool {
	res :=false
	c.mutex.Lock()
	if(c.mapNum>=(len(c.mapTask))){
		res=true
	}
	c.mutex.Unlock()
	return res
}

func (c *Coordinator) isFinishReduce() bool{
	res:=false
	c.mutex.Lock()
	if(c.reduceNum>=(len(c.reduceTask))){
		res=true
	}
	c.mutex.Unlock()
	return res
}
//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)//注册到RPC
	rpc.HandleHTTP()// 开启服务
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)//删除 name 指定的文件或目录
	l, e := net.Listen("unix", sockname)//端口监听
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
	c.mutex.Lock()

	ret=c.isFinish

	c.mutex.Unlock()

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.isFinish=false
	c.mapNum=-1
	c.reduceNum=-1
	c.file=files


	// Your code here.
	for index,_:=range files{
		tmp:=Task{MAP,false,false,index}
		c.mapTask=append(c.mapTask,tmp)
	}
	fmt.Printf("init files %v \n",len(files))
	for i:=0;i<nReduce;i++{
		tmp:=Task{REDUCE,false,false,i}
		c.reduceTask=append(c.reduceTask,tmp)
	}

	c.server()
	return &c
}
