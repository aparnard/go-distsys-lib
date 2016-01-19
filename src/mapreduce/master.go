package mapreduce

import "container/list"
import "fmt"


type WorkerInfo struct {
	address string
	// You can add definitions here.
}

var completed_channel = make(chan bool,0)
//var completed int

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) callWorker(doOp JobType,jobNum int,NumOtherPhase int) {
	var res DoJobReply
	res.OK= false
	wk:=new(Worker)
	
	args := &DoJobArgs{mr.file,doOp,jobNum,NumOtherPhase}
	err:=false		
	
	for err==false{
		wk.name = <-mr.FreeChannel
		err= call(wk.name, "Worker.DoJob", args, &res)
	}
	
	mr.completed=mr.completed+1
	mr.FreeChannel<-wk.name
	
	if doOp=="Map" && mr.completed == mr.nMap {
		completed_channel <- true
	} else if doOp=="Reduce" && mr.completed == mr.nReduce {
		completed_channel <- true
	} 
	
}


func (mr *MapReduce) RunMaster() *list.List {
	mr.completed=0
	for i := 0; i < mr.nMap; i++ {
		var val=i
		go func(i int) {
			mr.callWorker("Map",i,mr.nReduce)	
		}(val)
	}
	
	<-completed_channel
	
	fmt.Printf("\n Starting Reduce")
	
	mr.completed=0
	for j := 0; j < mr.nReduce; j++ {
		var val=j
		go func(i int) {
			mr.callWorker("Reduce",i,mr.nMap)
		}(val)
	} 

	<-completed_channel
	
	return mr.KillWorkers()
}