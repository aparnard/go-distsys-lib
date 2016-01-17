package mapreduce

import "container/list"
import "fmt"


type WorkerInfo struct {
	address string
	// You can add definitions here.
}


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



func (mr *MapReduce) RunMaster() *list.List {
	// Your code here

	for i := 0; i < mr.nMap; i++ {
			//var i=12
//		go func() {
			var res DoJobReply
			res.OK= false
			wk:=new(Worker)
			wk.name = <-mr.FreeChannel
			args := &DoJobArgs{mr.file,"Map",i,mr.nReduce}
			
			for res.OK==false{
			_= call(wk.name, "Worker.DoJob", args, &res)
			}
			mr.FreeChannel<-wk.name
//		}()
	}

	for i := 0; i < mr.nReduce; i++ {
		
//		go func() {
			var res DoJobReply
			res.OK= false
			wk:=new(Worker)
			name := <-mr.FreeChannel
			wk.name = name
			args := new(DoJobArgs)
			args.File=mr.file
			args.Operation="Reduce"
			args.JobNumber=i
			args.NumOtherPhase=mr.nMap
			
			for res.OK==false{
			_= call(wk.name, "Worker.DoJob", args, &res)
			//if err!=nil {
			//	fmt.Printf("Error in Reduce")
			//}
			}
			mr.FreeChannel<-wk.name
//		}()
	}
	//return 1
	return mr.KillWorkers()
}
