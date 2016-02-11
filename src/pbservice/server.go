package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"
import "strconv"
//import "errors"


type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	view 		viewservice.View
	backup      string
	synced		bool
	log			map[int64]LogEntry
	data		map[string]string

}

func (pb *PBServer) getCheckLogs(args *GetArgs) (bool,string){
	//fmt.Printf("\nChecking old logs")
	entry := pb.log[args.OpID]
	if entry.ClientID == args.ClientID && entry.Err==OK {
		fmt.Printf("\n FRom Previous LOg")
		return true, entry.Value
	} else {
		return false, ""
	}
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	if pb.view.Primary != pb.me {
		reply.OpID=args.OpID
		reply.Err=ErrWrongServer
		reply.Value=""
		return nil
	}

	pb.mu.Lock()
	
		//checking log. Atmost once implementation
	stat,val:=pb.getCheckLogs(args) 
	if stat==true {
		reply.OpID=args.OpID
		reply.Err=OK
		reply.Value=val
		pb.mu.Unlock()
		return nil
	}


	value:=pb.data[args.Key]
	
	var err Err
	err=OK
	if value == "" { 
		err=ErrNoKey
	}
	
	//logging result
	entry:=LogEntry{args.ClientID, "Get", err, args.Key,value}
	reply.Value=value
	pb.log[args.OpID]=entry			
		
		//send log to backup
	if pb.view.Backup !="" {
		foward:=Forwards{args.OpID,entry}
		var response Response
		e:=call(pb.view.Backup,"PBServer.Forward",foward,&response)
		if e == true && response.Err == OK{
			reply.OpID=args.OpID
			reply.Err=OK
			reply.Value=value
			pb.log[args.OpID]=entry
		} else if response.Err == ErrReject {
			reply.OpID=args.OpID
			reply.Err=ErrWrongServer
			reply.Value=""
	} 
	} else {
		reply.OpID=args.OpID
		reply.Err=OK
		
	
		}

	pb.mu.Unlock()
	return nil
}

func (pb *PBServer) Forward(forward *Forwards, reply *Response) error {
	pb.mu.Lock()
	if pb.view.Backup==pb.me {
		pb.log[forward.OpID]=forward.Log
		if forward.Log.Op=="Put" {
			pb.data[forward.Log.Key]=forward.Log.Value
		}
		reply.Err = OK

	} else {

		reply.Err = ErrReject
	}
	pb.mu.Unlock()
	return nil
}

func (pb *PBServer) putCheckLogs(args *PutAppendArgs) (bool,string){

	entry := pb.log[args.OpID]
	if entry.ClientID == args.ClientID && entry.Err==OK{
		return true, ""
	} else {
		return false, ""
	}
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	//fmt.Println("Put Recieved")
	// Your code here.
	//fmt.Println("\nOP ID "+strconv.Itoa(int(args.OpID)))
	if pb.view.Primary != pb.me {
		fmt.Println("I'm not the Primary")
		reply.OpID=args.OpID
		reply.Err=ErrWrongServer
		return nil
	}


	pb.mu.Lock()
	
	stat,_:=pb.putCheckLogs(args) 
	if stat==true {
		reply.OpID=args.OpID
		reply.Err=OK
		pb.mu.Unlock()
		return nil
		}

	value:=args.Value
	if args.DoAppend {
		old_value:=pb.data[args.Key]
		if old_value!=""{
				value=old_value+args.Value
			}}	

		//pb.log[args.OpID]=
	entry:=LogEntry{args.ClientID, "Put", OK, args.Key,value}
	pb.data[args.Key]=value
	pb.log[args.OpID]=entry
	//fmt.Println("\nUpdating Log. Number of entries: "+ strconv.Itoa(len(pb.log)) )
		
	if pb.hasBackup() {
			//send log to backup
		forward:=Forwards{args.OpID,entry}
		var response Response
		e:=call(pb.view.Backup,"PBServer.Forward",forward,&response)
		if e == true && response.Err == OK{
			pb.data[args.Key]=value
			pb.log[args.OpID]=entry
			reply.OpID=args.OpID
			reply.Err=OK
				//reply:=PutAppendReply{args.OpID,OK} // all ok reply to client
		} else if response.Err == ErrReject {
			reply.OpID=args.OpID
			reply.Err=ErrWrongServer }
				//reply:=PutAppendReply{args.OpID,ErrWrongServer} //backup rejected op hence not primary anymore}
	} 
	pb.mu.Unlock()
	return nil
	}


func (pb *PBServer) hasBackup() bool{
	return pb.view.Backup != ""
}
func (pb *PBServer) SyncBackup() {

	if pb.hasBackup() {
		
		var status Response
		packet:=Packet{pb.data,pb.log}
		e:=false
		for e==false{
			e=call(pb.view.Backup,"PBServer.Sync",packet,&status)
		}
	pb.synced=true
	}

}

func (pb *PBServer) Sync(packet *Packet, reply *Response) error{
	pb.mu.Lock()
	fmt.Println("\nBackup Syncing "+ strconv.Itoa(len(packet.Log)))
	pb.log=packet.Log
	pb.data=packet.Data
	//for id,entry := range packet.Log {
	//	pb.log[id]=entry
		//fmt.Printf("\n Packet Entry:OP:%s\tKey:%s",packet.Log[id].Op,packet.Log[id].Key)
		//fmt.Printf("\n Backup Entry:OP:%s\tKey:%s",pb.log[id].Op,pb.log[id].Key)
	//}

	reply.Err=OK
	pb.mu.Unlock()
	return nil

}
//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) printData() {

	for key, value := range pb.data{
		fmt.Println("Key "+key+" Value "+ value)
	}
}

func (pb *PBServer) reRunLog() {
	fmt.Println("\nReruning Previous Primary Logs: "+ strconv.Itoa(len(pb.log)))
	for _, entry := range pb.log{
		if entry.Op=="Put"  && entry.Err=="OK" {
			pb.data[entry.Key]=entry.Value
			fmt.Println("key: "+entry.Key+" Value: "+entry.Value)
		}
	}

}

func (pb *PBServer) tick() {
	// Your code here.
	pb.mu.Lock()
	view,_:= pb.vs.Ping(pb.view.Viewnum)
	//not primary before. But primary now
	if pb.view.Backup==pb.me && view.Primary==pb.me {
		fmt.Printf("\nI'm the new Primary now :%s",pb.me)
	//	pb.reRunLog();
		pb.backup=""
	}


	pb.view=view
	
	//I am the primary
	if pb.view.Primary== pb.me {
	
		//if backup has changed
		if pb.backup!= pb.view.Backup {
			//pb.synced=false
			pb.SyncBackup()
			pb.backup=pb.view.Backup
		}
		//backup hasnt synced
		if pb.synced==false{
			pb.SyncBackup()
		}
	} else if pb.view.Primary !=pb.me { //i'm no longer the primary
		pb.backup=""
		pb.synced=false
	}
	
	
	pb.mu.Unlock()
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

	// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}


func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.view = viewservice.View{0,"","",true}
	pb.synced=false
	//pb.isPrimary=false
	//pb.isBackup=false
	pb.data=make(map[string]string)
	pb.log=make(map[int64]LogEntry)

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
