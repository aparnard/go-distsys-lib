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
//import "strconv"
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
	//isPrimary	bool
	//isBackup	bool
	synced		bool
	log			map[int64]LogEntry
	data		map[string]string

}

func (pb *PBServer) getCheckLogs(args *GetArgs) (bool,string){
	//fmt.Printf("\nChecking old logs")
	entry := pb.log[args.OpID]
	if entry.ClientID == args.ClientID && entry.Err==OK {
		return true, entry.Value
	} else {
		return false, ""
	}
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	pb.mu.Lock()
	if pb.view.Primary==pb.me {
		//fmt.Printf("\n In GET as Primary")
		//checking log. Atmost once implementation
		stat,val:=pb.getCheckLogs(args) 
		if stat==true {
			//reply=GetReply{args.OpID,OK,val}
			reply.OpID=args.OpID
			reply.Err=OK
			reply.Value=val
			pb.mu.Unlock()
			return nil
		}

		//get value using the key	
		value:=pb.data[args.Key]
		var err Err
		if value == "" { 
			err=ErrNoKey
		} else { 
			err=OK
		}

		//logging result
		pb.log[args.OpID]=LogEntry{args.ClientID, "Get", err, args.Key,value}
		entry:=LogEntry{args.ClientID, "Get", err, args.Key,value}
		
		//send log to backup
		if pb.view.Backup !="" {
			foward:=Forwards{args.OpID,entry}
			var response Response
			e:=call(pb.view.Backup,"PBServer.Forward",foward,&response)
			if e == true && response.Err == OK{
				//reply=GetReply{args.OpID,err,value} // all ok reply to client
				//fmt.Printf("\nSuccessfully forward OP to backup?%s",response.Err)
				reply.OpID=args.OpID
				reply.Err=OK
				reply.Value=value
				pb.log[args.OpID]=entry
			} else if response.Err == ErrReject {
		//reply:=GetReply{args.OpID,ErrWrongServer,""} //backup rejected op hence not primary anymore
				reply.OpID=args.OpID
				reply.Err=ErrWrongServer
				reply.Value=""
			} 
		} else {
			reply.OpID=args.OpID
			reply.Err=OK
			reply.Value=value
			pb.log[args.OpID]=entry			}
	}else {
		//reply:=GetReply{args.OpID,ErrWrongServer,""} //not primary.
		reply.OpID=args.OpID
		reply.Err=ErrWrongServer
		reply.Value=""
	}
	pb.mu.Unlock()
	return nil
}

func (pb *PBServer) Forward(forward *Forwards, reply *Response) error {
	pb.mu.Lock()
	if pb.view.Backup==pb.me {
	//	fmt.Printf("\nAccepting OP from Primary")
		pb.log[forward.OpID]=forward.Log
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
	pb.mu.Lock()
	if pb.view.Primary==pb.me {
		//checking log. Atmost once implementation
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
				//tmp:=int(hash(old_value+args.Value))
				value=old_value+args.Value
			}}	

		//pb.log[args.OpID]=
		entry:=LogEntry{args.ClientID, "Put", OK, args.Key,value}
		
		if pb.view.Backup !="" {
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
		} else {
			pb.data[args.Key]=value
			pb.log[args.OpID]=entry } 
	} else {
		//reply:=PutAppendReply{args.OpID,ErrWrongServer} //not primary.
		fmt.Println("I'm not the Primary")
		reply.OpID=args.OpID
		reply.Err=ErrWrongServer
	}
	pb.mu.Unlock()
	return nil
	}


func (pb *PBServer) hasBackup() bool{
	return pb.view.Backup != ""
}
func (pb *PBServer) SyncBackup() {

	//fmt.Printf("\n Syncing Backup")
	if pb.hasBackup() {
		
		var status Response
		packet:=Packet{pb.data,pb.log}
		e:=false
		for e==false{
			e=call(pb.view.Backup,"PBServer.Sync",packet,&status)
			//fmt.Printf("\nError in sync",)
			pb.synced=true
		}
	}

}

func (pb *PBServer) Sync(packet *Packet, reply *Response) error{
	pb.mu.Lock()
	fmt.Println("Backup Syncing")
	//pb.data=packet.Data
	for key,val := range packet.Data {
		pb.data[key]=val
	}
	//pb.log=packet.Log
	for id,entry := range packet.Log {
		pb.log[id]=entry
	}
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
func (pb *PBServer) tick() {
	// Your code here.
	pb.mu.Lock()
	view,_:= pb.vs.Ping(pb.view.Viewnum)
	pb.view=view
	
	//not primary before. BUt primary now.
	if pb.view.Primary== pb.me {
		//fmt.Printf("\nI'm the Primary:%s\nMy Backup is:%s\nMy Sync status is:%t", pb.me,pb.view.Backup,pb.synced)
		if pb.synced==false{
			pb.SyncBackup()
		}
	//} else if pb.view.Backup== pb.me{
	//	fmt.Printf("\nI'm the Backup")
	} else if pb.view.Primary !=pb.me {
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
