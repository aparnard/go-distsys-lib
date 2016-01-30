package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       	sync.Mutex
	l        	net.Listener
	dead     	int32 // for testing
	rpccount 	int32 // for testing
	me        	string
	
	currView 	View
	primarytick int32
	backuptick 	int32

	idle 	 	map[string]int
	// Your declarations here.
}

func (vs *ViewServer) hasPrimary() bool {
	return vs.currView.Primary != ""
}

func (vs *ViewServer) isPrimary(name string) bool{
	return vs.currView.Primary ==  name
}

func (vs *ViewServer) hasBackup() bool {
	return vs.currView.Backup !=""
}

func (vs *ViewServer) isBackup(name string) bool{
	return vs.currView.Backup ==  name
}

func (vs *ViewServer) makeIdleBackup() {
	for server,tickcnt :=range vs.idle {
		if tickcnt < 5 {
			//fmt.Printf("\nIdle becomes backup")
			vs.currView.Backup=server
			delete(vs.idle,server)
			break
			}
		}
}

func (vs *ViewServer) printDetails() {
	fmt.Printf("\nView: %d\tAck:%t\tPrimary: %s\tBackup: %s",vs.currView.Viewnum,vs.currView.ack,vs.currView.Primary,vs.currView.Backup)
	for server,_:= range vs.idle {
		fmt.Printf("\tIdle Name: %s",server) }
}


//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	//fmt.Printf("\nView Number:%d\tAck:%t",args.Viewnum,vs.currView.ack)
	//vs.printDetails()
	vs.mu.Lock()

	switch{
		//initializing first view
		case !vs.hasPrimary() && vs.currView.Viewnum==0 :
			vs.currView.Primary=args.Me
			vs.currView.Viewnum=1
			vs.currView.ack=false

		//If primary	
		case vs.isPrimary(args.Me):
			//acknowledges
			if vs.currView.ack==false && args.Viewnum == vs.currView.Viewnum {
				//fmt.Printf("\nPrimary acknowledges: Args:%d\tVS:%d",args.Viewnum,vs.currView.Viewnum)
				vs.currView.ack=true }
			//Primary restarts
			if args.Viewnum == 0 && vs.currView.Viewnum >2 {
				//fmt.Printf("\nPrimary Resets")
				vs.currView.Primary=vs.currView.Backup
				//if there are idle servers
				vs.currView.Backup=""	
				vs.currView.Viewnum=vs.currView.Viewnum+1
				vs.currView.ack=false
				vs.makeIdleBackup()
			}
			if vs.isPrimary(args.Me) {
				vs.primarytick=0
			}

		//If Backup
		case vs.isBackup(args.Me):
		//backup resets	
			if args.Viewnum == 0 && vs.currView.Viewnum >2 {
				//fmt.Printf("\nBackup Resets")
				vs.currView.Backup=""
				vs.currView.Viewnum=vs.currView.Viewnum+1
				vs.currView.ack=false
				vs.makeIdleBackup()
			}
			if vs.isBackup(args.Me) {
				vs.backuptick=0
			}

		default:
			if  !vs.hasBackup() && vs.hasPrimary() { //} && vs.currView.ack==true{
				vs.currView.Backup=args.Me
				vs.currView.Viewnum=vs.currView.Viewnum+1
				vs.currView.ack=false
			}

			//if idle add to idle map
			if vs.hasPrimary() && vs.hasBackup() {
				vs.idle[args.Me]=0
			}		
	}


	reply.View=vs.currView

	vs.mu.Unlock()
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	vs.mu.Lock()
	reply.View=vs.currView
	vs.mu.Unlock()

	return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	// Your code here.
	vs.mu.Lock()
	vs.primarytick=vs.primarytick+1
	vs.backuptick=vs.backuptick+1
	//update tickcount for idle servers
	for server,tickcnt := range vs.idle {
		tickcnt = tickcnt+1
		if tickcnt > 4 {
			delete(vs.idle,server)
		} else {
			vs.idle[server]=tickcnt
		}
	}
	//check if primary hasn't responded in the last 5 intervals
	switch {

		case vs.primarytick>4 && vs.currView.ack==true :
			//fmt.Printf("\nPrimary Dead. Backup becomes Primary")
			vs.currView.Primary=vs.currView.Backup
			vs.currView.Viewnum=vs.currView.Viewnum+1
			vs.currView.ack=false
			vs.currView.Backup=""
			vs.makeIdleBackup()
				
			
	//check if backup hasn't responded in the last 5 intervals
		case vs.backuptick > 4 && vs.currView.ack==true :
			//fmt.Printf("\nBackup Dead")
			vs.currView.Backup=""
			vs.currView.Viewnum=vs.currView.Viewnum+1
			vs.currView.ack=false
			vs.makeIdleBackup()
	}
	vs.mu.Unlock()
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.currView=View{0,"","",true}
	vs.idle=make(map[string]int)
	
	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
