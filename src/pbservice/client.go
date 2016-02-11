	package pbservice

import "viewservice"
import "net/rpc"
import "fmt"
import "time"
import "crypto/rand"
import "math/big"
//import "math/rand"


type Clerk struct {
	vs *viewservice.Clerk //me and server
	// Your declarations here
	currView viewservice.View
	ClientID int64

}

// this may come in handy.
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	//ck.currView,_ = ck.getView()
	ck.currView=viewservice.View{}
	//rand.Seed(time.Now().UTC().UnixNano())
	ck.ClientID = nrand()

	// Your ck.* initializations here

	return ck
}

func (ck *Clerk) getView() {

	view,err := ck.vs.Ping(ck.currView.Viewnum)

	if err == nil {
	//	fmt.Printf("\n Got View:  View:%d\tPrimary:%s\tBackup:%s\tAck:%t",view.Viewnum,view.Primary,view.Backup,view.Ack)
		ck.currView = view
	}
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	//fmt.Println("Call error")
	fmt.Println(err)
	return false
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {

	// Your code here.
	if ck.currView.Viewnum==0 {
		ck.getView()
		//ck.currView=view
	}
	opid:=nrand()
	args:=GetArgs{opid,ck.ClientID,key}
	var reply GetReply
	for reply.Err != OK{
		err:=call(ck.currView.Primary,"PBServer.Get",args, &reply)
		
		if err == false || reply.Err==ErrWrongServer {
			ck.getView()
			time.Sleep(viewservice.PingInterval)
		} else if reply.Err ==ErrNoKey {
			fmt.Printf("\nNo Key Found")
			return ""
			}
	}
	return reply.Value
}

//
// send a Put or Append RPC
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	if ck.currView.Viewnum==0 {
		ck.getView()
		//ck.currView=view
	}
	opid:=nrand()
	// Your code here.
	var doAppend bool
	if op=="Put"{
		doAppend=false
	} else if op=="Append" {
		doAppend=true
	}
	
	args:=PutAppendArgs{opid,ck.ClientID,doAppend,key,value}
	var reply PutAppendReply

	for reply.Err != "OK"{
		//fmt.Println("Put to %s with Key %s and Value %s",ck.currView.Primary,key,value)
		err:=call(ck.currView.Primary,"PBServer.PutAppend",args, &reply)
		//fmt.Printf("ERROR IN PUT",reply.Err)
		if err == false || reply.Err == "ErrWrongServer" {
			ck.getView()
			time.Sleep(viewservice.PingInterval)
		}
	}
}


//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

//
// tell the primary to append to key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
