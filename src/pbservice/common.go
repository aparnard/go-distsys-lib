package pbservice

import "hash/fnv"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
	ErrReject	   = "ErrReject"
)

type Err string

type LogEntry struct {
	ClientID int64
	Op    	string
	Err 	Err
	Key 	string
	Value 	string //applicable only for gets
}

type Forwards struct {
	OpID     int64
	Log      LogEntry
}

type Packet struct {
	Data map[string]string
	Log  map[int64]LogEntry
}
type Response struct {  //sync response +P-B response
	Err     Err
}


// Put or Append
type PutAppendArgs struct {
	OpID int64
	ClientID int64
	DoAppend bool
	Key   string
	Value string
	// You'll have to add definitions here.

	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	OpID int64
	Err Err
}

type GetArgs struct {
	OpID int64
	ClientID int64
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	OpID int64
	Err   Err
	Value string
}


// Your RPC definitions here.

func hash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
}