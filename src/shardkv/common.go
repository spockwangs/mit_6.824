package shardkv
import "log"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	Seq int64
	Shard int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Shard int
}

type GetReply struct {
	Err   Err
	Value string
}

type TransferArgs struct {
	ConfigNum int
	SourceGid int
	DestGid int
	Shard int
	Db Db
}

type TransferReply struct {
	Err Err
	ConfigNum int
}

type DeleteShardArgs struct {
	Shard int
}

type DeleteShardReply struct {
	Err Err
}

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

