package kvraft

import (
	"../labgob"
	"../labrpc"
	"log"
	"../raft"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op string		// "Put" or "Append" or "Noop"
	Key string
	Value string
	ClientId int64
	Seq int64
}

type DbValue struct {
	value string
}
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db map[string]DbValue
	clientSeq map[int64]int64 // client id => last op seq
	lastApplied int
	term int
	cond *sync.Cond		// predicate: term changed or lastApplied changed
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	index, term, isLeader := kv.rf.Start(Op{
		Op: "Noop",
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	for kv.term <= term {
		if kv.lastApplied >= index {
			value, ok := kv.db[args.Key]
			if ok {
				reply.Value = value.value
				reply.Err = OK
			} else {
				reply.Value = ""
				reply.Err = ErrNoKey
			}
			return
		}
		kv.cond.Wait()
	}
	reply.Err = ErrWrongLeader
	return 
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	index, term, isLeader := kv.rf.Start(Op{
		Op: args.Op,
		Key: args.Key,
		Value: args.Value,
		Seq: args.Seq,
		ClientId: args.ClientId,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for kv.term <= term {
		if kv.lastApplied >= index {
			reply.Err = OK
			return
		}
		kv.cond.Wait()
	}
	reply.Err = ErrWrongLeader
	return 
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.db = make(map[string]DbValue)
	kv.clientSeq = make(map[int64]int64)
	kv.cond = sync.NewCond(&kv.mu)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.apply()
	
	return kv
}

func (kv *KVServer) apply() {
	for {
		select {
		case m, ok := <- kv.applyCh:
			if !ok {
				return
			}
			if !m.CommandValid {
				continue
			}

			op := m.Command.(Op)
			DPrintf("received op: %v\n", op)
			kv.mu.Lock()
			switch op.Op {
			case "Put":
				kv.db[op.Key] = DbValue{
					value: op.Value,
				}
			case "Append":
				oldValue, ok := kv.db[op.Key]
				if ok {
					// Detect duplicate reqs.
					oldSeq, ok2 := kv.clientSeq[op.ClientId]
					if !ok2 || op.Seq != oldSeq {
						kv.db[op.Key] = DbValue{
							value: oldValue.value + op.Value,
						}
					}
				} else {
					kv.db[op.Key] = DbValue{
						value: op.Value,
					}
				}
				kv.clientSeq[op.ClientId] = op.Seq
			}
			kv.lastApplied = m.CommandIndex
			kv.cond.Broadcast()
			kv.mu.Unlock()
		default:
			term, _ := kv.rf.GetState()
			kv.mu.Lock()
			if term > kv.term {
				kv.term = term
				kv.cond.Broadcast()
			}
			kv.mu.Unlock()
			time.Sleep(5*time.Millisecond)
		}
	}
}
