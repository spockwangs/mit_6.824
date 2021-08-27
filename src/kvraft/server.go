package kvraft

import (
	"../labgob"
	"../labrpc"
	"log"
	"../raft"
	"sync"
	"sync/atomic"
	"time"
	"bytes"
	"fmt"
	"container/list"
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

type OpResult struct {
	err Err
}

type OpResultCh struct {
	opResult OpResult
	term int
	refCnt int
}

type DbValue struct {
	Value string
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
	lastIncludedIndex int
	lastIncludedTerm int
	cond *sync.Cond		// predicate: lastIncludedIndex changed
	proposals *list.List
}

type Proposal struct {
	term int
	index int
	ch chan OpResult
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	index, is_leader := kv.rf.GetLastCommitIndex()
	if !is_leader {
		reply.Value = ""
		reply.Err = ErrWrongLeader
		DPrintf("Get: reply=%v\n", reply)
		return
	}
	kv.waitFor(index)

	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, ok := kv.db[args.Key]
	if ok {
		reply.Value = value.Value
		reply.Err = OK
	} else {
		reply.Value = ""
		reply.Err = ErrNoKey
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	opResult := kv.replicate(Op{
		Op: args.Op,
		Key: args.Key,
		Value: args.Value,
		Seq: args.Seq,
		ClientId: args.ClientId,
	})
	reply.Err = opResult.err
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
	kv.proposals = list.New()
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.apply()
	
	return kv
}

func (kv *KVServer) apply() {
	ticker := time.NewTicker(100*time.Millisecond)
	lastSnapshotIndex := 0
	for {
		select {
		case m, ok := <- kv.applyCh:
			if !ok {
				return
			}

			var opResult OpResult
			handled := false
			kv.mu.Lock()
			if !m.CommandValid {
				DPrintf("received snapshot: index=%v\n", m.CommandIndex)
				kv.readSnapshot(m.Snapshot)
			} else if m.Command != nil {
				op := m.Command.(Op)
				DPrintf("received op: index=%v %v\n", m.CommandIndex, op)
				opResult = kv.handleOp(op)
				handled = true
			}
			kv.lastIncludedIndex = m.CommandIndex
			kv.lastIncludedTerm = m.Term
			kv.mu.Unlock()
			kv.cond.Broadcast()
			if handled {
				for kv.proposals.Len() > 0 {
					e := kv.proposals.Front()
					kv.proposals.Remove(e)
					proposal := e.Value.(Proposal)
					if proposal.term == m.Term {
						if proposal.index == m.CommandIndex {
							proposal.ch <- opResult
						} else {
							panic(fmt.Sprintf("mismatched index: %v vs. %v", proposal.index, m.CommandIndex))
						}
						break
					} else {
						proposal.ch <- OpResult{
							err: ErrWrongLeader,
						}
					}
				}
			}
		case <-ticker.C:
			if kv.maxraftstate > 0 && kv.rf.GetStateSize() >= int(float64(kv.maxraftstate) * 0.7) {
				snapshot, lastIncludedIndex, lastIncludedTerm := kv.takeSnapshot(lastSnapshotIndex)
				if snapshot != nil {
					kv.rf.SaveStateAndSnapshot(snapshot, lastIncludedIndex, lastIncludedTerm)
					lastSnapshotIndex = lastIncludedIndex
				}
			}
		}
	}
}

func (kv *KVServer) handleOp(op Op) OpResult {
	if op.Op == "Noop" {
		return OpResult{ err: OK }
	}
	
	// Detect duplicate reqs.
	oldSeq, ok := kv.clientSeq[op.ClientId]
	if ok && op.Seq == oldSeq {
		return OpResult{ err: OK }
	}
	
	switch op.Op {
	case "Put":
		kv.db[op.Key] = DbValue{
			Value: op.Value,
		}
	case "Append":
		oldValue, ok := kv.db[op.Key]
		if ok {
			kv.db[op.Key] = DbValue{
				Value: oldValue.Value + op.Value,
			}
		} else {
			kv.db[op.Key] = DbValue{
				Value: op.Value,
			}
		}
	default:
		panic(fmt.Sprintf("invalid op: %v\n", op.Op))
	}
	kv.clientSeq[op.ClientId] = op.Seq
	return OpResult{ err: OK }
}

func (kv *KVServer) waitFor(index int) {
	kv.mu.Lock()
	for kv.lastIncludedIndex < index {
		kv.cond.Wait()
	}
	kv.mu.Unlock()
}
	
// Propose an operation until it is applied or rejected.
func (kv *KVServer) replicate(op Op) OpResult {
	kv.mu.Lock()
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.mu.Unlock()
		return OpResult{ err: ErrWrongLeader }
	}

	DPrintf("replicate op: index=%v, %v\n", index, op)
	ch := make(chan OpResult)
	kv.proposals.PushBack(Proposal{
		term: term,
		index: index,
		ch: ch,
	})
	kv.mu.Unlock()
	opResult := <- ch
	return opResult
}

func (kv *KVServer) takeSnapshot(lastSnapshotIndex int) (snapshot []byte, lastIncludedIndex, lastIncludedTerm int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.lastIncludedIndex <= lastSnapshotIndex {
		return nil, 0, 0
	}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.db)
	e.Encode(kv.clientSeq)

	snapshot = w.Bytes()
	lastIncludedIndex = kv.lastIncludedIndex
	lastIncludedTerm = kv.lastIncludedTerm
	return
}

func (kv *KVServer) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var db map[string]DbValue
	var clientSeq map[int64]int64
	if d.Decode(&db) != nil ||
		d.Decode(&clientSeq) != nil {
		panic(fmt.Sprintf("decode failed"))
	} else {
		kv.db = db
		kv.clientSeq = clientSeq
	}
}
