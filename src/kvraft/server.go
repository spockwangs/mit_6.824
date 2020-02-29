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
	opResultCh chan OpResult
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
	cond *sync.Cond		// predicate: currentTerm changed or lastIncludedIndex changed
	currentTerm int
	opResultChs map[int]OpResultCh
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	opResult := kv.commit(Op{ Op: "Noop" })
	if opResult.err != OK {
		reply.Err = opResult.err
		return
	}

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
	opResult := kv.commit(Op{
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
	kv.opResultChs = make(map[int]OpResultCh)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.apply()
	
	return kv
}

func (kv *KVServer) apply() {
	ticker := time.NewTicker(5*time.Millisecond)
	for {
		select {
		case m, ok := <- kv.applyCh:
			if !ok {
				return
			}
			kv.mu.Lock()
			if !m.CommandValid {
				DPrintf("received snapshot: index=%v\n", m.CommandIndex)
				kv.readSnapshot(m.Snapshot)
			} else {
				op := m.Command.(Op)
				DPrintf("received op: index=%v %v\n", m.CommandIndex, op)
				opResult := kv.handleOp(op)
				if opResultCh, ok := kv.opResultChs[m.CommandIndex]; ok {
					kv.mu.Unlock()
					opResultCh.opResultCh <- opResult
					kv.mu.Lock()
				}
			}
			kv.lastIncludedIndex = m.CommandIndex
			kv.lastIncludedTerm = m.Term
			if kv.lastIncludedTerm > kv.currentTerm {
				kv.currentTerm = kv.lastIncludedTerm
			}
			kv.cond.Broadcast()
			kv.mu.Unlock()
		case <-ticker.C:
			term, _ := kv.rf.GetState()
			kv.mu.Lock()
			if term > kv.currentTerm {
				kv.currentTerm = term
				kv.cond.Broadcast()
			}
			kv.mu.Unlock()
			
			if kv.maxraftstate > 0 && kv.rf.GetStateSize() >= kv.maxraftstate {
				snapshot, lastIncludedIndex, lastIncludedTerm := kv.takeSnapshot()
				kv.rf.SaveStateAndSnapshot(snapshot, lastIncludedIndex, lastIncludedTerm)
			}
		}
	}
}

func (kv *KVServer) handleOp(op Op) OpResult {

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
	}
	kv.clientSeq[op.ClientId] = op.Seq
	return OpResult{ err: OK }
}

func (kv *KVServer) commit(op Op) OpResult {
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		return OpResult{ err: ErrWrongLeader }
	}

	DPrintf("commit op: index=%v, %v\n", index, op)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// 虽然多个提交可能会使用同一个index（第一次提交时还是leader，然后立即失去leader角色，然后又
	// 重新获得leader，又一个client提交，这2次返回的index可能相同），但是最多只有一个client的提
	// 交会成功，所以只有一个client会从opResultCh收到通知。
	opResultCh, ok := kv.opResultChs[index]
	if ok {
		opResultCh.refCnt++
		kv.opResultChs[index] = opResultCh
	} else {
		opResultCh = OpResultCh{
			opResultCh: make(chan OpResult, 1),
			refCnt: 1,
		}
		kv.opResultChs[index] = opResultCh
	}
	defer func() {
		ch, ok := kv.opResultChs[index]
		if !ok {
			panic(fmt.Sprintf("index %v of kv.opResultChs does not exist\n", index))
		}
		if ch.refCnt == 1 {
			delete(kv.opResultChs, index)
		} else {
			ch.refCnt--
			kv.opResultChs[index] = ch
		}
	}()
	for kv.currentTerm <= term {
		if kv.lastIncludedIndex >= index {
			DPrintf("commit op: index=%v, %v, true\n", index, op)
			opResult := <- opResultCh.opResultCh
			return opResult
		}
		kv.cond.Wait()
	}
	DPrintf("commit op: index=%v, %v, false\n", index, op)
	return OpResult{ err: ErrWrongLeader }
}

func (kv *KVServer) takeSnapshot() (snapshot []byte, lastIncludedIndex, lastIncludedTerm int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
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
