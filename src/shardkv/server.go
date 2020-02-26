package shardkv


import "../shardmaster"
import "../labrpc"
import "../raft"
import "sync"
import "../labgob"
import "log"
import "sync/atomic"
import "time"
import "bytes"
import "fmt"

const Debug = 1

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
	Op string		// "Put" or "Append" or "Noop" or "Config"
	Key string
	Value string
	ClientId int64
	Seq int64
	Config shardmaster.Config
	Shard int
}

type OpResult struct {
	err Err
}

type OpResultCh struct {
	refCnt int
	opResultCh chan OpResult
}

type Db map[string]string

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	opResultCh map[int]OpResultCh
	shards map[int]Db	  // shard => db
	clientSeq map[int64]int64 // client id => last op seq
	lastIncludedIndex int
	lastIncludedTerm int
	cond *sync.Cond		// predicate: currentTerm changed or lastIncludedIndex changed
	currentTerm int
	shardConfig shardmaster.Config
	dead int32
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	_, ok := kv.commit(Op{ Op: "Noop" })
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	db, ok := kv.shards[args.Shard]
	if !ok {
		reply.Err = ErrWrongGroup
		return
	}
	value, ok := db[args.Key]
	if ok {
		reply.Value = value
		reply.Err = OK
	} else {
		reply.Value = ""
		reply.Err = ErrNoKey
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	opResult, ok := kv.commit(Op{
		Op: args.Op,
		Key: args.Key,
		Value: args.Value,
		Seq: args.Seq,
		ClientId: args.ClientId,
	})
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}
	
	reply.Err = opResult.err
	return 
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *ShardKV) killed() bool {
	return atomic.LoadInt32(&kv.dead) == 1
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.shards = make(map[int]Db)
	kv.clientSeq = make(map[int64]int64)
	kv.cond = sync.NewCond(&kv.mu)

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.opResultCh = make(map[int]OpResultCh)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.apply()
	go kv.updateShardConfigRoutine()

	return kv
}

func (kv *ShardKV) apply() {
	ticker := time.NewTicker(5*time.Millisecond)
	for {
		select {
		case m, ok := <- kv.applyCh:
			if !ok {
				return
			}
			kv.handleApplyMsg(m)
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

func (kv *ShardKV) handleApplyMsg(m raft.ApplyMsg) {
	kv.mu.Lock()
	if !m.CommandValid {
		kv.readSnapshot(m.Snapshot)
	} else {
		op := m.Command.(Op)
		opResult := kv.handleOp(op)
		kv.rf.GetStateSize
		DPrintf("handleOp: op=%v opResult=%v\n", op, opResult)
		if opResultCh, ok := kv.opResultCh[m.CommandIndex]; ok {
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
}

func (kv *ShardKV) handleOp(op Op) OpResult {
	if op.Op == "Noop" {
		return OpResult{ err: OK }
	}
	if op.Op == "Config" {
		kv.shardConfig = op.Config
		return OpResult{ err: OK }
	}

	if gid := kv.shardConfig.Shards[op.Shard]; gid != kv.gid {
		return OpResult{ err: ErrWrongGroup }
	}
	db, ok := kv.shards[op.Shard]
	if !ok {
		kv.shards[op.Shard] = make(Db)
		db, _ = kv.shards[op.Shard]
	}

	// Detect duplicate reqs.
	oldSeq, ok := kv.clientSeq[op.ClientId]
	if ok && op.Seq == oldSeq {
		return OpResult{ err: OK }
	}

	switch op.Op {
	case "Put":
		db[op.Key] = op.Value
	case "Append":
		oldValue, ok := db[op.Key]
		if ok {
			db[op.Key] = oldValue + op.Value
		} else {
			db[op.Key] = op.Value
		}
	default:
		log.Fatalf("invalid op: %v\n", op.Op)
	}

	kv.clientSeq[op.ClientId] = op.Seq
	return OpResult{ err: OK }
}

func (kv *ShardKV) commit(op Op) (result OpResult, ok bool) {
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		ok = false
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	opResultCh, ok2 := kv.opResultCh[index]
	if ok2 {
		opResultCh.refCnt++
		kv.opResultCh[index] = opResultCh
	} else {
		kv.opResultCh[index] = OpResultCh{
			refCnt: 1,
			opResultCh: make(chan OpResult),
		}
	}
	for kv.currentTerm <= term {
		if kv.lastIncludedIndex >= index {
			opResultCh, _ = kv.opResultCh[index]
			kv.mu.Unlock()
			result = <- opResultCh.opResultCh
			kv.mu.Lock()
			opResultCh.refCnt--
			kv.opResultCh[index] = opResultCh
			if opResultCh.refCnt == 0 {
				delete(kv.opResultCh, index)
			}
			ok = true
			return
		}
		kv.cond.Wait()
	}
	opResultCh, _ = kv.opResultCh[index]	
	opResultCh.refCnt--
	kv.opResultCh[index] = opResultCh
	if opResultCh.refCnt == 0 {
		delete(kv.opResultCh, index)
	}
	ok = false
	return 
}

func (kv *ShardKV) takeSnapshot() (snapshot []byte, lastIncludedIndex, lastIncludedTerm int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.shards)
	e.Encode(kv.clientSeq)

	snapshot = w.Bytes()
	lastIncludedIndex = kv.lastIncludedIndex
	lastIncludedTerm = kv.lastIncludedTerm
	return
}

func (kv *ShardKV) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var shards map[int]Db
	var clientSeq map[int64]int64
	if d.Decode(&shards) != nil ||
		d.Decode(&clientSeq) != nil {
		panic(fmt.Sprintf("decode failed"))
	} else {
		kv.shards = shards
		kv.clientSeq = clientSeq
	}
}

func (kv *ShardKV) updateShardConfigRoutine() {
	ticker := time.NewTicker(100*time.Millisecond)
	lastConfigNum := 0
	mck := shardmaster.MakeClerk(kv.masters)
	for !kv.killed() {
	loop:
		select {
		case <-ticker.C:
			_, isLeader := kv.rf.GetState()
			if !isLeader {
				break loop
			}
			config := mck.Query(-1)
			if config.Num > lastConfigNum {
				lastConfigNum = config.Num
				kv.commit(Op{
					Op: "Config",
					Config: config,
				})
			}
		}
	}
}
