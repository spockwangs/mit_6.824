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

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op string		// "PutAppend", "Get", "Config", or "Transfer"
	PutAppendReq PutAppendArgs
	GetReq GetArgs
	Config shardmaster.Config
	TransferReq TransferArgs
	DeleteShardReq DeleteShardArgs
}

type OpResult struct {
	putAppendReply PutAppendReply
	getReply GetReply
	transferReply TransferReply
	deleteShardReply DeleteShardReply
}

type OpResultCh struct {
	refCnt int
	opResultCh chan OpResult
}

type Db struct {
	Kv map[string]string
	ClientSeq map[int64]int64
}

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
	// Persistent state.
	shards map[int]Db	// shard => db
	shardConfig shardmaster.Config
	
	// Volatile state.
	opResultChs map[int]OpResultCh
	lastIncludedIndex int
	lastIncludedTerm int
	cond *sync.Cond		// predicate: currentTerm changed or lastIncludedIndex changed
	currentTerm int
	dead int32
	transfering bool	// wait for sending shards to next owner or my shards being sent to me
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	opResult, ok := kv.commit(Op{ Op: "Get", GetReq: *args })
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}
	*reply = opResult.getReply
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	opResult, ok := kv.commit(Op{
		Op: "PutAppend",
		PutAppendReq: *args,
	})
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}
	*reply = opResult.putAppendReply
}

func (kv *ShardKV) Transfer(args *TransferArgs, reply *TransferReply) {
	opResult, ok := kv.commit(Op{
		Op: "Transfer",
		TransferReq: *args,
	})
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}
	*reply = opResult.transferReply
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
	kv.cond = sync.NewCond(&kv.mu)

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.opResultChs = make(map[int]OpResultCh)
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
		_, isLeader := kv.rf.GetState()
		if isLeader {
			DPrintf("handleOp: gid=%v op=%v opResult=%v\n", kv.gid, op, opResult)
		}
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
}

func (kv *ShardKV) handleOp(op Op) OpResult {
	getDb := func (shard int) (Db, bool) {
		// Stop serving requests if the shard is not assigned to me.
		if gid := kv.shardConfig.Shards[shard]; gid != kv.gid {
			return Db{}, false
		}

		// Wait for the previous owner to transfer the shard to me before serving requests.
		db, ok := kv.shards[shard]
		if !ok {
			return Db{}, false
		}
		return db, true
	}

	switch op.Op {
	case "Config":
		// Reconfigure one at a time.
		if kv.shardConfig.Num >= op.Config.Num || kv.transfering {
			return OpResult{}
		}

		// For the first config, assign the shards that belongs to me, don't wait for the
		// previous owner.
		if op.Config.Num == 1 {
			for shard, gid := range op.Config.Shards {
				if gid == kv.gid {
					kv.shards[shard] = Db{
						Kv: make(map[string]string),
						ClientSeq: make(map[int64]int64),
					}
				}
			}
		}
		DPrintf("Config: gid=%v oldConfig=%v%v newConfig=%v%v\n",
			kv.gid, kv.shardConfig.Num, kv.shardConfig.Shards, op.Config.Num, op.Config.Shards)
		kv.shardConfig = op.Config
		kv.transfering = kv.isTransfering();
		return OpResult{}
	case "Get":
		db, found := getDb(op.GetReq.Shard)
		if !found {
			DPrintf("Get: gid=%v miss %v num=%v\n",
				kv.gid, op.GetReq.Shard, kv.shardConfig.Num)
			return OpResult{
				getReply: GetReply{ Err: ErrWrongGroup },
			}
		}
		value, ok := db.Kv[op.GetReq.Key]
		if ok {
			return OpResult{
				getReply: GetReply{ Err: OK, Value: value },
			}
		}
		return OpResult{
			getReply: GetReply{ Err: ErrNoKey },
		}
	case "PutAppend":
		db, found := getDb(op.PutAppendReq.Shard)
		if !found {
			DPrintf("PutAppend: gid=%v miss %v num=%v\n",
				kv.gid, op.PutAppendReq.Shard, kv.shardConfig.Num)
			return OpResult{
				putAppendReply: PutAppendReply{ Err: ErrWrongGroup },
			}
		}
		// Detect duplicate reqs.
		oldSeq, ok := db.ClientSeq[op.PutAppendReq.ClientId]
		if ok && op.PutAppendReq.Seq == oldSeq {
			return OpResult{
				putAppendReply: PutAppendReply{ Err: OK },
			}
		}
		if op.PutAppendReq.Op == "Put" {
			db.Kv[op.PutAppendReq.Key] = op.PutAppendReq.Value
		} else {
			oldValue, ok := db.Kv[op.PutAppendReq.Key]
			if ok {
				db.Kv[op.PutAppendReq.Key] = oldValue + op.PutAppendReq.Value
			} else {
				db.Kv[op.PutAppendReq.Key] = op.PutAppendReq.Value
			}
		}
		db.ClientSeq[op.PutAppendReq.ClientId] = op.PutAppendReq.Seq
		return OpResult{
			putAppendReply: PutAppendReply{ Err: OK },
		}
	case "Transfer":
		req := op.TransferReq
		if kv.shardConfig.Num != req.ConfigNum || kv.gid != req.DestGid {
			return OpResult{
				transferReply: TransferReply{ Err: ErrWrongGroup, ConfigNum: kv.shardConfig.Num },
			}
		}
		if _, found := kv.shards[req.Shard]; found {
			return OpResult{
				transferReply: TransferReply{ Err: OK, ConfigNum: kv.shardConfig.Num },
			}
		}
			
		kv.shards[req.Shard] = cloneDb(req.Db)
		kv.transfering = kv.isTransfering()
		return OpResult{
			transferReply: TransferReply{ Err: OK, ConfigNum: kv.shardConfig.Num },
		}
	case "DeleteShard":
		req := op.DeleteShardReq
		delete(kv.shards, req.Shard)
		kv.transfering = kv.isTransfering()
		return OpResult{
			deleteShardReply: DeleteShardReply{ Err: OK },
		}
	default:
		log.Fatalf("bad op: %v\n", op.Op)
	}
	return OpResult{}
}

func (kv *ShardKV) commit(op Op) (result OpResult, ok bool) {
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		ok = false
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
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
	for kv.currentTerm <= term {
		if kv.lastIncludedIndex >= index {
			result = <- opResultCh.opResultCh
			ok = true
			return
		}
		kv.cond.Wait()
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
	e.Encode(kv.shardConfig)

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
	var config shardmaster.Config
	if d.Decode(&shards) != nil ||
		d.Decode(&config) != nil {
		panic(fmt.Sprintf("decode failed"))
	} else {
		kv.shards = shards
		kv.shardConfig = config
		kv.transfering = kv.isTransfering()
	}
}

func (kv *ShardKV) updateShardConfigRoutine() {
	ticker := time.NewTicker(100*time.Millisecond)
	mck := shardmaster.MakeClerk(kv.masters)
	for !kv.killed() {
		select {
		case <-ticker.C:
			_, isLeader := kv.rf.GetState()
			if !isLeader {
				break
			}
			kv.mu.Lock()
			if kv.transfering {
				kv.mu.Unlock()
				kv.transferShards()
			} else {
				idx := kv.shardConfig.Num
				kv.mu.Unlock()
				config := mck.Query(idx+1);
				kv.mu.Lock()
				if config.Num > kv.shardConfig.Num {
					kv.mu.Unlock()
					kv.commit(Op{
						Op: "Config",
						Config: config,
					})
				} else {
					kv.mu.Unlock()
				}
			}
		}
	}
}

func (kv *ShardKV) transferShards() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	
	for shard := range kv.shards {
		gid := kv.shardConfig.Shards[shard]
		if gid == kv.gid {
			continue
		}
		transferArgs := TransferArgs{
			ConfigNum: kv.shardConfig.Num,
			Db: cloneDb(kv.shards[shard]),
			SourceGid: kv.gid,
			DestGid: gid,
			Shard: shard,
		}
		servers := kv.shardConfig.Groups[transferArgs.DestGid]
		kv.mu.Unlock()
		kv.sendTransfer(servers, &transferArgs)
		kv.commit(Op{
			Op: "DeleteShard",
			DeleteShardReq: DeleteShardArgs{
				Shard: shard,
			},
		})
		kv.mu.Lock()
	}
}

func (kv *ShardKV) sendTransfer(servers []string, args *TransferArgs) {
	for {
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])
			reply := TransferReply{}
			ok := srv.Call("ShardKV.Transfer", args, &reply)
			if ok && (reply.Err == OK || reply.ConfigNum > args.ConfigNum) {
				return
			}
			// ... not ok, or ErrWrongLeader
		}
	}
}

func cloneDb(db Db) Db {
	copy := Db{
		Kv: make(map[string]string),
		ClientSeq: make(map[int64]int64),
	}
	for k, v := range db.Kv {
		copy.Kv[k] = v
	}
	for k, v := range db.ClientSeq {
		copy.ClientSeq[k] = v
	}
	return copy
}

func (kv *ShardKV) isTransfering() bool {
	var outShards []int
	var inShards []int
	for shard := range kv.shards {
		gid := kv.shardConfig.Shards[shard]
		if gid != kv.gid {
			outShards = append(outShards, shard)
		}
	}
	for shard, gid := range kv.shardConfig.Shards {
		if gid == kv.gid {
			_, found := kv.shards[shard]
			if !found {
				inShards = append(inShards, shard)
				return true
			}
		}
	}
	DPrintf("gid=%v num=%v outShards=%v inShards=%v\n", kv.gid, kv.shardConfig.Num, outShards, inShards)
	if len(outShards) > 0 || len(inShards) > 0 {
		return true
	}
	return false
}
