package shardmaster


import "../raft"
import "../labrpc"
import "sync"
import "../labgob"
import "log"
import "fmt"
import "time"
import "bytes"
import "sort"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	cond *sync.Cond
	currentTerm int
	lastIncludedIndex int
	lastIncludedTerm int
	clientSeq map[int64]int64 // to deduplicate reqs
	configs []Config // indexed by config num
	maxraftstate int
}


type Op struct {
	// Your data here.
	Op string		// "Noop", "Join", "Leave", or "Move"
	ClientId int64
	Seq int64
	// For "Join"
	Servers map[int][]string // new GID -> servers mappings
	// For "Leave"
	GIDs []int
	// For "Move"
	Shard int
	GID int
}


func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	ok := sm.commit(Op{
		Op: "Join",
		ClientId: args.ClientId,
		Seq: args.Seq,
		Servers: args.Servers,
	})
	reply.WrongLeader = !ok
	return
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	for _, gid := range args.GIDs {
		if gid == 0 {
			reply.Err = Err(fmt.Sprintf("bad gid %v", gid))
			return
		}
	}
	
	ok := sm.commit(Op{
		Op: "Leave",
		ClientId: args.ClientId,
		Seq: args.Seq,
		GIDs: args.GIDs,
	})
	reply.WrongLeader = !ok
	return
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	ok := sm.commit(Op{
		Op: "Move",
		ClientId: args.ClientId,
		Seq: args.Seq,
		Shard: args.Shard,
		GID: args.GID,
	})
	reply.WrongLeader = !ok
	return
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	ok := sm.commit(Op{Op: "Noop"})
	if !ok {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false

	sm.mu.Lock()
	defer sm.mu.Unlock()
	if args.Num < 0 || args.Num >= len(sm.configs) {
		reply.Config = *sm.getLatestConfig()
	} else {
		reply.Config = sm.configs[args.Num]
	}
	return
}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.clientSeq = make(map[int64]int64)
	sm.cond = sync.NewCond(&sm.mu)
	go sm.apply()
	
	return sm
}

func (sm *ShardMaster) apply() {
	ticker := time.NewTicker(5*time.Millisecond)
	for {
		select {
		case m, ok := <- sm.applyCh:
			if !ok {
				return
			}
			sm.handleApplyMsg(m)
		case <-ticker.C:
			term, _ := sm.rf.GetState()
			sm.mu.Lock()
			if term > sm.currentTerm {
				sm.currentTerm = term
				sm.cond.Broadcast()
			}
			sm.mu.Unlock()
			
			if sm.maxraftstate > 0 && sm.rf.GetStateSize() >= sm.maxraftstate {
				snapshot, lastIncludedIndex, lastIncludedTerm := sm.takeSnapshot()
				sm.rf.SaveStateAndSnapshot(snapshot, lastIncludedIndex, lastIncludedTerm)
			}
		}
	}
}

func (sm *ShardMaster) handleApplyMsg(m raft.ApplyMsg) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if !m.CommandValid {
		sm.readSnapshot(m.Snapshot)
	} else {
		op := m.Command.(Op)
		// Detect duplicate reqs.
		oldSeq, ok := sm.clientSeq[op.ClientId]
		if !ok || op.Seq != oldSeq {
			switch op.Op {
			case "Join":
				newConfig := sm.allocateConfig()
				for gid, servers := range op.Servers {
					newConfig.Groups[gid] = servers
				}
				sm.rebalance(newConfig)
			case "Leave":
				newConfig := sm.allocateConfig()
				deletedGids := map[int]bool{}
				for _, gid := range op.GIDs {
					delete(newConfig.Groups, gid)
					deletedGids[gid] = true
				}
				for shard, gid := range newConfig.Shards {
					_, ok := deletedGids[gid]
					if ok {
						newConfig.Shards[shard] = 0
					}
				}
				sm.rebalance(newConfig)
			case "Move":
				newConfig := sm.allocateConfig()
				newConfig.Shards[op.Shard] = op.GID
			}
		}
		sm.clientSeq[op.ClientId] = op.Seq
	}
	sm.lastIncludedIndex = m.CommandIndex
	sm.lastIncludedTerm = m.Term
	if sm.lastIncludedTerm > sm.currentTerm {
		sm.currentTerm = sm.lastIncludedTerm
	}
	sm.cond.Broadcast()
}

func (sm *ShardMaster) takeSnapshot() (snapshot []byte, lastIncludedIndex, lastIncludedTerm int) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(sm.configs)
	e.Encode(sm.clientSeq)
	snapshot = w.Bytes()
	lastIncludedIndex = sm.lastIncludedIndex
	lastIncludedTerm = sm.lastIncludedTerm
	return
}

func (sm *ShardMaster) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var configs []Config
	var clientSeq map[int64]int64
	if d.Decode(&configs) != nil ||
		d.Decode(&clientSeq) != nil {
		panic(fmt.Sprintf("decode failed"))
	} else {
		sm.configs = configs
		sm.clientSeq = clientSeq
	}
}

func (sm *ShardMaster) commit(op Op) bool {
	index, term, isLeader := sm.rf.Start(op)
	if !isLeader {
		return false
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()
	for sm.currentTerm <= term {
		if sm.lastIncludedIndex >= index {
			if len(sm.configs) > 1 {
				DPrintf("before config: %v\n, after config: %v\n",
					sm.configs[len(sm.configs)-2].toString(),
					sm.getLatestConfig().toString())
			}
			return true
		}
		sm.cond.Wait()
	}
	return false
}

func (sm *ShardMaster) allocateConfig() *Config {
	newConfig := sm.configs[len(sm.configs)-1].clone()
	newConfig.Num++
	sm.configs = append(sm.configs, *newConfig)
	return &(sm.configs[len(sm.configs)-1])
}

func (c *Config) clone() *Config {
	newConfig := *c
	newConfig.Groups = make(map[int][]string)
	for gid, servers := range c.Groups {
		newConfig.Groups[gid] = servers
	}
	return &newConfig
}

func (sm *ShardMaster) getLatestConfig() *Config {
	return &sm.configs[len(sm.configs)-1]
}

func (c *Config) toString() string {
	var s string
	s += fmt.Sprintf("Num=%v\n", c.Num)
	for shard, gid := range c.Shards {
		s += fmt.Sprintf("shard %v => %v\n", shard, gid)
	}
	s += fmt.Sprintf("[")
	for gid, _ := range c.Groups {
		s += fmt.Sprintf("%v ", gid)
	}
	s += fmt.Sprintf("]")
	return s
}

func (sm *ShardMaster) rebalance(config *Config) {
	if len(config.Groups) == 0 {
		for shard := range config.Shards {
			config.Shards[shard] = 0
		}
		return
	}
	
	q := NShards/len(config.Groups)
	r := NShards % len(config.Groups)
	allocatedNumShards := func(i int) int {
		if i < r {
			return q+1
		}
		return q
	}
	
	unassignedShards := []int{}
	gid2shards := make(map[int][]int)
	for shard, gid := range config.Shards {
		if gid == 0 {
			unassignedShards = append(unassignedShards, shard)
		} else {
			gid2shards[gid] = append(gid2shards[gid], shard)
		}
	}

	type GidStat struct {
		gid int
		numShards int
	}
	gidNumShards := []GidStat{}
	for gid, _ := range config.Groups {
		shards, ok := gid2shards[gid]
		num := 0
		if ok {
			num = len(shards)
		}
		gidNumShards = append(gidNumShards, GidStat{gid: gid, numShards: num})
	}
	sort.Slice(gidNumShards, func(i, j int) bool {
		return gidNumShards[i].numShards > gidNumShards[j].numShards
	})
	DPrintf("gidNumShards %v\nconfig %v", gidNumShards, config.toString())
	
	for i, gidStat := range gidNumShards {
		gid := gidStat.gid
		num := allocatedNumShards(i)
		if len(gid2shards[gid]) > num {
			unassignedShards = append(unassignedShards, gid2shards[gid][num:]...)
			gid2shards[gid] = gid2shards[gid][:num]
		} else if len(gid2shards[gid]) < num {
			assignCnt := num - len(gid2shards[gid])
			shards := unassignedShards[:assignCnt]
			unassignedShards = unassignedShards[assignCnt:]
			for _, shard := range shards {
				config.Shards[shard] = gid
			}
		}
	}
}
