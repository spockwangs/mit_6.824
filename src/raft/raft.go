package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "sync/atomic"
import "../labrpc"

import "bytes"
import "../labgob"
import "math/rand"
import "time"
import "sort"
import "fmt"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	Term int
}

//
// A Go object implementing a single Raft peer.
//
const (
	FOLLOWER int = 0
	CANDIDATE int = 1
	LEADER int = 2
)
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor int
	logs []LogEntry
	logOffset int		// the absolute index of the first log entry, init to 0

	// Volatile state on all servers.
	commitIndex int
	lastApplied int		// Accessed by only one goroutine.
	status int
	electionExpireTime time.Time
	applyCh chan ApplyMsg
	
	// Volatile state on leaders
	nextIndex []int		// map peer's index to its next index
	matchIndex []int 	// map peer's index to its highest replicated log entry
	stateChanged *sync.Cond	// predicate: killed or term changed or logs appended
}
type LogEntry struct {
	Command interface{}
	Term int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.status == LEADER)

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	rf.persister.SaveRaftState(rf.writePersist())
}

func (rf *Raft) writePersist() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.logOffset)
	data := w.Bytes()
	return data
}	
//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votedFor int
	var logs []LogEntry
	var logOffset int
	if d.Decode(&term) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&logOffset) != nil {
		panic(fmt.Sprintf("decode failed"))
	} else {
		rf.currentTerm = term
		rf.votedFor = votedFor
		rf.logs = logs
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

func (voteReq *RequestVoteArgs) toString() string {
	return fmt.Sprintf("(Term=%v, CandidateId=%v, LastLogIndex=%v, LastLogTerm=%v)",
		voteReq.Term, voteReq.CandidateId, voteReq.LastLogIndex, voteReq.LastLogTerm)
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

func (voteResp *RequestVoteReply) toString() string {
	return fmt.Sprintf("(Term=%v, VoteGranted=%v)", voteResp.Term, voteResp.VoteGranted)
}
	
//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	changed := false
	defer func() {
		if changed {
			rf.persist()
			rf.stateChanged.Broadcast()
		}
	}()
	
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.status = FOLLOWER
		rf.votedFor = -1
		changed = true
	}
	
	if rf.votedFor >= 0 && rf.votedFor != args.CandidateId {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	lastLogTerm := rf.logs[len(rf.logs)-1].Term
	if args.LastLogTerm > lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex + 1 >= len(rf.logs) + rf.logOffset) {
		rf.status = FOLLOWER
		rf.reschedule(false)
		rf.votedFor = args.CandidateId
		changed = true
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		return
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	return
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

func (this *AppendEntriesArgs) toString() string {
	return fmt.Sprintf("(Term=%v, LeaderId=%v, PrevLogIndex=%v, PrevLogTerm=%v, #Entries=%v, LeaderCommit=%v)",
		this.Term, this.LeaderId, this.PrevLogIndex, this.PrevLogTerm, len(this.Entries), this.LeaderCommit)
}

type AppendEntriesReply struct {
	Term int
	Success bool
	ConflictTerm int
	ConflictIndex int
}

func (this *AppendEntriesReply) toString() string {
	return fmt.Sprintf("(Term=%v, Success=%v, ConflictTerm=%v, ConflictIndex=%v)",
		this.Term, this.Success, this.ConflictTerm, this.ConflictIndex)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	changed := false	// Is persistent state changed?
	defer func() {
		if changed {
			rf.persist()
			rf.stateChanged.Broadcast()
		}
	}()
	
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.status = FOLLOWER
	rf.reschedule(false)
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		changed = true
	}
	reply.Term = rf.currentTerm
	if rf.logOffset + len(rf.logs) <= args.PrevLogIndex {
		reply.Success = false
		reply.ConflictIndex = len(rf.logs) + rf.logOffset
		reply.ConflictTerm = 0
		return
	} else if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.ConflictTerm = rf.logs[args.PrevLogIndex].Term
		// Find the first entry with term equal to ConflictTerm.
		reply.ConflictIndex = 0
		for i := args.PrevLogIndex; i > 0; i-- {
			if rf.logs[i-1-rf.logOffset].Term != reply.ConflictTerm {
				reply.ConflictIndex = i
			}
		}
		return
	}

	// Find the first entry with conflicts.
	for i, j := args.PrevLogIndex + 1, 0; j < len(args.Entries); i, j = i+1, j+1 {
		if i >= len(rf.logs) + rf.logOffset || rf.logs[i-rf.logOffset].Term != args.Entries[j].Term {
			rf.logs = append(rf.logs[:i-rf.logOffset], args.Entries[j:]...)			
			changed = true
			break
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		newCommitIndex := min(args.LeaderCommit, len(rf.logs)+rf.logOffset-1)
		if newCommitIndex > rf.commitIndex {
			rf.commitIndex = newCommitIndex
			rf.stateChanged.Broadcast()
		}
	}
	reply.Success = true
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if rf.killed() {
		return false
	}
	rf.mu.Lock()
	DPrintf("me=%v%v, to=%v, appendEntriesReq=%v, appendEntriesResp=%v, ok=%v",
		rf.me, rf.toString(), server, args.toString(), reply.toString(), ok)
	rf.mu.Unlock()
	return ok
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if rf.killed() {
		return false
	}
	rf.mu.Lock()
	DPrintf("me=%v%v, to=%v, voteReq=%v, voteResp=%v, ok=%v\n",
		rf.me, rf.toString(), server, args.toString(), reply.toString(), ok)
	rf.mu.Unlock()
	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	changed := false
	defer func() {
		if changed {
			rf.persist()
		}
	}()
	
	if rf.status != LEADER {
		isLeader = false
		return index, term, isLeader
	}

	rf.logs = append(rf.logs, LogEntry {
		Term: rf.currentTerm,
			Command: command,
		})
	changed = true
	index = len(rf.logs) + rf.logOffset - 1
	term = rf.currentTerm

	// Start commit immediately can shorten the commit latency but use RPC bandwidth less
	// efficiently.
	// rf.startCommit()
	
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.stateChanged.Broadcast()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]LogEntry, 1)
	rf.logOffset = 0
	
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.status = FOLLOWER
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.logs) + rf.logOffset
	}
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.matchIndex {
		rf.matchIndex[i] = rf.logOffset
	}
	rf.applyCh = applyCh
	rf.stateChanged = sync.NewCond(&rf.mu)
	rf.reschedule(false)

	go rf.tick()
	go rf.apply()
	
	return rf
}

func (rf *Raft) tick() {
	for !rf.killed() {
		changed := false
		rf.mu.Lock()
		now := time.Now()
		if rf.electionExpireTime.After(now) {
			rf.mu.Unlock()
			time.Sleep(10*time.Millisecond)
			continue
		}
		
		switch rf.status {
		case FOLLOWER:
			rf.status = CANDIDATE
			fallthrough
		case CANDIDATE:
			rf.startVotes()
			changed = true
		case LEADER:
			rf.startCommit()
		}
		if changed {
			rf.persist()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) reschedule(now bool) {
	if now {
		rf.electionExpireTime = time.Now()
		return
	}
	
	const kHeartbeatIntervalMillis int = 200
	const kElectionTimeoutMillis int = 2*kHeartbeatIntervalMillis
	switch rf.status {
	case FOLLOWER, CANDIDATE:
		rf.electionExpireTime = time.Now().Add(
			time.Duration(kElectionTimeoutMillis + rand.Intn(kElectionTimeoutMillis))*time.Millisecond)
	case LEADER:
		rf.electionExpireTime = time.Now().Add(
			time.Duration(kHeartbeatIntervalMillis) * time.Millisecond)
	}
}

// Start to send vote requests to peers.
func (rf *Raft) startVotes() {
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.reschedule(false)

	voteReq := RequestVoteArgs{}
	voteReq.Term = rf.currentTerm
	voteReq.CandidateId = rf.me
	voteReq.LastLogIndex = len(rf.logs) + rf.logOffset - 1
	voteReq.LastLogTerm = rf.logs[len(rf.logs)-1].Term

	votes_mu := sync.Mutex{}
	votes := make([]bool, len(rf.peers))
	for i, _ := range rf.peers {
		if i == rf.me {
			votes_mu.Lock()
			votes[i] = true
			votes_mu.Unlock()
			continue
		}

		go func (i int) {
			voteResp := RequestVoteReply{}
			ok := rf.sendRequestVote(i, &voteReq, &voteResp)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			changed := false
			defer func() {
				if changed {
					rf.persist()
				}
			}()
			
			if voteResp.Term > rf.currentTerm {
				rf.currentTerm = voteResp.Term
				rf.votedFor = -1
				rf.status = FOLLOWER
				changed = true
				return
			}
			// Skip old resp.
			if voteReq.Term != rf.currentTerm {
				return
			}

			if rf.status == CANDIDATE {
				votes_mu.Lock()
				votes[i] = voteResp.VoteGranted
				vote_cnt := 0
				for _, vote := range votes {
					if vote {
						vote_cnt++
					}
				}
				votes_mu.Unlock()
				if vote_cnt >= (len(rf.peers)/2 + 1) {
					DPrintf("%v becomes leader\n", rf.me)
					rf.status = LEADER
					for i := range rf.nextIndex {
						rf.nextIndex[i] = len(rf.logs) + rf.logOffset
						rf.matchIndex[i] = rf.logOffset
					}
					// start heartbeat right now to prevent some
					// follower from becoming a candidate.
					rf.reschedule(true)
				}
			}
		} (i)
	}
}

// Start to send entries (or heartbeats if entries are empty)  to followers.
func (rf *Raft) startCommit() {
	rf.nextIndex[rf.me] = len(rf.logs) + rf.logOffset
	rf.matchIndex[rf.me] = rf.nextIndex[rf.me] - 1
	rf.reschedule(false)
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(i int) {
			for {
				appendEntriesReq := AppendEntriesArgs{}
				ok := func () bool {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.status != LEADER {
						return false
					}
					appendEntriesReq.Term = rf.currentTerm
					appendEntriesReq.LeaderId = rf.me
					appendEntriesReq.PrevLogIndex = rf.nextIndex[i] - 1
					appendEntriesReq.PrevLogTerm = rf.logs[rf.nextIndex[i]-1-rf.logOffset].Term
					appendEntriesReq.LeaderCommit = rf.commitIndex
					if rf.logOffset + len(rf.logs)-1 >= rf.nextIndex[i] {
						appendEntriesReq.Entries = rf.logs[rf.nextIndex[i]-rf.logOffset:]
					}
					return true
				}()
				if !ok {
					return
				}
				
				appendEntriesResp := AppendEntriesReply{}
				ok = rf.sendAppendEntries(i, &appendEntriesReq, &appendEntriesResp)
				if !ok {
					return
				}

				ok = func () bool {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					changed := false
					defer func() {
						if changed {
							rf.persist()
						}
					}()
					
					if appendEntriesResp.Term > rf.currentTerm {
						rf.currentTerm = appendEntriesResp.Term
						rf.status = FOLLOWER
						rf.votedFor = -1
						rf.reschedule(false)
						changed = true
						return true
					}
					// Skip old resp.
					if appendEntriesReq.Term != rf.currentTerm {
						return true
					}
					if appendEntriesResp.Success {
						// Use max() to protect against old resp.
						rf.matchIndex[i] = max(rf.matchIndex[i],
							appendEntriesReq.PrevLogIndex + len(appendEntriesReq.Entries))
						rf.nextIndex[i] = rf.matchIndex[i] + 1
						DPrintf("me=%v, matchIndex=%v, len(logs)=%v\n",
							rf.me, rf.matchIndex, len(rf.logs))
						// Sort matchIndex[] from big to small.
						sortedMatchIndex := make([]int, len(rf.matchIndex))
						copy(sortedMatchIndex, rf.matchIndex)
						sort.Slice(sortedMatchIndex, func(i, j int) bool {
							return sortedMatchIndex[i] > sortedMatchIndex[j]
						})
						for j := sortedMatchIndex[len(rf.peers)/2]; j > rf.commitIndex; j-- {
							if rf.logs[j-rf.logOffset].Term == rf.currentTerm {
								rf.commitIndex = j
								rf.stateChanged.Broadcast()
								// Start heartbeat right now to tell
								// followers the new commit index
								// immediately and speed up the
								// commit process.
								rf.reschedule(true)
								break
							}
						}
						return true
					}
					// AppendEntries fails because of log inconsistency.
					if appendEntriesReq.Term >= appendEntriesResp.Term {
						// oldNextIndex := rf.nextIndex[i]
						found := false
						j := rf.nextIndex[i]-2
						for ; j > 0; j-- {
							if rf.logs[j-rf.logOffset].Term == appendEntriesResp.ConflictTerm {
								found = true
								break
							}
						}
						if found {
							rf.nextIndex[i] = j + 1
						} else {
							rf.nextIndex[i] = appendEntriesResp.ConflictIndex
						}
						return false
					}
					return true
				}()
				if ok {
					break
				}
			}
		} (i)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// Apply commited log entries periodically.
func (rf *Raft) apply() {
	for {
		var entries []LogEntry
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			for rf.lastApplied >= rf.commitIndex {
				rf.stateChanged.Wait()
			}
			entries = append([]LogEntry{},
				rf.logs[rf.lastApplied+1-rf.logOffset:rf.commitIndex+1-rf.logOffset]...)
		}()
		if entries != nil {
			for _, entry := range entries {
				rf.lastApplied++
				rf.applyCh <- ApplyMsg {
					CommandValid: true,
						Command: entry.Command,
						CommandIndex: rf.lastApplied,
					}
			}
		}
	}
}

func (rf *Raft) toString() string {
	var s string
	switch rf.status {
	case FOLLOWER:
		s = "FOLLOWER"
	case CANDIDATE:
		s = "CANDIDATE"
	case LEADER:
		s = "LEADER"
	}
	return fmt.Sprintf("(term=%v, status=%v, commitIdx=%v)", rf.currentTerm, s, rf.commitIndex)
}

func (rf *Raft) GetStateSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) InstallSnapshot(kvSnapshot []byte, lastApplied int) {
	rf.mu.Lock()
	rf.logs = append([]LogEntry{}, rf.logs[lastApplied+1:]...)
	rf.logOffset = lastApplied+1
	rf.persister.SaveStateAndSnapshot(rf.writePersist(), kvSnapshot)
	rf.mu.Unlock()
}
