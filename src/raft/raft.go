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

// import "bytes"
// import "../labgob"
import "math/rand"
import "time"
import "sort"

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

	// Volatile state on all servers.
	commitIndex int
	lastApplied int
	status int
	electionExpireTime time.Time
	applyCh chan ApplyMsg
	
	// Volatile state on leaders
	nextIndex []int		// map peer's index to its next index
	matchIndex []int 	// map peer's index to its highest replicated log entry
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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.status = FOLLOWER
		rf.votedFor = -1
	}
	
	if rf.votedFor >= 0 && rf.votedFor != args.CandidateId {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	lastLogTerm := rf.logs[len(rf.logs)-1].Term
	if args.LastLogTerm > lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex + 1 >= len(rf.logs)) {
		rf.status = FOLLOWER
		rf.reschedule()
		rf.votedFor = args.CandidateId
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

type AppendEntriesReply struct {
	Term int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.status = FOLLOWER
	rf.reschedule()
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	reply.Term = rf.currentTerm
	if len(rf.logs) <= args.PrevLogIndex ||
		rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}
	i := args.PrevLogIndex + 1;
	j := 0
	for ; i < len(rf.logs); i++ {
		if rf.logs[i].Term != args.Entries[j].Term {
			break
		}
		j++
	}
	rf.logs = append(rf.logs[:i], args.Entries[j:]...)
	if args.LeaderCommit > rf.commitIndex {
		oldCommitIndex := rf.commitIndex
		rf.commitIndex = min(args.LeaderCommit, len(rf.logs)-1)
		DPrintf("me=%v, try to apply [%v, %v]\n",
			rf.me, oldCommitIndex+1, rf.commitIndex)
		for i := oldCommitIndex + 1; i <= rf.commitIndex; i++ {
			rf.applyCh <- ApplyMsg {
				CommandValid: true,
					Command: rf.logs[i].Command,
					CommandIndex: i,
				}
		}
	}
	reply.Success = true
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	if rf.status != LEADER {
		isLeader = false
		return index, term, isLeader
	}

	rf.logs = append(rf.logs, LogEntry {
		Term: rf.currentTerm,
			Command: command,
		})
	index = len(rf.logs) - 1
	term = rf.currentTerm
	rf.reschedule()
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(i int) {
			for {
				appendEntriesReq := AppendEntriesArgs{}
				func () {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.status != LEADER {
						return
					}
					appendEntriesReq.Term = rf.currentTerm
					appendEntriesReq.LeaderId = rf.me
					appendEntriesReq.PrevLogIndex = rf.nextIndex[i] - 1
					appendEntriesReq.PrevLogTerm = rf.logs[rf.nextIndex[i]-1].Term
					appendEntriesReq.LeaderCommit = rf.commitIndex
					if len(rf.logs)-1 >= rf.nextIndex[i] {
						appendEntriesReq.Entries = rf.logs[rf.nextIndex[i]:]
					}
				}()
				
				appendEntriesResp := AppendEntriesReply{}
				ok := rf.sendAppendEntries(i, &appendEntriesReq, &appendEntriesResp)
				DPrintf("me=%v, appendEntriesReq=%v, appendEntriesResp=%v\n",
					rf.me, appendEntriesReq, appendEntriesResp)
				if !ok {
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()
				if appendEntriesResp.Term > rf.currentTerm {
					rf.currentTerm = appendEntriesResp.Term
					rf.status = FOLLOWER
					rf.votedFor = -1
					rf.reschedule()
					return
				}
				if appendEntriesResp.Success {
					rf.nextIndex[i] += len(appendEntriesReq.Entries)
					rf.matchIndex[i] = rf.nextIndex[i] - 1
					// Sort matchIndex[] from big to small.
					sortedMatchIndex := make([]int, len(rf.matchIndex))
					copy(sortedMatchIndex, rf.matchIndex)
					sort.Slice(sortedMatchIndex, func(i, j int) bool {
						return sortedMatchIndex[i] > sortedMatchIndex[j]
					})
					majorityMaxIndex := sortedMatchIndex[len(rf.peers)/2]
					DPrintf("me=%v, majorityMaxIndex=%v\n", rf.me, majorityMaxIndex)
					if majorityMaxIndex > rf.commitIndex &&
						rf.logs[majorityMaxIndex].Term == rf.currentTerm {
						oldCommitIndex := rf.commitIndex
						rf.commitIndex = majorityMaxIndex
						DPrintf("me=%v, try to apply [%v, %v]\n",
							rf.me, oldCommitIndex+1, rf.commitIndex)
						for i := oldCommitIndex + 1; i <= rf.commitIndex; i++ {
							rf.applyCh <- ApplyMsg {
								CommandValid: true,
									Command: rf.logs[i].Command,
									CommandIndex: i,
								}
						}
					}
					return
				}
				rf.nextIndex[i]--
			}
		} (i)
	}
	

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
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.status = FOLLOWER
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.logs)
	}
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}
	rf.applyCh = applyCh
	rf.reschedule()
	
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.tick()
	
	return rf
}

func (rf *Raft) tick() {
	for !rf.killed() {
		rf.mu.Lock()
		now := time.Now()
		if rf.electionExpireTime.After(now) {
			rf.mu.Unlock()
			time.Sleep(10*time.Millisecond)
			continue
		}
		
		DPrintf("me=%v, status=%v\n", rf.me, rf.status)
		switch rf.status {
		case FOLLOWER:
			rf.status = CANDIDATE
			fallthrough
		case CANDIDATE:
			rf.startVotes()
		case LEADER:
			rf.heartbeat()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) reschedule() {
	const kElectionTimeoutMillis int = 100
	switch rf.status {
	case FOLLOWER, CANDIDATE:
		rf.electionExpireTime = time.Now().Add(
			time.Duration(kElectionTimeoutMillis + 100 + rand.Intn(100))*time.Millisecond)
	case LEADER:
		rf.electionExpireTime = time.Now().Add(
			time.Duration(kElectionTimeoutMillis) * time.Millisecond)
	}
}

func (rf *Raft) startVotes() {
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.reschedule()

	voteReq := RequestVoteArgs{}
	voteReq.Term = rf.currentTerm
	voteReq.CandidateId = rf.me
	voteReq.LastLogIndex = len(rf.logs) - 1
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
			DPrintf("me=%v, to=%v, voteReq=%v\n", rf.me, i, voteReq)
			ok := rf.sendRequestVote(i, &voteReq, &voteResp)
			DPrintf("me=%v, to=%v, voteResp=%v, ok=%v\n", rf.me, i, voteResp, ok)
			if !ok {
				return
			}
			func () {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if voteResp.Term > rf.currentTerm {
					rf.currentTerm = voteResp.Term
					rf.votedFor = -1
					rf.status = FOLLOWER
					return
				} else if voteResp.Term == rf.currentTerm && rf.status == CANDIDATE {
					votes_mu.Lock()
					defer votes_mu.Unlock()
					votes[i] = voteResp.VoteGranted
					vote_cnt := 0
					for _, vote := range votes {
						if vote {
							vote_cnt++
						}
					}
					if vote_cnt >= (len(rf.peers)/2 + 1) {
						DPrintf("%v becomes leader\n", rf.me)
						rf.status = LEADER
						for i := range rf.nextIndex {
							rf.nextIndex[i] = len(rf.logs)
							rf.matchIndex[i] = 0
						}
						rf.heartbeat()
					}
				}
			}()
		} (i)
	}
}

func (rf *Raft) heartbeat() {
	rf.reschedule()
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}

		DPrintf("me=%v, to=%v, heartbeat\n", rf.me, i)
		go func(i int) {
			DPrintf("me=%v, to=%v, heartbeat scheduled\n", rf.me, i)
			appendEntriesReq := AppendEntriesArgs{}
			func () {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.status != LEADER {
					return
				}
				appendEntriesReq.Term = rf.currentTerm
				appendEntriesReq.LeaderId = rf.me
				appendEntriesReq.PrevLogIndex = rf.nextIndex[i] - 1
				appendEntriesReq.PrevLogTerm = rf.logs[rf.nextIndex[i]-1].Term
				appendEntriesReq.LeaderCommit = rf.commitIndex
			}()
			
			appendEntriesResp := AppendEntriesReply{}
			DPrintf("me=%v, to=%v, heartbeatReq=%v\n", rf.me, i, appendEntriesReq)
			ok := rf.sendAppendEntries(i, &appendEntriesReq, &appendEntriesResp)
			DPrintf("me=%v, to=%v, heartbeatResp=%v, ok=%v\n",
				rf.me, i, appendEntriesResp, ok)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if appendEntriesResp.Term > rf.currentTerm {
				rf.currentTerm = appendEntriesResp.Term
				rf.status = FOLLOWER
				rf.votedFor = -1
				rf.reschedule()
				return
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
