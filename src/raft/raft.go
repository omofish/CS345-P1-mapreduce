package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// create a new Raft server.
//		rf = Make(...)
// start agreement on a new log entry
//		rf.Start(command interface{}) (index, term, isleader)
// ask a Raft for its current term, and whether it thinks it is leader
//		rf.GetState() (term, isLeader)
// each time a new entry is committed to the log, each Raft peer should send
// an ApplyMsg to the service (or tester) in the same server.
//		ApplyMsg
//

import (
	"labrpc"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	applyCh chan ApplyMsg // Channel for the commit to the state machine

	// Your data here (3, 4).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// JASON'S CODE START

	// persistent states (might need to be saved in persister instead)
	position    int        // 1 - follower, 2 - candidate, 3 - leader
	currentTerm int        // latest term server has seen (initialized to 0)
	votedFor    int        // CandidateID that received vote in current term (null if none)
	log         []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader

	// volatile states on all servers
	commitIndex      int // index of highest log entry known to be committed
	lastApplied      int // index of highest log entry applied to state machine
	lastElectionTime time.Time

	// volatile states on leaders
	// DANNY START.
	nextIndex  map[int]int // for each server, index of the next log entry to send to that server
	matchIndex map[int]int // for each server, index of highest log entry known to be replicated on server
	// DANNY END.

	// JASON'S CODE END
}

//
// return currentTerm and whether this server
// believes it is the leader.
//
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool

	// JASON'S CODE START
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.position == 3 {
		isleader = true
	} else {
		isleader = false
	}
	// JASON'S CODE END

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (4).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	// JASON: position, currentTerm, votedFor and log to be saved
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (4).
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

	// JASON: position, currentTerm, votedFor and log to be read
}

// LogEntry struct
// IMPL: JASON
type LogEntry struct {
	Command            interface{}
	TermLeaderReceived int
}

// RequestVoteArgs structure
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateID  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// RequestVoteReply structure
type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// DANNY START.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false

	if args.Term > rf.currentTerm { // if candidate's term is higher, then become a follower
		rf.becomeFollower(args.Term)
	}

	lastLogIndex, lastLogTerm := rf.lastLogIndexAndTerm()
	if (args.Term == rf.currentTerm) &&
		(rf.votedFor == -1 || rf.votedFor == args.CandidateID) &&
		(lastLogTerm < args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex)) { // if candidate log is up-to-date, grant vote
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.lastElectionTime = time.Now()
	}

	reply.Term = rf.currentTerm
	return
	// DANNY END.
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
// IMPL: JASON
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntriesArgs structure
type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderID     int        // so that the follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of PrevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat)
	LeaderCommit int        // leader's commitIndex - index of highest log entry known to be committed
}

// AppendEntriesReply structure
type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching PrevLogIndex and PrevLogTerm
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false

	if args.Term > rf.currentTerm { // if leader's term is higher, then become a follower
		rf.becomeFollower(args.Term)
	}

	if args.Term == rf.currentTerm {
		if rf.position != 1 { // if not already a follower, become follower
			rf.becomeFollower(args.Term)
		}
		rf.lastElectionTime = time.Now()

		// DANNY START.
		if args.PrevLogIndex == -1 ||
			(args.PrevLogIndex < len(rf.log) && args.PrevLogTerm == rf.log[args.PrevLogIndex].TermLeaderReceived) {
			reply.Success = true
		}
		// DANNY END.
	}

	reply.Term = rf.currentTerm
	return
}

//
// code to send a AppendEntries RPC to a server.
// IMPL: JASON
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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

	// Your code here (4).
	// DANNY START.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index = rf.commitIndex
	term = rf.currentTerm
	if rf.position == 3 {
		rf.log = append(rf.log, LogEntry{Command: command, TermLeaderReceived: term}) // if leader, directly append to log
	} else {
		isLeader = false
	}
	// DANNY END.

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.position = -1
	close(rf.applyCh)
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

	rf.applyCh = applyCh

	// persistent states
	rf.position = 1
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{}

	// volatile states
	rf.commitIndex = -1
	rf.lastApplied = -1

	// DANNY START.
	// leader volatile states
	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)
	// DANNY END.

	// fmt.Printf("\n\nNode %d initialized", rf.me)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func() {
		rf.mu.Lock()
		rf.lastElectionTime = time.Now()
		rf.mu.Unlock()
		rf.runElectionTimer()
	}()
	return rf
}

// runElectionTimer runs a timer that will signal when a follower should start an election again
func (rf *Raft) runElectionTimer() {
	timeoutDuration := rf.getDuration("election")
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	// every 10ms, run loop to check if should start an election
	electionTimeoutTicker := time.NewTicker(10 * time.Millisecond)
	defer electionTimeoutTicker.Stop()
	for {
		<-electionTimeoutTicker.C

		rf.mu.Lock()
		// exit if leader
		if rf.position == 3 {
			// fmt.Printf("\nnode %d already became leader. stopping election timer.", rf.me)
			rf.mu.Unlock()
			// break
			return
		}

		// exit if terms mismatch
		if currentTerm != rf.currentTerm {
			// fmt.Printf("\nwhile %s %d in election timer, stopping as own terms mismatch %d != %d", rf.getPosition(), rf.me, currentTerm, rf.currentTerm)
			rf.mu.Unlock()
			// break
			return
		}

		// run election after time has elapsed
		if elapsed := time.Since(rf.lastElectionTime); elapsed >= timeoutDuration {
			// fmt.Printf("\n%s %d starting election in term %d. election timout elapsed after ", rf.getPosition(), rf.me, currentTerm)
			// fmt.Print(elapsed)
			rf.startElection()
			rf.mu.Unlock()
			// break
			return
		}
		rf.mu.Unlock()
	}
}

// startElection lets the node become a candidate and start an election
func (rf *Raft) startElection() {
	rf.position = 2 // set position to candidate
	rf.currentTerm++
	currentTerm := rf.currentTerm // save current term
	rf.lastElectionTime = time.Now()
	rf.votedFor = rf.me // vote for self

	var votesReceived int32
	votesReceived = 1 // start with one vote from self

	// send concurrent RequestVote RPCs to other nodes
	for nPeer := 0; nPeer < len(rf.peers); nPeer++ {
		// do not request vote from self
		if nPeer == rf.me {
			continue
		}

		go func(nPeer int) {
			// set args and reply
			// DANNY START.
			rf.mu.Lock()
			lastLogIndex, lastLogTerm := rf.lastLogIndexAndTerm()
			rf.mu.Unlock()

			args := RequestVoteArgs{
				Term:         currentTerm,
				CandidateID:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			var reply RequestVoteReply
			// DANNY END.

			ok := rf.sendRequestVote(nPeer, &args, &reply)
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				// exit if candidate changes position midway
				if rf.position != 2 {
					// fmt.Printf("\nwhile %s %d waiting for vote to return, changed position", rf.getPosition(), rf.me)
					return
				}

				if reply.Term > currentTerm {
					rf.becomeFollower(reply.Term)
					return
				} else if reply.Term == currentTerm {
					if reply.VoteGranted {
						votes := int(atomic.AddInt32(&votesReceived, 1))
						if votes*2 > len(rf.peers) {
							rf.becomeLeader()
							return
						}
					}
				}
			}
		}(nPeer)
	}

	// run election timer in case of stalemate
	go rf.runElectionTimer()
}

// sends heartbeats to all nodes; also used for sending AppendEntries
func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()
	currentTerm := rf.currentTerm // save current term
	rf.mu.Unlock()

	for nPeer := 0; nPeer < len(rf.peers); nPeer++ {
		if nPeer == rf.me { // don't send heartbeat to self
			continue
		}

		go func(nPeer int) {
			// DANNY START.
			rf.mu.Lock()
			// set args and reply
			nextIndex := rf.nextIndex[nPeer]
			prevLogIndex := nextIndex - 1
			prevLogTerm := -1
			if prevLogIndex >= 0 {
				prevLogTerm = rf.log[prevLogIndex].TermLeaderReceived
			}
			entries := rf.log[nextIndex:]

			args := AppendEntriesArgs{
				Term:         currentTerm,
				LeaderID:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()
			var reply AppendEntriesReply
			// DANNY END.

			ok := rf.sendAppendEntries(nPeer, &args, &reply)
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > currentTerm { // become follower if out-of-date
					rf.becomeFollower(reply.Term)
					return
				}

				// DANNY START.
				// DANNY END.

				// // exit if terms mismatch (for concurrency)
				// if currentTerm != rf.currentTerm {
				// 	// fmt.Printf("\nwhile %s %d sending heartbeat, terms mismatch %d != %d, stopping", rf.getPosition(), rf.me, currentTerm, rf.currentTerm)
				// 	return
				// }
			}
		}(nPeer)
	}
}

// BECOMES

// becomeLeader changes a node into a leader and starts a ticker to make it send out heartbeats
func (rf *Raft) becomeLeader() {
	rf.position = 3
	// fmt.Printf("\nnode %d becomes leader", rf.me)

	// DANNY START.
	for nPeer := 0; nPeer < len(rf.peers); nPeer++ {
		rf.nextIndex[nPeer] = len(rf.log) // initialize all nextIndex values to the index just after the last one in its log
		rf.matchIndex[nPeer] = -1
	}
	// DANNY END.

	go func() {
		heartbeatTicker := time.NewTicker(rf.getDuration("heartbeat"))
		defer heartbeatTicker.Stop()

		for {
			// send heartbeats while leader
			rf.sendHeartbeats()

			// blocks until receiving from ticker
			<-heartbeatTicker.C

			rf.mu.Lock()
			// if no longer leader, stop function
			if rf.position != 3 {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
		}
	}()
}

// becomeFollower changes a node into a follower, updates its term, resets its vote and starts its electionTimer
func (rf *Raft) becomeFollower(term int) {
	// fmt.Printf("\n%s %d becomes follower while this long into timeout: ", rf.getPosition(), rf.me)
	rf.position = 1
	rf.currentTerm = term
	rf.votedFor = -1
	rf.lastElectionTime = time.Now()

	go rf.runElectionTimer()
}

// GETS

// getDuration returns the time of a node's ticker for election or heartbeat
func (rf *Raft) getDuration(timerType string) time.Duration {
	var duration time.Duration
	switch timerType {
	case "election":
		duration = time.Duration(200+rand.Intn(200)) * time.Millisecond

	// 150ms
	case "heartbeat":
		duration = 150 * time.Millisecond
	}
	return duration
}

// get position returns a human-readable string that represents a node's position
func (rf *Raft) getPosition() string {
	switch rf.position {
	case 1:
		return "follower"
	case 2:
		return "candidate"
	case 3:
		return "leader"
	default:
		return "invalid pos"
	}
}

// DANNY START.
func (rf *Raft) lastLogIndexAndTerm() (int, int) {
	if len(rf.log) > 0 {
		lastIndex := len(rf.log) - 1
		return lastIndex, rf.log[lastIndex].TermLeaderReceived
	}
	return -1, -1
}

// DANNY END.
