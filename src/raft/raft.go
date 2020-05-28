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
	"fmt"
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
	// 1 - follower, 2 - candidate, 3 - leader NOTE this wasnt in the paper but they didn't really specify how else to indicate leadership for peers
	position    int
	currentTerm int
	votedFor    int
	log         []*LogEntry

	// volatile states
	commitIndex   int
	lastApplied   int
	electionReset time.Time

	// leader volatile states
	nextIndex  []int
	matchIndex []int

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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
// IMPL: JASON
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  *LogEntry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
// IMPL: JASON
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// LogEntry struct
// IMPL: JASON
type LogEntry struct {
	Command            interface{}
	TermLeaderReceived int
}

//
// example RequestVote RPC handler.
// IMPL: JASON
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// become follower if own term outdated
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
		fmt.Printf("\n%s %d term %d < candidate term %d", rf.getPosition(), rf.me, rf.currentTerm, args.Term)
	}

	// if terms match and has not voted/already voted for candidate, grant vote. else dont.
	if args.Term == rf.currentTerm &&
		(rf.votedFor == -1 || rf.votedFor == args.CandidateID) {
		fmt.Printf("\n%s %d voted for candidate %d", rf.getPosition(), rf.me, args.CandidateID)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.electionReset = time.Now()
	} else {
		fmt.Printf("\n%s %d denied vote to candidate %d", rf.getPosition(), rf.me, args.CandidateID)
		reply.VoteGranted = false
	}

	reply.Term = rf.currentTerm
	return
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

//
// AppendEntriesRPC Arguments structure
// Invoked by leader
// IMPL: JASON
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*LogEntry
	LeaderCommit int
}

//
// AppendEntries RPC Reply structure
// IMPL: JASON
type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// AppendEntries RPC handler.
// IMPL: JASON
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("\n%s %d received heartbeat from leader %d", rf.getPosition(), rf.me, args.LeaderID)

	// become follower if own term outdated
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
		fmt.Printf("\n%s %d term %d < node term %d", rf.getPosition(), rf.me, rf.currentTerm, args.Term)
		reply.Success = false
	}

	if args.Term == rf.currentTerm {
		// if not already a follower, become follower
		fmt.Printf("\n%s %d acknowledges authority of leader %d", rf.getPosition(), rf.me, args.LeaderID)
		if rf.position != 1 {
			rf.becomeFollower(args.Term)
		}
		rf.electionReset = time.Now()
		reply.Success = true
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

	// JASON'S CODE START

	rf.applyCh = applyCh

	// persistent states
	rf.position = 1
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []*LogEntry{}

	// volatile states
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.electionReset = time.Now()

	// leader volatile states
	rf.nextIndex = []int{}
	rf.matchIndex = []int{}

	fmt.Printf("\n\nNode %d initialized", rf.me)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.startElectionTimer()

	// JASON'S CODE END
	return rf
}

// startElectionTimer runs a timer that will signal when a follower should start an election again
func (rf *Raft) startElectionTimer() {
	timeoutDuration := rf.getDuration("election")
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	// every 10ms, run loop to check if everything is ok
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C

		rf.mu.Lock()
		// exit if leader
		if rf.position == 3 {
			fmt.Printf("\nwhile %s %d, term %d in election timer, stopping", rf.getPosition(), rf.me, currentTerm)
			rf.mu.Unlock()
			return
		}

		// exit if terms mismatch (for concurrency)
		if currentTerm != rf.currentTerm {
			fmt.Printf("\nwhile %s %d in election timer, terms mismatch %d != %d, stopping", rf.getPosition(), rf.me, currentTerm, rf.currentTerm)
			rf.mu.Unlock()
			return
		}

		// run election after time has elapsed
		if elapsed := time.Since(rf.electionReset); elapsed >= timeoutDuration {
			fmt.Printf("\nwhile %s %d, term %d in election timer, stopping", rf.getPosition(), rf.me, currentTerm)
			rf.startElection()
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}
}

// startElection lets the node become a candidate and start an election
func (rf *Raft) startElection() {
	rf.position = 2
	rf.currentTerm++
	// save current term
	currentTerm := rf.currentTerm
	rf.electionReset = time.Now()
	rf.votedFor = rf.me
	fmt.Printf("\nnode %d becomes %s in term %d, starting election", rf.me, rf.getPosition(), currentTerm)

	// vote for itself
	var votesReceived int32
	votesReceived = 1

	// send RequestVote RPCs to other nodes
	for nPeer := 0; nPeer < len(rf.peers); nPeer++ {
		// do note request vote from self
		if nPeer == rf.me {
			continue
		}

		go func(nPeer int) {

			// set args and reply
			args := RequestVoteArgs{
				Term:        currentTerm,
				CandidateID: rf.me,
			}
			var reply RequestVoteReply

			fmt.Printf("\n%s %d requesting vote from node %d", rf.getPosition(), rf.me, nPeer)
			ok := rf.sendRequestVote(nPeer, &args, &reply)
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				fmt.Printf("\nreceived RequestVoteReply %+v", reply)

				// exit if candidate changes state midway
				if rf.position != 2 {
					fmt.Printf("\nwhile %s %d waiting for vote to return, lost candidacy", rf.getPosition(), rf.me)
					return
				}

				// exit if terms mismatch (for concurrency)
				if currentTerm != rf.currentTerm {
					fmt.Printf("\nwhile %s %d requesting votes, terms mismatch %d != %d, stopping", rf.getPosition(), rf.me, currentTerm, rf.currentTerm)
					return
				}

				if reply.VoteGranted {
					votes := int(atomic.AddInt32(&votesReceived, 1))
					if votes*2 > len(rf.peers) {
						fmt.Printf("\n%s %d won election", rf.getPosition(), rf.me)
						rf.becomeLeader()
						return
					}
				}

			}
		}(nPeer)

	}

	// run election timer in case of stalemate
	go rf.startElectionTimer()
}

// sendHeartbeats sends heartbeats to all nodes
func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()
	// save current term
	currentTerm := rf.currentTerm
	rf.mu.Unlock()
	for nPeer := 0; nPeer < len(rf.peers); nPeer++ {
		// do note send heartbeat to self
		if nPeer == rf.me {
			continue
		}

		go func(nPeer int) {

			// set args and reply
			args := AppendEntriesArgs{
				Term:     currentTerm,
				LeaderID: rf.me,
			}
			var reply AppendEntriesReply

			fmt.Printf("\n%s %d sending heartbeat to node %d", rf.getPosition(), rf.me, nPeer)
			ok := rf.sendAppendEntries(nPeer, &args, &reply)
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				fmt.Printf("\nreceived AppendEntriesReply %+v", reply)

				// become follower if out of date
				if reply.Term > currentTerm {
					fmt.Printf("\nnode %d term > %s %d's current term %d", nPeer, rf.getPosition(), rf.me, currentTerm)
					rf.becomeFollower(reply.Term)
					return
				}

				// exit if terms mismatch (for concurrency)
				if currentTerm != rf.currentTerm {
					fmt.Printf("\nwhile %s %d sending heartbeat, terms mismatch %d != %d, stopping", rf.getPosition(), rf.me, currentTerm, rf.currentTerm)
					return
				}

			}
		}(nPeer)

	}
}

// BECOMES

// becomeLeader changes a node into a leader and starts a ticker to make it send out heartbeats
func (rf *Raft) becomeLeader() {
	rf.position = 3
	fmt.Printf("\n%s %d becomes leader", rf.getPosition(), rf.me)

	go func() {
		ticker := time.NewTicker(rf.getDuration("heartbeat"))
		defer ticker.Stop()

		// send heartbeats while leader
		rf.sendHeartbeats()

		<-ticker.C

		rf.mu.Lock()
		if rf.position != 3 {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}()

}

// becomeFollower changes a node into a follower, resets its vote and starts its electionTimer
func (rf *Raft) becomeFollower(term int) {
	fmt.Printf("\n%s %d becomes follower", rf.getPosition(), rf.me)
	rf.position = 1
	rf.currentTerm = term
	rf.votedFor = -1
	rf.electionReset = time.Now()

	go rf.startElectionTimer()
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
		duration = time.Duration(150) * time.Millisecond
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
