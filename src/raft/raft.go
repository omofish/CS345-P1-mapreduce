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
	//"fmt"
	"labrpc"
	"math/rand"
	"sync"
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
	commitIndex          int
	lastApplied          int
	electionTimeoutTimer *time.Timer
	heartbeatTimer       *time.Timer

	// leader volatile states
	nextIndex  []int
	matchIndex []int

	id 			string

	// JASON'S CODE END

	//chris
	isdead 		bool
	election    bool
	votes       int
	sendbeat 	bool
	killTimer	*time.Timer
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
	term = rf.currentTerm
	positioncopy := rf.position
	rf.mu.Unlock()
	if positioncopy == 3 {
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
	// fmt.Print(args.CandidateID)
	// reply with current term

	//MUtex??
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	//votedForCopy := rf.votedFor
	length := len(rf.log)-1
	rf.mu.Unlock()
	

	// decide if will grant vote
	// Reply false if term < currentTerm
	// if votedFor is null or candidateId, and candidate’s log is atleast as up-to-date as receiver’s log, grant vote
	if !rf.ownTermSmallerThan(args.Term) {
		reply.VoteGranted = false
		DPrintf("\nNode %d denied vote for candidate %d, id = %s", rf.me, args.CandidateID, rf.id)
	} else if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && (args.LastLogIndex >= length) {
		DPrintf("\nNode %d voted for candidate %d, id = %s", rf.me, args.CandidateID, rf.id)
		//rf.electionTimeoutTimer.Stop()
		//rf.electionTimeoutTimer.Reset(rf.getDuration("election"))
		
		rf.mu.Lock()
		
		rf.votedFor = args.CandidateID
		rf.mu.Unlock()
		reply.VoteGranted = true
	} else {
		DPrintf("%t", (rf.votedFor == -1))
		DPrintf("%t", (rf.votedFor == args.CandidateID))
		DPrintf("%t", (args.LastLogIndex >= len(rf.log)-1))
		DPrintf("vote not granted\n")
		reply.VoteGranted = false
	}

	

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
	var vote bool
	if ok {
		// check if reply term is greater.
		rf.ownTermSmallerThan(reply.Term)

		vote = reply.VoteGranted
	} else {
		vote = false
	}
	return vote
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
	// reply with current term
	


	// if follower/candidate, reset election timeout
	rf.mu.Lock()
	DPrintf("Node %d in AppendEntries, id = %s", rf.me, rf.id)
	reply.Term = rf.currentTerm
	if rf.position == 1 || rf.position == 2 {
		rf.electionTimeoutTimer.Stop()
		rf.electionTimeoutTimer.Reset(time.Duration((200 + rand.Intn(100))) * time.Millisecond)
		rf.heartbeatTimer.Stop()
		rf.position = 1
		rf.election = false
	}
	rf.mu.Unlock()


	if reply.Term > args.Term {
		DPrintf("\nNode %d rejects leader %d, %d > %d", rf.me, args.LeaderID, rf.currentTerm, args.Term)
		reply.Success = false
	} else {
		reply.Success = true
	}

}

//
// code to send a AppendEntries RPC to a server.
// IMPL: JASON
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.ownTermSmallerThan(reply.Term)
	}

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
	// killing this process
	
	rf.mu.Lock()
	rf.position = 4
	rf.killTimer.Reset(time.Duration(1) * time.Millisecond)
	rf.mu.Unlock()
	
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

// If other term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
func (rf *Raft) ownTermSmallerThan(other int) bool {

	//maybe concurrency issue here
	var copycurrent int
	rf.mu.Lock()
	copycurrent = rf.currentTerm
	rf.mu.Unlock()
	if other > copycurrent {
		DPrintf("\nNode %d made follower, %d > %d, id = %s", rf.me, other, rf.currentTerm, rf.id)
		if !rf.electionTimeoutTimer.Stop(){
			<-rf.electionTimeoutTimer.C
			DPrintf("Draining channel?\n")
		}
		rf.electionTimeoutTimer.Reset(time.Duration((200 + rand.Intn(100))) * time.Millisecond)
		rf.heartbeatTimer.Stop()
		rf.mu.Lock()
		rf.votedFor = -1
		rf.position = 1
		
		rf.election = false
		rf.currentTerm = other
		rf.mu.Unlock()
		return true
	}
	
	return false
}

func (rf *Raft) AllServer(){
	//If commitIndex > lastApplied: increment lastApplied, apply
	//log[lastApplied] to state machine

	//If RPC request or response contains term T > currentTerm:
	//set currentTerm = T, convert to follower
	time.Sleep(10 * time.Millisecond)
	return
}


func (rf *Raft) Follower(){
	//respond to RPCs from candidates and leaders
	//become a candidate if there was a timeout
	//timeout resets when follower votes or when it recieves a heartbeat
	rf.AllServer()
	//give time to process RPCs
	time.Sleep(10 * time.Millisecond)
	return
}

func (rf *Raft) Candidate(){
	//starts election
	//votes for self
	//sends voterequests
	//become leader if you get majority
	//start new election if timeout
	DPrintf("Node %d, Candidate Lock, id = %s", rf.me, rf.id)
	rf.mu.Lock()
	if !rf.election {
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.votes = 1
		rf.election = true
		rf.electionTimeoutTimer.Stop()
		rf.electionTimeoutTimer.Reset((time.Duration(200 + rand.Intn(100)) * time.Millisecond))
		DPrintf("\nNode %d is starting an election, id = %s", rf.me, rf.id)
	}
	// Create RequestVoteArgs
	args := RequestVoteArgs{Term: rf.currentTerm, CandidateID: rf.me}
	if len(rf.log) > 0 {
		args.LastLogIndex = len(rf.log) - 1
		args.LastLogTerm = rf.log[args.LastLogIndex]
	}
	args.Term = rf.currentTerm
	rf.mu.Unlock()

	votes := 1

	DPrintf("Node %d is sending votes to peers, id = %s", rf.me, rf.id)
	var reply RequestVoteReply
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue				
		}
								
		ok := rf.sendRequestVote(i, &args, &reply)
					
		//time.Sleep(1 * time.Millisecond)

		if ok {
			DPrintf("Node %d got the vote, id = %s", rf.me, rf.id)
			votes++
		} else {
			DPrintf("Node %d didn't get the vote, id = %s", rf.me, rf.id)
		}
	}
				
				
	// If votes received from majority of servers: become leader
	if votes > len(rf.peers)/2 {
		DPrintf("Node %d, Win Election Lock, id = %s", rf.me, rf.id)
		rf.mu.Lock()
		DPrintf("\nNode %d has won the election, id = %s", rf.me, rf.id)
		rf.position = 3
		rf.heartbeatTimer.Stop()
		rf.heartbeatTimer.Reset(100 * time.Millisecond)
		rf.electionTimeoutTimer.Stop()
		rf.election = false
		rf.sendbeat = true
		rf.mu.Unlock()
	} 
	
	



	rf.AllServer()
	return

}

func (rf *Raft) Leader(){
	//send heartbeats
	DPrintf("Node %d, Leader Lock, id = %s", rf.me, rf.id)
	rf.mu.Lock()
	if rf.sendbeat {
		rf.sendbeat = false
		rf.mu.Unlock()
		// Create AppendEntriesArgs
		args := AppendEntriesArgs{Term: rf.currentTerm, LeaderID: rf.me}

		// Send AppendEntries RPCs to all other servers
		var reply AppendEntriesReply
				
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}

			rf.sendAppendEntries(i, &args, &reply)
		}
		rf.heartbeatTimer.Stop()
		rf.heartbeatTimer.Reset(100 * time.Millisecond)
		
	} else {
		rf.mu.Unlock()
	}
	
	//handle log stuff
	rf.AllServer()
	return
}



func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	//chris
	rf.isdead = false
	rf.id = randstring(20)
	rf.election = false
	rf.sendbeat = false

	// JASON'S CODE START

	rf.applyCh = applyCh

	// init as follower
	rf.position = 1

	rf.currentTerm = 0
	// init votedFor as -1 because nil value of ints is 0
	rf.votedFor = -1
	rf.log = []*LogEntry{}

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = []int{}
	rf.matchIndex = []int{}

	DPrintf("\nNode %d initialized", rf.me)

	// start election timeout timer
	rf.electionTimeoutTimer = time.NewTimer(time.Duration((200 + rand.Intn(100))) * time.Millisecond)

	// init heartbeat timer, but dont fire
	rf.heartbeatTimer = time.NewTimer(time.Duration(100) * time.Millisecond)
	rf.heartbeatTimer.Stop()

	rf.killTimer = time.NewTimer(time.Duration(1000) * time.Millisecond)
	rf.killTimer.Stop()

	go func() {
		for {
			//DPrintf("Node %d, Clock Lock, id = %s", rf.me, rf.id)
			rf.mu.Lock()
			switch rf.position {
			case 1:
				rf.mu.Unlock()
				//follower
				rf.Follower()
			case 2:
				rf.mu.Unlock()
				//candidate
				rf.Candidate()
			case 3:
				rf.mu.Unlock()
				//leader
				rf.Leader()
			default:
				DPrintf("Node %d is killing state machine, id = %s", rf.me, rf.id)
				rf.mu.Lock()
			}
			//time.sleep(100)
		}
	}()

	go func() {
		for {
			select {
			case <-rf.electionTimeoutTimer.C:
				DPrintf("Node %d election timer timeout, id = %s", rf.me, rf.id)
				
				rf.mu.Lock()
				rf.position = 2
				rf.election = false
				rf.mu.Unlock()
				
			case <-rf.heartbeatTimer.C:
				DPrintf("Node %d heartbeat timer timeout, id = %s", rf.me, rf.id)
				//send hearbeat
				rf.mu.Lock()
				rf.sendbeat = true
				rf.mu.Unlock()
			case <-rf.killTimer.C:
				DPrintf("Node %d is killing timer gofunc, id = %s", rf.me, rf.id)
				rf.mu.Lock()
				rf.mu.Lock()
			}
		}

		/*
		for {
			if rf.isdead {
				DPrintf("killing Node %d, id = %s\n",rf.me, rf.id)
				//deadlocking to kill the node(fixes output for debugging)
				rf.mu.Lock()
				rf.mu.Lock()
			}
			select {

			
				

			// code below is run whenever election timeout elapses
			case <-rf.electionTimeoutTimer.C:
				
				
				if rf.isdead {
					//deadlocking to kill the node(fixes output for debugging)
					DPrintf("killing Node %d, id = %s\n",rf.me, rf.id)
					rf.mu.Lock()
					rf.mu.Lock()
				}
				DPrintf("\nNode %d of position %d election timeout. Starting election, id = %s\n", rf.me, rf.position, rf.id)
				// ignore election if already leader
				if rf.position == 3 {
					DPrintf("\nNode %d is already leader, id = %s\n", rf.me, rf.id)
					rf.electionTimeoutTimer.Stop()
					
					continue
				} else {
					// if follower/candidate, become candidate
					rf.position = 2
					rf.votedFor = -1
				}

				// // If vote granted to candidate, convert to follower
				// if rf.votedFor == rf.me || rf.votedFor != -1 {
				// 	rf.position = 1
				// 	continue
				// }

				// START ELECTION
				DPrintf("\nCandidate %d is starting election, id = %s", rf.me, rf.id)

				// Increment current term
				rf.currentTerm++

				// Create RequestVoteArgs
				args := RequestVoteArgs{Term: rf.currentTerm, CandidateID: rf.me}
				if len(rf.log) > 0 {
					args.LastLogIndex = len(rf.log) - 1
					args.LastLogTerm = rf.log[args.LastLogIndex]
				}

				// Vote for self
				DPrintf("\nCandidate %d votes for itself, id = %s", rf.me, rf.id)
				rf.votedFor = rf.me
				votes := 1

				// Reset election timer
				rf.electionTimeoutTimer.Stop()
				rf.electionTimeoutTimer.Reset(rf.getDuration("election"))

				DPrintf("here\n")

				// Send RequestVote RPCs to all other servers
				var reply RequestVoteReply
				DPrintf("before lock\n")
				rf.mu.Lock()
				DPrintf("after lock\n")
				for i := 0; i < len(peers); i++ {
					if i == rf.me {
						DPrintf("hello \n")
						continue
						
					}
					DPrintf("blah\n")
					
					ok := rf.sendRequestVote(i, &args, &reply)
					
					DPrintf("damn\n")

					if ok {
						votes++
					} else {
						DPrintf("Node %d did not recieve vote, id = %s\n",rf.me, rf.id)
					}
				}
				rf.mu.Unlock()
				DPrintf("sondfsndfs\n")
				// If votes received from majority of servers: become leader
				if votes > len(peers)/2 {
					DPrintf("\nNode %d elected leader, id = %s", rf.me, rf.id)
					rf.position = 3
					rf.heartbeatTimer.Reset(0)
					rf.electionTimeoutTimer.Stop()
				} else {
					DPrintf("\nNode %d failed to be elected, id = %s", rf.me, rf.id)
					rf.electionTimeoutTimer.Stop()
					rf.electionTimeoutTimer.Reset(rf.getDuration("election"))
				}
				

			// code below is only run by leader
			case <-rf.heartbeatTimer.C:
				if rf.isdead {
					//deadlocking to kill the node(fixes output for debugging)
					DPrintf("killing Node %d, id = %s\n",rf.me, rf.id)
					rf.mu.Lock()
					rf.mu.Lock()
				}
				// if not leader, continue
				if rf.position != 3 {
					continue
				}
				DPrintf("\nNode %d sends heartbeat, id = %s", rf.me, rf.id)

				// Create AppendEntriesArgs
				args := AppendEntriesArgs{Term: rf.currentTerm, LeaderID: rf.me}

				// Send AppendEntries RPCs to all other servers
				var reply AppendEntriesReply
				rf.mu.Lock()
				for i := 0; i < len(peers); i++ {
					if i == rf.me {
						continue
					}

					rf.sendAppendEntries(i, &args, &reply)
				}
				rf.mu.Unlock()
				rf.heartbeatTimer.Stop()
				rf.heartbeatTimer.Reset(rf.getDuration("heartbeat"))

			}
		}
		*/
	}()

	// JASON'S CODE END

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) getDuration(timerType string) time.Duration {
	var t, r int
	switch timerType {
	// 200ms + random(600ms)
	case "election":
		t = 500
		r = 300
	// 150ms
	case "heartbeat":
		t = 100
		r = 1
	}

	duration := time.Duration(t) * time.Millisecond
	var randomDuration time.Duration
	//if r > 0 {
		randomDuration = time.Duration(rand.Intn(r)) * time.Millisecond
	//}

	return duration + randomDuration
}

