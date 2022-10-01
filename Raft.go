package raft

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   
	Snapshot    []byte 
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]
	currentTerm   int
	votedFor      int
	state         string
	myVotes       int
	lastHeartbeat int
	Log           []LogEntry

	//volatile state on all servers

	commitIndex     int
	lastApplied     int
	electionTimeout int //timeout in milliseconds

	applyCh        chan ApplyMsg
	signalToCommit chan bool

	nextIndex  []int
	matchIndex []int
	isLeader   bool
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	rf.mu.Lock()
	if rf.state == "Leader" {
		isleader = true
	} else {
		isleader = false
	}
	term = rf.currentTerm
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.Log)
	e.Encode(rf.state)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	rf.mu.Lock()
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.Log)
	d.Decode(&rf.state)
	rf.mu.Unlock()

}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}
type AppendEntriesReply struct {
	// Your data here.
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.

	//logic of how do you process when you recv requestVote RPC

	rf.mu.Lock()
	//fmt.Println("getting req vote", "me:", rf.me, "args", args, "currentTerm", rf.currentTerm, "log", rf.Log)
	myLastLogIndex := len(rf.Log) - 1
	myLastLogTerm := 0
	if len(rf.Log) == 0 {
		myLastLogTerm = -1
	} else {
		myLastLogTerm = rf.Log[len(rf.Log)-1].Term
	}
	reply.VoteGranted = false

	if args.Term > rf.currentTerm {
		//fmt.Println("coming here in 1st if")
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	if args.Term > rf.currentTerm && args.LastLogTerm == -1 { //for normal cases with no logs
		//fmt.Println("coming here in 2nd if")
		rf.currentTerm = args.Term
		rf.state = "Follower"
		rf.votedFor = args.CandidateID
		rf.myVotes = 0
		rf.persist()
		reply.VoteGranted = true
		rf.mu.Unlock()
		//fmt.Println("i am", rf.me, "i have voted for", args.CandidateID)
		return
	}
	//	fmt.Println("before if", "args.Term", args.Term, "args.LastLogTerm", args.LastLogTerm, "rf.votedFor", rf.votedFor, "myLastLogTerm", myLastLogTerm)
	if ((args.Term == rf.currentTerm) || (args.Term > rf.currentTerm)) && (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && (args.LastLogTerm > myLastLogTerm ||
		(args.LastLogTerm == myLastLogTerm && args.LastLogIndex >= myLastLogIndex)) {
		rf.currentTerm = args.Term
		rf.state = "Follower"
		rf.votedFor = args.CandidateID
		rf.myVotes = 0
		rf.persist()
		reply.VoteGranted = true
		rf.lastHeartbeat = int(time.Now().UnixMilli())
		//fmt.Println("i am", rf.me, "i have voted for", args.CandidateID)
	}

	reply.Term = rf.currentTerm

	rf.mu.Unlock()

}
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {

	//fmt.Println("sending request vote to", server, "me:", rf.me, args, "my log is:", rf.Log)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	rf.mu.Lock()

	if rf.state != "Candidate" {
		rf.mu.Unlock()
		return ok
		//do nothing since u r no longer a candidate
	} else {

		if ok {
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				//fmt.Println("turning to Follower")
				rf.state = "Follower"
				rf.votedFor = -1
				rf.myVotes = 0
				rf.persist()
			} else {
				if reply.VoteGranted {
					rf.myVotes += 1
					numOfServers := len(rf.peers)
					//fmt.Println("I am", rf.me, "number of votes gotten:", rf.myVotes, "voted for me", server)
					if rf.myVotes > (numOfServers / 2) { //here is the leader functionality
						//fmt.Println("becoming leader", rf.me)
						rf.leader()
					}
				}

			}
		}
	}
	rf.mu.Unlock()

	return ok
}

func (rf *Raft) leader() {

	rf.state = "Leader"
	rf.myVotes = 0
	rf.isLeader = true
	rf.persist()
	/*the leader here has first come to power so initializes all nextIndex values to the index just after the
	last one in its log*/
	//fmt.Println("log size when leader made is", len(rf.Log))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.Log)
		rf.matchIndex[i] = -1
	}
	go rf.sendHeartbeats()

}

func (rf *Raft) TakeMin(x1 int, x2 int) int {
	if x1 < x2 {
		return x1
	} else {
		return x2
	}
}
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here.
	//logic of how do you process when you recv AppendEntries RPC
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//fmt.Println("called in appendentries", "args are:", args, "i am", rf.state, rf.me, "currentTerm:", rf.currentTerm, "commitIndex", rf.commitIndex, rf.Log)
	rf.lastHeartbeat = int(time.Now().UnixMilli())
	reply.Success = false
	if args.Term > rf.currentTerm {
		//leader needs to send my most updated term so I will not give success
		rf.state = "Follower"
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
		return
	} else if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	} else if len(args.Entries) == 0 {
		//fmt.Println("i am saying TRUE BITCH (lenEntries) 0", rf.me)
		//reply.Success = true
		reply.Term = rf.currentTerm
		return

	} else {
		rf.Log = args.Entries

		//fmt.Println("i am ", rf.state, rf.me, "my log is", rf.Log)
		//fmt.Println("i am saying TRUE BITCH", rf.me)
		rf.state = "Follower"
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.votedFor = -1

		reply.Success = true
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = rf.TakeMin(args.LeaderCommit, len(rf.Log)-1)
			rf.signalToCommit <- true
			//	fmt.Println("i am ", rf.state, rf.me, "my commit index is", rf.commitIndex)

		}

		rf.persist()
	}
	// rf.heartbeat = time.Now().Nanosecond() / 1000000
	//fmt.Println("called in appendentries END", "args are:", args, "i am", rf.state, rf.me, "currentTerm:", rf.currentTerm, "commitIndex", rf.commitIndex, rf.Log, "reply", reply)

}
func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//fmt.Println("SENDING APPENDENTRIES TO", server, "args", args)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	//fmt.Println("REPLY RECEIVED FROM", server, "REPLY", reply)
	//processing reply of heartbeat sendAppendEntries RPC

	if reply.Term > rf.currentTerm {
		rf.state = "Follower"
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.myVotes = 0
		rf.persist()
		rf.mu.Unlock()
		return ok
	}
	if rf.state == "Leader" {

		if ok {
			if reply.Success {

				//fmt.Println("OLD match index of:", server, rf.matchIndex[server], "all matchindex", rf.matchIndex)
				rf.nextIndex[server] = len(rf.Log) //args.PrevLogIndex + 1 + len(args.Entries)
				rf.matchIndex[server] = len(rf.Log) - 1
				//fmt.Println("updating match index of:", server, rf.matchIndex[server], "all matchindex", rf.matchIndex)

				currentCommitIndex := rf.commitIndex
				for n := rf.commitIndex + 1; n < len(rf.Log); n++ {
					// make sure that leader only applies if its log entry is replicated in the majority
					if rf.Log[n].Term == rf.currentTerm {
						count := 0
						for j := 0; j < len(rf.peers); j++ {
							if j != rf.me {
								if rf.matchIndex[j] >= n {
									count += 1
								}
							}
						}

						if count >= (len(rf.peers))/2 {

							rf.commitIndex = n
							rf.persist()
							//	fmt.Println("i am ", rf.state, rf.me, "my commitindex is", rf.commitIndex)
						}
					}
				}
				if rf.commitIndex != currentCommitIndex {
					rf.signalToCommit <- true
				}
			} else {
				//fmt.Println("decrementing prev logindex")
				rf.nextIndex[server] -= 1
			}
		}

	}

	rf.mu.Unlock()
	return ok
}
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false
	rf.mu.Lock()

	index = len(rf.Log) + 1
	term = rf.currentTerm

	if rf.state == "Leader" && rf.isLeader {
		isLeader = true
		e := LogEntry{
			Term:    rf.currentTerm,
			Command: command,
		}
		rf.Log = append(rf.Log, e)

		//	fmt.Println("i am", rf.state, rf.me, "log is", rf.Log)
		rf.persist()

	} else {
		rf.mu.Unlock()
		return -1, -1, false
	}
	rf.mu.Unlock()
	return index, term, isLeader
}

func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) ApplyCommittedLogEntries() {
	for {
		select {
		case <-rf.signalToCommit:
			rf.mu.Lock()
			lastApplied := rf.lastApplied
			var entries []LogEntry
			if rf.commitIndex > rf.lastApplied {
				entries = rf.Log[rf.lastApplied+1 : rf.commitIndex+1]
				rf.lastApplied = rf.commitIndex
			}

			rf.mu.Unlock()
			lastApplied += 1
			for i := 0; i < len(entries); i++ {
				entry := ApplyMsg{
					Index:   lastApplied + i + 1,
					Command: entries[i].Command,
				}
				//	fmt.Println("applying to state machine", "i am", rf.state, rf.me, "index of apply:", entry.Index)
				rf.applyCh <- entry
			}

			rf.mu.Lock()
			rf.persist()
			rf.mu.Unlock()

		}
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	// initialize from state persisted before a crash

	//	fmt.Println("Wujood mai aaraha hu", me)
	rf.currentTerm = 0
	rf.myVotes = 0
	rf.state = "Follower"
	rf.votedFor = -1
	rf.electionTimeout = rand.Intn(500-300) + 300 //gives me randomized timeout
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.Log = make([]LogEntry, 0)
	rf.applyCh = applyCh
	rf.signalToCommit = make(chan bool)
	rf.isLeader = false

	rf.readPersist(persister.ReadRaftState())

	go rf.follower() //this go routine implements the follower logic.
	go rf.ApplyCommittedLogEntries()
	return rf
}

func (rf *Raft) follower() {
	rf.isLeader = false
	//The follower has to 1. keep track of timeout 2. Respond to RPCs by other servers
	for {
		time.Sleep(time.Duration(rf.electionTimeout) * time.Millisecond) //wait for election timeout
		rf.mu.Lock()
		timeout := rf.electionTimeout
		hb := rf.lastHeartbeat
		state := rf.state
		rf.mu.Unlock()
		if state == "Leader" {
			//fmt.Println("Still leader", rf.me)
		}
		if state != "Leader" && (int(time.Now().UnixMilli())-hb) > timeout {
			rf.startElection() //start election after you did not recv heartbeat in election timeout.
		}
	}

}
func (rf *Raft) sendHeartbeats() {
	for {

		rf.mu.Lock()
		if rf.state != "Leader" {
			rf.mu.Unlock()
			return
		}
		numOfServers := len(rf.peers)
		rf.mu.Unlock()
		rep := AppendEntriesReply{}
		for i := 0; i < numOfServers; i++ {
			if i == rf.me {
			} else {
				//prevLogIndex has to be inidvidually set for each follower in every iteration.
				rf.mu.Lock()
				//nextIndexFollower := rf.nextIndex[i]
				//fmt.Println("the index is", nextIndexFollower)
				prevLogIndex := -1
				prevLogTerm := -1
				// if prevLogIndex < 0 {
				// 	prevLogTerm = rf.currentTerm
				// } else {
				// 	prevLogTerm = rf.Log[prevLogIndex].Term
				// }
				//what if a follower does not have any log?
				//nextIndex would be 0 in that case and prevLogIndex would be -1. This is not accessible in rf.Log

				entries := rf.Log
				//fmt.Println("leader sending entries", entries)
				req := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderID:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      entries,
					LeaderCommit: rf.commitIndex,
				}

				rf.mu.Unlock()
				//fmt.Println("sending heartbeat to", i, req)
				go rf.sendAppendEntries(i, req, &rep)

			}
		}
		time.Sleep(100 * time.Millisecond)
		//	go rf.changeCommitIndex()

	}
}

func (rf *Raft) changeCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != "Leader" {
		return
	}
	currentCommitIndex := rf.commitIndex
	for n := rf.commitIndex + 1; n < len(rf.Log); n++ {
		// make sure that leader only applies if its log entry is replicated in the majority
		if rf.Log[n].Term == rf.currentTerm {
			count := 0
			for j := 0; j < len(rf.peers); j++ {
				if j != rf.me {
					if rf.matchIndex[j] >= n {
						count += 1
					}
				}
			}

			if count > (len(rf.peers))/2 {
				rf.commitIndex = n
				//fmt.Println("i am ", rf.state, rf.me, "commitindex is", rf.commitIndex)
				rf.persist()
			}
		}
	}
	if rf.commitIndex != currentCommitIndex {
		rf.signalToCommit <- true
	}
}

func (rf *Raft) startElection() {

	//this routine will send RequestVote RPCs.
	//going into candidate state after election timeout

	// fmt.Println(int(time.Now().UnixMilli()) - hb)
	//fmt.Println("timing out and my name is", rf.me, rf.state, "term:", rf.currentTerm)
	rf.mu.Lock()
	rf.lastHeartbeat = int(time.Now().UnixMilli())
	rf.state = "Candidate"
	rf.votedFor = rf.me
	rf.isLeader = false
	rf.myVotes = 0
	rf.currentTerm += 1
	rf.myVotes += 1 //vote for self
	lastLogTerm := 0
	if len(rf.Log) == 0 {
		lastLogTerm = -1
	} else {
		lastLogTerm = rf.Log[len(rf.Log)-1].Term
	}
	req := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: len(rf.Log) - 1,
		LastLogTerm:  lastLogTerm,
	}
	rf.electionTimeout = rand.Intn(500-300) + 300 //reset election timer
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ { //sending RequestVoteRPC to all peers
		if i != rf.me {
			rep := RequestVoteReply{}
			go rf.sendRequestVote(i, req, &rep)
		}
	}
}
