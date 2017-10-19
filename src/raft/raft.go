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

import(
    "sync"
    "labrpc"
    "time"
    "math/rand"
    "fmt"
    "reflect"
)

// import "bytes"
// import "encoding/gob"

// in Millisecond
const (
    heartbeatPeriod = 100
    electionPeriod = 800
    electionFluctuate = 900
)

type raftState int

const (
    leaderState  raftState = iota
    candidateState
    followerState
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
    Term        int
    Command     interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

    applyCh         chan ApplyMsg

    currentState    raftState

    currentTerm     int
    votedFor        int

    log             []LogEntry
    commitIndex     int
    lastApplied     int

    nextIndex       []int
    matchIndex      []int

    randGen         *rand.Rand
    appendRcvCh     chan bool
    voteGrantCh     chan bool
    electedCh       chan bool
    outdateCh       chan bool
}

func (rf *Raft) getNewElectionTimer() *time.Timer {
    howlong := time.Duration(electionPeriod + (rf.randGen.Int() % electionFluctuate)) * time.Millisecond
//    fmt.Println(howlong)
    return time.NewTimer(howlong)
}

func (rf *Raft) getNewHeartbeatTimer() *time.Timer {
    return time.NewTimer(time.Duration(heartbeatPeriod) * time.Millisecond)
}


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	return rf.currentTerm, rf.currentState == leaderState
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
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

type AppendEntriesArgs struct {
    Term            int
    LeaderId        int
    PrevLogIndex    int
    PrevLogTerm     int
    Entries         []LogEntry
    LeaderCommit    int
}

type AppendEntriesReply struct {
    Term            int
    Success         bool
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
    return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.mu.Lock()
//    fmt.Printf("%d received heartbeat from %d...\n", rf.me, args.LeaderId)
    defer rf.mu.Unlock()

    reply.Term = rf.currentTerm
    reply.Success = false

    if args.Term < rf.currentTerm {
        return
    } else if args.Term > rf.currentTerm {
        // find itself outdated ...
        rf.updateTerm(args.Term)
        reply.Term = args.Term
        rf.appendRcvCh <- true
        rf.outdateCh <- true
        return
    }

    rf.appendRcvCh <- true
    //TODO wrong index!!!
    if args.PrevLogIndex == -1 {
        reply.Success = true
        return
    }
    if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
        return
    }

    realAddStart := 0
    for _, entry := range args.Entries {
        supposedIndex := args.PrevLogIndex + 1 + realAddStart
        if supposedIndex >= len(rf.log) {
            // exceeds the rf.log length
            break
        }
        if rf.log[supposedIndex].Term != entry.Term {
            // conflicit
            rf.log = rf.log[:supposedIndex]
            break;
        }

        realAddStart += 1
    }

    if realAddStart != len(args.Entries) {
        rf.log = append(rf.log, args.Entries[realAddStart:]...)
    }
    // else { nothing to add }

    // if LeaderCommit > commitIndex, ...
    if args.LeaderCommit > rf.commitIndex {
        rf.commitIndex = args.LeaderCommit
        if args.LeaderCommit > len(rf.log) - 1 {
            rf.commitIndex = len(rf.log) - 1
        }
    }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
    Term            int
    CandidateId     int
    LastLogIndex    int
    LastLogTerm     int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
    Term            int
    VoteGranted     bool
}

func (rf *Raft) isLogUptodate(lastLogIndex int, lastLogTerm int) bool {
    length := len(rf.log)
    if length == 0 {
        return true;
    }
    if rf.log[length - 1].Term > lastLogTerm {
        return true;
    } else if rf.log[length - 1].Term < lastLogTerm {
        return false;
    } else {
        return lastLogIndex >= length;
    }
}

func (rf *Raft) updateTerm(term int) {
    rf.currentTerm = term
    rf.votedFor = -1
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
    rf.mu.Lock()
    defer rf.mu.Unlock()

    reply.Term = rf.currentTerm
    reply.VoteGranted = false

    if args.Term < rf.currentTerm {
        return
    } else if args.Term > rf.currentTerm {
        // find itself outdated
        rf.updateTerm(args.Term)
        rf.outdateCh <- true
    }

    fmt.Printf("%d received RPC from %d\n", rf.me, args.CandidateId)
    if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isLogUptodate(args.LastLogTerm, args.LastLogIndex) {
        // grant vote
        fmt.Printf("%d vote for %d\n", rf.me, args.CandidateId)
        rf.votedFor = args.CandidateId
        reply.VoteGranted = true
        rf.voteGrantCh <- true
        return
    } else {
        fmt.Printf("%d *refused* vote for %d\n", rf.me, args.CandidateId)
        return
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
// may fail or lose an election.
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
    isLeader = rf.currentState == leaderState
    if isLeader {
        term = rf.currentTerm
        index = len(rf.log)
        go rf.broadcastAppendEntries(command)
    }
    rf.mu.Unlock()

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

	// Your initialization code here (2A, 2B, 2C).

    npeers := len(peers)
    rf.applyCh = applyCh

    rf.currentState = followerState

    rf.updateTerm(0)

    rf.log = make([]LogEntry, 0)
    rf.commitIndex = -1
    rf.lastApplied = -1

    rf.nextIndex = make([]int, npeers)
    rf.matchIndex = make([]int, npeers)

    rf.randGen = rand.New(rand.NewSource(time.Now().UnixNano()))
    rf.appendRcvCh = make(chan bool)
    rf.voteGrantCh = make(chan bool)
    rf.electedCh   = make(chan bool)
    rf.outdateCh   = make(chan bool)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

    go rf.mainloop()

	return rf
}

// broadcast command as a new log entry to all followers
// command == nil for heartbeat
func (rf *Raft) broadcastAppendEntries(command interface{}) {
    rf.mu.Lock()
    npeers := len(rf.peers)
    me := rf.me
    currentTerm := rf.currentTerm
    args := AppendEntriesArgs {
        Term : rf.currentTerm,
        LeaderId : me,
        PrevLogIndex : len(rf.log) - 1,
        Entries : make([]LogEntry, 0),
        LeaderCommit : rf.commitIndex,
    }
    if len(rf.log) > 0 {
        args.PrevLogTerm = rf.log[len(rf.log) - 1].Term
    }
    if command != nil {
        newLog := LogEntry{Term : currentTerm, Command : command}
        args.Entries = append(args.Entries, newLog)
        rf.log = append(rf.log, newLog)
    }
    rf.mu.Unlock()


    var replies []AppendEntriesReply = make([]AppendEntriesReply, npeers)
    var oks []chan bool = make([]chan bool, npeers)
    for i := 0; i < npeers; i++ {
        oks[i] = make(chan bool)
    }

    for i := 0; i < npeers; i += 1 {
        if i != me {
            go func (i int) {
                oks[i] <- rf.sendAppendEntries(i, &args, &replies[i])
            } (i)
        }
    }

    cases := make([]reflect.SelectCase, npeers)
    for i, ok := range oks {
        cases[i] = reflect.SelectCase{
            Dir : reflect.SelectRecv,
            Chan : reflect.ValueOf(ok),
        }
    }

    for wk := 0; wk < npeers - 1; wk += 1 {
        i, ok, _ := reflect.Select(cases)
        if !ok.Interface().(bool) {
            continue
        }

        reply := &replies[i]

        if reply.Term > currentTerm {
            // outdated, convert to follower
            rf.mu.Lock()
            rf.updateTerm(reply.Term)
            rf.mu.Unlock()
            rf.outdateCh <- true
            return
        }

        if reply.Success {
            // update nextIndex and matchIndex


        } else {

        }
    }
}

func (rf *Raft) broadcastRequestVote() {
    rf.mu.Lock()
    fmt.Printf("%d becoming cand for term %d...\n", rf.me, rf.currentTerm)

    me := rf.me

    currentTerm := rf.currentTerm
    npeers := len(rf.peers)
    granted := 1

    args := RequestVoteArgs {
        Term : currentTerm,
        CandidateId : me,
        LastLogIndex : len(rf.log) - 1,
    }
    if len(rf.log) > 0 {
        args.LastLogTerm = rf.log[len(rf.log) - 1].Term
    }
    rf.mu.Unlock()

    var replies []RequestVoteReply = make([]RequestVoteReply, npeers)
    var oks []chan bool = make([]chan bool, npeers)
    for i := 0; i < npeers; i++ {
        oks[i] = make(chan bool)
    }

    for i := 0; i < npeers; i += 1 {
        if i != me {
            go func (i int) {
                oks[i] <- rf.sendRequestVote(i, &args, &replies[i])
            } (i)
        }
    }

    // Because Call might return after a while,
    // we can't just wait for one specific call to return,
    // instead, multiplexing all calls, when a majority returns,
    // we conclude this cand is elected to leader
    cases := make([]reflect.SelectCase, npeers)
    for i, ok := range oks {
        cases[i] = reflect.SelectCase{
            Dir : reflect.SelectRecv,
            Chan : reflect.ValueOf(ok),
        }
    }

    for i := 0; i < npeers - 1; i++ {
        i, ok, _ := reflect.Select(cases)
        if !ok.Interface().(bool) {
            continue
        }

        reply := &replies[i]
        if reply.Term > currentTerm {
            fmt.Printf("%d find itself outdated\n", me)
            rf.mu.Lock()
            rf.updateTerm(reply.Term)
            rf.mu.Unlock()
            rf.outdateCh <- true
            return
        }

        if reply.VoteGranted {
            fmt.Printf("%d got vote grant from %d\n", me, i)
            granted += 1
            if granted * 2 > npeers {
                // Become leader, stop electionTiming
                fmt.Printf("==> %d got %d among %d, becoming leader for term %d...\n", me, granted, npeers, rf.currentTerm)
                rf.electedCh <- true
                return
            }
        }
    }
}

//TODO: whether a useless running timer needs to be Stopped ?
// Any performance overhead ?
func (rf *Raft) mainloop() {
    for {
        switch rf.currentState {
        case followerState:
            electionTimer := rf.getNewElectionTimer()
            select {
            case <-electionTimer.C:
                // election timeout ==> become candidate
                rf.currentState = candidateState
            case <-rf.voteGrantCh:
                // vote granted to someone via RequestVote
                electionTimer.Stop()
            case <-rf.appendRcvCh:
                // received heartbeat via AppendEntries
                electionTimer.Stop()
            case <-rf.outdateCh:
                electionTimer.Stop()
            }
        case candidateState:
            // drain electedCh
            // select {
            // case <-rf.electedCh:
            //     fmt.Printf("%d electedCh drained\n", rf.me)
            // default:
            // }

            rf.currentTerm += 1
            rf.votedFor = rf.me
            electionTimer := rf.getNewElectionTimer()
            go rf.broadcastRequestVote()
            select {
            case <-electionTimer.C:
                fmt.Printf("%d electionTimer timeout\n", rf.me)
                // election timeout as a candidate, start new election
            case <-rf.electedCh:
                // elected chan filled by broadcastRequestVote 
                electionTimer.Stop()
                rf.mu.Lock()
                for i := 0; i < len(rf.peers); i++ {
                    rf.nextIndex[i] = len(rf.log)
                    rf.matchIndex[i] = 0
                }
                rf.mu.Unlock()
                rf.currentState = leaderState
                go rf.broadcastAppendEntries(nil)
            case <-rf.appendRcvCh:
                electionTimer.Stop()
                rf.currentState = followerState
            case <-rf.outdateCh:
                // find itself outdated by broadcastRequestVote
                electionTimer.Stop()
                rf.currentState = followerState
            }

        case leaderState:
            heartbeatTimer := rf.getNewHeartbeatTimer()
            select {
            case <-heartbeatTimer.C:
                go rf.broadcastAppendEntries(nil)
            case <-rf.outdateCh:
                // find itself outdated by broadcastAppendEntries
                heartbeatTimer.Stop()
                rf.currentState = followerState
            case <-rf.appendRcvCh:
                heartbeatTimer.Stop()
            }
        }
    }
}
