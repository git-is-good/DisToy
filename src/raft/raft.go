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
//    "fmt"
    "reflect"
)

import "bytes"
import "encoding/gob"

// in Millisecond
const (
    heartbeatPeriod = 100
    electionPeriod = 500
    electionFluctuate = 300
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
    electedCh       chan int
}

func (rf *Raft) getNewElectionTimer() *time.Timer {
    howlong := time.Duration(electionPeriod + (rf.randGen.Int() % electionFluctuate)) * time.Millisecond
    return time.NewTimer(howlong)
}

func (rf *Raft) getNewHeartbeatTimer() *time.Timer {
    return time.NewTimer(time.Duration(heartbeatPeriod) * time.Millisecond)
}


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
    rf.mu.Lock()
    defer rf.mu.Unlock()
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

    buf := new(bytes.Buffer)
    e := gob.NewEncoder(buf)
    e.Encode(rf.currentTerm)
    e.Encode(rf.votedFor)
    e.Encode(rf.log)
    data := buf.Bytes()
    rf.persister.SaveRaftState(data)
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

    r := bytes.NewBuffer(data)
    d := gob.NewDecoder(r)
    d.Decode(&rf.currentTerm)
    d.Decode(&rf.votedFor)
    d.Decode(&rf.log)
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

    // here is not "type safe", 
    // if ConflictStart == -2, it's for "lack"
    // then ConflictTerm is the follower's suggested
    // nextIndex. it's multiple use of a variable with poor name...
    ConflictTerm    int
    ConflictStart   int
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
    reply.ConflictStart = -1
    reply.ConflictTerm = -1

    if args.Term < rf.currentTerm {
        return
    }

    go func() {
        rf.appendRcvCh <- true
    } ()
    if args.Term > rf.currentTerm {
        // find itself outdated ...
        rf.updateTermAndConvert(args.Term)
    }

    if len(rf.log) <= args.PrevLogIndex {
        // lack
        reply.ConflictStart = -2
        reply.ConflictTerm = len(rf.log)
        return
    } else if (args.PrevLogIndex >= 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
        reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
        reply.ConflictStart = 0
        for i := args.PrevLogIndex - 1; i >= 0; i-- {
            if rf.log[i].Term != reply.ConflictTerm {
                reply.ConflictStart = i + 1
                break
            }
        }
        return
    }

    reply.Success = true

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
        rf.persist()
    }

    if args.LeaderCommit > rf.commitIndex {
        rf.commitIndex = args.LeaderCommit
        if args.LeaderCommit > len(rf.log) - 1 {
            rf.commitIndex = len(rf.log) - 1
        }
    }
    go rf.checkApply()
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

    if lastLogIndex == -1 {
        return false
    }

    if lastLogTerm > rf.log[length - 1].Term {
        return true;
    } else if lastLogTerm < rf.log[length - 1].Term {
        return false;
    } else {
        return lastLogIndex + 1 >= length;
    }
}

func (rf *Raft) updateTermAndConvert(term int) {
    rf.currentTerm = term
    rf.votedFor = -1
    rf.currentState = followerState
    rf.persist()
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
    }

    if args.Term > rf.currentTerm {
        // find itself outdated
        rf.updateTermAndConvert(args.Term)
    }

    //fmt.Printf("%d received RPC from %d\n", rf.me, args.CandidateId)
    if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isLogUptodate(args.LastLogIndex, args.LastLogTerm) {
        // grant vote
        // fmt.Printf("%d vote for %d\n", rf.me, args.CandidateId)
        rf.votedFor = args.CandidateId
        rf.persist()
        reply.VoteGranted = true
        go func() {
            rf.voteGrantCh <- true
        } ()
        return
    } else {
        // fmt.Printf("%d *refused* vote for %d", rf.me, args.CandidateId)
        // if ( !rf.isLogUptodate(args.LastLogIndex, args.LastLogTerm) ) {
        //     var lastTerm int
        //     if len(rf.log) == 0 {
        //         lastTerm = -1
        //     } else {
        //         lastTerm = rf.log[len(rf.log) - 1].Term
        //     }
        //     fmt.Printf(", because of not up-to-date: term %d:%d; last %d:%d\n",
        //                 lastTerm,
        //                 args.LastLogTerm,
        //                 len(rf.log) - 1,
        //                 args.LastLogIndex)
        // } else {
        //     fmt.Printf("\n")
        // }
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
	isLeader := false

	// Your code here (2B).

    rf.mu.Lock()
    isLeader = rf.currentState == leaderState
    if isLeader {
        term = rf.currentTerm
        index = len(rf.log)
        newLog := LogEntry{Term : rf.currentTerm, Command : command}
        rf.log = append(rf.log, newLog)
        rf.persist()
        go rf.broadcastAppendEntries()
    }
    rf.mu.Unlock()

    // in raft paper, index from 1
	return index + 1, term, isLeader
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

    rf.currentTerm = 0
    rf.votedFor = -1

    rf.log = make([]LogEntry, 0)
    rf.commitIndex = -1
    rf.lastApplied = -1

    rf.nextIndex = make([]int, npeers)
    rf.matchIndex = make([]int, npeers)

    rf.randGen = rand.New(rand.NewSource(time.Now().UnixNano()))
    rf.appendRcvCh = make(chan bool)
    rf.voteGrantCh = make(chan bool)
    rf.electedCh   = make(chan int)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

    go rf.mainloop()

	return rf
}


// broadcast command as a new log entry to all followers
// command == nil for heartbeat
func (rf *Raft) broadcastAppendEntries() {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    // *must* check first whether it's still a leader
    // because updateTermAndConvert() will increment
    // its term. without this check, it will broadcastAppendEntries
    // as if it was a leader of *new* term
    if rf.currentState != leaderState {
        return
    }
    npeers := len(rf.peers)
    me := rf.me
    currentTerm := rf.currentTerm
    leaderCommit := rf.commitIndex
    nlog := len(rf.log)

    for i := 0; i < npeers; i++ {
        if i == me {
            continue
        }

        nextIndexI := rf.nextIndex[i]
        args := AppendEntriesArgs {
            Term : currentTerm,
            LeaderId : me,
            PrevLogIndex : nextIndexI - 1,
            LeaderCommit : leaderCommit,
        }

        // *must* copy, otherwise rf.log might be changed 
        // when the goroutine is excecuting
        args.Entries = append([]LogEntry(nil), rf.log[nextIndexI:]...)
        if args.PrevLogIndex >= 0 {
            args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
        }

        go func(i int) {
            var reply AppendEntriesReply

            // need to wait a while
            ok := rf.sendAppendEntries(i, &args, &reply)
            if !ok {
                // RPC failed
                return
            }

            if reply.Term > currentTerm {
                // outdated, convert to follower
                rf.mu.Lock()
                rf.updateTermAndConvert(reply.Term)
                rf.mu.Unlock()
                return
            }

            if reply.Success {
                // update nextIndex and matchIndex
                rf.mu.Lock()
                rf.nextIndex[i] = nlog
                rf.matchIndex[i] = nlog - 1
                rf.mu.Unlock()
            } else {
                // caused by log inconsistency
                if nextIndexI == 0 {
                    panic("nextIndexI == 0 cannot happen here")
                }

                rf.mu.Lock()
                newPrev := -1
                if reply.ConflictStart == -2 {
                    // lack
                    newPrev = reply.ConflictTerm - 1
                } else if reply.ConflictStart >= 0 {
                    newPrev = reply.ConflictStart - 1
                    for j := nextIndexI - 1; j >= reply.ConflictStart; j-- {
                        if j < len(rf.log) && rf.log[j].Term == reply.ConflictTerm {
                            newPrev = j
                            break
                        }
                    }
                }
                rf.nextIndex[i] = newPrev + 1
//                fmt.Printf("reply.ConflictStart = %d, nextIndex[%d] = %d\n", reply.ConflictStart, i, rf.nextIndex[i])
                rf.mu.Unlock()
            }
        } (i)
    }
}

func (rf *Raft) broadcastRequestVote() {
    rf.mu.Lock()
//    fmt.Printf("%d becoming cand for term %d...\n", rf.me, rf.currentTerm)

    me := rf.me
    nlog := len(rf.log)

    currentTerm := rf.currentTerm
    npeers := len(rf.peers)
    granted := 1

    args := RequestVoteArgs {
        Term : currentTerm,
        CandidateId : me,
        LastLogIndex : len(rf.log) - 1,
    }
    if nlog > 0 {
        args.LastLogTerm = rf.log[nlog - 1].Term
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
            // fmt.Printf("%d find itself outdated\n", me)
            rf.mu.Lock()
            rf.updateTermAndConvert(reply.Term)
            rf.mu.Unlock()
            return
        }

        if reply.VoteGranted {
            //fmt.Printf("%d got vote grant from %d\n", me, i)
            granted += 1
            if granted * 2 > npeers {
                // Become leader, stop electionTiming
//                fmt.Printf("==> %d got %d among %d, becoming leader for term %d...\n", me, granted, npeers, currentTerm)
                rf.electedCh <- currentTerm
                return
            }
        }
    }
}

func (rf *Raft) checkCommit() {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if rf.currentState != leaderState {
        return
    }

    nlog := len(rf.log)
    npeers := len(rf.peers)

    if nlog == 0 {
        return
    }
    for maybeCommit := nlog - 1; maybeCommit >= rf.commitIndex + 1; maybeCommit-- {
        if rf.log[maybeCommit].Term != rf.currentTerm {
            return
        }

        // check whether maybeCommit is agreed by a majority
        count := 1
        for i := 0; i < npeers; i++ {
            if i == rf.me {
                continue
            }
            if rf.matchIndex[i] >= maybeCommit {
                count += 1
            }
        }
        if count * 2 > npeers {
            rf.commitIndex = maybeCommit
            go rf.checkApply()
            return
        }
    }
}

func (rf *Raft) checkApply() {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if rf.lastApplied == rf.commitIndex {
        return
    }

    go func (from int, to int) {
        for i := from; i <= to; i++ {
            rf.mu.Lock()
            applyMsg := ApplyMsg {
                // raft paper start from 1
                Index : i + 1,
                Command : rf.log[i].Command,
            }
            rf.mu.Unlock()
            rf.applyCh <- applyMsg
        }
        //fmt.Printf("%d committed from %d to %d\n", rf.me, from, to)
    } (rf.lastApplied + 1, rf.commitIndex)

    rf.lastApplied = rf.commitIndex
}

func (rf *Raft) mainloop() {
    for {
        rf.mu.Lock()
        switch rf.currentState {
        case followerState:
            rf.mu.Unlock()
            electionTimer := rf.getNewElectionTimer()
            select {
            case <-electionTimer.C:
                // election timeout ==> become candidate
                rf.mu.Lock()
                rf.currentState = candidateState
                rf.mu.Unlock()
            case <-rf.voteGrantCh:
                // vote granted to someone via RequestVote
                electionTimer.Stop()
            case <-rf.appendRcvCh:
                // received heartbeat via AppendEntries
                electionTimer.Stop()
            }
        case candidateState:
            rf.currentTerm += 1
            rf.votedFor = rf.me
            rf.persist()
            rf.mu.Unlock()
            electionTimer := rf.getNewElectionTimer()
            go rf.broadcastRequestVote()
            select {
            case <-electionTimer.C:
//                fmt.Printf("%d electionTimer timeout\n", rf.me)
                // election timeout as a candidate, start new election
            case v := <-rf.electedCh:
                // elected chan filled by broadcastRequestVote 
                electionTimer.Stop()
                rf.mu.Lock()
                // maybe because of scheduling of goroutine,
                // this electedCh can be outdated, if it's really
                // the case, *must* ignore it, otherwise this
                // term will have multiple leaders
                if v == rf.currentTerm {
                    for i := 0; i < len(rf.peers); i++ {
                        rf.nextIndex[i] = len(rf.log)
                        rf.matchIndex[i] = -1
                    }
                    rf.currentState = leaderState
                    go rf.broadcastAppendEntries()
                }
                rf.mu.Unlock()
            case <-rf.appendRcvCh:
                electionTimer.Stop()
            }

        case leaderState:
            rf.mu.Unlock()
            heartbeatTimer := rf.getNewHeartbeatTimer()
            <-heartbeatTimer.C
            rf.checkCommit()
            go rf.broadcastAppendEntries()
        }
    }
}
