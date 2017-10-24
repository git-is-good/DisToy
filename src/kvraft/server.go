package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
    "time"
//    "fmt"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
    Service     string
    Key         string
    Value       string

    Ckid        int64
    Seqno       int64
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

    // map clientid to its last seqno
    lastSeqRecords      map[int64]int64
    lastSeqCh           map[int64]chan int64

    keyValueMap         map[string]string
}

func (kv *RaftKV) chanConsumerLoop() {
    for {
        applyMsg := <-kv.applyCh
        op := applyMsg.Command.(Op)
        clientId := op.Ckid
        seqno := op.Seqno

        var infoCh chan int64
        kv.mu.Lock()
        lastSeq := kv.lastSeqRecords[clientId]
        kv.mu.Unlock()

        if seqno == lastSeq {
            continue
        } else if seqno < lastSeq {
            panic("seqno should never < lastSeq")
        }

        // a real new command
        kv.mu.Lock()
        infoCh = kv.lastSeqCh[clientId]
        switch op.Service {
        case "Put", "Append":
            if op.Service == "Put" {
                kv.keyValueMap[op.Key] = op.Value
            } else {
                // Append
                kv.keyValueMap[op.Key] += op.Value
            }
        case "Get":
//            fmt.Printf("Server %v: Get successfully applied for client %v\n", kv.me, clientId)
        default:
            panic("Known service" + op.Service)
        }
        kv.lastSeqRecords[clientId] = seqno
        kv.mu.Unlock()
        go func() {
            select {
            case infoCh <- seqno:
            case <- time.After(10 * time.Millisecond):
                return
            }
        } ()
    }
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
    clientId := args.Ckid

    kv.mu.Lock()
    lastSeqno := kv.lastSeqRecords[clientId]
    if kv.lastSeqCh[clientId] == nil {
        kv.lastSeqCh[clientId] = make(chan int64)
    }
    infoCh := kv.lastSeqCh[clientId]
    kv.mu.Unlock()

//    fmt.Printf("Server %v received Get from %v: k=%v\n", kv.me, clientId, args.Key)

    if args.Seqno <= lastSeqno {
        kv.mu.Lock()
        reply.Value = kv.keyValueMap[args.Key]
        kv.mu.Unlock()
        reply.Err = OK
        return
    }

    op := Op {
        Service : "Get",
        Key : args.Key,
        Ckid : clientId,
        Seqno : args.Seqno,
    }

    _, _, isLeader := kv.rf.Start(op)
    if !isLeader {
        reply.Err = ErrNotLeader
        reply.WrongLeader = true
        return
    }

//    fmt.Printf("Server %v selecting...\n", kv.me)
    timer := time.NewTimer(2000 * time.Millisecond)
    for {
        select {
        case <- timer.C:
            reply.Err = ErrTimeout
            return
        case maybeSeq := <-infoCh:
            if maybeSeq == args.Seqno {
//              fmt.Printf("Server %v infoCh selected...\n", kv.me)
                reply.Err = OK
                kv.mu.Lock()
                reply.Value = kv.keyValueMap[args.Key]
                kv.mu.Unlock()
                return
            }
        }
    }
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
    clientId := args.Ckid
    var infoCh chan int64

    kv.mu.Lock()
    lastSeqno := kv.lastSeqRecords[clientId]
    if kv.lastSeqCh[clientId] == nil {
        kv.lastSeqCh[clientId] = make(chan int64)
    }
    infoCh = kv.lastSeqCh[clientId]
    kv.mu.Unlock()

    // if this operation has been committed,
    // then even follower can reply
    if args.Seqno <= lastSeqno {
        reply.Err = OK
        return
    }

    op := Op {
        Service : args.Op,
        Key : args.Key,
        Value : args.Value,
        Ckid : clientId,
        Seqno : args.Seqno,
    }

    _, _, isLeader := kv.rf.Start(op)
    if !isLeader {
        reply.WrongLeader = true
        reply.Err = ErrNotLeader
        return
    }

    timer := time.NewTimer(2000 * time.Millisecond)
    for {
        select {
        case <- timer.C:
            reply.Err = "Time out"
            return
        case maybeSeq := <-infoCh:
            if maybeSeq == args.Seqno {
                reply.Err = OK
                return
            }
        }
    }
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
    kv.lastSeqRecords = make(map[int64]int64)
    kv.lastSeqCh = make(map[int64]chan int64)
    kv.keyValueMap = make(map[string]string)
    go kv.chanConsumerLoop()

	return kv
}
