package shardmaster


import "raft"
import "labrpc"
import "sync"
import "encoding/gob"

import (
    "bytes"
    "time"
    "fmt"
)

type request [2]int64

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
    lastSeqRecords      map[int64]int64
    lastSeqCh           map[int64]chan int64

    maxraftstate    int
    persister       *raft.Persister
	configs         []Config // indexed by config num

    // private info for a specific request
    // request is indentified by (clientId, seqno)
    privInfo        map[request]interface{}
}

const (
    maxServerResponseDelay = 500
    infoChDropDelay = 80
)

type Op struct {
	// Your data here.
    Service     string

    // for Join
    Servers     map[int][]string

    // for Leave
    GIDs        []int

    // for Move
    Shard       int
    GID         int

    // for Query
    Num         int

    Ckid        int64
    Seqno       int64
}

func (sm *ShardMaster) readSnapshot(data []byte) {
    if data == nil || len(data) == 0 {
        return
    }
    r := bytes.NewBuffer(data)
    d := gob.NewDecoder(r)

    d.Decode(&sm.lastSeqRecords)
    d.Decode(&sm.configs)
}

func (sm *ShardMaster) produceSnapshot() []byte {
    buf := new(bytes.Buffer)
    e := gob.NewEncoder(buf)

    e.Encode(sm.lastSeqRecords)
    e.Encode(sm.configs)
    return buf.Bytes()
}

func getShardFromStore(me int, want int, newConfig *Config, gidToShards map[int][]int) {
    for gid, shards := range(gidToShards) {
        if want == 0 {
            break
        }
        if len(shards) == 0 {
            continue
        }
        canGetHere := len(shards)
        if want < canGetHere {
            canGetHere = want
        }

        for i, shardIndex := range(shards) {
            if i > canGetHere {
                break
            }
            newConfig.Shards[shardIndex] = me
        }
        gidToShards[gid] = gidToShards[gid][canGetHere:]
        want -= canGetHere
    }
}

func (sm *ShardMaster) handleJoin(servers map[int][]string) {
    sm.mu.Lock()
    lenConfigs := len(sm.configs)
    newConfig := sm.configs[lenConfigs - 1].deepCopy()
    sm.mu.Unlock()

    newConfig.Num = lenConfigs

    // in case some servers are not new...
    for gid, _ := range(servers) {
        if _, ok := newConfig.Groups[gid]; ok {
            delete(servers, gid)
        }
    }
    // no real added server...
    if len(servers) == 0 {
        sm.mu.Lock()
        sm.configs = append(sm.configs, *newConfig)
        sm.mu.Unlock()
        return
    }

    for gid, serverNames := range(servers) {
        newConfig.Groups[gid] = serverNames
    }

    nowTotalGs := len(newConfig.Groups)
    nowEveryoneSupposedToHave := NShards / nowTotalGs
    howmanyCanHaveMore := NShards % nowTotalGs

    gidToShards := make(map[int][]int)

    // in case one group has no shard
    for gid, _ := range(newConfig.Groups) {
        gidToShards[gid] = nil
    }
    for shardIndex, gid := range(newConfig.Shards) {
        gidToShards[gid] = append(gidToShards[gid], shardIndex)
    }


    shouldReceive := make([]int, 0)
    origSize := make(map[int]int)

    toBeDetermine := make(map[int]bool)
    for gid, shards := range(gidToShards) {
        if gid == 0 {
            continue
        }

        if len(shards) > nowEveryoneSupposedToHave {
            // need to contribute, need not receive
            if howmanyCanHaveMore > 0 {
                gidToShards[gid] = gidToShards[gid][nowEveryoneSupposedToHave + 1:]
                howmanyCanHaveMore -= 1
            } else {
                gidToShards[gid] = gidToShards[gid][nowEveryoneSupposedToHave:]
            }
        } else {
            // need not contribute 
            gidToShards[gid] = nil
            if len(shards) < nowEveryoneSupposedToHave {
                shouldReceive = append(shouldReceive, gid)
                origSize[gid] = len(shards)
            } else {
                //don't know whether need to receive
                toBeDetermine[gid] = true
            }
        }
    }

    // now those in gidToShards will be dispatched to newcommer
    for newcommer, _ := range(servers) {
        want := nowEveryoneSupposedToHave
        if howmanyCanHaveMore > 0 {
            want += 1
            howmanyCanHaveMore -= 1
        }
        getShardFromStore(newcommer, want, newConfig, gidToShards)
    }

    for _, receiver := range(shouldReceive) {
        want := nowEveryoneSupposedToHave - origSize[receiver]
        if howmanyCanHaveMore > 0 {
            want += 1
            howmanyCanHaveMore -= 1
        }
        getShardFromStore(receiver, want, newConfig, gidToShards)
    }

    // if now stil howmanyCanHaveMore > 0, then those in toBeDetermine
    // should receive
    for maybeReceiver, _ := range(toBeDetermine) {
        if howmanyCanHaveMore == 0 {
            break
        }

        want := 1
        getShardFromStore(maybeReceiver, want, newConfig, gidToShards)
    }

    if howmanyCanHaveMore > 0 {
        panic("howmanyCanHaveMore must == 0")
    }
    for _, shards := range(gidToShards) {
        if len(shards) != 0 {
            panic("after handleJoin, gidToShards must be empty")
        }
    }

//    fmt.Printf("handleJoin fini: %v\n", newConfig)
    sm.mu.Lock()
    sm.configs = append(sm.configs, *newConfig)
    sm.mu.Unlock()
}

// GIDs should be short, so traverse array is ok
func isIn(e int, arr []int) bool {
    for _, v := range(arr) {
        if v == e {
            return true
        }
    }
    return false
}

func (sm *ShardMaster) handleLeave(GIDs []int) {
    sm.mu.Lock()
    lenConfigs := len(sm.configs)
    newConfig := sm.configs[lenConfigs - 1].deepCopy()
    sm.mu.Unlock()

    newConfig.Num = lenConfigs
    for _, gid := range(GIDs) {
        delete(newConfig.Groups, gid)
    }

    nowTotalGs := len(newConfig.Groups)
    if nowTotalGs == 0 {
        for i := 0; i < NShards; i++ {
            newConfig.Shards[i] = 0
        }
        sm.mu.Lock()
        sm.configs = append(sm.configs, *newConfig)
        sm.mu.Unlock()
    }
    nowEveryoneSupposedToHave := NShards / nowTotalGs
    howmanyCanHaveMore := NShards % nowTotalGs

    toRemoveGidToShards := make(map[int][]int)
    toRemainGidToShards := make(map[int][]int)

    // in case one group has no shard, it should be included to receive
    for gid, _ := range(newConfig.Groups) {
        toRemainGidToShards[gid] = nil
    }
    for shardIndex, gid := range(newConfig.Shards) {
        if gid > 0 {
            if isIn(gid, GIDs) {
                toRemoveGidToShards[gid] = append(toRemoveGidToShards[gid], shardIndex)
            } else {
                toRemainGidToShards[gid] = append(toRemainGidToShards[gid], shardIndex)
            }
        }
    }

    for gid, shards := range(toRemainGidToShards) {
        if len(shards) > nowEveryoneSupposedToHave {
            var toDrop []int
            if howmanyCanHaveMore > 0 {
                howmanyCanHaveMore -= 1
                toDrop = toRemainGidToShards[gid][nowEveryoneSupposedToHave + 1:]
            } else {
                toDrop = toRemainGidToShards[gid][nowEveryoneSupposedToHave:]
            }
            toRemoveGidToShards[gid] = toDrop
            delete(toRemainGidToShards, gid)
        }
        // if len(shards) <= nowEveryoneSupposedToHave: must not drop
    }

    for gid, shards := range(toRemainGidToShards) {
        want := nowEveryoneSupposedToHave - len(shards)
        if howmanyCanHaveMore > 0 {
            want += 1
            howmanyCanHaveMore -= 1
        }
        getShardFromStore(gid, want, newConfig, toRemoveGidToShards)
    }

    if howmanyCanHaveMore > 0 {
        panic("howmanyCanHaveMore must == 0")
    }
    for _, shards := range(toRemoveGidToShards) {
        if len(shards) != 0 {
            fmt.Println(toRemoveGidToShards)
            fmt.Println(newConfig)
            panic("after handleLeave, toRemoveGidToShards must be empty")
        }
    }

//    fmt.Printf("handleLeave fini: %v\n", newConfig)
    sm.mu.Lock()
    sm.configs = append(sm.configs, *newConfig)
    sm.mu.Unlock()
}

func (sm *ShardMaster) handleMove(shard int, GID int) {
    sm.mu.Lock()
    lenConfigs := len(sm.configs)
    newConfig := sm.configs[lenConfigs - 1].deepCopy()
    newConfig.Num = lenConfigs
    newConfig.Shards[shard] = GID
//    fmt.Printf("handleMove fini: %v\n", newConfig)
    sm.configs = append(sm.configs, *newConfig)
    sm.mu.Unlock()
}

func (sm *ShardMaster) chanConsumerLoop() {
    for {
        applyMsg := <-sm.applyCh
        if applyMsg.UseSnapshot {
            sm.mu.Lock()
            sm.readSnapshot(applyMsg.Snapshot)
            sm.mu.Unlock()
            continue
        }
        op := applyMsg.Command.(Op)
        clientId := op.Ckid
        seqno := op.Seqno

        sm.mu.Lock()
        lastSeq := sm.lastSeqRecords[clientId]
        infoCh := sm.lastSeqCh[clientId]
        sm.mu.Unlock()

        if seqno == lastSeq {
            continue
        } else if seqno < lastSeq {
            panic("seqno should never < lastSeq")
        }

        // a real new command
        switch op.Service {
        case "Join":
//            fmt.Printf("Join got Ckid:%v, seq:%v, newservers:%v\n", op.Ckid, op.Seqno, op.Servers)
            sm.handleJoin(op.Servers)
        case "Leave":
//            fmt.Printf("Leave got, GIDs:%v\n", op.GIDs)
            sm.handleLeave(op.GIDs)
        case "Move":
//            fmt.Printf("Move got, Shard:%v, GID:%v\n", op.Shard, op.GID)
            sm.handleMove(op.Shard, op.GID)
        case "Query":
//            fmt.Printf("Query got, Num:%v\n", op.Num)
            if op.Num == -1 {
                // query the latest config
                thisReq := request{op.Ckid, op.Seqno}
                sm.mu.Lock()
                sm.privInfo[thisReq] = len(sm.configs) - 1
                sm.mu.Unlock()
            }
        default:
            panic("Unknown service" + op.Service)
        }

        sm.mu.Lock()
        sm.lastSeqRecords[clientId] = seqno
        if sm.maxraftstate != -1 && sm.persister.RaftStateSize() > sm.maxraftstate {
            sm.rf.StartSnapshot(sm.produceSnapshot(), applyMsg.Index - 1)
        }
        sm.mu.Unlock()

        go func() {
            select {
            case infoCh <- seqno:
            case <- time.After(infoChDropDelay * time.Millisecond):
                return
            }
        } ()
    }
}

func (sm *ShardMaster) getLastSeqAndInfoCh(clientId int64) (int64, <-chan int64) {
    if sm.lastSeqCh[clientId] == nil {
        sm.lastSeqCh[clientId] = make(chan int64)
    }
    return sm.lastSeqRecords[clientId], sm.lastSeqCh[clientId]
}

// timeout in Millisecond
func waitInfoChArriveUntil(infoCh <-chan int64, seqno int64, timeout int) bool {
    timer := time.NewTimer(time.Duration(timeout) * time.Millisecond)
    for {
        select {
        case <- timer.C:
            return false
        case maybeSeq := <-infoCh:
            if maybeSeq == seqno {
                return true
            }
        }
    }
}

// reture whether success
func (sm *ShardMaster) execGeneralCommand(clientId int64, seqno int64, op *Op) bool {
    sm.mu.Lock()
    lastSeqno, infoCh := sm.getLastSeqAndInfoCh(clientId)
    sm.mu.Unlock()

    if seqno <= lastSeqno {
        return false
    }

    op.Ckid = clientId
    op.Seqno = seqno

    _, _, isLeader := sm.rf.Start(*op)

    return isLeader && waitInfoChArriveUntil(infoCh, seqno, maxServerResponseDelay)
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
    op := Op {
        Service : "Join",
        Servers : args.Servers,
    }
    reply.WrongLeader = !sm.execGeneralCommand(args.Ckid, args.Seqno, &op)
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
    op := Op {
        Service : "Leave",
        GIDs : args.GIDs,
    }
    reply.WrongLeader = !sm.execGeneralCommand(args.Ckid, args.Seqno, &op)
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
    op := Op {
        Service : "Move",
        Shard : args.Shard,
        GID : args.GID,
    }
    reply.WrongLeader = !sm.execGeneralCommand(args.Ckid, args.Seqno, &op)

}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
    op := Op {
        Service : "Query",
        Num : args.Num,
    }
    success := sm.execGeneralCommand(args.Ckid, args.Seqno, &op)

    if success {
        reply.WrongLeader = false
        configIndex := args.Num
//        fmt.Printf("Query RPC success: %v\n", args.Num)
        sm.mu.Lock()
        if args.Num == -1 {
            thisReq := request{args.Ckid, args.Seqno}
            configIndex = sm.privInfo[thisReq].(int)
        }
        reply.Config = sm.configs[configIndex]
        sm.mu.Unlock()
    } else {
        reply.WrongLeader = true
    }
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

	gob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
    sm.lastSeqRecords = make(map[int64]int64)
    sm.lastSeqCh = make(map[int64]chan int64)
    sm.privInfo = make(map[request]interface{})
    sm.persister = persister
    sm.maxraftstate = -1

    sm.readSnapshot(persister.ReadSnapshot())
//    fmt.Printf("ShardMaster start mainloop...\n")
    go sm.chanConsumerLoop()

	return sm
}
