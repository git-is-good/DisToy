package shardkv


import "shardmaster"
import "labrpc"
import "raft"
import "sync"
import "encoding/gob"

import (
    "time"
)

/********** Key Points *************
 * 1, Every operation in a cluster must get CONSENSUS before execution
 * 2, Raft is atmost-once semantics, no one knows whether an
 *    operation will be ACTUALLY executed when an operation
 *    is sent by Start
 */

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

    Service                 string

    // for "MaybeShardStatesUpdate"
    NewConfig               *shardmaster.Config

    // for "Migration"
    Shard       Shard
    ConfigNum   int
    ChIndex     int

}

const (
    checkConfigDelay = 80
    maxServerResponseDelay = 500
    infoChDropDelay = 80
    sendMigrationRequestDelay = 55
)

const chanPoolSize = 100

type PerShardState int

/*                           got Migration RPC, start sending YouCanDelete
             -- WorkingLocked <---------------- Entering <----\
            /                                                  \
           /                                                    \
          / got UnlockInform RPC                                 \ 
         /                                                        \ query config
        /                                                          \
       \/  query config          send Migration RPC to receiver     \
    Working ----------> Leaving ---------------------------------> NotHere
          ^           ^        \        got YouCanDelete Feedback, send UnlockInform
           \         /          \                         
          query config           \ got Stale Feedback
             \     /              \
              \   /                \
               \ /                  \
                <----- Pending <-----\

        1, Migration RPC
        2, MigrationFeedback RPC
        3, UnlockInform RPC

        *NOTE* no one can check these states in its on machine, *MUST* via Raft

        Every RPC contains (shardId, configNum) pair.
        For a receiver, when an RPC comes with stale pair, reject with Stale response.
        Higher pair is impossible for MigrationFeedback and UnlockInform;
        for Migration, the current state cannot be Working/Leaving/Pending/WorkingLocked(panic),
        wait a moment for updating my state, then accept it

        
        A phony group gid=0 must REALLY exist. An extreme case is,
        when all groups are "Leave"ed, then the semantics of a key-value
        store necessitatees the persistance of those data, so when a group
        rejoints, it can restore all previously recorded data.
        Since the only participant at this point is the shardmaster,
        the shardmaster needs to play the role of a group of gid=0.
        
        When the shardmaster starts, it's the gid=0 with states for every shard
        Working. When a group starts, its states for every shard is NotHere.
        With these starting states and a gid=0 group, the previous state machine
        is complete. There is no difference in state transition rules for gid=0.
*/

const (
    shardWorking PerShardState = iota
    shardWorkingLocked
    shardNotHere
    shardLeaving
    shardPending
    shardEntering
)

type Shard struct {
    ShardId         int
    KeyValueMap     map[string]string
}

func (sd *Shard) deepcopy() *Shard {
    res := Shard{ShardId : sd.ShardId}
    res.KeyValueMap := make(map[string]string)
    for k, v := sd.KeyValueMap {
        res[k] = v
    }
    return &res
}

type chanPoolEntry {
    chIndex     int
    ch          chan interface{}
}

func (cpe *chanPoolEntry) restore() {
    cpe.ch = make(chan interface{})
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
    shards          [shardmaster.NShards]Shard
    shardStates     [shardmaster.NShards]PerShardState

    // for Migration as a receiver
    shardConfigs    [shardmaster.NShards]int

    migSeqno        int64

    // for checking config
    masterClerk     *shardmaster.Clerk
    curConfig       *shardmaster.Config

    // (gid, me) -> lastSeq
    gidMeToSeqno    map[[2]int]int64

    // a chan pool of chanPoolSize channels
    chanPool        chan *chanPoolEntry
    allChans        [chanPoolSize]*chanPoolEntry
}

// already in lock
func (kv *ShardKV) removeShard(shardId int) {


}

func (kv *ShardKV) startAndWaitAtChan(myCh *chanPoolEntry) (bool, interface{}) {
    _, _, isLeader := kv.rf.Start(op)
    if !isLeader {
        myCh.restore()
        kv.chanPool <- myCh
        return false, interface{}
    }
    select {
    case time.After(time.Duration(maxServerResponseDelay) * time.Millisecond):
        myCh.restore()
        kv.chanPool <- myCh
        return false, interface{}
    case res := <-myCh.ch:
        kv.chanPool <- myCh
        return true, res
    }
}

const (
    FeedbackYouCanDelete int = iota
    FeedbackStale
)

type MigrationFeedbackArgs struct {
    GID             int
    Seqno           int

    ShardId         int
    ConfigNum       int
    Result          int
}

type MigrationFeedbackReply struct {
    Processed       int
}

// Just send
func (kv *ShardKV) sendMigrationFeedback(machines []string, shardId int, configNum int, result int) {
    kv.mu.Lock()
    kv.migSeqno += 1
    seqno := kv.migSeqno
    args := MigrationFeedbackArgs {
        GID : kv.gid,
        Seqno : seqno,

        ShardId : shardId,
        ConfigNum : configNum,
        Result : result,
    }
    kv.mu.Unlock()

    var reply MigrationFeedbackReply
    for {
        for machine := range(machines) {
            if kv.make_end[machine].Call("ShardKV.MigrationFeedback", &args, &reply) && reply.Processed {
                return
            }
        }
        time.Sleep(time.Duration(sendMigrationRequestDelay) * time.Millisecond)
    }
}

type MigrationArgs {
    GID         int
    Machines    []string
    Me          int
    Seqno       int64

    Shard       Shard
    ConfigNum   int
}

type MigrationReply {
    Processed   bool
}

func (kv *ShardKV) Migration(args *MigrationArgs, reply *MigrationReply) {
    kv.mu.Lock()
    lastSeqno := kv.gidMeToSeqno[[2]int{args.GID, args.Me}]
    kv.mu.Unlock()

    if args.Seqno <= lastSeqno {
        reply.Processed = true
        return
    }

    myCh <- kv.chanPool
    op := Op {
        Service : "Migration",
        Shard : args.Shard,
        ConfigNum : args.ConfigNum,
        ChIndex : myCh.chIndex,
    }

    done, res := kv.startAndWaitAtChan(&myCh)
    reply.Processed = done
    go sendMigrationFeedback(args.Machines, args.ShardId, args.ConfigNum, res.(int))
}

func (kv *ShardKV) sendMigration(machines []string, shardId int, configNum int) {
    kv.mu.Lock()
    kv.migSeqno += 1
    seqno := kv.migSeqno
    args := MigrationArgs {
        GID : kv.gid,
        Seqno : seqno,
        Shard : *kv.shards[shardId].deepcopy(),
        ConfigNum : configNum,
    }
    kv.mu.Unlock()

    var reply MigrationReply

    for {
        for machine := range(machines) {
            if kv.make_end[machine].Call("ShardKV.Migration", &args, &reply) && reply.Processed {
                return
            }
        }
        time.Sleep(time.Duration(sendMigrationRequestDelay) * time.Millisecond)
    }
}

func (kv *ShardMaster) chanConsumerLoop() {
    for {
        applyMsg := <-kv.applyCh
        if applyMsg.UseSnapshot {
            kv.mu.Lock()
            kv.readSnapshot(applyMsg.Snapshot)
            kv.mu.Unlock()
            continue
        }
        op := applyMsg.Command.(Op)
        switch op.Service {
        case "MaybeShardStatesUpdate":
            newConfig := op.NewConfig
            kv.mu.Lock()
            for i := 0; i < shardmaster.NShards; i++ {
                if kv.shardStates[i] == shardLeaving {
                    continue
                }
                if kv.shardStates[i] == shardWorking && newConfig.Shards[i] != kv.gid {
                    // a shard is leaving
                    kv.shardStates[i] = shardLeaving

                } else if kv.shardStates[i] == shardEntering && newConfig.Shards[i] != kv.gid {
                    // a shard leaves before actually installed
                    kv.shardStates[i] = shardNotHere

                } else if kv.shardStates[i] == shardNotHere && newConfig.Shards[i] == kv.gid {
                    // a shard is entering
                    kv.shardStates[i] = shardEntering

                }
            }
            kv.mu.Unlock()
        case "Migration":

        }
    }
}


func (kv *ShardKV) selfCheckConfigLoop() {
    for {
        <-time.After(time.Duration(checkConfigDelay) * Millisecond)
        newConfig := kv.masterClerk.Query(-1)
        kv.mu.Lock()
        needToUpdate := false
        for i := 0; i < shardmaster.NShards; i++ {
            if kv.shardStates[i] == shardLeaving {
                continue
            }

            if (kv.shardStates[i] == shardWorking || kv.shardStates[i] == shardEntering)
                && newConfig.Shards[i] != kv.gid {
                needToUpdate = true
                break
            } else if kv.shardStates[i] == shardNotHere && newConfig.Shards[i] == kv.gid {
                needToUpdate = true
                break
            }
        }
        kv.mu.Unlock()
        if needToUpdate {
            op := Op {
                Service : "MaybeShardStatesUpdate",
                NewConfig : newConfig,
            }
            kv.rf.Start(op)
        }
    }
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
    clientId := args.Ckid
    seqno := args.Seqno


}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)


	return kv
}
