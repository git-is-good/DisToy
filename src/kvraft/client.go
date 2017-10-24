package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"

import "time"
//import "fmt"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

    id      int64
    seqno   int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.

    // So we use a big rand number to identify
    // a client. it's ad hoc, however, we can
    // use Raft to acheive a globally incrementing
    // id...
    ck.id = nrand()
    ck.seqno = 1

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
    ck.seqno += 1

    args := GetArgs {
        Key : key,

        Ckid : ck.id,
        Seqno : ck.seqno,
    }

    var reply GetReply

//    fmt.Printf("Client %v Get called: k=%v\n", ck.id, key)
    for {
        for i := 0; i < len(ck.servers); i++ {
            ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
            if !ok {
                // RPC failure
                continue
            }

            if reply.Err == OK {
//                fmt.Printf("Client %v Got: k=%v, v=%v\n", ck.id, key, reply.Value)
                return reply.Value
            }
        }
        time.Sleep(500 * time.Millisecond)
    }
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
    ck.seqno += 1

    args := PutAppendArgs {
        Key : key,
        Value : value,
        Op : op,

        Ckid : ck.id,
        Seqno : ck.seqno,
    }

    var reply PutAppendReply

    for {
        for i := 0; i < len(ck.servers); i++ {
            ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
            if !ok {
                // RPC failure
                continue
            }

            if reply.Err == OK {
                return
            }
        }
        time.Sleep(500 * time.Millisecond)
    }
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
