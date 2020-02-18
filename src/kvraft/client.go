package kvraft

import "../labrpc"
import "crypto/rand"
import "math/big"
import "time"
import "sync/atomic"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeader int32
	clientId int64
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
	ck.lastLeader = 0
	ck.clientId = nrand()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	req := GetArgs {
		Key: key,
		}
	for i := int(atomic.LoadInt32(&ck.lastLeader)); i < len(ck.servers); i = (i+1)%len(ck.servers) {
		resp := GetReply{}
		ok := ck.servers[i].Call("KVServer.Get", &req, &resp)
		if ok {
			switch resp.Err {
			case OK:
				atomic.StoreInt32(&ck.lastLeader, int32(i))
				return resp.Value
			case ErrNoKey:
				atomic.StoreInt32(&ck.lastLeader, int32(i))
				return ""
			case ErrWrongLeader:
				continue
			}
		}
		time.Sleep(10*time.Millisecond)
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	req := PutAppendArgs {
		Key: key,
			Value: value,
			Op: op,
			Seq: nrand(),
			ClientId: ck.clientId,
			
		}
	for i := int(atomic.LoadInt32(&ck.lastLeader)); i < len(ck.servers); i = (i+1)%len(ck.servers) {
		resp := PutAppendReply{}
		ok := ck.servers[i].Call("KVServer.PutAppend", &req, &resp)
		if ok && resp.Err == OK {
			atomic.StoreInt32(&ck.lastLeader, int32(i))
			return
		}
		time.Sleep(10*time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
