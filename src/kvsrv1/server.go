package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex
	// Your definitions here.
	kvMap map[string]Content
}

type Content struct {
	value   string
	version rpc.Tversion
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.kvMap = map[string]Content{}

	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	content, ok := kv.kvMap[args.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
		return
	}
	reply.Value = content.value
	reply.Version = content.version
	reply.Err = rpc.OK
	return
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	content, ok := kv.kvMap[args.Key]
	if ok {
		// key 存在
		if args.Version != content.version {
			reply.Err = rpc.ErrVersion
			return
		}
		content.version++
		content.value = args.Value
		kv.kvMap[args.Key] = content
		reply.Err = rpc.OK
		return
	} else {
		// key 不存在
		if args.Version != 0 {
			reply.Err = rpc.ErrNoKey
			return
		}
		kv.kvMap[args.Key] = Content{
			value:   args.Value,
			version: 1,
		}
		reply.Err = rpc.OK
		return
	}
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
