package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

const (
	Follower           = 1
	Candidate          = 2
	Leader             = 3
	HeartBeatGap       = 150
	ApplyGap           = 20
	ElectionTimeoutMin = 600
	ElectionTimeoutMax = 800
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	stat      int
	votesGot  int
	lastHeart time.Time
	// 持久化状态
	currentTerm int
	voteFor     int
	log         []LogEntry
	// 易变状态
	commitIndex int
	lastApplied int
	// leader 上的易变状态
	nextIndex  []int
	matchIndex []int
}

type LogEntry struct {
	Command interface{}
	Term    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isLeader = rf.stat == Leader
	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
// 处理目标索要票权的请求
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 如果candidate的term落后
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	//  all servers 降级机制
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.stat = Follower
		rf.voteFor = -1
	}
	// 如果candidate的term与当前节点一样新或者更新
	// 每个节点只能投票一次(如果voteFor是该candidate 需要保持RPC重发的幂等性)
	if rf.voteFor != -1 && rf.voteFor != args.CandidateId {
		// 说明投给了其他节点
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	// 如果没投过票 或者在之前的丢失RPC中投给了该candidate
	// 投票条件是 candidate的log至少与节点的一样新或者更新 5.4
	logTerm := rf.log[len(rf.log)-1].Term
	logIndex := len(rf.log) - 1
	if logTerm > args.LastLogTerm {
		// 如果candidate log term落后 拒绝投票
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	} else if logTerm == args.LastLogTerm {
		// 如果一样新 需要比较index
		if logIndex > args.LastLogIndex {
			// 如果candidate log index落后 拒绝投票
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
		} else {
			// 如果candidate log index至少与节点一样新或者更新 允许投票
			reply.VoteGranted = true
			rf.voteFor = args.CandidateId
			reply.Term = rf.currentTerm
			rf.lastHeart = time.Now()
		}
	} else {
		// 如果candidate log term更新 允许投票
		reply.VoteGranted = true
		rf.voteFor = args.CandidateId
		reply.Term = rf.currentTerm
		rf.lastHeart = time.Now()
	}
}

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
// 当前节点若要成为leader 通过该方法发起投票 请求server(int)的票权
// 原理是让server用rpc远程调用 RequestVote 来处理该请求
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 如果leader的term落后 返回false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// all servers 降级机制
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.voteFor = -1
	}
	// 处理心跳时 term相同时也要触发降级 所以stat更新拿出来写了 是args.Term >= rf.currentTerm的情况
	// 包含=的情况 是因为leader需要通知同期的candidate降级

	// 如果leader的term至少与当前节点一样新或者更新
	rf.stat = Follower

	// 刷新计时器
	rf.lastHeart = time.Now()
	// 如果日志为空 当作心跳处理
	if args.Entries == nil || len(args.Entries) == 0 {
		reply.Term = rf.currentTerm
		reply.Success = true
		// 更新commitIndex TODO AI修正方案 心跳也要更新 commitIndex
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		return
	}
	// 判断日志匹配 要求rf.log[args.PrevLogIndex].term == args.PrevLogTerm
	if len(rf.log) <= args.PrevLogIndex {
		// 节点log不够长 需要leader的PrevLogIndex前移
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// 如果节点log足够长
	if rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
		// 表示遇到了匹配的entry
		rf.log = append(rf.log[:(args.PrevLogIndex+1)], args.Entries...)
		reply.Term = rf.currentTerm
		reply.Success = true
		// 更新commitIndex
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	} else {
		// 表示该项entry不匹配 需要leader的PrevLogIndex前移来寻找匹配项
		reply.Term = rf.currentTerm
		reply.Success = false
	}
}

// 方法会保证返回ok 如果一段时间收不到reply会返回false 有超时机制
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1 // 如果命令插入成功 命令 entry 在日志中的索引
	term := -1  // 当前 term
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = rf.stat == Leader
	if !isLeader {
		return index, term, false
	}
	rf.log = append(rf.log, LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	})
	index = len(rf.log) - 1
	term = rf.currentTerm
	return index, term, isLeader
}

// 选举计时器
func (rf *Raft) ticker() {
	for {
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.stat != Leader {
			electionTimeout := rand.Intn(ElectionTimeoutMax-ElectionTimeoutMin) + ElectionTimeoutMin
			if time.Now().After(rf.lastHeart.Add(time.Duration(electionTimeout) * time.Millisecond)) {
				rf.mu.Unlock()
				go rf.launchElection()
			} else {
				rf.mu.Unlock()
			}
		} else {
			rf.mu.Unlock()
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// 发起选举
func (rf *Raft) launchElection() {
	rf.mu.Lock()
	rf.lastHeart = time.Now()
	rf.stat = Candidate
	rf.currentTerm++
	rf.votesGot = 1 // 给自己投一票
	rf.voteFor = rf.me
	rf.mu.Unlock()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.doElection(i)
	}
}

// 并发执行选举
func (rf *Raft) doElection(server int) {
	rf.mu.Lock()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	reply := RequestVoteReply{}
	rf.mu.Unlock()
	ok := rf.sendRequestVote(server, &args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	if rf.stat != Candidate {
		rf.mu.Unlock()
		return
	}
	if reply.VoteGranted {
		// 如果获得票选
		rf.votesGot++
		// 如果获得了过半的票
		// 加CAS判断
		if rf.votesGot > len(rf.peers)/2 && rf.stat == Candidate {
			rf.stat = Leader
			// 更新leader的一些易变字段
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = len(rf.log)
				if i == rf.me {
					rf.matchIndex[i] = len(rf.log) - 1
				} else {
					rf.matchIndex[i] = 0
				}
			}
			rf.mu.Unlock()
			go rf.heartBeat()
		} else {
			rf.mu.Unlock()
		}
	} else {
		// 没有获得票选 需要检查是不是自己的term落后了
		if rf.currentTerm < reply.Term && rf.stat == Candidate {
			// candidate的term落后 降级机制
			rf.currentTerm = reply.Term
			rf.stat = Follower
			rf.voteFor = -1
		}
		rf.mu.Unlock()
	}
}

// leader的心跳机制
func (rf *Raft) heartBeat() {
	for {
		rf.mu.Lock()
		if rf.stat != Leader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go rf.doHeartBeat(i)
		}
		time.Sleep(HeartBeatGap * time.Millisecond)
	}
}

// leader心跳机制的实现
func (rf *Raft) doHeartBeat(server int) {
	rf.mu.Lock()
	var entries []LogEntry
	entries = rf.log[rf.nextIndex[server]:]
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextIndex[server] - 1,
		PrevLogTerm:  rf.log[rf.nextIndex[server]-1].Term,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	reply := AppendEntriesReply{}
	if rf.stat != Leader {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	ok := rf.sendAppendEntries(server, &args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 如果该leader的term落后
	// 加CAS
	if reply.Term > rf.currentTerm && rf.stat == Leader {
		// 降级机制
		rf.stat = Follower
		rf.currentTerm = reply.Term
		rf.voteFor = -1
		rf.lastHeart = time.Now()
		return
	}
	// 如果leader的term正常 下面判断reply
	if !reply.Success {
		// 若返回false
		rf.nextIndex[server]--
	} else if entries != nil && len(entries) != 0 {
		// 若返回true 并且不是心跳 说明日志更新成功
		rf.matchIndex[server] = args.PrevLogIndex + len(entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		// 每成功更新一个节点的日志 尝试更新commitIndex
		for n := len(rf.log) - 1; n > rf.commitIndex; n-- {
			count := 1 // 自己算一个
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				if rf.matchIndex[i] >= n {
					count++
				}
			}
			if count > len(rf.peers)/2 && rf.log[n].Term == rf.currentTerm {
				rf.commitIndex = n
				break
			}
		}
	}
}

// 应用log 实际上是放入上层服务的channel中
func (rf *Raft) applyLog(applyCh chan raftapi.ApplyMsg) {
	for {
		rf.mu.Lock()
		msgs := make([]raftapi.ApplyMsg, 0)
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			entry := rf.log[rf.lastApplied]
			msg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: rf.lastApplied,
			}
			msgs = append(msgs, msg)
		}
		rf.mu.Unlock()
		for _, msg := range msgs {
			applyCh <- msg
		}
		time.Sleep(ApplyGap * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.stat = Follower
	rf.votesGot = 0
	rf.lastHeart = time.Now()
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry{
		Command: nil,
		Term:    0,
	}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = 1
	}
	rf.matchIndex = make([]int, len(rf.peers))
	for j := 0; j < len(rf.matchIndex); j++ {
		rf.matchIndex[j] = 0
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyLog(applyCh)
	return rf
}
