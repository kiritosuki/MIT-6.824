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
	HeartGap           = 150
	electionTimeoutMin = 600
	electionTimeoutMax = 1000
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
	currentTerm int        // 当前任期
	voteFor     int        // 投票给谁 candidateId
	log         []logEntry // 日志
	commitIndex int        // committed logEntry 的最高索引
	lastApplied int        // 状态机执行过的 logEntry 的最高索引
	nextIndex   []int      // leader 字段 应该发送给服务器的下一个 logEntry 的索引
	matchIndex  []int      // leader 字段 已经复制到目标服务器的 logEntry 的最高索引
	stat        int        // 身份状态
	lastHeart   time.Time  // 上次收到心跳的时间
}

type logEntry struct {
	command interface{} // 用户发送的命令
	term    int         // leader 接受该 entry 时的 term
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.stat == Leader
	return term, isleader
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
	Term         int // candidate 的 term
	CandidateId  int // candidate 的 id
	LastLogIndex int // candidate 最新 log 的索引
	LastLogTerm  int // candidate 最新 log 的 term
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // 用于 candidate 及时发现自己可能不是最新的
	VoteGranted bool // 是否赢得选票
}

// example RequestVote RPC handler.
// 处理目标索要票权的请求
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		// 自己的 term 过期了 立刻降级并更新状态
		rf.stat = Follower
		rf.currentTerm = args.Term
		rf.lastHeart = time.Now()
		rf.voteFor = args.CandidateId
		// 返回 RPC
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		return
	} else if args.Term == rf.currentTerm {
		// candidate 的 term 和自己相同
		if rf.voteFor == -1 {
			// 如果自己还没投票 就投给他
			rf.voteFor = args.CandidateId
			rf.lastHeart = time.Now()
			// 返回 RPC
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			return
		} else {
			// 如果自己已经投过票了 简单更新选举计时器
			rf.lastHeart = time.Now()
			// 返回 RPC
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			return
		}
	} else {
		// 自己的 term 更新 对方 candidate 的 term 过期了
		// 直接返回 RPC 提醒对方过期
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
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
	Term         int        // leader 的 term
	LeaderId     int        // 便于 client 把客户端的请求重定向到 leader
	PrevLogIndex int        // leader 要发送的 entry 的前一条 entry 的索引 prevLogIndex = nextIndex[i] - 1
	PrevLogTerm  int        // prevLogIndex entry 的 term
	Entries      []logEntry // 要发送的日志
	LeaderCommit int        // leader 的 commitIndex
}

type AppendEntriesReply struct {
	Term    int  // 用于 leader 发现自己可能不是最新的
	Success bool // 判断 follower 的 entry 是否匹配 prevLogIndex 和 prevLogTerm
}

// 处理发送来的日志/心跳
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// 目前只写了心跳处理逻辑 TODO
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term >= rf.currentTerm {
		// 如果对方的 term 更大或者相等 承认它的 leader 身份
		// 更新自身状态
		rf.currentTerm = args.Term
		rf.stat = Follower
		rf.lastHeart = time.Now()
		// 返回 RPC
		reply.Term = rf.currentTerm
		return
	} else {
		// 如果 leader 的 term 过期了
		// 直接返回 RPC 及时提醒他
		reply.Term = rf.currentTerm
		return
	}
}

// 发送日志/心跳
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
	if rf.stat != Leader {
		isLeader = false
		rf.mu.Unlock()
		return index, term, isLeader
	}

	return index, term, isLeader
}

type heartResult struct {
	ok           bool
	replyTerm    int
	replySuccess bool
}

// leader 发送心跳
func (rf *Raft) heartBeat() {
	for {
		rf.mu.Lock()
		if rf.stat != Leader {
			rf.mu.Unlock()
			return
		}
		leaderTerm := rf.currentTerm
		// leader 每次也要更新一下自己的选举计时器 防止一旦 leader 降级 选举计时器直接过期 发起选举
		rf.lastHeart = time.Now()
		resultChan := make(chan *heartResult, len(rf.peers)-1)
		var wg sync.WaitGroup
		rf.mu.Unlock()
		// leader 发送带有空日志的 RPC
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			wg.Add(1)
			go rf.doHeartBeat(leaderTerm, i, resultChan, &wg)
		}

		// 单独设置一个 go 程来关闭通道
		go func() {
			wg.Wait()
			close(resultChan)
		}()

		for res := range resultChan {
			rf.mu.Lock()
			if res.ok && res.replyTerm > rf.currentTerm {
				rf.currentTerm = res.replyTerm
				rf.stat = Follower
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
		}
		time.Sleep(HeartGap * time.Millisecond)
	}
}

func (rf *Raft) doHeartBeat(leaderTerm int, serverId int, resultChan chan *heartResult, wg *sync.WaitGroup) {
	defer wg.Done()
	args := AppendEntriesArgs{
		Term:     leaderTerm,
		LeaderId: rf.me,
		Entries:  nil,
	}
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(serverId, &args, &reply)
	resultChan <- &heartResult{
		ok:           ok,
		replyTerm:    reply.Term,
		replySuccess: reply.Success,
	}
}

type electionResult struct {
	ok               bool
	replyTerm        int
	replyVoteGranted bool
}

// 发起选举
func (rf *Raft) launchElection() {
	var wg sync.WaitGroup
	rf.mu.Lock()
	rf.currentTerm++
	currentTerm := rf.currentTerm
	rf.stat = Candidate
	peers := rf.peers
	votesGot := 1
	rf.voteFor = rf.me
	resultChan := make(chan *electionResult, len(rf.peers)-1)
	rf.mu.Unlock()
	for i := range peers {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go rf.doElection(currentTerm, i, resultChan, &wg)
	}

	// 单独设置一个 go 程来关闭通道
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	for res := range resultChan {
		if !res.ok {
			// RPC 丢失 不用管 对方可能下次投给别的 candidate 没影响
			continue
		}
		if res.replyVoteGranted {
			// 对方给自己投票
			votesGot++
			if votesGot > len(peers)/2 {
				// 赢得了过半选票 成为 leader
				rf.mu.Lock()
				// 不能直接在这里关闭channel 可能有其他go程还在写
				rf.stat = Leader
				rf.mu.Unlock()
				go rf.heartBeat()
				return
			}
		} else {
			// 对方不给自己投票
			// 原因：自己的 term 太旧，对方投过票了，RPC 丢失，leader 已经选出
			if res.replyTerm > currentTerm {
				// 如果自己的 term 太旧
				rf.mu.Lock()
				rf.stat = Follower
				rf.currentTerm = res.replyTerm
				rf.mu.Unlock()
				return
			}
			// 如果自己的 term 是新的 没投票是因为对方投过票或者 RPC 丢失 不需要处理
			// 如果 leader 已经选出 也不用处理 leader 会通过心跳机制来通知这个 candidate 降级
			// 而且这个 candidate 也不会再收到过半的票了
		}
	}
}

func (rf *Raft) doElection(currentTerm int, serverId int, resultChan chan *electionResult, wg *sync.WaitGroup) {
	defer wg.Done()
	args := RequestVoteArgs{
		Term:        currentTerm,
		CandidateId: rf.me,
	}
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(serverId, &args, &reply)
	resultChan <- &electionResult{
		ok:               ok,
		replyTerm:        reply.Term,
		replyVoteGranted: reply.VoteGranted,
	}
}

// 选举计时器
func (rf *Raft) ticker() {
	for {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.stat != Leader {
			// follower 和 candidate 才能触发选举计时器
			lastHeart := rf.lastHeart
			rf.mu.Unlock()
			electionTimeout := rand.Intn(electionTimeoutMax-electionTimeoutMin) + electionTimeoutMin
			if time.Now().After(lastHeart.Add(time.Duration(electionTimeout) * time.Millisecond)) {
				// 超时了 触发选举
				go rf.launchElection()
			}
		} else {
			// leader 不触发选举计时器
			rf.mu.Unlock()
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
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
	rf.currentTerm = 0
	rf.voteFor = -1 // -1 表示没给任何人投票
	rf.stat = Follower
	rf.lastHeart = time.Now()
	// 日志相关
	rf.log = make([]logEntry, 0)
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = -1
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
