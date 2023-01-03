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

import (
	"bytes"
	"dissys/src/labrpc"
	"encoding/gob"
	"log"
	"math/rand"
	"runtime"
	"strings"
	"sync"
	"time"
)

const (
	SHOW_LOG_INFO = true

	MIN_ELECTION_TIMEOUT_MS = 300
	MAX_ELECTION_TIMEOUT_MS = 400
	HEARTBEAT_INTERVAL_MS   = 100
	RPC_RETRY_MS            = 50
)

func minInt(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func parallelRpc[TArgs any, TReply any](
	rf *Raft, rpcName string,
	getArgs func(server int) (args TArgs, giveUp bool),
	replyHandler func(server int, args TArgs, reply TReply) bool, // Return whether to retry
) {
	for server := 0; server < len(rf.peers); server += 1 {
		if server == rf.me {
			continue
		}
		go func(server int) {
			for {
				rf.mu.Lock()
				args, giveUp := getArgs(server)
				rf.mu.Unlock()
				if giveUp {
					break
				}

				var reply TReply
				rf.logInfo("RPC %s issued to server#%d, args=%v", rpcName, server, args)
				ok := rf.peers[server].Call("Raft."+rpcName, args, &reply)
				if ok {
					rf.logInfo("RPC %s successed from server#%d, args=%v, reply=%v", rpcName, server, args, reply)
				}
				if !ok {
					time.Sleep(RPC_RETRY_MS * time.Millisecond)
					continue
				}

				rf.mu.Lock()
				retry := replyHandler(server, args, reply)
				rf.mu.Unlock()

				if retry {
					time.Sleep(RPC_RETRY_MS * time.Millisecond)
					continue
				}
				break
			}
		}(server)
	}
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]
	applyCh   chan ApplyMsg

	// Persistent state on all servers
	currentTerm int
	votedFor    int // -1 for no vote granted
	log         []LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int
	role        string // follower, candidate, leader, killed

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	electionTicker  *time.Ticker
	heartbeatTicker *time.Ticker
}

func (rf *Raft) logInfo(format string, args ...interface{}) {
	if !SHOW_LOG_INFO {
		return
	}
	pc, _, _, ok := runtime.Caller(1)
	details := runtime.FuncForPC(pc)
	funcNames := strings.Split(details.Name(), ".")
	funcName := funcNames[len(funcNames)-1]
	newArgs := []interface{}{rf.me, funcName}
	newArgs = append(newArgs, args...)
	if ok && details != nil {
		log.Printf("[#%d %s] "+format, newArgs...)
	}
}

func (rf *Raft) PrintStatus() {
	log.Printf("[Status#%d] role=%s, currentTerm=%d, votedFor=%d, commitIndex=%d, lastApplied=%d", rf.me, rf.role, rf.currentTerm, rf.votedFor, rf.commitIndex, rf.lastApplied)
	log.Printf("[Status#%d]     log=%v", rf.me, rf.log)
	if rf.role == "leader" {
		log.Printf("[Status#%d]     nextIndex=%v, matchIndex=%v", rf.me, rf.nextIndex, rf.matchIndex)
	}
}

func (rf *Raft) setTerm(newTerm int) {
	if newTerm <= rf.currentTerm {
		return
	}
	rf.currentTerm = newTerm
	rf.logInfo("currentTerm set to %d", newTerm)
	rf.votedFor = -1
	rf.becomeFollower()
}

func (rf *Raft) setCommitIndex(newCommitIndex int) {
	if newCommitIndex <= rf.commitIndex {
		return
	}
	rf.commitIndex = newCommitIndex
	rf.logInfo("commitIndex set to %d", newCommitIndex)

	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied += 1
		rf.applyCh <- ApplyMsg{
			Index:   rf.lastApplied + 1,
			Command: rf.log[rf.lastApplied].Command,
		}
		rf.logInfo("log #%d committed: %v", rf.lastApplied, rf.log[rf.lastApplied].Command)
	}
}

func (rf *Raft) setMatchIndex(server int, newMatchIndex int) {
	if newMatchIndex <= rf.matchIndex[server] {
		return
	}
	rf.matchIndex[server] = newMatchIndex
	rf.logInfo("matchIndex of server#%d set to %d", server, newMatchIndex)

	count := 0
	for _, matchIndex := range rf.matchIndex {
		if matchIndex < newMatchIndex {
			continue
		}
		count += 1
	}
	if count > len(rf.peers)/2 && rf.getTermByIndex(newMatchIndex) == rf.currentTerm {
		rf.setCommitIndex(newMatchIndex)
	}
}

func (rf *Raft) getTermByIndex(logIndex int) int {
	if logIndex < 0 {
		return 0
	}
	return rf.log[logIndex].Term
}

func (rf *Raft) getLastLogIndexAndTerm() (lastLogIndex int, lastLogTerm int) {
	lastLogIndex = len(rf.log) - 1
	lastLogTerm = rf.getTermByIndex(lastLogIndex)
	return
}

func (rf *Raft) isMyLogMoreUpToDate(otherLastLogIndex int, otherLastLogTerm int) bool {
	// According to the last paragraph of $5.4.1
	myLastLogIndex, myLastLogTerm := rf.getLastLogIndexAndTerm()
	if myLastLogTerm != otherLastLogTerm {
		return myLastLogTerm > otherLastLogTerm
	}
	return myLastLogIndex > otherLastLogIndex
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	term := rf.currentTerm
	isLeader := rf.role == "leader"
	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	rf.setTerm(args.Term)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.logInfo("Vote rejected to server#%d due to outdated term, args.Term=%d, currentTerm=%d", args.CandidateId, args.Term, rf.currentTerm)
		reply.VoteGranted = false
		return
	}
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && !rf.isMyLogMoreUpToDate(args.LastLogIndex, args.LastLogTerm) {
		rf.logInfo("Vote granted to server#%d", args.CandidateId)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.resetElectionTicker()
		return
	}
}

func (rf *Raft) startElection() {
	rf.logInfo("Start election, term=%d", rf.currentTerm+1)
	rf.currentTerm += 1
	rf.votedFor = rf.me

	voteGranted := 1 // voted for self
	parallelRpc(rf, "RequestVote", func(server int) (args RequestVoteArgs, giveUp bool) {
		if rf.role != "candidate" {
			return RequestVoteArgs{}, true
		}
		lastLogIndex, lastLogTerm := rf.getLastLogIndexAndTerm()
		return RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}, false
	}, func(server int, args RequestVoteArgs, reply RequestVoteReply) bool {
		rf.setTerm(reply.Term)
		// Not voted for this term
		if rf.role != "candidate" || args.Term != rf.currentTerm {
			return false
		}
		if reply.VoteGranted {
			rf.logInfo("Vote received from server#%d, voteGranted=%d", server, voteGranted+1)
			voteGranted += 1
			if voteGranted > len(rf.peers)/2 {
				rf.becomeLeader()
				return false
			}
		} else {
			rf.logInfo("Vote rejected by server#%d", server)
		}
		return false
	})
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

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	rf.setTerm(args.Term)
	if rf.role == "candidate" {
		rf.becomeFollower()
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.logInfo("AppendEntries rejected to server#%d due to outdated term, args.Term=%d, currentTerm=%d", args.LeaderId, args.Term, rf.currentTerm)
		reply.Success = false
		return
	}
	rf.resetElectionTicker()
	if args.PrevLogIndex >= len(rf.log) || rf.getTermByIndex(args.PrevLogIndex) != args.PrevLogTerm {
		rf.logInfo("AppendEntries rejected to server#%d due to mismatched prevLogIndex, prevLogIndex=%d", args.LeaderId, args.PrevLogIndex)
		reply.Success = false
		return
	}
	reply.Success = true
	if len(args.Entries) > 0 {
		rf.log = rf.log[:args.PrevLogIndex+1]
		rf.log = append(rf.log, args.Entries...)
		rf.logInfo("AppendEntries applied, log=%v", rf.log)
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.setCommitIndex(minInt(args.LeaderCommit, args.PrevLogIndex+1))
	}
}

func (rf *Raft) sendAppendEntriesToEachServer() {
	rf.resetHeartbeatTicker()
	parallelRpc(rf, "AppendEntries", func(server int) (args AppendEntriesArgs, giveUp bool) {
		if rf.role != "leader" {
			return AppendEntriesArgs{}, true
		}
		return AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[server] - 1,
			PrevLogTerm:  rf.getTermByIndex(rf.nextIndex[server] - 1),
			Entries:      rf.log[rf.nextIndex[server]:],
			LeaderCommit: rf.commitIndex,
		}, false
	}, func(server int, args AppendEntriesArgs, reply AppendEntriesReply) bool {
		rf.setTerm(reply.Term)
		if rf.role != "leader" || args.Term != rf.currentTerm {
			return false
		}
		if !reply.Success {
			if rf.nextIndex[server] > 0 {
				rf.nextIndex[server] -= 1
			}
			rf.logInfo("AppendEntries rejected by server#%d, decrement nextIndex to %d", server, rf.nextIndex[server])
			return true
		}
		rf.logInfo("AppendEntries applied by server#%d, set nextIndex to %d", server, args.PrevLogIndex+len(args.Entries)+1)
		rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
		rf.setMatchIndex(server, rf.nextIndex[server]-1)
		return false
	})
}

func (rf *Raft) becomeFollower() {
	if rf.role == "follower" {
		return
	}
	rf.logInfo("Become follower")
	rf.role = "follower"
	rf.votedFor = -1
}

func (rf *Raft) becomeCandidate() {
	if rf.role == "candidate" {
		return
	}
	rf.logInfo("Become candidate")
	rf.role = "candidate"

	rf.startElection()
}

func (rf *Raft) becomeLeader() {
	if rf.role == "leader" {
		return
	}
	rf.logInfo("Become leader")
	rf.role = "leader"

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for server := 0; server < len(rf.peers); server += 1 {
		rf.nextIndex[server] = len(rf.log)
		rf.matchIndex[server] = -1
	}
	rf.matchIndex[rf.me] = len(rf.log) - 1

	rf.sendAppendEntriesToEachServer()
}

func (rf *Raft) resetElectionTicker() {
	timeoutMs := MIN_ELECTION_TIMEOUT_MS + rand.Int()%(MAX_ELECTION_TIMEOUT_MS-MIN_ELECTION_TIMEOUT_MS)
	timeout := time.Duration(timeoutMs) * time.Millisecond
	if rf.electionTicker == nil {
		rf.electionTicker = time.NewTicker(timeout)
	} else {
		rf.electionTicker.Reset(timeout)
	}
}

func (rf *Raft) resetHeartbeatTicker() {
	timeout := HEARTBEAT_INTERVAL_MS * time.Millisecond
	if rf.heartbeatTicker == nil {
		rf.heartbeatTicker = time.NewTicker(timeout)
	} else {
		rf.heartbeatTicker.Reset(timeout)
	}
}

func (rf *Raft) electionTickerLoop() {
	for range rf.electionTicker.C {
		rf.mu.Lock()
		if rf.role == "killed" {
			rf.mu.Unlock()
			break
		}
		if rf.role != "follower" && rf.role != "candidate" {
			rf.mu.Unlock()
			continue
		}
		rf.logInfo("Election timeout")

		rf.votedFor = -1
		if rf.role == "follower" {
			rf.becomeCandidate()
		} else if rf.role == "candidate" {
			rf.startElection()
		}

		rf.persist()
		rf.mu.Unlock()
	}
}

func (rf *Raft) heartbeatTickerLoop() {
	for range rf.heartbeatTicker.C {
		rf.mu.Lock()
		if rf.role == "killed" {
			rf.mu.Unlock()
			break
		}
		if rf.role != "leader" {
			rf.mu.Unlock()
			continue
		}
		rf.logInfo("Heartbeat timeout")

		rf.sendAppendEntriesToEachServer()

		rf.persist()
		rf.mu.Unlock()
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.role != "leader" {
		return -1, -1, false
	}
	rf.logInfo("Received command: %+v", command)

	rf.log = append(rf.log, LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	})
	rf.nextIndex[rf.me] += 1
	rf.matchIndex[rf.me] += 1

	go func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		defer rf.persist()

		rf.sendAppendEntriesToEachServer()
	}()

	index, term := rf.getLastLogIndexAndTerm()
	return index + 1, term, true
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	rf.logInfo("Killed")
	rf.role = "killed"
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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.becomeFollower()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.resetElectionTicker()
	rf.resetHeartbeatTicker()
	go rf.electionTickerLoop()
	go rf.heartbeatTickerLoop()

	rf.logInfo("Raft created")
	return rf
}
