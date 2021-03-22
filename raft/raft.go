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
	"6.824/labgob"
	"bytes"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Role string

const (
	CANDIDATE = "candidate"
	FOLLOWER  = "follower"
	LEADER    = "leader"
)

const (
	ElectionTimeout  = time.Millisecond * 300
	HeartBeatTimeout = time.Millisecond * 150
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role           Role
	voteFor        int
	currentTerm    int
	heartbeatTimer *time.Timer
	electionTimer  *time.Timer
	logEntries     []LogEntry
	commitIndex    int
	lastApplied    int
	nextIndex      []int
	matchIndex     []int
	applyChan      chan ApplyMsg
	applyCon       *sync.Cond
}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

// Change the role of current server
func (rf *Raft) changeRole(role Role) {
	rf.role = role
	if role == CANDIDATE {
		rf.currentTerm++
		rf.resetElectionTimer()
		rf.voteFor = rf.me
		rf.persist()
	} else if role == FOLLOWER {
		rf.resetElectionTimer()
		rf.persist()
	} else if role == LEADER {
		lastIdx := rf.getLastLog().Index
		for i, _ := range rf.nextIndex {
			rf.nextIndex[i] = lastIdx + 1
			rf.matchIndex[i] = 0
		}
		rf.resetHeartbeatTimer()
	} else {
		panic("Unknown role \n")
	}
}

// Get the last log entry of current server
func (rf *Raft) getLastLog() LogEntry {
	entry := LogEntry{}
	if len(rf.logEntries) == 0 {
		entry.Term = 0
		entry.Index = 0
	} else {
		entry = rf.logEntries[len(rf.logEntries)-1]
	}
	return entry
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	var term int
	var isleader bool

	if rf.role == LEADER {
		isleader = true
	} else {
		isleader = false
	}
	term = rf.currentTerm
	rf.mu.Unlock()
	return term, isleader
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
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logEntries)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var logEntries []LogEntry
	d.Decode(&currentTerm)
	d.Decode(&voteFor)
	d.Decode(&logEntries)
	rf.currentTerm = currentTerm
	rf.voteFor = voteFor
	rf.logEntries = logEntries
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	rf.mu.Lock()
	isLeader := rf.role == LEADER

	if isLeader {
		log := LogEntry{}
		log.Command = command
		log.Term = rf.currentTerm
		log.Index = len(rf.logEntries) + 1
		index = log.Index
		term = log.Term
		rf.logEntries = append(rf.logEntries, log)
		rf.persist()
		// reset heartbeat before sending append entry request
		rf.resetHeartbeatTimer()
		// sending append entry request
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go rf.requestAppendEntries(i, false)
		}
	}
	rf.mu.Unlock()
	return index, term, isLeader
}

// Apply the log when log is committed
func (rf *Raft) doApplyMsg() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCon.Wait()
		}
		for s := rf.lastApplied + 1; s <= rf.commitIndex; s++ {
			DPrintf("Server %d apply log %d \n", rf.me, s)
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.logEntries[s-1].Command,
				CommandIndex: s,
			}
			rf.mu.Unlock()
			rf.applyChan <- applyMsg
			rf.mu.Lock()
			rf.lastApplied = s
		}
		rf.mu.Unlock()
	}
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

	}
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
	rf.electionTimer = time.NewTimer(randElectionTimeout())
	rf.heartbeatTimer = time.NewTimer(HeartBeatTimeout)
	rf.voteFor = -1
	rf.role = FOLLOWER
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyChan = applyCh
	rf.applyCon = sync.NewCond(&rf.mu)
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex = append(rf.nextIndex, 1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections and send heartbeat
	go func() {
		for !rf.killed() {
			select {
			case <-rf.electionTimer.C:
				go rf.startElection()
			case <-rf.heartbeatTimer.C:
				rf.sendHeartbeat()
			}
		}
	}()

	// apply log when it is committed
	go rf.doApplyMsg()

	return rf
}
