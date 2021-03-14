package raft

import (
	"math/rand"
	"time"
)

// RPC Args
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RPC Reply
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RPC Call
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// refuse to vote if currentTerm > args.term
	if rf.currentTerm > args.Term {
		DPrintf("Server %d disagree vote to server %d in term %d\n", rf.me, args.CandidateId, args.Term)
		return
	}
	if rf.currentTerm == args.Term {
		// Server Already vote for other server in same term
		if rf.voteFor != -1 && rf.voteFor != args.CandidateId {
			DPrintf("Server %d disagree vote to server %d in term %d\n", rf.me, args.CandidateId, args.Term)
			return
		}
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.changeRole(FOLLOWER)
	}
	isLogOk := rf.checkLogUpdate(args.LastLogIndex, args.LastLogTerm)
	if !isLogOk {
		DPrintf("Server %d disagree vote to server %d in term %d\n", rf.me, args.CandidateId, args.Term)
		return
	}
	rf.currentTerm = args.Term
	rf.voteFor = args.CandidateId
	reply.VoteGranted = true
	rf.changeRole(FOLLOWER)
	DPrintf("Server %d agree vote to server %d in term %d\n", rf.me, args.CandidateId, args.Term)
}

// Send RPC Call
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// Check if the candidate's log is up to date
func (rf *Raft) checkLogUpdate(srcLastLogIdx int, srcLastLogTerm int) bool {
	lastLog := rf.getLastLog()
	if lastLog.Term > srcLastLogTerm {
		return false
	} else if lastLog.Term < srcLastLogTerm {
		return true
	} else {
		return srcLastLogIdx >= lastLog.Index
	}
}

// Random the election time
func randElectionTimeout() time.Duration {
	r := time.Duration(rand.Int63()) % ElectionTimeout
	return ElectionTimeout + r
}

// Reset election timer
func (rf *Raft) resetElectionTimer() {
	if !rf.electionTimer.Stop() {
		select {
		case <-rf.electionTimer.C:
		default:
		}
	}
	rf.electionTimer.Reset(randElectionTimeout())
}

// Follower start election when election timer timeout
// and become candidate
func (rf *Raft) startElection() {
	rf.mu.Lock()
	role := rf.role
	lastLog := rf.getLastLog()

	// Leader do not need to start election
	if role == LEADER {
		rf.resetElectionTimer()
		rf.mu.Unlock()
		return
	}

	rf.changeRole(CANDIDATE)
	currentTerm := rf.currentTerm
	DPrintf("Server %d start Election in term %d\n at %v", rf.me, rf.currentTerm, time.Now())
	rf.mu.Unlock()

	finished := 1
	granted := 1
	votesCh := make(chan bool, len(rf.peers)-1)

	for server := 0; server < len(rf.peers); server++ {
		if server == rf.me {
			continue
		}
		// Send vote request concurrently
		go func(server int, ch chan bool) {
			DPrintf("Server %d send vote request to %d", rf.me, server)
			args := &RequestVoteArgs{}
			args.CandidateId = rf.me
			args.Term = currentTerm
			args.LastLogTerm = lastLog.Term
			args.LastLogIndex = lastLog.Index
			reply := &RequestVoteReply{}
			rf.sendRequestVote(server, args, reply)
			rf.mu.Lock()
			if rf.currentTerm < reply.Term {
				rf.currentTerm = reply.Term
				rf.voteFor = -1
				rf.changeRole(FOLLOWER)
			}
			rf.mu.Unlock()
			ch <- reply.VoteGranted
		}(server, votesCh)
	}

	for {
		r := <-votesCh
		finished += 1
		if r == true {
			granted += 1
		}
		// Once get the majority vote, candidate can become the leader
		if finished == len(rf.peers) || granted > len(rf.peers)/2 || finished-granted > len(rf.peers)/2 {
			break
		}
	}

	rf.mu.Lock()
	// Make sure current server is still a candidate before becoming a leader
	if currentTerm == rf.currentTerm && granted >= len(rf.peers)/2+1 && rf.role == CANDIDATE {
		DPrintf("Server %d become a leader in term %d \n", rf.me, currentTerm)
		rf.changeRole(LEADER)
	} else {
		DPrintf("Server %d fail to become a leader in term %d \n", rf.me, currentTerm)
		rf.resetElectionTimer()
	}
	rf.mu.Unlock()
}
