package raft

import (
	"math/rand"
	"time"
)

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

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	currentTerm := rf.currentTerm
	voteFor := rf.voteFor

	reply.VoteGranted = false
	vote := false

	if args.Term > currentTerm {
		vote = true
	} else if args.Term == currentTerm {
		if voteFor == -1 || voteFor == args.CandidateId {
			vote = true
		}
	}
	rf.currentTerm = max(currentTerm, args.Term)

	if vote {
		reply.VoteGranted = true
		rf.voteFor = args.CandidateId
		rf.changeRole(FOLLOWER)
		DPrintf("Server %d agree vote to server %d in term %d\n", rf.me, args.CandidateId, args.Term)
	} else {
		reply.VoteGranted = false
		DPrintf("Server %d disagree vote to server %d in term %d\n", rf.me, args.CandidateId, args.Term)
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func randElectionTimeout() time.Duration {
	r := time.Duration(rand.Int63()) % ElectionTimeout
	return ElectionTimeout + r
}

func (rf *Raft) resetElectionTimer() {
	if !rf.electionTimer.Stop() {
		select {
		case <-rf.electionTimer.C:
		default:
		}
	}
	rf.electionTimer.Reset(randElectionTimeout())
}

func (rf *Raft) resetHeartbeatTimer() {
	if !rf.heartbeatTimer.Stop() {
		select {
		case <-rf.heartbeatTimer.C:
		default:
		}
	}
	rf.heartbeatTimer.Reset(HeartBeatTimeout)
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	role := rf.role
	if role == LEADER {
		rf.resetElectionTimer()
		rf.mu.Unlock()
		return
	}

	rf.changeRole(CANDIDATE)
	currentTerm := rf.currentTerm
	DPrintf("Server %d start Election in term %d\n", rf.me, rf.currentTerm)
	args := &RequestVoteArgs{}
	args.CandidateId = rf.me
	args.Term = rf.currentTerm
	rf.mu.Unlock()

	finished := 1
	granted := 1
	votesCh := make(chan bool, len(rf.peers)-1)

	for server := 0; server < len(rf.peers); server++ {
		if server == rf.me {
			continue
		}
		go func(server int, ch chan bool, args *RequestVoteArgs) {
			DPrintf("Server %d send vote request to %d", rf.me, server)
			reply := &RequestVoteReply{}
			rf.sendRequestVote(server, args, reply)
			ch <- reply.VoteGranted
			rf.mu.Lock()
			if reply.Term > currentTerm {
				rf.voteFor = -1
			}
			rf.mu.Unlock()
		}(server, votesCh, args)
	}

	for {
		r := <-votesCh
		finished += 1
		if r == true {
			granted += 1
		}
		if finished == len(rf.peers) || granted > len(rf.peers)/2 || finished-granted > len(rf.peers)/2 {
			break
		}
	}

	rf.mu.Lock()
	if args.Term == rf.currentTerm && granted >= len(rf.peers)/2+1 && rf.role == CANDIDATE {
		DPrintf("Server %d become a leader in term %d \n", rf.me, args.Term)
		rf.changeRole(LEADER)
	} else {
		DPrintf("Server %d fail to become a leader in term %d \n", rf.me, args.Term)
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendHeartbeat() {
	rf.mu.Lock()
	if rf.role != LEADER {
		rf.mu.Unlock()
		return
	}
	currentTerm := rf.currentTerm
	rf.resetHeartbeatTimer()
	rf.mu.Unlock()
	for server := 0; server < len(rf.peers); server++ {
		if server == rf.me {
			continue
		}
		go func(server int) {
			args := &AppendEntriesArgs{}
			args.Term = currentTerm
			args.LeaderId = rf.me
			reply := &AppendEntriesReply{}
			rf.sendAppendEntries(server, args, reply)
			rf.mu.Lock()
			if reply.Success == false && rf.role == LEADER {
				rf.changeRole(FOLLOWER)
				rf.voteFor = -1
				rf.currentTerm = reply.Term
			}
			rf.mu.Unlock()
		}(server)
	}
}
