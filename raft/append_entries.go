package raft

import (
	"os"
	"time"
)

// RPC Args
type AppendEntriesArgs struct {
	LeaderId     int
	Term         int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// RPC Reply
type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	SIndex  int
}

// RPC Call
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.SIndex = -1
	reply.XTerm = -1
	reply.XIndex = -1
	reply.Success = false
	// Ignore the request if term < currentTerm
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		return
	}
	// Always change to follower if args.term >= rf.currentTerm
	// if current server is leader, since there is only one leader in same term
	// args.term must greater than current term
	rf.currentTerm = max(rf.currentTerm, args.Term)
	rf.changeRole(FOLLOWER)
	reply.Term = rf.currentTerm
	DPrintf("Server %d receive entries %v from %d in term %d at %v", rf.me, args.Entries, args.LeaderId, args.Term, time.Now())
	if rf.voteFor != args.LeaderId {
		rf.voteFor = -1
	}

	// Reply false if log does not contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	log, find := rf.getLogAtIdx(args.PrevLogIndex)
	logMatch := args.PrevLogIndex == 0 || args.PrevLogIndex == rf.LastIncludeIndex || (find && log.Term == args.PrevLogTerm)
	if !logMatch {
		if !find {
			reply.SIndex = len(rf.logEntries) + 1
			if rf.LastIncludeIndex != -1 {
				reply.SIndex = len(rf.logEntries) + rf.LastIncludeIndex + 1
			}
		} else {
			reply.XTerm = log.Term
			reply.XIndex = rf.firstIdxWithTerm(log.Term)
		}
	} else {
		reply.Success = true
		conflictIdx := -1
		isConflict := false
		// If an existing entry conflicts with a new one (same index
		// but different terms), delete the existing entry and all that
		// follow it
		for _, entry := range args.Entries {
			localEntry, find := rf.getLogAtIdx(entry.Index)
			if find {
				if entry.Term != localEntry.Term {
					isConflict = true
					conflictIdx = localEntry.Index - rf.logEntries[0].Index //localIndex
					break
				}
			} else {
				rf.logEntries = append(rf.logEntries, entry)
			}
		}
		// Append any new entries not already in the log
		if isConflict {
			rf.logEntries = rf.logEntries[:conflictIdx]
			beginIndex := rf.getLastLog().Index
			for _, entry := range args.Entries {
				if entry.Index > beginIndex {
					rf.logEntries = append(rf.logEntries, entry)
				}
			}
		}
		DPrintf("Server %d log become %v\n", rf.me, rf.logEntries)
		curCommitIdx := rf.commitIndex
		// If leaderCommit > commitIndex, set commitIndex =
		// min(leaderCommit, index of last new entry)
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, rf.getLastLog().Index)
		}
		if rf.commitIndex > curCommitIdx {
			rf.applyCon.Signal()
		}
	}
	rf.persist()
}

func (rf *Raft) requestAppendEntries(server int, isHeartBeat bool) {
	logEntries := []LogEntry{}
	rf.mu.Lock()
	// Only leader can send append entries
	if rf.role != LEADER {
		rf.mu.Unlock()
		return
	}
	nextIndex := rf.nextIndex[server]
	for i := rf.nextIndex[server]; i <= rf.getLastLog().Index; i++ {
		entry, find := rf.getLogAtIdx(i)
		if !find {
			// todo
			os.Exit(-1)
		}
		logEntries = append(logEntries, entry)
	}
	if len(logEntries) == 0 && isHeartBeat == false {
		rf.mu.Unlock()
		return
	}
	DPrintf("Server %d send log entries %v to server %d in term %d\n", rf.me, logEntries, server, rf.currentTerm)
	args := AppendEntriesArgs{
		LeaderId:     rf.me,
		Term:         rf.currentTerm,
		PrevLogIndex: nextIndex - 1,
		Entries:      logEntries,
		LeaderCommit: rf.commitIndex,
	}
	if args.PrevLogIndex == 0 {
		args.PrevLogTerm = rf.currentTerm
	} else {
		args.PrevLogTerm = rf.logEntries[args.PrevLogIndex-1].Term
	}
	reply := AppendEntriesReply{}
	rf.mu.Unlock()
	// Send entries
	ok := rf.sendAppendEntries(server, &args, &reply)
	isContinue := false
	if ok {
		isContinue = rf.handleAppendEntriesRes(server, &args, &reply)
		if isContinue {
			go rf.requestAppendEntries(server, false)
		}
	}
}

// Handle the response of appendEntries RPC call
func (rf *Raft) handleAppendEntriesRes(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != LEADER {
		return false
	}
	// Discover server with higher term
	// Change to follower
	if reply.Term > args.Term {
		rf.voteFor = -1
		rf.currentTerm = max(rf.currentTerm, reply.Term)
		rf.changeRole(FOLLOWER)
		return false
	}

	isContinue := false
	isApplied := false
	// Update nextIndex and matchIndex
	if reply.Success {
		rf.matchIndex[server] = max(rf.matchIndex[server], len(args.Entries)+args.PrevLogIndex)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		isApplied = rf.updateCommitForLeader()
	} else {
		if reply.SIndex != -1 {
			rf.nextIndex[server] = reply.SIndex
		} else {
			index := rf.lastIdxWithTerm(reply.XTerm)
			if index == -1 {
				rf.nextIndex[server] = reply.XIndex
			} else {
				rf.nextIndex[server] = index + 1
			}
		}
		if rf.nextIndex[server] < 1 {
			rf.nextIndex[server] = 1
		}
		isContinue = true
	}
	// Applied the new logs
	if isApplied {
		rf.applyCon.Signal()
	}
	return isContinue
}

// Update the commit index based on matchIndex for Leader
func (rf *Raft) updateCommitForLeader() bool {
	lastCommittedIndex := -1
	updated := false
	for beginIndex := rf.commitIndex + 1; beginIndex <= rf.getLastLog().Index; beginIndex++ {
		granted := 1
		for server := 0; server < len(rf.peers); server++ {
			if server == rf.me {
				continue
			}
			if rf.matchIndex[server] >= beginIndex {
				granted++
			}
		}
		if granted > len(rf.peers)/2 && (rf.logEntries[beginIndex-1].Term == rf.currentTerm) {
			lastCommittedIndex = beginIndex
		}
	}
	if lastCommittedIndex > rf.commitIndex {
		rf.commitIndex = lastCommittedIndex
		DPrintf("Server %d change commit idx to %d\n", rf.me, rf.commitIndex)
		updated = true
	}
	return updated
}

// Send RPC Call
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Leader send heartbeat to all the servers when heartbeat timer timeout
func (rf *Raft) sendHeartbeat() {
	rf.mu.Lock()
	// Only leader can send heartbeat
	if rf.role != LEADER {
		rf.mu.Unlock()
		return
	}
	rf.resetHeartbeatTimer()
	rf.mu.Unlock()
	for server := 0; server < len(rf.peers); server++ {
		if server == rf.me {
			continue
		}
		// Send heartbeat to all the server
		go func(server int) {
			rf.requestAppendEntries(server, true)
		}(server)
	}
}

// Rest heartbeat timer
func (rf *Raft) resetHeartbeatTimer() {
	if !rf.heartbeatTimer.Stop() {
		select {
		case <-rf.heartbeatTimer.C:
		default:
		}
	}
	rf.heartbeatTimer.Reset(HeartBeatTimeout)
}
