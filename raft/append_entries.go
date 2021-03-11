package raft

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
}

// RPC Call
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	success := false

	// ignore the request if term < currentTerm
	if rf.currentTerm <= args.Term {
		// receive the heartbeat request
		if len(args.Entries) == 0 {
			success = true
			rf.voteFor = args.Term
			DPrintf("Server %d receive heartbeat from %d in term %d \n", rf.me, args.LeaderId, args.Term)
		} else {
			// receive the log replication request
			// TODO
		}
		rf.changeRole(FOLLOWER)
	}
	rf.currentTerm = max(rf.currentTerm, args.Term)
	reply.Term = rf.currentTerm
	reply.Success = success
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
