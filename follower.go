package raft

// followerAppendEntries is invoked when we are in the follwer state and
// get an append entries RPC call
func (r *Raft) followerAppendEntries(rpc RPC, a *AppendEntriesRequest) {
}

// followerRequestVote is invoked when we are in the follwer state and
// get an request vote RPC call
func (r *Raft) followerRequestVote(rpc RPC, req *RequestVoteRequest) {
}
