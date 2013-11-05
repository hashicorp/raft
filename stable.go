package raft

// StableStore is used to provide stable storage
// of key configurations to ensure safety.
type StableStore interface {
	// Returns the current term
	CurrentTerm() (uint64, error)

	// Returns the candidate we voted for this term
	VotedFor() (string, error)

	// Sets the current term. Clears the current vote.
	SetCurrentTerm(uint64) error

	// Sets a candidate vote for the current term
	SetVote(string) error

	// Returns our candidate ID. This should be unique
	// and constant across runs
	CandidateID() (string, error)
}
