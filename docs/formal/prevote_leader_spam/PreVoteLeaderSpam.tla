---- MODULE PreVoteLeaderSpam ----
EXTENDS Naturals, TLC

CONSTANTS Followers, OldLeader, ElectionTimeout, UpdateLastContactOnPreVote

Nodes == Followers \cup {OldLeader}
Roles == {"Leader", "Follower", "Candidate"}

VARIABLES role, sinceContact, knownLeader, tick

vars == <<role, sinceContact, knownLeader, tick>>

TypeOK ==
    /\ role \in [Nodes -> Roles]
    /\ role[OldLeader] = "Leader"
    /\ sinceContact \in [Followers -> 0..ElectionTimeout]
    /\ knownLeader \in [Followers -> BOOLEAN]
    /\ tick \in {0, 1}

Init ==
    /\ role = [n \in Nodes |-> IF n = OldLeader THEN "Leader" ELSE "Follower"]
    /\ sinceContact = [f \in Followers |-> 0]
    /\ knownLeader = [f \in Followers |-> TRUE]
    /\ tick = 0

SpamPreVote ==
    /\ role[OldLeader] = "Leader"
    /\ role' = role
    /\ knownLeader' = knownLeader
    /\ tick' = 1 - tick
    /\ sinceContact' =
        [f \in Followers |->
            IF role[f] = "Follower" THEN
                IF UpdateLastContactOnPreVote
                    THEN 0
                    ELSE IF sinceContact[f] = ElectionTimeout
                        THEN ElectionTimeout
                        ELSE sinceContact[f] + 1
            ELSE sinceContact[f]]

TimeoutToCandidate(f) ==
    /\ f \in Followers
    /\ role[f] = "Follower"
    /\ sinceContact[f] = ElectionTimeout
    /\ role' = [role EXCEPT ![f] = "Candidate"]
    /\ knownLeader' = [knownLeader EXCEPT ![f] = FALSE]
    /\ sinceContact' = sinceContact
    /\ tick' = tick

ElectCandidate(f) ==
    /\ f \in Followers
    /\ role[f] = "Candidate"
    /\ role' = [role EXCEPT ![f] = "Leader"]
    /\ knownLeader' = knownLeader
    /\ sinceContact' = sinceContact
    /\ tick' = tick

Next ==
    \/ SpamPreVote
    \/ (\E f \in Followers: TimeoutToCandidate(f))
    \/ (\E f \in Followers: ElectCandidate(f))

NoCandidateBeforeTimeout ==
    \A f \in Followers:
        role[f] = "Candidate" => sinceContact[f] = ElectionTimeout

EventualFollowerLeader ==
    <> (\E f \in Followers: role[f] = "Leader")

Spec ==
    /\ Init
    /\ [][Next]_vars
    /\ WF_vars(SpamPreVote)
    /\ WF_vars(\E f \in Followers: TimeoutToCandidate(f))
    /\ WF_vars(\E f \in Followers: ElectCandidate(f))

====
