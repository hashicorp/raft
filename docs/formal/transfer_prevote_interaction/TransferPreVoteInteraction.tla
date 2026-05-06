---- MODULE TransferPreVoteInteraction ----
EXTENDS Naturals, TLC

CONSTANTS OldLeader, Target, Other, SkipPreVoteOnTimeoutNow, RespectTransferVoteFlag

Nodes == {OldLeader, Target, Other}
Roles == {"Leader", "Follower", "PreCandidate", "Candidate"}

VARIABLES role, knownLeader, transferInProgress, voteGranted, tick

vars == <<role, knownLeader, transferInProgress, voteGranted, tick>>

TypeOK ==
    /\ role \in [Nodes -> Roles]
    /\ role[Target] \in {"Follower", "PreCandidate", "Candidate", "Leader"}
    /\ role[Other] = "Follower"
    /\ knownLeader \in [{Target, Other} -> BOOLEAN]
    /\ transferInProgress \in BOOLEAN
    /\ voteGranted \in BOOLEAN
    /\ tick \in {0, 1}

Init ==
    /\ role = [n \in Nodes |-> IF n = OldLeader THEN "Leader" ELSE "Follower"]
    /\ knownLeader = [n \in {Target, Other} |-> TRUE]
    /\ transferInProgress = FALSE
    /\ voteGranted = FALSE
    /\ tick = 0

StartTransfer ==
    /\ ~transferInProgress
    /\ role[OldLeader] = "Leader"
    /\ transferInProgress' = TRUE
    /\ voteGranted' = FALSE
    /\ tick' = 1 - tick
    /\ UNCHANGED <<role, knownLeader>>

TimeoutNowToTarget ==
    /\ transferInProgress
    /\ role[Target] = "Follower"
    /\ role' =
        [role EXCEPT
            ![Target] = IF SkipPreVoteOnTimeoutNow THEN "Candidate" ELSE "PreCandidate"]
    /\ knownLeader' = [knownLeader EXCEPT ![Target] = FALSE]
    /\ tick' = 1 - tick
    /\ UNCHANGED <<transferInProgress, voteGranted>>

PreVoteFromTarget ==
    /\ transferInProgress
    /\ role[Target] = "PreCandidate"
    /\ IF knownLeader[Other] THEN
          /\ role' = role
          /\ knownLeader' = knownLeader
       ELSE
          /\ role' = [role EXCEPT ![Target] = "Candidate"]
          /\ knownLeader' = knownLeader
    /\ tick' = 1 - tick
    /\ UNCHANGED <<transferInProgress, voteGranted>>

RequestVoteFromTarget ==
    /\ transferInProgress
    /\ role[Target] = "Candidate"
    /\ ~voteGranted
    /\ IF knownLeader[Other] /\ ~RespectTransferVoteFlag THEN
          /\ voteGranted' = FALSE
       ELSE
          /\ voteGranted' = TRUE
    /\ tick' = 1 - tick
    /\ UNCHANGED <<role, knownLeader, transferInProgress>>

ElectTarget ==
    /\ transferInProgress
    /\ role[Target] = "Candidate"
    /\ voteGranted
    /\ role' =
        [role EXCEPT
            ![OldLeader] = "Follower",
            ![Target] = "Leader"]
    /\ knownLeader' = [n \in {Target, Other} |-> FALSE]
    /\ transferInProgress' = FALSE
    /\ voteGranted' = voteGranted
    /\ tick' = 1 - tick

SpamPreVoteFromOldLeader ==
    /\ role[OldLeader] = "Leader"
    /\ knownLeader' = [knownLeader EXCEPT ![Other] = TRUE]
    /\ tick' = 1 - tick
    /\ UNCHANGED <<role, transferInProgress, voteGranted>>

Next ==
    \/ StartTransfer
    \/ TimeoutNowToTarget
    \/ PreVoteFromTarget
    \/ RequestVoteFromTarget
    \/ ElectTarget
    \/ SpamPreVoteFromOldLeader

TransferImpliesVoteGranted ==
    role[Target] = "Leader" => voteGranted

TransferEventuallyCompletes ==
    <> (role[Target] = "Leader" /\ ~transferInProgress)

Spec ==
    /\ Init
    /\ [][Next]_vars
    /\ WF_vars(StartTransfer)
    /\ WF_vars(TimeoutNowToTarget)
    /\ WF_vars(PreVoteFromTarget)
    /\ WF_vars(RequestVoteFromTarget)
    /\ WF_vars(ElectTarget)
    /\ WF_vars(SpamPreVoteFromOldLeader)

====
