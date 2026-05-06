# Leadership Transfer x Pre-Vote Interaction Model

This model explores liveness risk when leadership transfer interacts with pre-vote gating.

It focuses on two behavior toggles:

- `SkipPreVoteOnTimeoutNow` - when `TRUE`, transfer target goes directly to vote (current intended behavior via `candidateFromLeadershipTransfer`).
- `RespectTransferVoteFlag` - when `TRUE`, followers can grant transfer votes despite known leader.

The target liveness property is:

- `TransferEventuallyCompletes == <> (role[Target] = "Leader" /\ ~transferInProgress)`

## Why this matters

HashiCorp Raft combines:

- pre-vote rejection when a known leader exists, and
- a leadership-transfer vote override (`RequestVoteRequest.LeadershipTransfer`)

If either interaction is broken, transfer can stall under normal known-leader conditions.

## Files

- `TransferPreVoteInteraction.tla` - core interaction model
- `TransferPreVoteInteraction.cfg` - intended behavior (expected pass)
- `TransferPreVoteInteractionBugSkipPreVote.cfg` - target runs pre-vote first (expected liveness failure)
- `TransferPreVoteInteractionBugTransferFlag.cfg` - transfer vote flag ignored (expected liveness failure)

## Run TLC

From this directory:

```bash
cd "$(git rev-parse --show-toplevel)/docs/formal/transfer_prevote_interaction"
```

Intended behavior:

```bash
java -XX:+UseParallelGC -cp "../tools/tla2tools-v1.8.0.jar" tlc2.TLC \
  -workers 4 -cleanup -metadir /tmp/tlc-transfer-prevote-fixed \
  TransferPreVoteInteraction.tla -config TransferPreVoteInteraction.cfg
```

Bug profile 1:

```bash
java -XX:+UseParallelGC -cp "../tools/tla2tools-v1.8.0.jar" tlc2.TLC \
  -workers 4 -cleanup -metadir /tmp/tlc-transfer-prevote-bug1 \
  TransferPreVoteInteraction.tla -config TransferPreVoteInteractionBugSkipPreVote.cfg
```

Bug profile 2:

```bash
java -XX:+UseParallelGC -cp "../tools/tla2tools-v1.8.0.jar" tlc2.TLC \
  -workers 4 -cleanup -metadir /tmp/tlc-transfer-prevote-bug2 \
  TransferPreVoteInteraction.tla -config TransferPreVoteInteractionBugTransferFlag.cfg
```

Expected results:

- fixed config: no violation
- both bug configs: liveness counterexample for `TransferEventuallyCompletes`

## Test mapping

The model maps to deterministic test intent:

- non-leader pre-vote rejection should still hold during known-leader windows
- transfer vote path should still be able to complete because transfer requests bypass the known-leader vote gate
