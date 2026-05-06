# Pre-Vote Leader-Spam Liveness Model

This model targets the pre-vote liveness bug fixed by [PR #609](https://github.com/hashicorp/raft/pull/609): granting pre-votes must not refresh follower liveness (`lastContact`), or followers can be prevented from timing out and becoming candidates.

## What this model captures

- An old leader keeps spamming followers with pre-vote requests.
- Followers track contact age (`sinceContact`) used for election timeout.
- A config toggle controls whether granting pre-vote refreshes contact:
  - `UpdateLastContactOnPreVote = FALSE` (fixed behavior)
  - `UpdateLastContactOnPreVote = TRUE` (buggy behavior)

The liveness property checks that some follower eventually becomes leader:

- `EventualFollowerLeader == <> (\E f \in Followers: role[f] = "Leader")`

If pre-vote incorrectly refreshes contact, followers can remain followers forever and violate the property.

## Files

- `PreVoteLeaderSpam.tla` - core model
- `PreVoteLeaderSpam.cfg` - fixed behavior (expected pass)
- `PreVoteLeaderSpamBug.cfg` - buggy behavior (expected liveness counterexample)

## Run TLC

From this directory:

```bash
cd "$(git rev-parse --show-toplevel)/docs/formal/prevote_leader_spam"
```

Fixed behavior:

```bash
java -XX:+UseParallelGC -cp "../tools/tla2tools-v1.8.0.jar" tlc2.TLC \
  -workers 4 -cleanup -metadir /tmp/tlc-prevote-fixed \
  PreVoteLeaderSpam.tla -config PreVoteLeaderSpam.cfg
```

Buggy behavior:

```bash
java -XX:+UseParallelGC -cp "../tools/tla2tools-v1.8.0.jar" tlc2.TLC \
  -workers 4 -cleanup -metadir /tmp/tlc-prevote-bug \
  PreVoteLeaderSpam.tla -config PreVoteLeaderSpamBug.cfg
```

Expected result:

- fixed config: no violation for `EventualFollowerLeader`
- bug config: temporal property violation with a loop where followers never timeout into candidacy

## Test mapping

This model maps to `TestRaft_PreVote_LeaderSpam` in `integ_test.go`, which should pass in fixed code and fail if `requestPreVote` updates `lastContact` when granting pre-vote.
