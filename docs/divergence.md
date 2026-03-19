# HashiCorp Raft Divergences

In 2013 HashiCorp created its own Raft implementation based on the just
released [Raft paper by Diego Ongaro and John Ousterhout][paper]. This was
before [Diego's subsequent Raft dissertation][diss] in 2014, and long before
third party analyses such as Heidi Howard and Ittai Abraham's [Raft does not
Guarantee Liveness in the face of Network Faults ][live]
in 2020.[^1]

HashiCorp's Raft library usage grew rapidly through its use in [Consul][consul]
and [Nomad][nomad], and [later Vault][vault], in parallel with rapidly
expanding use in [etcd][etcd] and other implementations.

The explosion in activity between live systems and research led to a wide
divergence between not only implementations, but implementations and the
original paper and dissertation.

This document attempts to explain where HashiCorp Raft either meaningfully diverges
from the original Raft paper, or makes an implementation choice not explicitly
outlined in the paper.

This is **not** expected to be a comprehensive list. Additions and edits are
welcome!

## Asynchronous Heartbeats

The Raft paper defines heartbeats as empty AppendEntries RPCs which are sent by
the leader to each server after elections and during idle periods to prevent
election timeouts.

HashiCorp Raft performs [heartbeating concurrently][async-heart] with other
AppendEntries RPCs to avoid having to set the election timeout high enough to
account for the max acceptable disk operation. This allows the heartbeat
timeout to detect network partitions much more quickly without risking causing
an election during periodic but ephemeral spikes in disk io latency.

## Rejecting votes when there's already a leader

The [Raft does not Guarantee liveness][live] paper describes how certain
partitions can prevent Raft clusters from making progress by causing continual
elections.

HashiCorp Raft implements the second of the suggested fixes from Howard's
paper: rejecting vote request RPCs when there is already an established leader.
The paper defines this more precisely as:

> ...ignore RequestVote RPCs if they have received an AppendEntries RPC from
> the leader within the election timeout. 

This approach is actually mentioned in the Cluster membership changes section
of the original Raft paper, but explicitly excludes its use during "normal"
elections:

> To prevent this problem, servers disregard RequestVote RPCs when they believe
> a current leader exists. Specifically, if a server receives a RequestVote RPC
> within the minimum election timeout of hearing from a current leader, it does
> not update its term or grant its vote.  This does not affect normal
> elections...

So HashiCorp Raft follows the later paper's suggestion and ignores the original
paper's exclusion of this logic during normal operation.

## Pre-Vote

[HashiCorp Raft implements the Pre-Vote extension][prevote-pr] defined in the
[Raft dissertation][diss] (§9.6). Pre-Vote is an optimization where a candidate
discovers whether its index is up to date and therefore able to win an election
before incrementing its term and causing an election.

The Pre-Vote extension is enabled by default but may be disabled in using the
[Config.PreVoteDisabled][prevote-config] flag.

## Leadership Transfer

[HashiCorp Raft implements the Leadership Transfer extension][transleader-pr]
as defined in the [Raft dissertation][diss] (§3.10). Leadership transfer is an
optimization that allows the current leader to hand off leadership to a
follower to avoid waiting for the election timeout during regular operations
such as restarts and upgrades.

While leadership transfer in defined in the Raft dissertation, HashiCorp Raft
extends the specification slightly because of _another_ divergence in HashiCorp
Raft: [rejecting votes when there's already a
leader](#rejecting-votes-when-theres-already-a-leader). Since other followers
would reject the intended new-leader's request for a vote, HashiCorp Raft adds
an extra [`LeadershipTransfer` flag][transleader-flag] to override that
behavior in the case of leadership transfers.

All Raft members should support leadership transfers before a transfer is
attempted. The feature is **not** enabled by default and requires explicitly
triggering at the application level. Consul was the first to implement this via
mechanisms in their [API/CLI][transleader-cli] and [graceful agent
shutdown][transleader-shutdown].

[^1]: See https://raft.github.io/ for a comprehensive list of papers and
    resources.

[paper]: https://raft.github.io/raft.pdf
[diss]: https://github.com/ongardie/dissertation#readme
[live]: https://decentralizedthoughts.github.io/2020-12-12-raft-liveness-full-omission/
[consul]: https://github.com/hashicorp/consul
[nomad]: https://github.com/hashicorp/nomad
[vault]: https://github.com/hashicorp/vault
[etcd]: https://etcd.io/
[async-heart]: https://github.com/hashicorp/raft/blob/v1.7.3/replication.go#L385-L387
[prevote-pr]: https://github.com/hashicorp/raft/pull/530
[prevote-config]: https://pkg.go.dev/github.com/hashicorp/raft#Config.PreVoteDisabled
[transleader-pr]: https://github.com/hashicorp/raft/pull/306
[transleader-flag]: https://pkg.go.dev/github.com/hashicorp/raft#RequestVoteRequest.LeadershipTransfer
[transleader-cli]: https://github.com/hashicorp/consul/issues/5405
[transleader-shutdown]: https://github.com/hashicorp/consul/issues/5406
