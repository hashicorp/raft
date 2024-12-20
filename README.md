raft [![Build Status](https://github.com/hashicorp/raft/workflows/ci/badge.svg)](https://github.com/hashicorp/raft/actions)
====

raft is a [Go](http://www.golang.org) library that manages a replicated
log and can be used with an FSM to manage replicated state machines. It
is a library for providing [consensus](http://en.wikipedia.org/wiki/Consensus_(computer_science)).

The use cases for such a library are far-reaching, such as replicated state
machines which are a key component of many distributed systems. They enable
building Consistent, Partition Tolerant (CP) systems, with limited
fault tolerance as well.

## Building

If you wish to build raft you'll need Go version 1.16+ installed.

Please check your installation with:

```
go version
```

## Documentation

For complete documentation, see the associated [Godoc](http://godoc.org/github.com/hashicorp/raft).

To prevent complications with cgo, the primary backend `MDBStore` is in a separate repository,
called [raft-mdb](http://github.com/hashicorp/raft-mdb). That is the recommended implementation
for the `LogStore` and `StableStore`.

A pure Go backend using [Bbolt](https://github.com/etcd-io/bbolt) is also available called
[raft-boltdb](https://github.com/hashicorp/raft-boltdb). It can also be used as a `LogStore`
and `StableStore`.


## Community Contributed Examples 
- [Raft gRPC Example](https://github.com/Jille/raft-grpc-example) - Utilizing the Raft repository with gRPC
- [Raft-based KV-store Example](https://github.com/otoolep/hraftd) - Uses Hashicorp Raft to build a distributed key-value store


## Tagged Releases

As of September 2017, HashiCorp will start using tags for this library to clearly indicate
major version updates. We recommend you vendor your application's dependency on this library.

* v0.1.0 is the original stable version of the library that was in main and has been maintained
with no breaking API changes. This was in use by Consul prior to version 0.7.0.

* v1.0.0 takes the changes that were staged in the library-v2-stage-one branch. This version
manages server identities using a UUID, so introduces some breaking API changes. It also versions
the Raft protocol, and requires some special steps when interoperating with Raft servers running
older versions of the library (see the detailed comment in config.go about version compatibility).
You can reference https://github.com/hashicorp/consul/pull/2222 for an idea of what was required
to port Consul to these new interfaces.

    This version includes some new features as well, including non voting servers, a new address
    provider abstraction in the transport layer, and more resilient snapshots.

## Protocol

raft is based on ["Raft: In Search of an Understandable Consensus Algorithm"](https://raft.github.io/raft.pdf)

A high level overview of the Raft protocol is described below, but for details please read the full
[Raft paper](https://raft.github.io/raft.pdf)
followed by the raft source. Any questions about the raft protocol should be sent to the
[raft-dev mailing list](https://groups.google.com/forum/#!forum/raft-dev).

### Protocol Description

Raft nodes are always in one of three states: follower, candidate or leader. All
nodes initially start out as a follower. In this state, nodes can accept log entries
from a leader and cast votes. If no entries are received for some time, nodes
self-promote to the candidate state. In the candidate state nodes request votes from
their peers. If a candidate receives a quorum of votes, then it is promoted to a leader.
The leader must accept new log entries and replicate to all the other followers.
In addition, if stale reads are not acceptable, all queries must also be performed on
the leader.

Once a cluster has a leader, it is able to accept new log entries. A client can
request that a leader append a new log entry, which is an opaque binary blob to
Raft. The leader then writes the entry to durable storage and attempts to replicate
to a quorum of followers. Once the log entry is considered *committed*, it can be
*applied* to a finite state machine. The finite state machine is application specific,
and is implemented using an interface.

An obvious question relates to the unbounded nature of a replicated log. Raft provides
a mechanism by which the current state is snapshotted, and the log is compacted. Because
of the FSM abstraction, restoring the state of the FSM must result in the same state
as a replay of old logs. This allows Raft to capture the FSM state at a point in time,
and then remove all the logs that were used to reach that state. This is performed automatically
without user intervention, and prevents unbounded disk usage as well as minimizing
time spent replaying logs.

Lastly, there is the issue of updating the peer set when new servers are joining
or existing servers are leaving. As long as a quorum of nodes is available, this
is not an issue as Raft provides mechanisms to dynamically update the peer set.
If a quorum of nodes is unavailable, then this becomes a very challenging issue.
For example, suppose there are only 2 peers, A and B. The quorum size is also
2, meaning both nodes must agree to commit a log entry. If either A or B fails,
it is now impossible to reach quorum. This means the cluster is unable to add,
or remove a node, or commit any additional log entries. This results in *unavailability*.
At this point, manual intervention would be required to remove either A or B,
and to restart the remaining node in bootstrap mode.

A Raft cluster of 3 nodes can tolerate a single node failure, while a cluster
of 5 can tolerate 2 node failures. The recommended configuration is to either
run 3 or 5 raft servers. This maximizes availability without
greatly sacrificing performance.

In terms of performance, Raft is comparable to Paxos. Assuming stable leadership,
committing a log entry requires a single round trip to half of the cluster.
Thus performance is bound by disk I/O and network latency.


  ## Metrics Emission and Compatibility

  This library can emit metrics using either `github.com/armon/go-metrics` or `github.com/hashicorp/go-metrics`. Choosing between the libraries is controlled via build tags. 

  **Build Tags**
  * `armonmetrics` - Using this tag will cause metrics to be routed to `armon/go-metrics`
  * `hashicorpmetrics` - Using this tag will cause all metrics to be routed to `hashicorp/go-metrics`

  If no build tag is specified, the default behavior is to use `armon/go-metrics`. 

  **Deprecating `armon/go-metrics`**

  Emitting metrics to `armon/go-metrics` is officially deprecated. Usage of `armon/go-metrics` will remain the default until mid-2025 with opt-in support continuing to the end of 2025.

  **Migration**
  To migrate an application currently using the older `armon/go-metrics` to instead use `hashicorp/go-metrics` the following should be done.

  1. Upgrade libraries using `armon/go-metrics` to consume `hashicorp/go-metrics/compat` instead. This should involve only changing import statements. All repositories in the `hashicorp` namespace
  2. Update an applications library dependencies to those that have the compatibility layer configured.
  3. Update the application to use `hashicorp/go-metrics` for configuring metrics export instead of `armon/go-metrics`
     * Replace all application imports of `github.com/armon/go-metrics` with `github.com/hashicorp/go-metrics`
     * Instrument your build system to build with the `hashicorpmetrics` tag.

  Eventually once the default behavior changes to use `hashicorp/go-metrics` by default (mid-2025), you can drop the `hashicorpmetrics` build tag.
