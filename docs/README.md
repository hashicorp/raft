# Raft Developer Documentation

This documentation provides a high level introduction to the `hashicorp/raft`
implementation. The intended audience is anyone interested in understanding
or contributing to the code.

## Contents

1. [Terminology](#terminology)
2. [Operations](#operations)
   1. [Apply](./apply.md)
3. [Threads](#threads)


## Terminology

This documentation uses the following terms as defined.

* **Cluster** - the set of peers in the raft configuration
* **Peer** - a node that participates in the consensus protocol using `hashicorp/raft`. A
  peer may be in one of the following states: **follower**, **candidate**, or **leader**.
* **Log** - the full set of log entries.
* **Log Entry** - an entry in the log. Each entry has an index that is used to order it
  relative to other log entries.
  * **Committed** -  A log entry is considered committed if it is safe for that entry to be
    applied to state machines. A log entry is committed once the leader that created the
    entry has replicated it on a majority of the peers. A peer has successfully
    replicated the entry once it is persisted.
  * **Applied** - log entry applied to the state machine (FSM)
* **Term** - raft divides time into terms of arbitrary length. Terms are numbered with
  consecutive integers. Each term begins with an election, in which one or more candidates
  attempt to become leader. If a candidate wins the election, then it serves as leader for
  the rest of the term. If the election ends with a split vote, the term will end with no
  leader.
* **FSM** - finite state machine, stores the cluster state
* **Client** - the application that uses the `hashicorp/raft` library

## Operations

### Leader Write

Most write operations must be performed on the leader.

* RequestConfigChange - update the raft peer list configuration
* Apply - apply a log entry to the log on a majority of peers, and the FSM. See [raft apply](apply.md) for more details.
* Barrier - a special Apply that does not modify the FSM, used to wait for previous logs to be applied
* LeadershipTransfer - stop accepting client requests, and tell a different peer to start a leadership election
* Restore (Snapshot) - overwrite the cluster state with the contents of the snapshot (excluding cluster configuration)
* VerifyLeader - send a heartbeat to all voters to confirm the peer is still the leader

### Follower Write

* BootstrapCluster - store the cluster configuration in the local log store


### Read

Read operations can be performed on a peer in any state.

* AppliedIndex - get the index of the last log entry applied to the FSM
* GetConfiguration - return the latest cluster configuration
* LastContact - get the last time this peer made contact with the leader
* LastIndex - get the index of the latest stored log entry
* Leader - get the address of the peer that is currently the leader
* Snapshot - snapshot the current state of the FSM into a file
* State - return the state of the peer
* Stats - return some stats about the peer and the cluster

## Threads

Raft uses the following threads to handle operations. The name of the thread is in bold,
and a short description of the operation handled by the thread follows. The main thread is
responsible for handling many operations.

* **run** (main thread) - different behaviour based on peer state
   * follower
      * processRPC (from rpcCh)
         * AppendEntries
         * RequestVote
         * InstallSnapshot
         * TimeoutNow
      * liveBootstrap (from bootstrapCh)
      * periodic heartbeatTimer (HeartbeatTimeout)
   * candidate - starts an election for itself when called
      * processRPC (from rpcCh) - same as follower
      * acceptVote (from askPeerForVote)
   * leader - first starts replication to all peers, and applies a Noop log to ensure the new leader has committed up to the commit index
      * processRPC (from rpcCh) - same as follower, however we don’t actually expect to receive any RPCs other than a RequestVote
      * leadershipTransfer (from leadershipTransferCh) - 
      * commit (from commitCh) -
      * verifyLeader (from verifyCh) -
      * user restore snapshot (from userRestoreCh) -
      * changeConfig (from configurationChangeCh) -
      * dispatchLogs (from applyCh) - handle client Raft.Apply requests by persisting logs to disk, and notifying replication goroutines to replicate the new logs
      * checkLease (periodically LeaseTimeout) -
* **runFSM** - has exclusive access to the FSM, all reads and writes must send a message to this thread. Commands:
   * apply logs to the FSM, from the fsmMutateCh, from processLogs, from leaderLoop (leader) or appendEntries RPC (follower/candidate)
   * restore a snapshot to the FSM, from the fsmMutateCh, from restoreUserSnapshot (leader) or installSnapshot RPC (follower/candidate)
   * capture snapshot, from fsmSnapshotCh, from takeSnapshot (runSnapshot thread)
* **runSnapshot** - handles the slower part of taking a snapshot. From a pointer captured by the FSM.Snapshot operation, this thread persists the snapshot by calling FSMSnapshot.Persist. Also calls compactLogs to delete old logs.
   * periodically (SnapshotInterval) takeSnapshot for log compaction
   * user snapshot, from userSnapshotCh, takeSnapshot to return to the user
* **askPeerForVote (candidate only)** - short lived goroutine that synchronously sends a RequestVote RPC to all voting peers, and waits for the response. One goroutine per voting peer.
* **replicate (leader only)** - long running goroutine that synchronously sends log entry AppendEntry RPCs to all peers. Also starts the heartbeat thread, and possibly the pipelineDecode thread. Runs sendLatestSnapshot when AppendEntry fails.
   * **heartbeat (leader only)** - long running goroutine that synchronously sends heartbeat AppendEntry RPCs to all peers.
   * **pipelineDecode (leader only)**
