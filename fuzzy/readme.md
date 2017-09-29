# Fuzzy Raft

Inspired by http://colin-scott.github.io/blog/2015/10/07/fuzzing-raft-for-fun-and-profit/ this package 
is a framework and set of test scenarios for testing the behavoir and correctness of the raft library
under various conditions.

## Framework

The framework allows you to construct multiple node raft clusters, connected by an instrumented transport 
that allows a test to inject various transport level behavours to simulate various scenarios (e.g. you 
can have your hook fail all transport calls to a particular node to simulate it being partitioned off 
the network). There are helper classes to create and Apply well know sequences of test data, and to 
examine the final state of the cluster, the nodes FSMs and the raft log. 

## Running

The tests run with the standard go test framework, run with go test . [from this dir] or use make fuzz from
the parent directory. As these tests are looking for timing and other edge cases, a pass from a single run
itsn't enough, the tests needs running repeatedly to build up confidence.

## Test Scenarios

The follow test scenario's are currently implemented. Each test concludes with a standard set of validations

 * Each node raft log contains the same set of entries (term/index/data).
 * The raft log contains data matching the client request for each call to raft.Apply() that reported success.
 * Each node's FSM saw the same sequence of Apply(*raft.Log) calls.
 * A verifier at the transport level verifies a number of transport level invariants.

Most tests run with a background workload that is constantly apply()ing new entries to the log. [when there's a leader]

### TestRaft_LeaderPartitions

This creates a 5 node cluster and then repeated partitions multiple nodes off (including the current leader), 
then heals the partition and repeats. At the end all partitions are removed. [clearly inspired by Jepson]

### TestRaft_NoIssueSanity

Is a basic 5 node cluster test, it starts a 5 node cluster applies some data, then does the verifications

### TestRaft_SlowSendVote

Tests what happens when RequestVote requests are delaying being sent to other nodes

### TestRaft_SlowRecvVote

Tests what happens when RequestVote responses are delaying being received by the sender.

### TestRaft_AddMembership

Starts a 3 node cluster, and then adds 2 new members to the cluster.

### TestRaft_AddRemoveNodesNotLeader

Starts a 5 node cluster, and then then removes 2 follower nodes from the cluster.

### TestRaft_RemoveLeader

Starts a 5 node cluster, and then removes the node that is the leader.

### TestRaft_RemovePartitionedNode

Starts a 5 node cluster, partitions one of the follower nodes off the network, and then tells the leader to remove that node, then heals the partition.
