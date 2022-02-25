# Raft Apply

Apply is the primary operation provided by raft.


This sequence diagram shows the steps involved in a `raft.Apply` operation. Each box
across the top is a separate thread. The name in the box identifies the state of the peer
(leader or follower) and the thread (`<peer state>:<thread name>`). When there are
multiple copies of the thread, it is indicated with `(each peer)`.

```mermaid
sequenceDiagram
   autonumber
 
   participant client
   participant leadermain as leader:main
   participant leaderfsm as leader:fsm
   participant leaderreplicate as leader:replicate (each peer)
   participant followermain as follower:main (each peer)
   participant followerfsm as follower:fsm (each peer)
 
   client-)leadermain: applyCh to dispatchLogs
   leadermain->>leadermain: store logs to disk
 
   leadermain-)leaderreplicate: triggerCh
   leaderreplicate-->>followermain: AppendEntries RPC
 
   followermain->>followermain: store logs to disk
 
   opt leader commit index is ahead of peer commit index
       followermain-)followerfsm: fsmMutateCh <br>apply committed logs
       followerfsm->>followerfsm: fsm.Apply
   end
 
   followermain-->>leaderreplicate: respond success=true
   leaderreplicate->>leaderreplicate: update commitment
 
   opt quorum commit index has increased
       leaderreplicate-)leadermain: commitCh
       leadermain-)leaderfsm: fsmMutateCh
       leaderfsm->>leaderfsm: fsm.Apply
       leaderfsm-)client: future.respond
   end

```
