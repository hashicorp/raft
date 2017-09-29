package fuzzy

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/go-msgpack/codec"
	"github.com/hashicorp/raft"
)

var (
	codecHandle codec.MsgpackHandle
)

type appendEntries struct {
	source      string
	target      raft.ServerAddress
	term        uint64
	firstIndex  uint64
	lastIndex   uint64
	commitIndex uint64
}

type transports struct {
	sync.RWMutex
	nodes map[string]*transport
	log   *log.Logger
}

func newTransports(log *log.Logger) *transports {
	return &transports{
		nodes: make(map[string]*transport),
		log:   log,
	}
}

func (tc *transports) AddNode(n string, hooks TransportHooks) *transport {
	t := newTransport(n, tc, hooks)
	t.log = tc.log
	tc.Lock()
	defer tc.Unlock()
	tc.nodes[n] = t
	return t
}

// TransportHooks allow a test to customize the behavour of the transport.
// [if you return an error from a PreXXX call, then the error is returned to the caller, and the RPC never made]
type TransportHooks interface {
	// PreRPC is called before every single RPC call from the transport
	PreRPC(src, target string, r *raft.RPC) error
	// PostRPC is called after the RPC call has been processed by the target, but before the source see's the response
	PostRPC(src, target string, r *raft.RPC, result *raft.RPCResponse) error
	// PreREquestVote is called before sending a RequestVote RPC request.
	PreRequestVote(src, target string, r *raft.RequestVoteRequest) (*raft.RequestVoteResponse, error)
	// PreAppendEntries is called before sending an AppendEntries RPC request.
	PreAppendEntries(src, target string, r *raft.AppendEntriesRequest) (*raft.AppendEntriesResponse, error)
}

type transport struct {
	log        *log.Logger
	transports *transports
	node       string
	ae         []appendEntries

	consumer chan raft.RPC
	hooks    TransportHooks
}

func newTransport(node string, tc *transports, hooks TransportHooks) *transport {
	return &transport{
		node:       node,
		transports: tc,
		hooks:      hooks,
		consumer:   make(chan raft.RPC),
		ae:         make([]appendEntries, 0, 50000),
	}
}

// Consumer returns a channel that can be used to
// consume and respond to RPC requests.
func (t *transport) Consumer() <-chan raft.RPC {
	return t.consumer
}

// LocalAddr is used to return our local address to distinguish from our peers.
func (t *transport) LocalAddr() raft.ServerAddress {
	return raft.ServerAddress(t.node)
}

func (t *transport) sendRPC(target string, req interface{}, resp interface{}) error {
	t.transports.RLock()
	tt := t.transports.nodes[target]
	if tt == nil {
		t.log.Printf("sendRPC target=%v but unknown node (transports=%v)", target, t.transports.nodes)
		t.transports.RUnlock()
		return fmt.Errorf("Unknown target host %v", target)
	}
	t.transports.RUnlock()
	rc := make(chan raft.RPCResponse, 1)

	buff := bytes.Buffer{}
	if err := codec.NewEncoder(&buff, &codecHandle).Encode(req); err != nil {
		return err
	}
	rpc := raft.RPC{RespChan: rc}
	var reqVote raft.RequestVoteRequest
	var appEnt raft.AppendEntriesRequest
	dec := codec.NewDecoderBytes(buff.Bytes(), &codecHandle)
	switch req.(type) {
	case *raft.RequestVoteRequest:
		if err := dec.Decode(&reqVote); err != nil {
			return err
		}
		rpc.Command = &reqVote
	case *raft.AppendEntriesRequest:
		if err := dec.Decode(&appEnt); err != nil {
			return err
		}
		rpc.Command = &appEnt
	default:
		t.log.Printf("Unexpected request type %T %+v", req, req)
	}
	var result *raft.RPCResponse
	if t.hooks != nil {
		if err := t.hooks.PreRPC(t.node, target, &rpc); err != nil {
			return err
		}
		switch req.(type) {
		case *raft.RequestVoteRequest:
			hr, err := t.hooks.PreRequestVote(t.node, target, &reqVote)
			if hr != nil || err != nil {
				result = &raft.RPCResponse{Response: hr, Error: err}
			}
		case *raft.AppendEntriesRequest:
			hr, err := t.hooks.PreAppendEntries(t.node, target, &appEnt)
			if hr != nil || err != nil {
				result = &raft.RPCResponse{Response: hr, Error: err}
			}
		}
	}
	//t.log.Printf("sendRPC %v -> %v : %+v", t.node, target, rpc.Command)
	if result == nil {
		tt.consumer <- rpc
		cr := <-rc
		result = &cr
	}

	if t.hooks != nil {
		err := t.hooks.PostRPC(t.node, target, &rpc, result)
		if err != nil {
			result.Error = err
		}
	}
	//t.log.Printf("sendRPC %v <- %v: %+v %v", t.node, target, result.Response, result.Error)
	buff = bytes.Buffer{}
	codec.NewEncoder(&buff, &codecHandle).Encode(result.Response)
	codec.NewDecoderBytes(buff.Bytes(), &codecHandle).Decode(resp)
	return result.Error
}

// AppendEntries sends the appropriate RPC to the target node.
func (t *transport) AppendEntries(id raft.ServerID, target raft.ServerAddress, args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) error {
	ae := appendEntries{
		source:      t.node,
		target:      target,
		firstIndex:  firstIndex(args),
		lastIndex:   lastIndex(args),
		commitIndex: args.LeaderCommitIndex,
	}
	if len(t.ae) < cap(t.ae) {
		t.ae = append(t.ae, ae)
	}
	return t.sendRPC(string(target), args, resp)
}

func (t *transport) DumpLog(dir string) {
	fw, _ := os.Create(filepath.Join(dir, t.node+".transport"))
	w := bufio.NewWriter(fw)
	for i := range t.ae {
		e := &t.ae[i]
		fmt.Fprintf(w, "%v -> %v\t%8d - %8d : %8d\n", e.source, e.target, e.firstIndex, e.lastIndex, e.commitIndex)
	}
	w.Flush()
	fw.Close()
}

func firstIndex(a *raft.AppendEntriesRequest) uint64 {
	if len(a.Entries) == 0 {
		return 0
	}
	return a.Entries[0].Index
}

func lastIndex(a *raft.AppendEntriesRequest) uint64 {
	if len(a.Entries) == 0 {
		return 0
	}
	return a.Entries[len(a.Entries)-1].Index
}

// RequestVote sends the appropriate RPC to the target node.
func (t *transport) RequestVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestVoteRequest, resp *raft.RequestVoteResponse) error {
	return t.sendRPC(string(target), args, resp)
}

// InstallSnapshot is used to push a snapshot down to a follower. The data is read from
// the ReadCloser and streamed to the client.
func (t *transport) InstallSnapshot(id raft.ServerID, target raft.ServerAddress, args *raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse, data io.Reader) error {
	t.log.Printf("INSTALL SNAPSHOT *************************************")
	return errors.New("huh")
}

// EncodePeer is used to serialize a peer name.
func (t *transport) EncodePeer(id raft.ServerID, p raft.ServerAddress) []byte {
	return []byte(p)
}

// DecodePeer is used to deserialize a peer name.
func (t *transport) DecodePeer(p []byte) raft.ServerAddress {
	return raft.ServerAddress(p)
}

// SetHeartbeatHandler is used to setup a heartbeat handler
// as a fast-pass. This is to avoid head-of-line blocking from
// disk IO. If a Transport does not support this, it can simply
// ignore the call, and push the heartbeat onto the Consumer channel.
func (t *transport) SetHeartbeatHandler(cb func(rpc raft.RPC)) {
}

// AppendEntriesPipeline returns an interface that can be used to pipeline
// AppendEntries requests.
func (t *transport) AppendEntriesPipeline(id raft.ServerID, target raft.ServerAddress) (raft.AppendPipeline, error) {
	p := &pipeline{
		t:        t,
		id:       id,
		target:   target,
		work:     make(chan *appendEntry, 100),
		consumer: make(chan raft.AppendFuture, 100),
	}
	go p.run()
	return p, nil
}

type appendEntry struct {
	req      *raft.AppendEntriesRequest
	res      *raft.AppendEntriesResponse
	start    time.Time
	err      error
	ready    chan error
	consumer chan raft.AppendFuture
}

func (e *appendEntry) Request() *raft.AppendEntriesRequest {
	return e.req
}

func (e *appendEntry) Response() *raft.AppendEntriesResponse {
	<-e.ready
	return e.res
}

func (e *appendEntry) Start() time.Time {
	return e.start
}

func (e *appendEntry) Error() error {
	<-e.ready
	return e.err
}

func (e *appendEntry) Respond(err error) {
	e.err = err
	close(e.ready)
	e.consumer <- e
}

type pipeline struct {
	t        *transport
	target   raft.ServerAddress
	id       raft.ServerID
	work     chan *appendEntry
	consumer chan raft.AppendFuture
}

func (p *pipeline) run() {
	for ap := range p.work {
		err := p.t.AppendEntries(p.id, p.target, ap.req, ap.res)
		ap.Respond(err)
	}
}

// AppendEntries is used to add another request to the pipeline.
// The send may block which is an effective form of back-pressure.
func (p *pipeline) AppendEntries(args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) (raft.AppendFuture, error) {
	e := &appendEntry{
		req:      args,
		res:      resp,
		start:    time.Now(),
		ready:    make(chan error),
		consumer: p.consumer,
	}
	p.work <- e
	return e, nil
}

func (p *pipeline) Consumer() <-chan raft.AppendFuture {
	return p.consumer
}

// Closes pipeline and cancels all inflight RPCs
func (p *pipeline) Close() error {
	close(p.work)
	return nil
}
