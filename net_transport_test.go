package raft

import (
	"bytes"
	"fmt"
	"net"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/require"
)

type testAddrProvider struct {
	addr string
}

func (t *testAddrProvider) ServerAddr(id ServerID) (ServerAddress, error) {
	return ServerAddress(t.addr), nil
}

func TestNetworkTransport_CloseStreams(t *testing.T) {
	// Transport 1 is consumer
	trans1, err := NewTCPTransportWithLogger("localhost:0", nil, 2, time.Second, newTestLogger(t))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer trans1.Close()
	rpcCh := trans1.Consumer()

	// Make the RPC request
	args := AppendEntriesRequest{
		Term:         10,
		PrevLogEntry: 100,
		PrevLogTerm:  4,
		Entries: []*Log{
			{
				Index: 101,
				Term:  4,
				Type:  LogNoop,
			},
		},
		LeaderCommitIndex: 90,
		RPCHeader:         RPCHeader{Addr: []byte("cartman")},
	}

	resp := AppendEntriesResponse{
		Term:    4,
		LastLog: 90,
		Success: true,
	}

	// Listen for a request
	go func() {
		for {
			select {
			case rpc := <-rpcCh:
				// Verify the command
				req := rpc.Command.(*AppendEntriesRequest)
				if !reflect.DeepEqual(req, &args) {
					t.Errorf("command mismatch: %#v %#v", *req, args)
					return
				}
				rpc.Respond(&resp, nil)

			case <-time.After(200 * time.Millisecond):
				return
			}
		}
	}()

	// Transport 2 makes outbound request, 3 conn pool
	trans2, err := NewTCPTransportWithLogger("localhost:0", nil, 3, time.Second, newTestLogger(t))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer trans2.Close()
	var i int
	for i = 0; i < 2; i++ {
		// Create wait group
		wg := &sync.WaitGroup{}
		wg.Add(5)

		appendFunc := func() {
			defer wg.Done()
			var out AppendEntriesResponse
			if err := trans2.AppendEntries("id1", trans1.LocalAddr(), &args, &out); err != nil {
				t.Fatalf("err: %v", err)
			}

			// Verify the response
			if !reflect.DeepEqual(resp, out) {
				t.Fatalf("command mismatch: %#v %#v", resp, out)
			}
		}

		// Try to do parallel appends, should stress the conn pool
		for i = 0; i < 5; i++ {
			go appendFunc()
		}

		// Wait for the routines to finish
		wg.Wait()

		// Check the conn pool size
		addr := trans1.LocalAddr()
		if len(trans2.connPool[addr]) != 3 {
			t.Fatalf("Expected 3 pooled conns!")
		}

		if i == 0 {
			trans2.CloseStreams()
			if len(trans2.connPool[addr]) != 0 {
				t.Fatalf("Expected no pooled conns after closing streams!")
			}
		}
	}
}

func TestNetworkTransport_StartStop(t *testing.T) {
	trans, err := NewTCPTransportWithLogger("localhost:0", nil, 2, time.Second, newTestLogger(t))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	trans.Close()
}

func TestNetworkTransport_Heartbeat_FastPath(t *testing.T) {
	// Transport 1 is consumer
	trans1, err := NewTCPTransportWithLogger("localhost:0", nil, 2, time.Second, newTestLogger(t))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer trans1.Close()

	// Make the RPC request
	args := AppendEntriesRequest{
		Term:      10,
		RPCHeader: RPCHeader{ProtocolVersion: ProtocolVersionMax, Addr: []byte("cartman")},
		Leader:    []byte("cartman"),
	}

	resp := AppendEntriesResponse{
		Term:    4,
		LastLog: 90,
		Success: true,
	}

	invoked := false
	fastpath := func(rpc RPC) {
		// Verify the command
		req := rpc.Command.(*AppendEntriesRequest)
		if !reflect.DeepEqual(req, &args) {
			t.Fatalf("command mismatch: %#v %#v", *req, args)
		}

		rpc.Respond(&resp, nil)
		invoked = true
	}
	trans1.SetHeartbeatHandler(fastpath)

	// Transport 2 makes outbound request
	trans2, err := NewTCPTransportWithLogger("localhost:0", nil, 2, time.Second, newTestLogger(t))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer trans2.Close()

	var out AppendEntriesResponse
	if err := trans2.AppendEntries("id1", trans1.LocalAddr(), &args, &out); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Verify the response
	if !reflect.DeepEqual(resp, out) {
		t.Fatalf("command mismatch: %#v %#v", resp, out)
	}

	// Ensure fast-path is used
	if !invoked {
		t.Fatalf("fast-path not used")
	}
}

func TestNetworkTransport_AppendEntries(t *testing.T) {

	for _, useAddrProvider := range []bool{true, false} {
		// Transport 1 is consumer
		trans1, err := makeTransport(t, useAddrProvider, "localhost:0")
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		defer trans1.Close()
		rpcCh := trans1.Consumer()

		// Make the RPC request
		args := AppendEntriesRequest{
			Term:         10,
			PrevLogEntry: 100,
			PrevLogTerm:  4,
			Entries: []*Log{
				{
					Index: 101,
					Term:  4,
					Type:  LogNoop,
				},
			},
			LeaderCommitIndex: 90,
			RPCHeader:         RPCHeader{Addr: []byte("cartman")},
		}

		resp := AppendEntriesResponse{
			Term:    4,
			LastLog: 90,
			Success: true,
		}

		// Listen for a request
		go func() {
			select {
			case rpc := <-rpcCh:
				// Verify the command
				req := rpc.Command.(*AppendEntriesRequest)
				if !reflect.DeepEqual(req, &args) {
					t.Errorf("command mismatch: %#v %#v", *req, args)
					return
				}

				rpc.Respond(&resp, nil)

			case <-time.After(200 * time.Millisecond):
				t.Errorf("timeout")
			}
		}()

		// Transport 2 makes outbound request
		trans2, err := makeTransport(t, useAddrProvider, string(trans1.LocalAddr()))
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		defer trans2.Close()

		var out AppendEntriesResponse
		if err := trans2.AppendEntries("id1", trans1.LocalAddr(), &args, &out); err != nil {
			t.Fatalf("err: %v", err)
		}

		// Verify the response
		if !reflect.DeepEqual(resp, out) {
			t.Fatalf("command mismatch: %#v %#v", resp, out)
		}

	}
}

func TestNetworkTransport_AppendEntriesPipeline(t *testing.T) {

	for _, useAddrProvider := range []bool{true, false} {
		// Transport 1 is consumer
		trans1, err := makeTransport(t, useAddrProvider, "localhost:0")
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		defer trans1.Close()
		rpcCh := trans1.Consumer()

		// Make the RPC request
		args := AppendEntriesRequest{
			Term:         10,
			PrevLogEntry: 100,
			PrevLogTerm:  4,
			Entries: []*Log{
				{
					Index: 101,
					Term:  4,
					Type:  LogNoop,
				},
			},
			LeaderCommitIndex: 90,
			RPCHeader:         RPCHeader{Addr: []byte("cartman")},
		}

		resp := AppendEntriesResponse{
			Term:    4,
			LastLog: 90,
			Success: true,
		}

		// Listen for a request
		go func() {
			for i := 0; i < 10; i++ {
				select {
				case rpc := <-rpcCh:
					// Verify the command
					req := rpc.Command.(*AppendEntriesRequest)
					if !reflect.DeepEqual(req, &args) {
						t.Errorf("command mismatch: %#v %#v", *req, args)
						return
					}
					rpc.Respond(&resp, nil)

				case <-time.After(200 * time.Millisecond):
					t.Errorf("timeout")
					return
				}
			}
		}()

		// Transport 2 makes outbound request
		trans2, err := makeTransport(t, useAddrProvider, string(trans1.LocalAddr()))
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		defer trans2.Close()
		pipeline, err := trans2.AppendEntriesPipeline("id1", trans1.LocalAddr())
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		for i := 0; i < 10; i++ {
			out := new(AppendEntriesResponse)
			if _, err := pipeline.AppendEntries(&args, out); err != nil {
				t.Fatalf("err: %v", err)
			}
		}

		respCh := pipeline.Consumer()
		for i := 0; i < 10; i++ {
			select {
			case ready := <-respCh:
				// Verify the response
				if !reflect.DeepEqual(&resp, ready.Response()) {
					t.Fatalf("command mismatch: %#v %#v", &resp, ready.Response())
				}
			case <-time.After(200 * time.Millisecond):
				t.Fatalf("timeout")
			}
		}
		pipeline.Close()

	}
}

func TestNetworkTransport_AppendEntriesPipeline_CloseStreams(t *testing.T) {
	// Transport 1 is consumer
	trans1, err := makeTransport(t, true, "localhost:0")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer trans1.Close()
	rpcCh := trans1.Consumer()

	// Make the RPC request
	args := AppendEntriesRequest{
		Term:         10,
		PrevLogEntry: 100,
		PrevLogTerm:  4,
		Entries: []*Log{
			{
				Index: 101,
				Term:  4,
				Type:  LogNoop,
			},
		},
		LeaderCommitIndex: 90,
		RPCHeader:         RPCHeader{Addr: []byte("cartman")},
	}

	resp := AppendEntriesResponse{
		Term:    4,
		LastLog: 90,
		Success: true,
	}

	shutdownCh := make(chan struct{})
	defer close(shutdownCh)

	// Listen for a request
	go func() {
		for {
			select {
			case rpc := <-rpcCh:
				// Verify the command
				req := rpc.Command.(*AppendEntriesRequest)
				if !reflect.DeepEqual(req, &args) {
					t.Errorf("command mismatch: %#v %#v", *req, args)
					return
				}
				rpc.Respond(&resp, nil)

			case <-shutdownCh:
				return
			}
		}
	}()

	// Transport 2 makes outbound request
	trans2, err := makeTransport(t, true, string(trans1.LocalAddr()))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer trans2.Close()

	for _, cancelStreams := range []bool{true, false} {
		pipeline, err := trans2.AppendEntriesPipeline("id1", trans1.LocalAddr())
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		for i := 0; i < 100; i++ {
			// On the last one, close the streams on the transport one.
			if cancelStreams && i == 10 {
				trans1.CloseStreams()
				time.Sleep(10 * time.Millisecond)
			}

			out := new(AppendEntriesResponse)
			if _, err := pipeline.AppendEntries(&args, out); err != nil {
				break
			}
		}

		var futureErr error
		respCh := pipeline.Consumer()
	OUTER:
		for i := 0; i < 100; i++ {
			select {
			case ready := <-respCh:
				if err := ready.Error(); err != nil {
					futureErr = err
					break OUTER
				}

				// Verify the response
				if !reflect.DeepEqual(&resp, ready.Response()) {
					t.Fatalf("command mismatch: %#v %#v %v", &resp, ready.Response(), ready.Error())
				}
			case <-time.After(200 * time.Millisecond):
				t.Fatalf("timeout when cancel streams is %v", cancelStreams)
			}
		}

		if cancelStreams && futureErr == nil {
			t.Fatalf("expected an error due to the streams being closed")
		} else if !cancelStreams && futureErr != nil {
			t.Fatalf("unexpected error: %v", futureErr)
		}

		pipeline.Close()
	}
}

func TestNetworkTransport_RequestVote(t *testing.T) {

	for _, useAddrProvider := range []bool{true, false} {
		// Transport 1 is consumer
		trans1, err := makeTransport(t, useAddrProvider, "localhost:0")
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		defer trans1.Close()
		rpcCh := trans1.Consumer()

		// Make the RPC request
		args := RequestVoteRequest{
			Term:         20,
			LastLogIndex: 100,
			LastLogTerm:  19,
			RPCHeader:    RPCHeader{Addr: []byte("butters")},
		}

		resp := RequestVoteResponse{
			Term:    100,
			Granted: false,
		}

		// Listen for a request
		go func() {
			select {
			case rpc := <-rpcCh:
				// Verify the command
				req := rpc.Command.(*RequestVoteRequest)
				if !reflect.DeepEqual(req, &args) {
					t.Errorf("command mismatch: %#v %#v", *req, args)
					return
				}

				rpc.Respond(&resp, nil)

			case <-time.After(200 * time.Millisecond):
				t.Errorf("timeout")
				return
			}
		}()

		// Transport 2 makes outbound request
		trans2, err := makeTransport(t, useAddrProvider, string(trans1.LocalAddr()))
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		defer trans2.Close()
		var out RequestVoteResponse
		if err := trans2.RequestVote("id1", trans1.LocalAddr(), &args, &out); err != nil {
			t.Fatalf("err: %v", err)
		}

		// Verify the response
		if !reflect.DeepEqual(resp, out) {
			t.Fatalf("command mismatch: %#v %#v", resp, out)
		}

	}
}

func TestNetworkTransport_InstallSnapshot(t *testing.T) {

	for _, useAddrProvider := range []bool{true, false} {
		// Transport 1 is consumer
		trans1, err := makeTransport(t, useAddrProvider, "localhost:0")
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		defer trans1.Close()
		rpcCh := trans1.Consumer()

		// Make the RPC request
		args := InstallSnapshotRequest{
			Term:         10,
			LastLogIndex: 100,
			LastLogTerm:  9,
			Peers:        []byte("blah blah"),
			Size:         10,
			RPCHeader:    RPCHeader{Addr: []byte("kyle")},
		}

		resp := InstallSnapshotResponse{
			Term:    10,
			Success: true,
		}

		// Listen for a request
		go func() {
			select {
			case rpc := <-rpcCh:
				// Verify the command
				req := rpc.Command.(*InstallSnapshotRequest)
				if !reflect.DeepEqual(req, &args) {
					t.Errorf("command mismatch: %#v %#v", *req, args)
					return
				}

				// Try to read the bytes
				buf := make([]byte, 10)
				rpc.Reader.Read(buf)

				// Compare
				if bytes.Compare(buf, []byte("0123456789")) != 0 {
					t.Errorf("bad buf %v", buf)
					return
				}

				rpc.Respond(&resp, nil)

			case <-time.After(200 * time.Millisecond):
				t.Errorf("timeout")
			}
		}()

		// Transport 2 makes outbound request
		trans2, err := makeTransport(t, useAddrProvider, string(trans1.LocalAddr()))
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		defer trans2.Close()
		// Create a buffer
		buf := bytes.NewBuffer([]byte("0123456789"))

		var out InstallSnapshotResponse
		if err := trans2.InstallSnapshot("id1", trans1.LocalAddr(), &args, &out, buf); err != nil {
			t.Fatalf("err: %v", err)
		}

		// Verify the response
		if !reflect.DeepEqual(resp, out) {
			t.Fatalf("command mismatch: %#v %#v", resp, out)
		}

	}
}

func TestNetworkTransport_EncodeDecode(t *testing.T) {
	// Transport 1 is consumer
	trans1, err := NewTCPTransportWithLogger("localhost:0", nil, 2, time.Second, newTestLogger(t))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer trans1.Close()

	local := trans1.LocalAddr()
	enc := trans1.EncodePeer("id1", local)
	dec := trans1.DecodePeer(enc)

	if dec != local {
		t.Fatalf("enc/dec fail: %v %v", dec, local)
	}
}

func TestNetworkTransport_EncodeDecode_AddressProvider(t *testing.T) {
	addressOverride := "localhost:11111"
	config := &NetworkTransportConfig{MaxPool: 2, Timeout: time.Second, Logger: newTestLogger(t), ServerAddressProvider: &testAddrProvider{addressOverride}}
	trans1, err := NewTCPTransportWithConfig("localhost:0", nil, config)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer trans1.Close()

	local := trans1.LocalAddr()
	enc := trans1.EncodePeer("id1", local)
	dec := trans1.DecodePeer(enc)

	if dec != ServerAddress(addressOverride) {
		t.Fatalf("enc/dec fail: %v %v", dec, addressOverride)
	}
}

func TestNetworkTransport_PooledConn(t *testing.T) {
	// Transport 1 is consumer
	trans1, err := NewTCPTransportWithLogger("localhost:0", nil, 2, time.Second, newTestLogger(t))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer trans1.Close()
	rpcCh := trans1.Consumer()

	// Make the RPC request
	args := AppendEntriesRequest{
		Term:         10,
		PrevLogEntry: 100,
		PrevLogTerm:  4,
		Entries: []*Log{
			{
				Index: 101,
				Term:  4,
				Type:  LogNoop,
			},
		},
		LeaderCommitIndex: 90,
		RPCHeader:         RPCHeader{Addr: []byte("cartman")},
	}

	resp := AppendEntriesResponse{
		Term:    4,
		LastLog: 90,
		Success: true,
	}

	// Listen for a request
	go func() {
		for {
			select {
			case rpc := <-rpcCh:
				// Verify the command
				req := rpc.Command.(*AppendEntriesRequest)
				if !reflect.DeepEqual(req, &args) {
					t.Errorf("command mismatch: %#v %#v", *req, args)
					return
				}
				rpc.Respond(&resp, nil)

			case <-time.After(200 * time.Millisecond):
				return
			}
		}
	}()

	// Transport 2 makes outbound request, 3 conn pool
	trans2, err := NewTCPTransportWithLogger("localhost:0", nil, 3, time.Second, newTestLogger(t))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer trans2.Close()

	// Create wait group
	wg := &sync.WaitGroup{}
	wg.Add(5)

	appendFunc := func() {
		defer wg.Done()
		var out AppendEntriesResponse
		if err := trans2.AppendEntries("id1", trans1.LocalAddr(), &args, &out); err != nil {
			t.Fatalf("err: %v", err)
		}

		// Verify the response
		if !reflect.DeepEqual(resp, out) {
			t.Fatalf("command mismatch: %#v %#v", resp, out)
		}
	}

	// Try to do parallel appends, should stress the conn pool
	for i := 0; i < 5; i++ {
		go appendFunc()
	}

	// Wait for the routines to finish
	wg.Wait()

	// Check the conn pool size
	addr := trans1.LocalAddr()
	if len(trans2.connPool[addr]) != 3 {
		t.Fatalf("Expected 3 pooled conns!")
	}
}

func makeTransport(t *testing.T, useAddrProvider bool, addressOverride string) (*NetworkTransport, error) {
	if useAddrProvider {
		config := &NetworkTransportConfig{MaxPool: 2, Timeout: time.Second, Logger: newTestLogger(t), ServerAddressProvider: &testAddrProvider{addressOverride}}
		return NewTCPTransportWithConfig("localhost:0", nil, config)
	}
	return NewTCPTransportWithLogger("localhost:0", nil, 2, time.Second, newTestLogger(t))
}

type testCountingWriter struct {
	t        *testing.T
	numCalls *int32
}

func (tw testCountingWriter) Write(p []byte) (n int, err error) {
	atomic.AddInt32(tw.numCalls, 1)
	if !strings.Contains(string(p), "failed to accept connection") {
		tw.t.Error("did not receive expected log message")
	}
	tw.t.Log("countingWriter:", string(p))
	return len(p), nil
}

type testCountingStreamLayer struct {
	numCalls *int32
}

func (sl testCountingStreamLayer) Accept() (net.Conn, error) {
	*sl.numCalls++
	return nil, fmt.Errorf("intentional error in test")
}

func (sl testCountingStreamLayer) Close() error {
	return nil
}

func (sl testCountingStreamLayer) Addr() net.Addr {
	panic("not needed")
}

func (sl testCountingStreamLayer) Dial(address ServerAddress, timeout time.Duration) (net.Conn, error) {
	return nil, fmt.Errorf("not needed")
}

// TestNetworkTransport_ListenBackoff tests that Accept() errors in NetworkTransport#listen()
// do not result in a tight loop and spam the log. We verify this here by counting the number
// of calls against Accept() and the logger
func TestNetworkTransport_ListenBackoff(t *testing.T) {

	// testTime is the amount of time we will allow NetworkTransport#listen() to run
	// This needs to be long enough that to verify that maxDelay is in force,
	// but not so long as to be obnoxious when running the test suite.
	const testTime = 4 * time.Second

	var numAccepts int32
	var numLogs int32
	countingWriter := testCountingWriter{t, &numLogs}
	countingLogger := hclog.New(&hclog.LoggerOptions{
		Name:   "test",
		Output: countingWriter,
		Level:  hclog.DefaultLevel,
	})
	transport := NetworkTransport{
		logger:     countingLogger,
		stream:     testCountingStreamLayer{&numAccepts},
		shutdownCh: make(chan struct{}),
	}

	go transport.listen()

	// sleep (+yield) for testTime seconds before asking the accept loop to shut down
	time.Sleep(testTime)
	transport.Close()

	// Verify that the method exited (but without block this test)
	// maxDelay == 1s, so we will give the routine 1.25s to loop around and shut down.
	select {
	case <-transport.shutdownCh:
	case <-time.After(1250 * time.Millisecond):
		t.Error("timed out waiting for NetworkTransport to shut down")
	}
	require.True(t, transport.shutdown)

	// In testTime==4s, we expect to loop approximately 12 times
	// with the following delays (in ms):
	//   0+5+10+20+40+80+160+320+640+1000+1000+1000 == 4275 ms
	// Too few calls suggests that the minDelay is not in force; too many calls suggests that the
	// maxDelay is not in force or that the back-off isn't working at all.
	// We'll leave a little flex; the important thing here is the asymptotic behavior.
	// If the minDelay or maxDelay in NetworkTransport#listen() are modified, this test may fail
	// and need to be adjusted.
	require.True(t, numAccepts > 10)
	require.True(t, numAccepts < 13)
	require.True(t, numLogs > 10)
	require.True(t, numLogs < 13)
}
