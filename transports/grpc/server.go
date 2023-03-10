package grpctrans

import (
	"context"
	"io"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	transportv1 "github.com/hashicorp/raft/proto/transport/v1"
)

var _ transportv1.RaftTransportServiceServer = &gRPCRaftService{}

type gRPCRaftService struct {
	rpcCh  chan raft.RPC
	logger hclog.Logger

	heartbeatFn     func(raft.RPC)
	heartbeatFnLock sync.Mutex
}

func (g *gRPCRaftService) AppendEntries(ctx context.Context, req *transportv1.AppendEntriesRequest) (*transportv1.AppendEntriesResponse, error) {
	var raftReq raft.AppendEntriesRequest
	transportv1.AppendEntriesRequestToStruct(req, &raftReq)

	respCh := make(chan raft.RPCResponse, 1)
	rpc := raft.RPC{
		Command:  &raftReq,
		RespChan: respCh,
	}

	fastpathed := false
	if isHeartbeat(req) {
		g.heartbeatFnLock.Lock()
		fn := g.heartbeatFn
		g.heartbeatFnLock.Unlock()

		if fn != nil {
			// fastpath heartbeat processing
			fn(rpc)
			fastpathed = true
		}
	}

	if !fastpathed {
		select {
		case g.rpcCh <- rpc:
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
	}

	select {
	case rpcResp := <-respCh:
		if rpcResp.Error != nil {
			return nil, status.Error(codes.Internal, rpcResp.Error.Error())
		}

		var resp transportv1.AppendEntriesResponse
		transportv1.AppendEntriesResponseFromStruct(rpcResp.Response.(*raft.AppendEntriesResponse), &resp)
		return &resp, nil
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
}

func (g *gRPCRaftService) AppendEntriesPipeline(srv transportv1.RaftTransportService_AppendEntriesPipelineServer) error {
	g.logger.Info("handling an append entries pipeline")
	// This is not as efficient as it could be. To improve this we could
	// queue up the append entries requests in one go routine and wait
	// on their completion and send results back in a second go routine.
	// If sending responses is experiencing extra latency then we would
	// be better pipelining the requests. However the existing net_transport
	// processes commands for a singular connection serially waiting on one
	// RPC to finish before processing more commands. Therefore this should
	// be at least as performant as that transport.
	for {
		req, err := srv.Recv()
		if err != nil {
			g.logger.Info("failed to recv pipelined entries", "error", err)
			return err
		}
		resp, err := g.AppendEntries(srv.Context(), req)
		if err != nil {
			g.logger.Info("failed to append pipelined entries", "error", err)
			return err
		}

		err = srv.Send(resp)
		if err != nil {
			g.logger.Info("failed to send pipelined response")
			return err
		}
	}
}

func (g *gRPCRaftService) RequestVote(ctx context.Context, req *transportv1.RequestVoteRequest) (*transportv1.RequestVoteResponse, error) {
	var raftReq raft.RequestVoteRequest
	transportv1.RequestVoteRequestToStruct(req, &raftReq)

	respCh := make(chan raft.RPCResponse, 1)
	rpc := raft.RPC{
		Command:  &raftReq,
		RespChan: respCh,
	}

	select {
	case g.rpcCh <- rpc:
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}

	select {
	case rpcResp := <-respCh:
		if rpcResp.Error != nil {
			return nil, status.Error(codes.Internal, rpcResp.Error.Error())
		}

		var resp transportv1.RequestVoteResponse
		transportv1.RequestVoteResponseFromStruct(rpcResp.Response.(*raft.RequestVoteResponse), &resp)
		return &resp, nil
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
}

func (g *gRPCRaftService) InstallSnapshot(srv transportv1.RaftTransportService_InstallSnapshotServer) error {
	var raftReq raft.InstallSnapshotRequest

	// receive the initial snapshot metadata
	req, err := srv.Recv()
	if err != nil {
		return err
	}

	meta := req.GetMetadata()
	if meta != nil {
		return status.Error(codes.InvalidArgument, "first message in the InstallSnapshot stream must include snapshot metadata")
	}

	transportv1.InstallSnapshotMetadataToStruct(meta, &raftReq)

	errCh := make(chan error, 1)
	dataCh := make(chan []byte)
	respCh := make(chan raft.RPCResponse)
	rdr := snapshotStreamReader{
		chunks: dataCh,
	}

	go g.processSnapshotChunks(srv, dataCh, errCh, meta.Size)

	rpc := raft.RPC{
		Command:  raftReq,
		Reader:   &rdr,
		RespChan: respCh,
	}

	select {
	case g.rpcCh <- rpc:
	case <-srv.Context().Done():
		return status.FromContextError(srv.Context().Err()).Err()
	}

	select {
	case rpcResp := <-respCh:
		if rpcResp.Error != nil {
			return status.Error(codes.Internal, rpcResp.Error.Error())
		}

		var resp transportv1.InstallSnapshotResponse
		transportv1.InstallSnapshotResponseFromStruct(rpcResp.Response.(*raft.InstallSnapshotResponse), &resp)

		srv.Send(&resp)
		return nil
	case err := <-errCh:
		return err
	case <-srv.Context().Done():
		return status.FromContextError(srv.Context().Err()).Err()
	}
}

func (g *gRPCRaftService) processSnapshotChunks(srv transportv1.RaftTransportService_InstallSnapshotServer, dataCh chan<- []byte, errCh chan<- error, size int64) {
	defer close(dataCh)
	for size > 0 {
		req, err := srv.Recv()
		if err != nil {
			errCh <- err
			return
		}

		chunk := req.GetChunk()
		if chunk == nil {
			errCh <- status.Error(codes.FailedPrecondition, "All InstallSnapshot stream messages after initial metadata must contain snapshot chunk data")
			return
		}

		if int64(len(chunk.SnapshotData)) > size {
			errCh <- status.Error(codes.FailedPrecondition, "More snapshot data sent than snapshot metadata foretold")
			return
		}

		select {
		case dataCh <- chunk.SnapshotData:
			size -= int64(len(chunk.SnapshotData))
		case <-srv.Context().Done():
			return
		}
	}
}

func (g *gRPCRaftService) TimeoutNow(ctx context.Context, req *transportv1.TimeoutNowRequest) (*transportv1.TimeoutNowResponse, error) {
	var raftReq raft.TimeoutNowRequest
	transportv1.TimeoutNowRequestToStruct(req, &raftReq)

	respCh := make(chan raft.RPCResponse, 1)
	rpc := raft.RPC{
		Command:  &raftReq,
		RespChan: respCh,
	}

	select {
	case g.rpcCh <- rpc:
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}

	select {
	case rpcResp := <-respCh:
		if rpcResp.Error != nil {
			return nil, status.Error(codes.Internal, rpcResp.Error.Error())
		}

		var resp transportv1.TimeoutNowResponse
		transportv1.TimeoutNowResponseFromStruct(rpcResp.Response.(*raft.TimeoutNowResponse), &resp)
		return &resp, nil
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
}

func (g *gRPCRaftService) setHeartbeatHandler(cb func(raft.RPC)) {
	g.heartbeatFnLock.Lock()
	g.heartbeatFn = cb
	g.heartbeatFnLock.Unlock()
}

func (g *gRPCRaftService) registerService(srv grpc.ServiceRegistrar) {
	transportv1.RegisterRaftTransportServiceServer(srv, g)
}

func isHeartbeat(req *transportv1.AppendEntriesRequest) bool {
	return req.Term != 0 && req.GetRpcHeader().Addr != nil &&
		req.PreviousLogEntry == 0 && req.PreviousLogTerm == 0 &&
		len(req.Entries) == 0 && req.LeaderCommitIndex == 0
}

type snapshotStreamReader struct {
	streamErr chan<- error
	chunks    <-chan []byte
	extra     []byte
}

func (s *snapshotStreamReader) Read(buf []byte) (int, error) {
	// Check if we need more data from the stream
	if s.extra == nil {
		ok := false
		s.extra, ok = <-s.chunks

		if !ok {
			return 0, io.EOF
		}
	}

	// This may only partially fill the buffer with data available.
	// According to the io.Reader interface that is okay. We could
	// make this more efficient by proactively populating more
	// buffer and by looping to fill the provided buffer. For now
	// this is simple and should work.
	copied := copy(buf, s.extra)

	if copied == len(s.extra) {
		// we have consumed all the buffered data so nil it out
		s.extra = nil
	} else {
		// reslice s.extra to move past the already copied data
		s.extra = s.extra[copied:]
	}

	return copied, nil
}
