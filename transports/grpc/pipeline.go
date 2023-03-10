package grpctrans

import (
	"fmt"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"

	transportv1 "github.com/hashicorp/raft/proto/transport/v1"
)

const (
	// rpcMaxPipeline controls the maximum number of outstanding
	// AppendEntries RPC calls.
	rpcMaxPipeline = 128
)

type gRPCPipeline struct {
	cancel func()

	logger hclog.Logger

	doneCh       chan raft.AppendFuture
	inprogressCh chan *appendFuture

	stream transportv1.RaftTransportService_AppendEntriesPipelineClient
}

func newGRPCPipeline(logger hclog.Logger, stream transportv1.RaftTransportService_AppendEntriesPipelineClient, cancel func()) *gRPCPipeline {
	pipeline := &gRPCPipeline{
		logger: logger,
		stream: stream,

		doneCh:       make(chan raft.AppendFuture, rpcMaxPipeline),
		inprogressCh: make(chan *appendFuture, rpcMaxPipeline),
		cancel:       cancel,
	}

	go pipeline.processResponses()
	return pipeline
}

func (p *gRPCPipeline) processResponses() {
	for {
		select {
		case <-p.stream.Context().Done():
			return
		case future := <-p.inprogressCh:
			resp, err := p.stream.Recv()
			transportv1.AppendEntriesResponseToStruct(resp, future.resp)
			future.Respond(err)

			select {
			case p.doneCh <- future:
			case <-p.stream.Context().Done():
				return
			}
		}
	}
}

func (p *gRPCPipeline) AppendEntries(args *raft.AppendEntriesRequest, out *raft.AppendEntriesResponse) (raft.AppendFuture, error) {
	select {
	case <-p.stream.Context().Done():
		p.logger.Info("AppenedEntries called on a closed pipeline")
		return nil, raft.ErrPipelineShutdown
	default:
	}
	future := &appendFuture{
		start: time.Now(),
		req:   args,
		resp:  out,
	}

	future.Init()

	var req transportv1.AppendEntriesRequest
	transportv1.AppendEntriesRequestFromStruct(args, &req)
	if err := p.stream.Send(&req); err != nil {
		return nil, fmt.Errorf("failed to send the AppendEntries RPC: %w", err)
	}

	select {
	case p.inprogressCh <- future:
		return future, nil
	case <-p.stream.Context().Done():
		p.logger.Info("pipeline cancelled which waiting to queue the future")
		return nil, raft.ErrPipelineShutdown
	}
}

func (p *gRPCPipeline) Consumer() <-chan raft.AppendFuture {
	return p.doneCh
}

func (p *gRPCPipeline) Close() error {
	p.cancel()
	p.logger.Info("closing the grpc pipeline")
	return p.stream.CloseSend()
}
