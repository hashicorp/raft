package grpctrans

import (
	"context"
	"fmt"
	"io"

	"github.com/hashicorp/go-hclog"
	transportv1 "github.com/hashicorp/raft/proto/transport/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/hashicorp/raft"
)

var _ raft.Transport = &GRPCTransport{}

const (
	// snapshotBufferSize is the size of the snapshot chunks that will be
	// sent when making InstallSnapshot RPCs
	snapshotChunkSize = 256 * 1024 // 256KB
)

type GRPCTransport struct {
	logger hclog.Logger

	serverAddressProvider raft.ServerAddressProvider

	consumeCh chan raft.RPC

	service *gRPCRaftService

	localAddr raft.ServerAddress
}

type GRPCTransportConfig struct {
	// ServerAddressProvider is used to override the target address when establishing a connection to invoke an RPC
	ServerAddressProvider raft.ServerAddressProvider

	Logger hclog.Logger

	LocalAddr raft.ServerAddress
}

func New(conf GRPCTransportConfig) *GRPCTransport {
	rpcCh := make(chan raft.RPC)
	return &GRPCTransport{
		logger:                conf.Logger,
		localAddr:             conf.LocalAddr,
		serverAddressProvider: conf.ServerAddressProvider,
		consumeCh:             rpcCh,
		service:               &gRPCRaftService{rpcCh: rpcCh, logger: conf.Logger},
	}
}

func (g *GRPCTransport) Close() error {
	return nil
}

func (g *GRPCTransport) Consumer() <-chan raft.RPC {
	return g.consumeCh
}

func (g *GRPCTransport) LocalAddr() raft.ServerAddress {
	return g.localAddr
}

func (g *GRPCTransport) getClient(ctx context.Context, target raft.ServerAddress) (transportv1.RaftTransportServiceClient, error) {
	conn, err := grpc.DialContext(ctx, string(target), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	return transportv1.NewRaftTransportServiceClient(conn), nil
}

func (g *GRPCTransport) AppendEntriesPipeline(id raft.ServerID, target raft.ServerAddress) (raft.AppendPipeline, error) {
	ctx, cancel := context.WithCancel(context.Background())

	client, err := g.getClient(ctx, target)
	if err != nil {
		cancel()
		return nil, err
	}

	stream, err := client.AppendEntriesPipeline(ctx)
	if err != nil {
		cancel()
		return nil, err
	}

	return newGRPCPipeline(g.logger.With("id", id, "address", target), stream, cancel), nil
}

func (g *GRPCTransport) AppendEntries(id raft.ServerID, target raft.ServerAddress, args *raft.AppendEntriesRequest, out *raft.AppendEntriesResponse) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var req transportv1.AppendEntriesRequest
	transportv1.AppendEntriesRequestFromStruct(args, &req)

	client, err := g.getClient(ctx, target)
	if err != nil {
		return err
	}

	resp, err := client.AppendEntries(ctx, &req)
	if err != nil {
		return err
	}
	transportv1.AppendEntriesResponseToStruct(resp, out)
	return nil
}

func (g *GRPCTransport) RequestVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestVoteRequest, out *raft.RequestVoteResponse) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var req transportv1.RequestVoteRequest
	transportv1.RequestVoteRequestFromStruct(args, &req)

	client, err := g.getClient(ctx, target)
	if err != nil {
		return err
	}

	resp, err := client.RequestVote(ctx, &req)
	if err != nil {
		return err
	}
	transportv1.RequestVoteResponseToStruct(resp, out)
	return nil
}

func (g *GRPCTransport) InstallSnapshot(id raft.ServerID, target raft.ServerAddress, args *raft.InstallSnapshotRequest, out *raft.InstallSnapshotResponse, data io.Reader) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var meta transportv1.InstallSnapshotMetadata

	transportv1.InstallSnapshotMetadataFromStruct(args, &meta)

	req := transportv1.InstallSnapshotRequest{
		Message: &transportv1.InstallSnapshotRequest_Metadata{
			Metadata: &meta,
		},
	}

	client, err := g.getClient(ctx, target)
	if err != nil {
		return err
	}

	stream, err := client.InstallSnapshot(ctx)
	if err != nil {
		return fmt.Errorf("failed to initiate InstallSnapshot stream")
	}

	err = stream.Send(&req)
	if err != nil {
		return fmt.Errorf("failed to send initial snapshot metadata: %w", err)
	}

	// make sure we don't read more than the snapshot size
	rdr := io.LimitReader(data, args.Size)

	var written int64
	buf := make([]byte, snapshotChunkSize)
	var chunk transportv1.InstallSnapshotChunk
	done := false
	for !done {
		n, err := rdr.Read(buf)
		if n > 0 {
			if written+int64(n) > args.Size {
				return fmt.Errorf("provided snapshots data would exceed specified size")
			}

			req.Reset()
			chunk.Reset()
			chunk.SnapshotData = buf[:n]
			req.Message = &transportv1.InstallSnapshotRequest_Chunk{Chunk: &chunk}

			err := stream.Send(&req)
			if err != nil {
				return fmt.Errorf("failed to send snapshot data: %w", err)
			}
			written += int64(n)
		}

		if err == io.EOF {
			if written < args.Size {
				return fmt.Errorf("provided snapshot data is smaller than specified size")
			}
			done = true
		} else if err != nil {
			return fmt.Errorf("encountered an error while reading snapshot data: %w", err)
		}
	}

	resp, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("error receiving InstallSnapshot response: %w", err)
	}

	transportv1.InstallSnapshotResponseToStruct(resp, out)
	return nil
}

func (g *GRPCTransport) EncodePeer(id raft.ServerID, addr raft.ServerAddress) []byte {
	return []byte(g.getProviderAddressOrFallback(id, addr))
}

func (g *GRPCTransport) DecodePeer(data []byte) raft.ServerAddress {
	return raft.ServerAddress(data)
}

func (g *GRPCTransport) SetHeartbeatHandler(cb func(rpc raft.RPC)) {
	g.service.setHeartbeatHandler(cb)
}

func (g *GRPCTransport) TimeoutNow(id raft.ServerID, target raft.ServerAddress, args *raft.TimeoutNowRequest, out *raft.TimeoutNowResponse) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var req transportv1.TimeoutNowRequest
	transportv1.TimeoutNowRequestFromStruct(args, &req)

	conn, err := grpc.Dial(string(target), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}

	client := transportv1.NewRaftTransportServiceClient(conn)
	resp, err := client.TimeoutNow(ctx, &req)
	if err != nil {
		return err
	}
	transportv1.TimeoutNowResponseToStruct(resp, out)
	return nil
}

func (g *GRPCTransport) getProviderAddressOrFallback(id raft.ServerID, target raft.ServerAddress) raft.ServerAddress {
	if g.serverAddressProvider != nil {
		serverAddressOverride, err := g.serverAddressProvider.ServerAddr(id)
		if err != nil {
			g.logger.Warn("unable to get address for server, using fallback address", "id", id, "fallback", target, "error", err)
		} else {
			return serverAddressOverride
		}
	}
	return target
}

func (g *GRPCTransport) RegisterRaftRPCServices(srv grpc.ServiceRegistrar) {
	g.service.registerService(srv)
}
