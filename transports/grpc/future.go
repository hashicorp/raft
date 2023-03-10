package grpctrans

import (
	"time"

	"github.com/hashicorp/raft"
)

type appendFuture struct {
	raft.DeferError

	start time.Time
	req   *raft.AppendEntriesRequest
	resp  *raft.AppendEntriesResponse
}

func (f *appendFuture) Start() time.Time {
	return f.start
}

func (f *appendFuture) Request() *raft.AppendEntriesRequest {
	return f.req
}

func (f *appendFuture) Response() *raft.AppendEntriesResponse {
	return f.resp
}
