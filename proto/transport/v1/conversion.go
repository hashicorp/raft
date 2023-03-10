package transportv1

import (
	"time"

	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

func TimeFromProto(t *timestamppb.Timestamp) time.Time {
	return t.AsTime()
}

func TimeToProto(t time.Time) *timestamppb.Timestamp {
	return timestamppb.New(t)
}
