module github.com/hashicorp/raft

go 1.16

retract v1.1.3 // Deleted original tag; module checksum may not be accurate.

require (
	github.com/armon/go-metrics v0.0.0-20190430140413-ec5e00d3c878
	github.com/hashicorp/go-hclog v0.9.1
	github.com/hashicorp/go-msgpack v0.5.5
	github.com/stretchr/testify v1.7.0
	google.golang.org/grpc v1.51.0
	google.golang.org/protobuf v1.28.1
)
