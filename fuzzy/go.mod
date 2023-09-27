module github.com/hashicorp/raft/fuzzy

go 1.16

require (
	github.com/boltdb/bolt v1.3.1 // indirect
	github.com/hashicorp/go-hclog v1.5.0
	github.com/hashicorp/go-msgpack v0.5.5
	github.com/hashicorp/raft v1.2.0
	github.com/hashicorp/raft-boltdb v0.0.0-20171010151810-6e5ba93211ea
)

replace github.com/hashicorp/raft => ../
