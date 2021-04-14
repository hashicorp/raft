module github.com/hashicorp/raft/fuzzy

go 1.16

require (
	github.com/boltdb/bolt v1.3.1 // indirect
	github.com/hashicorp/go-hclog v0.9.1
	github.com/hashicorp/go-msgpack v0.5.5
	github.com/hashicorp/raft v1.2.0
	github.com/hashicorp/raft-boltdb v0.0.0-20171010151810-6e5ba93211ea
	golang.org/x/sys v0.0.0-20210414055047-fe65e336abe0 // indirect
)

replace github.com/hashicorp/raft => ../
