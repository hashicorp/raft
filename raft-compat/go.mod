module github.com/hashicorp/raft/compat

go 1.20

require github.com/stretchr/testify v1.8.4

require (
	github.com/armon/go-metrics v0.4.1 // indirect
	github.com/fatih/color v1.13.0 // indirect
	github.com/hashicorp/go-hclog v1.6.2 // indirect
	github.com/hashicorp/go-immutable-radix v1.0.0 // indirect
	github.com/hashicorp/go-msgpack v0.5.5 // indirect
	github.com/hashicorp/go-msgpack/v2 v2.1.1 // indirect
	github.com/hashicorp/golang-lru v0.5.0 // indirect
	github.com/mattn/go-colorable v0.1.12 // indirect
	github.com/mattn/go-isatty v0.0.14 // indirect
	golang.org/x/sys v0.13.0 // indirect
)

replace github.com/hashicorp/raft-previous-version => ./raft-previous-version

replace github.com/hashicorp/raft => ../

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/hashicorp/raft v1.6.1
	github.com/hashicorp/raft-previous-version v1.2.0
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
