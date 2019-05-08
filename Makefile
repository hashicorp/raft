DEPS = $(go list -f '{{range .TestImports}}{{.}} {{end}}' ./...)
ENV := $(shell go env GOPATH)
test:
	go test -timeout=60s .

integ: test
	INTEG_TESTS=yes go test -timeout=25s -run=Integ .

fuzz:
	go test -timeout=300s ./fuzzy
	
deps: dep-linter
	go get -t -d -v ./...
	echo $(DEPS) | xargs -n1 go get -d

lint:
	gofmt -s -w .
	golangci-lint run -c .golangci-lint.yml

lint-ci: 
	golangci-lint run -c .golangci-lint.yml -disable=gofmt

dep-linter:
    curl -sfL https://install.goreleaser.com/github.com/golangci/golangci-lint.sh | sh -s -- -b $(ENV)/bin latest

cov:
	INTEG_TESTS=yes gocov test github.com/hashicorp/raft | gocov-html > /tmp/coverage.html
	open /tmp/coverage.html

.PHONY: test cov integ deps dep-linter
