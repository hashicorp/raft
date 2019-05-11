DEPS = $(go list -f '{{range .TestImports}}{{.}} {{end}}' ./...)
ENV  = $(shell go env GOPATH)
GO_VERSION  = $(shell go version)

# Look for versions prior to 1.10 which have a different fmt output
# and don't lint with gofmt against them.
ifneq (,$(findstring go version go1.8, $(GO_VERSION)))
	FMT=
else ifneq (,$(findstring go version go1.9, $(GO_VERSION)))
	FMT=
else
    FMT=--enable gofmt
endif

test: lint
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
	golangci-lint run -c .golangci-lint.yml $(FMT)

dep-linter:
	curl -sfL https://install.goreleaser.com/github.com/golangci/golangci-lint.sh | sh -s -- -b $(ENV)/bin v1.16.0

cov:
	INTEG_TESTS=yes gocov test github.com/hashicorp/raft | gocov-html > /tmp/coverage.html
	open /tmp/coverage.html

.PHONY: test cov integ deps dep-linter
