DEPS = $(go list -f '{{range .TestImports}}{{.}} {{end}}' ./...)
ENV  = $(shell go env GOPATH)
GO_VERSION  = $(shell go version)
GOLANG_CI_VERSION = v1.19.0

# Look for versions prior to 1.10 which have a different fmt output
# and don't lint with gofmt against them.
ifneq (,$(findstring go version go1.8, $(GO_VERSION)))
	FMT=
else ifneq (,$(findstring go version go1.9, $(GO_VERSION)))
	FMT=
else
    FMT=--enable gofmt
endif

TEST_RESULTS_DIR?=/tmp/test-results

test:
	GOTRACEBACK=all go test $(TESTARGS) -timeout=180s -race .
	GOTRACEBACK=all go test $(TESTARGS) -timeout=180s -tags batchtest -race .

integ: test
	INTEG_TESTS=yes go test $(TESTARGS) -timeout=60s -run=Integ .
	INTEG_TESTS=yes go test $(TESTARGS) -timeout=60s -tags batchtest -run=Integ .

fuzz:
	cd ./fuzzy && go test $(TESTARGS) -timeout=20m .
	cd ./fuzzy && go test $(TESTARGS) -timeout=20m -tags batchtest .

deps:
	go get -t -d -v ./...
	echo $(DEPS) | xargs -n1 go get -d

lint:
	gofmt -s -w .
	golangci-lint run -c .golangci-lint.yml $(FMT) .

dep-linter:
	curl -sfL https://install.goreleaser.com/github.com/golangci/golangci-lint.sh | sh -s -- -b $(ENV)/bin $(GOLANG_CI_VERSION)

cov:
	INTEG_TESTS=yes gocov test github.com/hashicorp/raft | gocov-html > /tmp/coverage.html
	open /tmp/coverage.html

.PHONY: proto	
proto: proto-tools proto-no-tools
	
.PHONY: proto-no-tools
proto-no-tools:
	buf generate
	mog -source './proto/transport/v1/*.pb.go'
	
.PHONY: proto-tools
proto-tools:
	go install github.com/bufbuild/buf/cmd/buf@v1.11.0
	go install google.golang.org/protobuf/cmd/protoc-gen-go@$(shell grep google.golang.org/protobuf go.mod | awk '{print $$2}')
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2.0
	go install github.com/hashicorp/mog@v0.3.0

.PHONY: test cov integ deps dep-linter lint
