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
	go test -timeout=60s -race .

integ: test
	INTEG_TESTS=yes go test -timeout=25s -run=Integ .

ci.test:
	gotestsum --format=short-verbose --junitfile $(TEST_RESULTS_DIR)/gotestsum-report-test.xml -- -timeout=60s -race .

ci.integ: ci.test
	INTEG_TESTS=yes gotestsum --format=short-verbose --junitfile $(TEST_RESULTS_DIR)/gotestsum-report-integ.xml -- -timeout=25s -run=Integ .

fuzz:
	go test -timeout=300s ./fuzzy

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

.PHONY: test cov integ deps dep-linter lint
