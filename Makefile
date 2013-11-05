test:
	go test ./...

integ:
	INTEG_TESTS=yes go test ./...

cov:
	gocov test github.com/hashicorp/raft | gocov-html > /tmp/coverage.html
	open /tmp/coverage.html

.PNONY: test cov integ
