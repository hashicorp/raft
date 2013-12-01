test:
	go test -timeout=3s ./...

integ: test
	INTEG_TESTS=yes go test -timeout=3s -run=Integ ./...

cov:
	INTEG_TESTS=yes gocov test github.com/hashicorp/raft | gocov-html > /tmp/coverage.html
	open /tmp/coverage.html

.PNONY: test cov integ
