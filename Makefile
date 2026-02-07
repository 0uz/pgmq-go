.PHONY: fmt
fmt:
	gofmt -w .

.PHONY: fmt-check
fmt-check:
	@test -z "$$(gofmt -l .)" || (echo "Files not formatted:"; gofmt -l .; exit 1)

.PHONY: test
test:
	go test -race -coverprofile=coverage.out -covermode=atomic ./...

.PHONY: lint
lint: fmt-check
	golangci-lint run ./...

.PHONY: tidy
tidy:
	go mod tidy
