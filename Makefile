.PHONY: test
test:
	go test -race -coverprofile=coverage.out -covermode=atomic ./...

.PHONY: lint
lint:
	golangci-lint run ./...

.PHONY: tidy
tidy:
	go mod tidy
