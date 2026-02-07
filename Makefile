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

.PHONY: release
release:
ifndef VERSION
	$(error VERSION is required. Usage: make release VERSION=v1.1.0)
endif
	@echo "Running tests..."
	@$(MAKE) test
	@echo "Creating tag $(VERSION)..."
	-git tag -d $(VERSION) 2>/dev/null
	-git push origin :refs/tags/$(VERSION) 2>/dev/null
	git tag $(VERSION)
	git push origin $(VERSION)
	@echo "Release $(VERSION) tagged and pushed."
	@echo "Create the release at: https://github.com/0uz/pgmq-go/releases/new?tag=$(VERSION)"
