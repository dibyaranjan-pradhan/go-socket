# go-socket — common build, test, static analysis, and coverage targets.
# Run `make` or `make help` for a list of targets.

.DEFAULT_GOAL := help

GO        ?= go
MODULE    := $(shell $(GO) list -m -q 2>/dev/null || sed -n 's/^module //p' go.mod | head -1)

COVERAGE_OUT  ?= coverage.out
COVERAGE_HTML ?= coverage.html

# Pin versions in CI by replacing @latest with a module@version if needed.
GOVULNCHECK ?= golang.org/x/vuln/cmd/govulncheck@latest
ERRCHECK    ?= github.com/kisielk/errcheck@latest
STATICCHECK ?= honnef.co/go/tools/cmd/staticcheck@latest

.PHONY: help all ci check \
	fmt vet build test test-short test-race \
	coverage coverage-html print-coverage \
	mod-verify tidy \
	vulncheck errcheck staticcheck \
	clean

## help: Show this list
help:
	@echo "go-socket ($(MODULE))"
	@echo ""
	@echo "Development:"
	@echo "  make fmt              go fmt ./..."
	@echo "  make vet              go vet ./..."
	@echo "  make build            go build ./..."
	@echo "  make test             go test ./... -count=1"
	@echo "  make test-short       go test -short ./... -count=1"
	@echo "  make test-race        go test ./... -count=1 -race"
	@echo ""
	@echo "Coverage:"
	@echo "  make coverage         write $(COVERAGE_OUT)"
	@echo "  make coverage-html    coverage + $(COVERAGE_HTML)"
	@echo "  make print-coverage   run tests with coverage and print total %%"
	@echo ""
	@echo "Module / security:"
	@echo "  make mod-verify       go mod verify"
	@echo "  make tidy             go mod tidy"
	@echo "  make vulncheck        govulncheck ./..."
	@echo ""
	@echo "Static analysis (via go run; network on first use):"
	@echo "  make errcheck         errcheck ./..."
	@echo "  make staticcheck      staticcheck ./..."
	@echo ""
	@echo "Aggregates:"
	@echo "  make check            fmt vet errcheck staticcheck vulncheck"
	@echo "  make all              check + test + print-coverage"
	@echo "  make ci               stricter CI-like pipeline (includes -race)"

## all: fmt, vet, static tools, tests, and coverage total
all: check test print-coverage

## ci: pipeline suitable for CI (race detector + tools + coverage)
ci: fmt vet vulncheck errcheck staticcheck test-race print-coverage mod-verify

## check: formatting, vet, and common linters (no tests)
check: fmt vet errcheck staticcheck vulncheck

## fmt: go fmt
fmt:
	$(GO) fmt ./...

## vet: go vet
vet:
	$(GO) vet ./...

## build: compile all packages
build:
	$(GO) build ./...

## test: run all tests
test:
	$(GO) test ./... -count=1

## test-short: skip long tests if any use testing.Short()
test-short:
	$(GO) test -short ./... -count=1

## test-race: tests with race detector
test-race:
	$(GO) test ./... -count=1 -race

## coverage: generate coverage profile (atomic mode; good for concurrency)
coverage: mod-verify
	$(GO) test ./... -count=1 -coverprofile=$(COVERAGE_OUT) -covermode=atomic

## coverage-html: coverage report as HTML
coverage-html: coverage
	$(GO) tool cover -html=$(COVERAGE_OUT) -o $(COVERAGE_HTML)
	@echo "Wrote $(COVERAGE_HTML)"

## print-coverage: run coverage and print the total statement coverage line
print-coverage: coverage
	@echo ""
	@$(GO) tool cover -func=$(COVERAGE_OUT) | tail -1 | sed 's/^/Total coverage: /'
	@echo ""

## mod-verify: verify module checksums
mod-verify:
	$(GO) mod verify

## tidy: go mod tidy
tidy:
	$(GO) mod tidy

## vulncheck: known vulnerable dependencies and patterns (govulncheck)
vulncheck:
	$(GO) run $(GOVULNCHECK) ./...

## errcheck: find unchecked errors
errcheck:
	$(GO) run $(ERRCHECK) ./...

## staticcheck: honnef.co/go/tools static analysis
staticcheck:
	$(GO) run $(STATICCHECK) ./...

## clean: remove generated coverage artifacts
clean:
	rm -f $(COVERAGE_OUT) $(COVERAGE_HTML)
