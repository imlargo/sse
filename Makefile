GO ?= go

.PHONY: all test race bench fuzz deps redis compat fiber vet lint check cover clean

all: vet lint test deps

## test: full suite, with goroutine leak detection
test:
	$(GO) test -count=1 ./...

## race: the whole suite under the race detector (RP-5)
race:
	$(GO) test -race -count=1 ./...

## bench: hot-path benchmarks with the allocation budget (RNF-3, RP-6)
bench:
	$(GO) test -run '^$$' -bench . -benchmem ./...

## fuzz: short fuzzing pass over the parser (RP-2)
fuzz:
	$(GO) test -run '^$$' -fuzz FuzzDecoder -fuzztime 30s ./wire

## redis: integration tests for the distribution seam (needs a Redis)
redis:
	cd logs/redis && $(GO) test -race -count=1 ./...

## compat: third-party framework compatibility (Gin, Echo)
compat:
	cd compat && $(GO) test -race -count=1 ./...

## fiber: the adapter for the one framework that needs one
fiber:
	cd adapters/fiber && $(GO) test -race -count=1 ./...

## deps: RF-H1 — the root module must depend on the standard library only
deps:
	@out=$$($(GO) list -deps -f '{{if not .Standard}}{{.ImportPath}}{{end}}' ./... \
		| grep -v '^github.com/imlargo/sse' || true); \
	if [ -n "$$out" ]; then \
		echo "RF-H1 violated: the core module pulled in external dependencies:"; \
		echo "$$out" | sed 's/^/  /'; \
		exit 1; \
	fi; \
	echo "RF-H1 ok: standard library only"

STATICCHECK ?= honnef.co/go/tools/cmd/staticcheck@latest

# Every module, not just the root: the submodules are where third-party
# dependencies live and are exactly where a lint finding is easiest to miss.
MODULES = . logs/redis compat adapters/fiber examples/04-distributed

vet:
	@for m in $(MODULES); do echo "vet $$m"; (cd $$m && $(GO) vet ./...) || exit 1; done

## lint: staticcheck across every module
lint:
	@for m in $(MODULES); do echo "staticcheck $$m"; \
		(cd $$m && $(GO) run $(STATICCHECK) ./...) || exit 1; done

## check: what CI runs, minus the services
check: vet lint test deps

cover:
	$(GO) test -count=1 -coverprofile=coverage.out ./...
	$(GO) tool cover -html=coverage.out -o coverage.html

clean:
	rm -f coverage.out coverage.html
