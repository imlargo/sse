GO ?= go

.PHONY: all test race bench fuzz deps redis vet lint cover clean

all: vet test deps

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

vet:
	$(GO) vet ./...

cover:
	$(GO) test -count=1 -coverprofile=coverage.out ./...
	$(GO) tool cover -html=coverage.out -o coverage.html

clean:
	rm -f coverage.out coverage.html
