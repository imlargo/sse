module github.com/imlargo/sse/logs/redislog

go 1.25.0

require (
	github.com/imlargo/sse v0.1.0
	github.com/redis/go-redis/v9 v9.22.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/sys v0.30.0 // indirect
)

// Local development only. These are stripped at release time by
// `make release-prep`, because a replace in a published module is ignored
// by everyone who depends on it and would leave this requiring a version
// of the core that does not resolve. `make release-check` enforces it.
replace github.com/imlargo/sse => ../..
