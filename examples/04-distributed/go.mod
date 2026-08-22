module github.com/imlargo/sse/examples/04-distributed

go 1.25

require (
	github.com/imlargo/sse v0.0.0
	github.com/imlargo/sse/logs/redis v0.0.0
	github.com/redis/go-redis/v9 v9.22.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/sys v0.30.0 // indirect
)

replace github.com/imlargo/sse => ../..

replace github.com/imlargo/sse/logs/redis => ../../logs/redis
