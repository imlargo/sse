// Package redislog implements the sse.Log interface on top of Redis Streams.
//
// It is the whole of what is needed to run a stream across many nodes. Nothing
// else changes: the subscriber registry, topic matching, per-subscriber queues,
// the backpressure policies, replay, gap declaration and metrics all live above
// this seam and are shared with the single-node case.
//
// That is the point of cutting the seam here. A coarser interface — publish,
// subscribe, shut down — forces every integration to reimplement exactly the
// parts where a library's quality lives, and each one does it differently and
// worse. Here an integration is an append, a follow and a describe.
//
// # One reader per node, not per subscriber
//
// A node tails the stream once, in a single background goroutine, and mirrors
// what it reads into an in-process window that every local subscriber reads
// from. Subscribers never touch Redis.
//
// That is not an optimisation, it is the difference between working and not. A
// blocking XREAD occupies a connection for as long as it blocks, so a reader
// per subscriber means a Redis connection per subscriber: past the client's
// pool size — eighty by default — every further subscriber waits for a
// connection that never frees, and the node stops delivering. Measured on a
// laptop, 250 subscribers saturated the pool with 80 blocked connections and
// nothing got through.
//
// A subscriber whose position predates the local window is caught up straight
// from Redis with XRANGE, which does not block and returns its connection
// immediately, and then joins the local tail. So a node holds exactly one
// long-lived Redis connection however many clients it serves.
//
// # Mapping onto Redis Streams
//
// A Redis stream entry id is "<milliseconds>-<sequence>". Offsets pack that
// into a uint64 as ms<<20|seq: 44 bits of milliseconds, which lasts to the year
// 2527, and 20 bits of sequence, which allows a million entries per
// millisecond. The packing preserves order, so an offset comparison is a Redis
// id comparison.
//
// Because ids are time-based they are globally increasing even across a
// deletion of the stream, so a stale cursor lands in the past rather than on
// unrelated data. The epoch is still stored and checked: it is what covers the
// case of the stream being replaced by a different one under the same name.
package redislog

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/imlargo/sse"
	"github.com/redis/go-redis/v9"
)

// Log is an sse.Log backed by a Redis stream.
type Log struct {
	rdb    redis.UniversalClient
	key    string
	epoch  sse.Epoch
	policy sse.Retention
	block  time.Duration
	logger *slog.Logger

	// local mirrors the stream for this node. Every subscriber reads from it;
	// only the tailer below reads from Redis.
	local *sse.MemoryLog

	// window is how much of the stream this node keeps locally. It bounds
	// memory per node rather than per subscriber, and a subscriber older than
	// it is caught up from Redis instead.
	window sse.Retention

	// epoch is read again periodically, because a stream can be wiped or
	// replaced underneath a node that is still running. It is atomic so the
	// tailer can update it while subscribers read it.
	currentEpoch  atomic.Uint64
	epochInterval time.Duration

	stop     context.CancelFunc
	tailDone chan struct{}
	closeOne sync.Once

	// trimFailures counts consecutive failures to apply the age limit, so the
	// log complains once and then periodically rather than on every publish.
	trimFailures atomic.Int64
}

// WithLogger sets where the log reports problems it cannot fail a call for.
func WithLogger(l *slog.Logger) Option {
	return func(lg *Log) { lg.logger = l }
}

// reportTrimFailure complains about a failing age limit, loudly the first time
// and then at a rate that will not drown a log file.
func (l *Log) reportTrimFailure(ctx context.Context, err error) {
	n := l.trimFailures.Add(1)
	if n != 1 && n%1000 != 0 {
		return
	}
	l.logger.WarnContext(ctx,
		"redislog: could not apply the age-based retention limit; the stream will keep growing",
		"stream", l.key, "consecutiveFailures", n, "error", err)
}

// Option configures a Log.
type Option func(*Log)

// WithBlockTimeout sets how long the tailer waits on Redis before looping. It
// bounds how quickly the log notices it has been closed.
func WithBlockTimeout(d time.Duration) Option {
	return func(l *Log) { l.block = d }
}

// WithEpochCheckInterval sets how often the node re-reads the stream's
// generation.
//
// A stream can be wiped, replaced, or failed over to an empty replica while a
// node is running, and the node would otherwise keep reporting the generation
// it read at startup — accepting cursors that now point at unrelated events.
// Checking is one small read, so the default is frequent enough to bound the
// window and cheap enough to ignore.
func WithEpochCheckInterval(d time.Duration) Option {
	return func(l *Log) { l.epochInterval = d }
}

// WithLocalWindow sets how much of the stream this node keeps in memory.
//
// It is a per-node cost, not a per-subscriber one: every subscriber reads from
// the same window. Bigger means fewer catch-up round trips to Redis when a
// client reconnects; smaller means less memory. The default holds a few
// thousand events or a minute, whichever comes first.
func WithLocalWindow(r sse.Retention) Option {
	return func(l *Log) { l.window = r }
}

// New returns a Log over the Redis stream at key.
//
// The retention is applied by Redis itself on append, so it holds across every
// node without coordination.
func New(ctx context.Context, rdb redis.UniversalClient, key string, r sse.Retention, opts ...Option) (*Log, error) {
	l := &Log{
		rdb:           rdb,
		key:           key,
		policy:        r,
		block:         5 * time.Second,
		logger:        slog.Default(),
		window:        sse.Retention{Events: 4096, For: time.Minute},
		epochInterval: 30 * time.Second,
		tailDone:      make(chan struct{}),
	}
	for _, opt := range opts {
		opt(l)
	}
	if l.logger == nil {
		l.logger = slog.Default()
	}

	epoch, err := l.loadEpoch(ctx)
	if err != nil {
		return nil, err
	}
	l.epoch = epoch
	l.currentEpoch.Store(uint64(epoch))

	// The local mirror carries Redis's own offsets and generation, so a cursor
	// minted here resolves on any other node reading the same stream.
	l.local = sse.NewMemoryLog(l.window,
		sse.WithEpoch(epoch), sse.WithExternalOffsets())

	// Start where the stream is now. Replaying the whole history into every
	// node at startup would cost what the history costs, per node, for nothing:
	// a subscriber that wants older events is caught up from Redis directly.
	info, err := l.Info(ctx)
	if err != nil {
		return nil, err
	}

	tailCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	l.stop = cancel
	go l.tail(tailCtx, info.Newest)

	return l, nil
}

// tail is the node's single reader. It is the only thing here that holds a
// Redis connection for any length of time.
func (l *Log) tail(ctx context.Context, from sse.Offset) {
	defer close(l.tailDone)

	pos := from
	var backoff time.Duration
	lastEpochCheck := time.Now()

	for {
		if ctx.Err() != nil {
			return
		}

		if l.epochInterval > 0 && time.Since(lastEpochCheck) >= l.epochInterval {
			lastEpochCheck = time.Now()
			l.recheckEpoch(ctx)
		}

		res, err := l.rdb.XRead(ctx, &redis.XReadArgs{
			Streams: []string{l.key, formatID(pos)},
			Block:   l.block,
			Count:   512,
		}).Result()

		switch {
		case err == nil, err == redis.Nil:
			backoff = 0
		case ctx.Err() != nil:
			return
		default:
			// Redis is unreachable, restarting, failing over. Retry rather than
			// give up: the stream is still there and the subscribers on this
			// node are still connected. What they must not get is silence
			// pretending to be an empty stream, so anything missed while this
			// is down surfaces as a declared gap once reading resumes.
			backoff = nextBackoff(backoff)
			lastEpochCheck = time.Time{} // a failure is a likely moment for a wipe
			l.logger.WarnContext(ctx, "redislog: cannot read the stream, retrying",
				"stream", l.key, "retryIn", backoff, "error", err)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return
			}
			continue
		}

		for _, stream := range res {
			for _, msg := range stream.Messages {
				off, perr := parseID(msg.ID)
				if perr != nil {
					continue
				}
				pos = off
				if aerr := l.local.AppendAt(ctx, off, frameOf(msg)); aerr != nil {
					if ctx.Err() != nil {
						return
					}
					l.logger.WarnContext(ctx, "redislog: dropping an entry the local window refused",
						"stream", l.key, "offset", off, "error", aerr)
				}
			}
		}
	}
}

// recheckEpoch re-reads the stream's generation and adopts it if it changed.
//
// A change means the stream this node has been mirroring is not the one in
// Redis any more, so every position a client holds from before is meaningless.
// Reporting the new generation is what turns that into a declared gap instead
// of a client being handed unrelated events.
func (l *Log) recheckEpoch(ctx context.Context) {
	epoch, err := l.loadEpoch(ctx)
	if err != nil {
		return // unreachable Redis is the tailer's problem, not this one's
	}
	previous := sse.Epoch(l.currentEpoch.Swap(uint64(epoch)))
	if previous == epoch {
		return
	}
	l.logger.WarnContext(ctx,
		"redislog: the stream's generation changed; positions held by clients no longer resolve",
		"stream", l.key, "was", previous, "now", epoch)
}

// nextBackoff grows the wait between failed reads, bounded so recovery is still
// prompt once Redis comes back.
func nextBackoff(current time.Duration) time.Duration {
	const (
		first = 50 * time.Millisecond
		limit = 2 * time.Second
	)
	if current == 0 {
		return first
	}
	if next := current * 2; next < limit {
		return next
	}
	return limit
}

// loadEpoch reads the stream's generation, creating it once if absent.
//
// SETNX is what makes this safe: every node that starts against the same stream
// converges on the same epoch, so a client can reconnect to any replica and its
// cursor still resolves.
func (l *Log) loadEpoch(ctx context.Context) (sse.Epoch, error) {
	key := l.key + ":epoch"

	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0, err
	}
	candidate := binary.BigEndian.Uint64(b[:]) | 1

	if err := l.rdb.SetNX(ctx, key, strconv.FormatUint(candidate, 10), 0).Err(); err != nil {
		return 0, fmt.Errorf("redislog: reading epoch: %w", err)
	}
	got, err := l.rdb.Get(ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("redislog: reading epoch: %w", err)
	}
	n, err := strconv.ParseUint(got, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("redislog: malformed epoch %q: %w", got, err)
	}
	return sse.Epoch(n), nil
}

func (l *Log) Append(ctx context.Context, f sse.Frame) (sse.Offset, error) {
	args := &redis.XAddArgs{
		Stream: l.key,
		Values: map[string]any{
			"b": f.Body,
			"t": f.Topic,
			"n": f.Name,
			"k": f.Key,
		},
	}
	// Retention is enforced by Redis on write, so it holds identically on every
	// node with no coordination and no sweeper.
	if l.policy.Events > 0 {
		args.MaxLen = int64(l.policy.Events)
		args.Approx = true
	}

	id, err := l.rdb.XAdd(ctx, args).Result()
	if err != nil {
		return 0, fmt.Errorf("redislog: append: %w", err)
	}
	if l.policy.For > 0 {
		// Trimming by age is a separate call, and failing it must not fail the
		// publish — the event is already stored and refusing now would lose it
		// for no reason.
		//
		// It is reported rather than swallowed. A trim that fails every time —
		// a permissions change, a read-only replica — means the age limit
		// silently stops applying and the stream grows without bound, which is
		// exactly the kind of thing nobody notices until the memory does.
		minID := strconv.FormatInt(time.Now().Add(-l.policy.For).UnixMilli(), 10) + "-0"
		if err := l.rdb.XTrimMinID(ctx, l.key, minID).Err(); err != nil {
			l.reportTrimFailure(ctx, err)
		} else {
			l.trimFailures.Store(0)
		}
	}
	return parseID(id)
}

// Read ignores the filter hint in ReadOptions. It exists to spare a scheduler
// the cost of waking goroutines that will discard the event, and a reader here
// is parked on a network call rather than on a Go channel, so there is nothing
// to save.
// Read returns a reader for one subscriber.
//
// It costs no Redis connection in the common case: the node's local window is
// already being filled by the tailer, so a subscriber reading recent events
// reads out of memory. Only a position older than that window needs Redis, and
// then through XRANGE, which does not block.
func (l *Log) Read(ctx context.Context, after sse.Offset, opts sse.ReadOptions) (sse.Reader, error) {
	info, err := l.Info(ctx)
	if err != nil {
		return nil, err
	}

	var gap *sse.Gap
	if after > 0 && info.Oldest > 0 && after < info.Oldest-1 {
		// Behind everything Redis still holds. Declared, and resumed from the
		// oldest that does exist rather than quietly starting later.
		gap = &sse.Gap{Reason: sse.GapRetention, From: after, Through: info.Oldest - 1}
		after = info.Oldest - 1
	}

	local, err := l.local.Read(ctx, after, opts)
	if err != nil {
		return nil, err
	}

	// Where the local window currently begins. Anything the subscriber wants
	// before that has to come from Redis first.
	localInfo, err := l.local.Info(ctx)
	if err != nil {
		local.Close()
		return nil, err
	}

	r := &reader{log: l, local: local, gap: gap}
	if localInfo.Oldest == 0 || after+1 < localInfo.Oldest {
		// The local reader will begin at the window's first entry, so catch-up
		// covers exactly what precedes it. Opening the local reader first is
		// what makes the join exact: nothing published in between is missed,
		// and nothing is delivered twice.
		r.catchUpFrom = after
		r.catchUpTo = localInfo.Oldest
		if localInfo.Oldest == 0 {
			r.catchUpTo = info.Newest + 1
		}
		r.catchingUp = true
	}
	return r, nil
}

func (l *Log) Info(ctx context.Context) (sse.LogInfo, error) {
	info := sse.LogInfo{
		Epoch:     sse.Epoch(l.currentEpoch.Load()),
		Resumable: !l.policy.IsZero(),
		Retention: l.policy,
	}

	res, err := l.rdb.XInfoStream(ctx, l.key).Result()
	if err != nil {
		if err == redis.Nil || strings.Contains(err.Error(), "no such key") {
			return info, nil // an empty stream is not an error
		}
		return info, fmt.Errorf("redislog: info: %w", err)
	}
	if res.FirstEntry.ID != "" {
		if off, err := parseID(res.FirstEntry.ID); err == nil {
			info.Oldest = off
		}
	}
	if res.LastGeneratedID != "" {
		if off, err := parseID(res.LastGeneratedID); err == nil {
			info.Newest = off
		}
	}
	if res.MaxDeletedEntryID != "" && res.MaxDeletedEntryID != "0-0" {
		if off, err := parseID(res.MaxDeletedEntryID); err == nil {
			info.EvictedThrough = off
		}
	}
	if info.Oldest > 1 && info.EvictedThrough == 0 {
		// Trimming does not always update max-deleted-entry-id, so anything
		// before the oldest surviving entry counts as evicted.
		info.EvictedThrough = info.Oldest - 1
	}
	return info, nil
}

// reader serves one subscriber: first whatever precedes this node's local
// window, straight from Redis, then the local window itself.
//
// There is no live handover to race. The local reader is opened before catch-up
// begins and holds its position throughout, so the two ranges meet exactly:
// catch-up ends where the local window starts.
type reader struct {
	log   *Log
	local sse.Reader
	gap   *sse.Gap

	catchingUp  bool
	catchUpFrom sse.Offset
	catchUpTo   sse.Offset
	pending     []sse.Entry

	closeOnce sync.Once
}

func (r *reader) Gap() *sse.Gap {
	if r.gap != nil {
		return r.gap
	}
	return r.local.Gap()
}

func (r *reader) Close() error {
	r.closeOnce.Do(func() { r.local.Close() })
	return nil
}

func (r *reader) Next(ctx context.Context) (sse.Entry, error) {
	for r.catchingUp {
		if len(r.pending) > 0 {
			e := r.pending[0]
			r.pending = r.pending[1:]
			r.catchUpFrom = e.Offset
			return e, nil
		}
		if err := r.fetchCatchUp(ctx); err != nil {
			return sse.Entry{}, err
		}
	}
	return r.local.Next(ctx)
}

// fetchCatchUp pulls the next batch of history from Redis. XRANGE does not
// block, so the connection goes straight back to the pool.
func (r *reader) fetchCatchUp(ctx context.Context) error {
	const batch = 512

	msgs, err := r.log.rdb.XRangeN(ctx,
		r.log.key,
		"("+formatID(r.catchUpFrom),
		formatID(r.catchUpTo-1),
		batch,
	).Result()
	if err != nil && err != redis.Nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("redislog: catching up: %w", err)
	}

	if len(msgs) == 0 {
		// Nothing left before the window; join the local tail.
		r.catchingUp = false
		return nil
	}
	for _, msg := range msgs {
		off, perr := parseID(msg.ID)
		if perr != nil {
			continue
		}
		r.pending = append(r.pending, sse.Entry{Offset: off, Frame: frameOf(msg)})
	}
	return nil
}

func frameOf(msg redis.XMessage) sse.Frame {
	f := sse.Frame{Time: time.UnixMilli(int64(idMillis(msg.ID)))}
	if v, ok := msg.Values["b"].(string); ok {
		f.Body = []byte(v)
	}
	if v, ok := msg.Values["t"].(string); ok {
		f.Topic = v
	}
	if v, ok := msg.Values["n"].(string); ok {
		f.Name = v
	}
	if v, ok := msg.Values["k"].(string); ok {
		f.Key = v
	}
	return f
}

// Offsets pack a Redis id into a uint64 so ordering is preserved: 44 bits of
// milliseconds and 20 bits of sequence.
const seqBits = 20

func parseID(id string) (sse.Offset, error) {
	msPart, seqPart, ok := strings.Cut(id, "-")
	if !ok {
		return 0, fmt.Errorf("redislog: malformed stream id %q", id)
	}
	ms, err := strconv.ParseUint(msPart, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("redislog: malformed stream id %q: %w", id, err)
	}
	seq, err := strconv.ParseUint(seqPart, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("redislog: malformed stream id %q: %w", id, err)
	}
	if seq >= 1<<seqBits {
		return 0, fmt.Errorf("redislog: sequence %d in id %q exceeds what an offset can carry", seq, id)
	}
	return sse.Offset(ms<<seqBits | seq), nil
}

func formatID(off sse.Offset) string {
	if off == 0 {
		return "0-0"
	}
	return strconv.FormatUint(uint64(off)>>seqBits, 10) + "-" +
		strconv.FormatUint(uint64(off)&(1<<seqBits-1), 10)
}

func idMillis(id string) uint64 {
	msPart, _, _ := strings.Cut(id, "-")
	ms, _ := strconv.ParseUint(msPart, 10, 64)
	return ms
}

// Close stops the node's tailer and releases the local window. It does not
// touch the stream in Redis, which belongs to the deployment rather than to any
// one node.
func (l *Log) Close() error {
	l.closeOne.Do(func() {
		if l.stop != nil {
			l.stop()
		}
		<-l.tailDone
		l.local.Close()
	})
	return nil
}
