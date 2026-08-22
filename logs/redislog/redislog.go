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
	"strconv"
	"strings"
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
}

// Option configures a Log.
type Option func(*Log)

// WithBlockTimeout sets how long a follow waits on Redis before looping. It
// bounds how quickly a reader notices its context was cancelled.
func WithBlockTimeout(d time.Duration) Option {
	return func(l *Log) { l.block = d }
}

// New returns a Log over the Redis stream at key.
//
// The retention is applied by Redis itself on append, so it holds across every
// node without coordination.
func New(ctx context.Context, rdb redis.UniversalClient, key string, r sse.Retention, opts ...Option) (*Log, error) {
	l := &Log{rdb: rdb, key: key, policy: r, block: 5 * time.Second}
	for _, opt := range opts {
		opt(l)
	}
	epoch, err := l.loadEpoch(ctx)
	if err != nil {
		return nil, err
	}
	l.epoch = epoch
	return l, nil
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
		// Best effort: trimming by age is a separate call, and failing to trim
		// must not fail the publish.
		minID := strconv.FormatInt(time.Now().Add(-l.policy.For).UnixMilli(), 10) + "-0"
		_ = l.rdb.XTrimMinID(ctx, l.key, minID).Err()
	}
	return parseID(id)
}

// Read ignores the filter hint in ReadOptions. It exists to spare a scheduler
// the cost of waking goroutines that will discard the event, and a reader here
// is parked on a network call rather than on a Go channel, so there is nothing
// to save.
func (l *Log) Read(ctx context.Context, after sse.Offset, _ sse.ReadOptions) (sse.Reader, error) {
	info, err := l.Info(ctx)
	if err != nil {
		return nil, err
	}
	r := &reader{log: l, pos: after}
	if after > 0 && info.Oldest > 0 && after < info.Oldest-1 {
		// The client's position is behind everything Redis still holds.
		r.gap = &sse.Gap{Reason: sse.GapRetention, From: after, Through: info.Oldest - 1}
	}
	return r, nil
}

func (l *Log) Info(ctx context.Context) (sse.LogInfo, error) {
	info := sse.LogInfo{
		Epoch:     l.epoch,
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

// reader follows the stream from a position. It holds only that position, the
// same as the in-memory log: an extra subscriber is an offset, not a queue.
type reader struct {
	log    *Log
	pos    sse.Offset
	gap    *sse.Gap
	buf    []sse.Entry
	closed bool
}

func (r *reader) Gap() *sse.Gap { return r.gap }

func (r *reader) Close() error {
	r.closed = true
	return nil
}

func (r *reader) Next(ctx context.Context) (sse.Entry, error) {
	for {
		if len(r.buf) > 0 {
			e := r.buf[0]
			r.buf = r.buf[1:]
			r.pos = e.Offset
			return e, nil
		}
		if r.closed {
			return sse.Entry{}, sse.ErrLogClosed
		}
		if err := ctx.Err(); err != nil {
			return sse.Entry{}, err
		}

		// XREAD serves replay and live delivery with the same call: it returns
		// whatever is already past the position, and blocks when there is
		// nothing. There is no separate catch-up mode to hand over from, which
		// is why the handover cannot race.
		res, err := r.log.rdb.XRead(ctx, &redis.XReadArgs{
			Streams: []string{r.log.key, formatID(r.pos)},
			Block:   r.log.block,
			Count:   256,
		}).Result()
		if err != nil {
			if err == redis.Nil {
				continue // block timeout, nothing new
			}
			if ctx.Err() != nil {
				return sse.Entry{}, ctx.Err()
			}
			return sse.Entry{}, fmt.Errorf("redislog: read: %w", err)
		}
		for _, stream := range res {
			for _, msg := range stream.Messages {
				off, err := parseID(msg.ID)
				if err != nil {
					continue
				}
				r.buf = append(r.buf, sse.Entry{Offset: off, Frame: frameOf(msg)})
			}
		}
	}
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
