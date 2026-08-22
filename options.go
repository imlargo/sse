package sse

import (
	"fmt"
	"log/slog"
	"strings"
	"time"
)

// Defaults. KeepAlive follows the WHATWG specification's own authoring note,
// which recommends a comment line roughly every 15 seconds to survive proxies
// that drop idle connections.
const (
	DefaultKeepAlive    = 15 * time.Second
	DefaultWriteTimeout = 10 * time.Second
	DefaultRetry        = 3 * time.Second
	DefaultRetryJitter  = 0.5
	DefaultMaxEventSize = 1 << 20 // 1 MiB
	DefaultSendQueue    = 64
	DefaultPrefix       = "sse."
)

// An Option configures a handler. Options are applied and validated once, at
// construction; there is no public mutable state that could be changed while
// the server is running (RF-E6). Mutable configuration on a live server is a
// known source of data races in existing libraries.
type Option func(*config) error

type config struct {
	keepAlive    time.Duration
	writeTimeout time.Duration
	retry        time.Duration
	retryJitter  float64
	maxEventSize int
	sendQueue    int
	padding      int
	prefix       string
	codec        Codec
	logger       *slog.Logger
	lifecycle    *Lifecycle
	fullDuplex   bool
}

func newConfig(opts []Option) (*config, error) {
	c := &config{
		keepAlive:    DefaultKeepAlive,
		writeTimeout: DefaultWriteTimeout,
		retry:        DefaultRetry,
		retryJitter:  DefaultRetryJitter,
		maxEventSize: DefaultMaxEventSize,
		sendQueue:    DefaultSendQueue,
		prefix:       DefaultPrefix,
		codec:        JSONCodec{},
		logger:       slog.Default(),
		fullDuplex:   true,
	}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(c); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// WithKeepAlive sets how long the stream may stay silent before a comment line
// is emitted. The same timer is the liveness probe: a dead peer is detected by
// the keep-alive write failing, which is the only signal available in every
// environment.
//
// Zero disables it, which also disables periodic liveness detection. Do that
// only when something else guarantees traffic.
func WithKeepAlive(d time.Duration) Option {
	return func(c *config) error {
		if d < 0 {
			return fmt.Errorf("sse: WithKeepAlive: %v is negative", d)
		}
		c.keepAlive = d
		return nil
	}
}

// WithWriteTimeout bounds how long a single write may block before the session
// is considered failed. Without it, a peer that stops reading holds a goroutine
// and a connection indefinitely: the request context does not save you.
func WithWriteTimeout(d time.Duration) Option {
	return func(c *config) error {
		if d < 0 {
			return fmt.Errorf("sse: WithWriteTimeout: %v is negative", d)
		}
		c.writeTimeout = d
		return nil
	}
}

// WithRetry sets the reconnection delay advertised to clients.
func WithRetry(d time.Duration) Option {
	return func(c *config) error {
		if d < time.Millisecond {
			return fmt.Errorf("sse: WithRetry: %v is below the one-millisecond wire resolution", d)
		}
		c.retry = d
		return nil
	}
}

// WithRetryJitter sets the fraction by which each client's advertised
// reconnection delay is randomly spread, between 0 and 1.
//
// It is the only lever the protocol offers against a reconnection stampede: if
// fifty thousand clients drop at once and all wait exactly the same delay, they
// come back at once too (RF-E2).
func WithRetryJitter(f float64) Option {
	return func(c *config) error {
		if f < 0 || f > 1 {
			return fmt.Errorf("sse: WithRetryJitter: %v is outside [0, 1]", f)
		}
		c.retryJitter = f
		return nil
	}
}

// WithMaxEventSize caps the encoded size of one event. An oversized event
// multiplied by every subscriber is a denial of service against yourself.
func WithMaxEventSize(n int) Option {
	return func(c *config) error {
		if n <= 0 {
			return fmt.Errorf("sse: WithMaxEventSize: %d must be positive", n)
		}
		c.maxEventSize = n
		return nil
	}
}

// WithSendQueue sets how many events may be queued between the producer and the
// writer of a session.
func WithSendQueue(n int) Option {
	return func(c *config) error {
		if n < 0 {
			return fmt.Errorf("sse: WithSendQueue: %d is negative", n)
		}
		c.sendQueue = n
		return nil
	}
}

// WithPadding writes n bytes of comment padding when the stream opens.
//
// It is off by default. Unlike the keep-alive comment, padding is not in the
// specification: it is a workaround for old proxies and XHR-based polyfills
// that deliver nothing until some volume of bytes has accumulated. Turn it on
// only if you have such a client in front of you.
func WithPadding(n int) Option {
	return func(c *config) error {
		if n < 0 {
			return fmt.Errorf("sse: WithPadding: %d is negative", n)
		}
		c.padding = n
		return nil
	}
}

// WithReservedPrefix changes the namespace the library uses for its own events,
// so application events can never collide with them.
func WithReservedPrefix(p string) Option {
	return func(c *config) error {
		if p == "" {
			return fmt.Errorf("sse: WithReservedPrefix: prefix must not be empty")
		}
		if strings.ContainsAny(p, "\r\n") {
			return fmt.Errorf("sse: WithReservedPrefix: prefix must fit on one line")
		}
		c.prefix = p
		return nil
	}
}

// WithCodec replaces the serializer used for values that are not already a
// [Payload].
func WithCodec(codec Codec) Option {
	return func(c *config) error {
		if codec == nil {
			return fmt.Errorf("sse: WithCodec: codec must not be nil")
		}
		c.codec = codec
		return nil
	}
}

// WithLogger sets the logger. Logging goes through log/slog so the library
// imposes no dependency (RNF-9).
func WithLogger(l *slog.Logger) Option {
	return func(c *config) error {
		if l == nil {
			return fmt.Errorf("sse: WithLogger: logger must not be nil")
		}
		c.logger = l
		return nil
	}
}

// WithLifecycle registers sessions with l so they can be drained on shutdown.
// It is opt-in: a handler without one keeps no registry at all, so the simple
// case pays nothing for it (RNF-4).
func WithLifecycle(l *Lifecycle) Option {
	return func(c *config) error {
		if l == nil {
			return fmt.Errorf("sse: WithLifecycle: lifecycle must not be nil")
		}
		c.lifecycle = l
		return nil
	}
}

// WithFullDuplex controls whether the handler asks the server to allow reading
// the request body while the response is being written.
//
// It is on by default because streaming over POST is a first-class case: MCP
// uses it. Turning it off restores the standard library's default, where the
// server may consume the request body before the handler runs.
func WithFullDuplex(enabled bool) Option {
	return func(c *config) error {
		c.fullDuplex = enabled
		return nil
	}
}
