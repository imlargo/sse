package sse

import (
	"encoding/json"
	"io"
)

// A Payload is an already-decided event body.
//
// The interface is closed on purpose: its method is unexported, so the set of
// payload kinds is fixed. Nothing in the delivery path is an open extension
// point, because that is where the library's guarantees live. What is pluggable
// is the [Codec] used for values that are not already a Payload.
type Payload interface {
	appendData(dst []byte, c Codec) ([]byte, error)
}

// Text sends a string as-is, with no serialization.
//
// This is not an advanced escape hatch. It is the main path for hypermedia
// interfaces such as htmx, Datastar and Turbo Streams, which push HTML
// fragments, and for language-model proxies, which already hold the bytes.
func Text(s string) Payload { return textPayload(s) }

// Raw sends bytes as-is. They must be valid UTF-8, since an event stream is
// always UTF-8 and there is no way to select another encoding.
//
// The slice must not be modified after the call: it may still be queued.
func Raw(b []byte) Payload { return rawPayload(b) }

// From reads r to end of stream and sends the result as one event.
func From(r io.Reader) Payload { return readerPayload{r} }

type textPayload string

func (p textPayload) appendData(dst []byte, _ Codec) ([]byte, error) {
	return append(dst, p...), nil
}

type rawPayload []byte

func (p rawPayload) appendData(dst []byte, _ Codec) ([]byte, error) {
	return append(dst, p...), nil
}

type readerPayload struct{ r io.Reader }

func (p readerPayload) appendData(dst []byte, _ Codec) ([]byte, error) {
	b, err := io.ReadAll(p.r)
	if err != nil {
		return dst, err
	}
	return append(dst, b...), nil
}

// valuePayload carries an arbitrary value through the configured Codec. It is
// what Send falls back to when the value is not already a Payload.
type valuePayload struct{ v any }

func (p valuePayload) appendData(dst []byte, c Codec) ([]byte, error) {
	return c.Append(dst, p.v)
}

// A Codec serializes application values.
//
// Serialization happens once per published event, before fan-out, never once
// per subscriber: that is what keeps the cost of an extra subscriber close to
// the cost of a write (RNF-1). The default uses encoding/json, whose reflection
// is therefore off the hot path — the hot path is fan-out and writing, which is
// reflection-free and allocation-free.
type Codec interface {
	// Name identifies the encoding, for documentation and capability
	// reporting.
	Name() string
	// Append serializes v and appends it to dst.
	Append(dst []byte, v any) ([]byte, error)
}

// JSONCodec is the default codec.
type JSONCodec struct{}

func (JSONCodec) Name() string { return "json" }

func (JSONCodec) Append(dst []byte, v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return dst, err
	}
	return append(dst, b...), nil
}
