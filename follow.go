package sse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/imlargo/sse/wire"
)

// Follow is a [StreamFunc] that streams a session's configured log.
//
//	log := sse.NewMemoryLog(sse.Retention{For: 10 * time.Minute})
//	http.Handle("/job", sse.Handler(sse.Follow, sse.WithLog(log)))
//
// Everything the client needs happens without the application writing any of
// it: the incoming Last-Event-ID is resolved, any history that cannot be
// provided is declared first, and delivery then continues from the client's
// position.
func Follow(ctx context.Context, s *Session) error { return s.Follow(ctx) }

// Cursor returns the position the client presented in Last-Event-ID, already
// decoded.
//
// Handing over a typed value rather than a header string is the point: in every
// Go library reviewed, resolving that header is work the application is left to
// do, and doing it by hand is where resumption quietly goes wrong. If the token
// could not be resolved, the cursor is zero and [Session.ResumeGap] says why.
func (s *Session) Cursor() Cursor { return s.cursor }

// ResumeGap reports history the client asked for and could not be given, or nil
// if there was none. It is set before any event is delivered.
func (s *Session) ResumeGap() *Gap { return s.resumeGap }

// Resumable reports whether this session can actually resume. It is false when
// no log is configured, when the log retains nothing, or when the cursor would
// not fit the header budget — and in each case the client is told so when the
// stream opens, rather than discovering it after a disconnection.
func (s *Session) Resumable() bool { return s.resumable }

// Follow streams the session's log from the client's position until the context
// ends, the client disappears or the log closes.
//
// It owns the read loop, so replay ordering, gap declaration and the transition
// to live delivery are the library's responsibility rather than the
// application's. There is no transition to get wrong: a reader holds a position
// and advances it, so replay is live delivery from an older offset.
func (s *Session) Follow(ctx context.Context) error {
	if s.log == nil {
		return fmt.Errorf("sse: Follow: no log configured; pass sse.WithLog to the handler")
	}
	s.following.Store(true)
	defer s.following.Store(false)

	// Where a client with no usable position begins.
	//
	// The default is everything still retained, and retention is the knob that
	// decides how much that is. It degenerates correctly: a log with no
	// configured retention holds only the small in-flight window, so "from the
	// oldest" is effectively "from now". A dashboard that must not replay to
	// fresh clients asks for StartTail.
	after := Offset(0)
	if s.cfg.start == StartTail {
		info, err := s.log.Info(ctx)
		if err != nil {
			return err
		}
		after = info.Newest
	}

	// A resolved cursor overrides the default. A cursor that could not be
	// resolved does not: the gap is declared and the client is then given
	// whatever history does still exist, which is more useful than nothing and
	// no less honest.
	if e, ok := s.cursor.Lookup(s.logID); ok && s.resumeGap == nil {
		after = e.Offset
	}

	reader, err := s.log.Read(ctx, after)
	if err != nil {
		return err
	}
	defer reader.Close()

	// Whatever the reader knows was lost is merged with whatever was already
	// determined from the cursor itself, and declared before a single event is
	// delivered (RF-C4).
	if g := reader.Gap(); g != nil && s.resumeGap == nil {
		s.resumeGap = g
	}
	if s.resumeGap != nil {
		if err := s.writeGap(ctx, s.resumeGap); err != nil {
			return err
		}
	}

	for {
		entry, err := reader.Next(ctx)
		if err != nil {
			if errors.Is(err, ErrLogClosed) || errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		if err := s.writeEntry(ctx, entry); err != nil {
			return err
		}
	}
}

// writeEntry sends one stored frame, prefixed with this client's position.
//
// The frame itself is shared with every other subscriber and is never copied or
// re-encoded; only the id line is per-subscriber (RNF-1).
func (s *Session) writeEntry(ctx context.Context, e Entry) error {
	cursor := s.cursor.With(CursorEntry{Log: s.logID, Epoch: s.epoch, Offset: e.Offset})
	s.cursor = cursor

	id := wire.NoID()
	if s.resumable {
		var err error
		if id, err = wire.NewID(cursor.String()); err != nil {
			return err
		}
	}

	buf := getBuf()
	out, err := wire.AppendIDLine((*buf)[:0], id)
	if err != nil {
		putBuf(buf)
		return err
	}
	out = append(out, e.Frame.Body...)
	if len(out) > s.cfg.maxEventSize {
		putBuf(buf)
		return fmt.Errorf("%w: %d bytes exceeds the %d byte limit",
			ErrEventTooLarge, len(out), s.cfg.maxEventSize)
	}
	*buf = out
	return s.enqueue(ctx, buf)
}

// gapPayload is what a client receives when history could not be provided.
type gapPayload struct {
	Reason  GapReason `json:"reason"`
	From    Offset    `json:"from"`
	Through Offset    `json:"through"`
	Detail  string    `json:"detail"`
}

// writeGap tells the client exactly what it did not get.
//
// This is the requirement the whole design is organised around: the library
// never pretends. A declared failure is acceptable, and a silent one that
// corrupts the client's state is not. It is delivered before any replayed
// event, in the reserved namespace so it can never be confused with an
// application event.
func (s *Session) writeGap(ctx context.Context, g *Gap) error {
	body, err := json.Marshal(gapPayload{
		Reason:  g.Reason,
		From:    g.From,
		Through: g.Through,
		Detail:  gapDetail(g.Reason),
	})
	if err != nil {
		return err
	}
	return s.emit(ctx, wire.Event{Name: s.cfg.prefix + "gap"}, Raw(body))
}

func gapDetail(r GapReason) string {
	switch r {
	case GapRetention:
		return "Events after your position are no longer retained. Reload your state from the application rather than assuming the stream is continuous."
	case GapEpoch:
		return "Your position belongs to an earlier generation of this stream and cannot be resolved against the current one. Reload your state."
	case GapUnresolvable:
		return "Your resumption token could not be decoded. Reload your state."
	case GapUnsupported:
		return "This stream does not retain history, so nothing could be replayed. Reload your state."
	default:
		return "History could not be replayed. Reload your state."
	}
}

// resolveResumption works out where a client is, before anything is written.
//
// Every outcome is explicit. An undecodable token, a token from an earlier
// generation of the log, and a position that has aged out are three different
// gaps with three different reasons, and none of them silently becomes
// "start from now".
func (s *Session) resolveResumption(ctx context.Context, r *http.Request) {
	if s.log == nil {
		return
	}

	info, err := s.log.Info(ctx)
	if err != nil {
		s.resumeGap = &Gap{Reason: GapUnresolvable}
		return
	}
	s.epoch = info.Epoch
	s.logID = s.cfg.logID

	raw := ""
	if r != nil {
		raw = r.Header.Get("Last-Event-ID")
	}

	// RF-C12: the token travels in a header, and proxies cap header size. If
	// this session's cursor cannot fit the budget, it declares that it does not
	// support resumption rather than emitting one that will be truncated.
	probe := NewCursor(CursorEntry{Log: s.logID, Epoch: info.Epoch, Offset: ^Offset(0)})
	s.resumable = info.Resumable && probe.Size() <= MaxCursorSize

	if raw == "" {
		return
	}
	if !info.Resumable {
		s.resumeGap = &Gap{Reason: GapUnsupported}
		return
	}

	cursor, err := ParseCursor(raw)
	if err != nil {
		s.resumeGap = &Gap{Reason: GapUnresolvable}
		return
	}
	entry, ok := cursor.Lookup(s.logID)
	if !ok {
		// The client has a position, but not in this log. Nothing was lost
		// here; it simply starts from now.
		s.cursor = cursor
		return
	}
	if entry.Epoch != info.Epoch {
		// The offsets in this token refer to events that no longer exist.
		// Resolving them against current offsets would hand the client
		// unrelated events, which is the silent corruption RF-C5 forbids.
		s.resumeGap = &Gap{Reason: GapEpoch, From: entry.Offset}
		return
	}
	s.cursor = cursor
}
