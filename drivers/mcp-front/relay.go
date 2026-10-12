package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// The largest answer, and the largest stream event, the front relays.
	// Each is buffered whole (to filter and scrub it), so it is bounded.
	maxResponseBody = 4 << 20
	maxEventBytes   = 4 << 20
	bufferUnit      = 1 << 20
	// The bytes the relays hold at once across every request, in units.
	// Each answer (or stream event) claims what reading and cleaning it
	// can hold at its peak (its buffer as it grows, the scrubber's view
	// and copy, the rewrite's decoded members and output), so the heap
	// holds at most this much of them, whatever the servers send.
	bufferBudgetUnits = 96
	budgetWait        = 10 * time.Second
	// A buffer starts this small and doubles: a short event costs little.
	initialBuffer = 4 << 10
	// An answer of at most smallAnswer bytes claims smallUnits; a larger
	// one gives that back and claims largeUnits at once (never waiting
	// while it holds units, so answers cannot starve one another).
	smallAnswer = 192 << 10
	smallUnits  = 1
	largeUnits  = 24
)

var (
	errBusy       = errors.New("the front is at its buffer budget")
	errUnreadable = errors.New("the server's answer is unreadable")
	errTooLarge   = errors.New("the server's answer is too large")
)

// budget bounds the bytes the front buffers at once. One taker takes its
// units at a time, so two never hold part of what each needs at once.
type budget struct {
	mu     sync.Mutex
	tokens chan struct{}
}

func newBudget(units int) *budget {
	b := &budget{tokens: make(chan struct{}, units)}
	for i := 0; i < units; i++ {
		b.tokens <- struct{}{}
	}
	return b
}

func unitsFor(size int64) int {
	if size <= 0 || size > maxResponseBody {
		size = maxResponseBody
	}
	return int((size + bufferUnit - 1) / bufferUnit)
}

// take takes units, all or none, waiting at most budgetWait.
func (b *budget) take(ctx context.Context, units int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, budgetWait)
	defer cancel()
	for got := 0; got < units; got++ {
		select {
		case <-b.tokens:
		case <-ctx.Done():
			b.give(got)
			return false
		}
	}
	return true
}

func (b *budget) give(units int) {
	for i := 0; i < units; i++ {
		b.tokens <- struct{}{}
	}
}

func (b *budget) acquire(ctx context.Context, units int) (func(), bool) {
	if !b.take(ctx, units) {
		return nil, false
	}
	return func() { b.give(units) }, true
}

// allotment is what one answer (or one stream event) holds of the budget:
// nothing until its first bytes, then the small claim, then (past
// smallAnswer) the large one; given back whole once the answer is written.
type allotment struct {
	budget *budget
	ctx    context.Context
	units  int
	large  bool
}

func (b *budget) allot(ctx context.Context) *allotment {
	return &allotment{budget: b, ctx: ctx}
}

// fit makes the claim cover an answer of n bytes. Moving to the large
// claim, it gives the small one back before it waits.
func (a *allotment) fit(n int) error {
	switch {
	case a.large:
		return nil
	case n <= smallAnswer:
		if a.units == 0 {
			if !a.budget.take(a.ctx, smallUnits) {
				return errBusy
			}
			a.units = smallUnits
		}
		return nil
	}
	a.release()
	if !a.budget.take(a.ctx, largeUnits) {
		return errBusy
	}
	a.units, a.large = largeUnits, true
	return nil
}

func (a *allotment) release() {
	a.budget.give(a.units)
	a.units, a.large = 0, false
}

// reserve makes room in buf for n more bytes, doubling its capacity (never
// past limit), within the answer's claim.
func reserve(buf []byte, n, limit int, allot *allotment) ([]byte, error) {
	need := len(buf) + n
	if need > limit {
		return buf, errTooLarge
	}
	if err := allot.fit(need); err != nil {
		return buf, err
	}
	if need <= cap(buf) {
		return buf, nil
	}
	size := cap(buf) * 2
	if size < initialBuffer {
		size = initialBuffer
	}
	for size < need {
		size *= 2
	}
	if size > limit {
		size = limit
	}
	grown := make([]byte, len(buf), size)
	copy(grown, buf)
	return grown, nil
}

// readBody reads a JSON answer of at most maxResponseBody bytes into a
// buffer charged to allot (sized from Content-Length when it is given).
func readBody(body io.Reader, length int64, allot *allotment) ([]byte, error) {
	if length > maxResponseBody {
		return nil, errTooLarge
	}
	limit := maxResponseBody + 1 // one byte past: an answer that long is too long
	var buf []byte
	if length > 0 {
		var err error
		if buf, err = reserve(buf, int(length)+1, limit, allot); err != nil {
			return nil, err
		}
	}
	for {
		if len(buf) == cap(buf) {
			var err error
			if buf, err = reserve(buf, 1, limit, allot); err != nil {
				return nil, err
			}
		}
		n, err := body.Read(buf[len(buf):cap(buf)])
		buf = buf[:len(buf)+n]
		if len(buf) > maxResponseBody {
			return nil, errTooLarge
		}
		if errors.Is(err, io.EOF) {
			return buf, nil
		}
		if err != nil {
			return nil, errUnreadable
		}
	}
}

// relayJSON reads one answer within the budget, filters and scrubs it
// (clean) and writes it with its status. It returns the bytes written.
func relayJSON(ctx context.Context, w http.ResponseWriter, response *http.Response, buffers *budget, allowed func(string) bool, scrub *scrubber) (int, error) {
	allot := buffers.allot(ctx)
	defer allot.release()
	raw, err := readBody(response.Body, response.ContentLength, allot)
	if err != nil {
		return 0, err
	}
	if strings.Contains(strings.ToLower(response.Header.Get("Content-Type")), "json") {
		if raw, err = clean(raw, allowed, scrub); err != nil {
			return 0, errUnreadable
		}
	} else {
		raw = scrub.apply(raw)
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(raw)))
	w.WriteHeader(response.StatusCode)
	return w.Write(raw)
}

// relayEvents copies a server-sent event stream event by event, each
// within the budget: an idle stream holds none of it (the budget is taken
// once an event's first byte arrives). Each event's fields other than data
// are written as they came, scrubbed; its data lines are joined and
// cleaned (filtered and scrubbed, the credential also across the lines it
// was split into) and written as data lines, flushed at once. An event is
// bounded by maxEventBytes. An event whose data the front cannot read as
// JSON, where a tool list or an error may hide, is dropped (dropped is
// told of it).
func relayEvents(ctx context.Context, w http.ResponseWriter, body io.Reader, buffers *budget, allowed func(string) bool, scrub *scrubber, dropped func()) (int, error) {
	flusher, _ := w.(http.Flusher)
	reader := bufio.NewReaderSize(body, 64*1024)
	written := 0
	for {
		if _, err := reader.Peek(1); err != nil {
			if errors.Is(err, io.EOF) {
				return written, nil
			}
			return written, err
		}
		allot := buffers.allot(ctx)
		raw, readErr := readEvent(reader, allot)
		if errors.Is(readErr, errTooLarge) || errors.Is(readErr, errBusy) {
			allot.release()
			return written, readErr
		}
		n, err := writeEvent(w, raw, allowed, scrub, dropped)
		allot.release()
		written += n
		if n > 0 && flusher != nil {
			flusher.Flush()
		}
		if err != nil {
			return written, err
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return written, nil
			}
			return written, readErr
		}
	}
}

// readEvent reads the lines of one event, up to the blank line that ends
// it, into one buffer charged to allot: each line without its CR or LF,
// followed by one LF. Blank lines before the first field are skipped.
func readEvent(reader *bufio.Reader, allot *allotment) ([]byte, error) {
	var buf []byte
	start := 0 // where the line being read starts
	for {
		chunk, err := reader.ReadSlice('\n')
		if len(chunk) > 0 {
			var grown error
			if buf, grown = reserve(buf, len(chunk), maxEventBytes, allot); grown != nil {
				return nil, grown
			}
			buf = append(buf, chunk...)
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		buf = buf[:start+len(bytes.TrimRight(buf[start:], "\r\n"))]
		if len(buf) == start {
			if start > 0 || err != nil {
				return buf, err // a blank line ends the event
			}
			continue
		}
		var grown error
		if buf, grown = reserve(buf, 1, maxEventBytes, allot); grown != nil {
			return nil, grown
		}
		buf = append(buf, '\n')
		start = len(buf)
		if err != nil {
			return buf, err
		}
	}
}

// writeEvent writes one event read by readEvent. Its data lines are moved
// to the front of raw in place, joined by LF (what is written never runs
// ahead of what is read), cleaned, and written as data lines; its other
// fields are written first, as they came, each scrubbed.
func writeEvent(w io.Writer, raw []byte, allowed func(string) bool, scrub *scrubber, dropped func()) (int, error) {
	if len(raw) == 0 {
		return 0, nil
	}
	written := 0
	write := func(parts ...[]byte) error {
		for _, part := range parts {
			n, err := w.Write(part)
			written += n
			if err != nil {
				return err
			}
		}
		return nil
	}
	end, hasData := 0, false
	for start := 0; start < len(raw); {
		stop := start + bytes.IndexByte(raw[start:], '\n')
		line := raw[start:stop]
		if value, ok := bytes.CutPrefix(line, []byte("data:")); ok {
			value = bytes.TrimPrefix(value, []byte(" "))
			if hasData {
				raw[end] = '\n'
				end++
			}
			end += copy(raw[end:], value)
			hasData = true
		} else {
			if err := write(scrub.apply(line), newline); err != nil {
				return written, err
			}
		}
		start = stop + 1
	}
	if hasData {
		payload, err := clean(raw[:end], allowed, scrub)
		if err != nil {
			if dropped != nil {
				dropped()
			}
		} else {
			for {
				i := bytes.IndexByte(payload, '\n')
				if i < 0 {
					if err := write(dataPrefix, payload, newline); err != nil {
						return written, err
					}
					break
				}
				if err := write(dataPrefix, payload[:i], newline); err != nil {
					return written, err
				}
				payload = payload[i+1:]
			}
		}
	}
	return written, write(newline)
}

var (
	dataPrefix = []byte("data: ")
	newline    = []byte("\n")
)
