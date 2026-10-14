package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

type journalOp struct {
	row     map[string]any
	state   map[string]any
	barrier chan struct{}
}

// One writer owns every exchange event/state operation for a session. A state
// operation is a flush barrier, so ended can never overtake buffered events.
type journal struct {
	guard  func() bool
	x      *exchange
	id     string
	mu     sync.Mutex
	queue  []journalOp
	wake   chan struct{}
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

func newJournal(x *exchange, id string) *journal {
	ctx, cancel := context.WithCancel(context.Background())
	return &journal{x: x, id: id, wake: make(chan struct{}, 1), ctx: ctx, cancel: cancel, done: make(chan struct{})}
}
func (j *journal) add(op journalOp) {
	j.mu.Lock()
	j.queue = append(j.queue, op)
	n := len(j.queue)
	j.mu.Unlock()
	if n >= 50 || op.state != nil || op.barrier != nil {
		select {
		case j.wake <- struct{}{}:
		default:
		}
	}
}
func (j *journal) row(row map[string]any) {
	if row != nil {
		j.add(journalOp{row: boundedRow(row)})
	}
}
func boundedRow(row map[string]any) map[string]any {
	b, _ := json.Marshal(row)
	if len(b) < maxJSON-128 {
		return row
	}
	// The engine permits 8 MiB, but the exchange admits 1 MiB. Preserve identity
	// and a digest rather than silently truncating a transcript or sending an
	// invalid exchange request. SSE still carries the complete mapped event.
	sum := sha256.Sum256(b)
	r := map[string]any{"kind": "error", "payload": map[string]any{"code": "journal_event_too_large", "original_kind": row["kind"], "bytes": len(b), "sha256": fmt.Sprintf("%x", sum)}}
	if id, ok := row["event_id"].(string); ok {
		if len(id) <= 1024 {
			r["event_id"] = id
		} else {
			digest := sha256.Sum256([]byte(id))
			r["event_id"] = fmt.Sprintf("srw_oversized_%x", digest)
		}
	}
	return r
}
func (j *journal) state(state map[string]any) { j.add(journalOp{state: state}) }
func (j *journal) flush(ctx context.Context) bool {
	b := make(chan struct{})
	j.add(journalOp{barrier: b})
	select {
	case <-b:
		return true
	case <-j.done:
		return false
	case <-ctx.Done():
		return false
	}
}
func (j *journal) run() {
	defer close(j.done)
	for {
		select {
		case <-j.ctx.Done():
			return
		case <-j.wake:
		case <-j.x.clock.After(250 * time.Millisecond):
		}
		j.mu.Lock()
		ops := j.queue
		j.queue = nil
		j.mu.Unlock()
		rows := []map[string]any{}
		size := 14
		flush := func() bool {
			if len(rows) == 0 {
				return true
			}
			ok := j.retry(sessionPath(j.id)+"/events", map[string]any{"events": rows})
			rows = nil
			size = 14
			return ok
		}
		for _, op := range ops {
			if op.row != nil {
				b, _ := json.Marshal(op.row)
				if len(rows) >= 50 || size+len(b)+1 > maxJSON {
					if !flush() {
						return
					}
				}
				rows = append(rows, op.row)
				size += len(b) + 1
				continue
			}
			if !flush() {
				return
			}
			if op.state != nil && !j.retry(sessionPath(j.id)+"/state", op.state) {
				return
			}
			if op.barrier != nil {
				close(op.barrier)
			}
		}
		if !flush() {
			return
		}
	}
}

func (j *journal) retry(path string, body any) bool {
	delay := 250 * time.Millisecond
	for j.ctx.Err() == nil {
		if j.guard != nil && !j.guard() {
			return false
		}
		status, err := j.x.post(j.ctx, path, body, nil)
		if err == nil && status < 500 {
			if status >= 400 {
				j.x.log("exchange_refused", map[string]any{"status": status})
			}
			return true
		}
		if !pause(j.ctx, j.x.clock, delay) {
			return false
		}
		delay = nextBackoff(delay)
	}
	return false
}
