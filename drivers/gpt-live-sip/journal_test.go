package main

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestJournalBatchSizeAndTimer(t *testing.T) {
	f := newFixture(t)
	j := newJournal(f.d.x, "batch")
	go j.run()
	defer func() { j.cancel(); <-j.done }()
	for i := 0; i < 51; i++ {
		j.row(map[string]any{"event_id": fmt.Sprint(i), "kind": "dtmf", "text": "1", "payload": map[string]any{}})
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if !j.flush(ctx) {
		t.Fatal("flush")
	}
	f.mu.Lock()
	seq := 0
	for _, r := range f.records {
		if strings.HasSuffix(r.path, "/events") {
			events := r.body["events"].([]any)
			if len(events) > 50 {
				t.Fatal("batch cap")
			}
			for _, v := range events {
				if object(v)["event_id"] != fmt.Sprint(seq) {
					t.Fatal("order", seq, v)
				}
				seq++
			}
		}
	}
	f.mu.Unlock()
	if seq != 51 {
		t.Fatal(seq)
	}
	j.row(map[string]any{"kind": "dtmf", "event_id": "timer"})
	before := f.pathCount(sessionPath("batch") + "/events")
	advanceTick(t, f.clock, 250*time.Millisecond)
	eventually(t, func() bool { return f.pathCount(sessionPath("batch")+"/events") == before+1 })
}
func TestJournalRetriesPreserveOrderBeforeState(t *testing.T) {
	f := newFixture(t)
	f.mu.Lock()
	f.journalFailures = 1
	f.mu.Unlock()
	j := newJournal(f.d.x, "retry")
	go j.run()
	defer func() { j.cancel(); <-j.done }()
	j.row(map[string]any{"kind": "dtmf", "event_id": "first"})
	j.state(map[string]any{"status": "ended"})
	eventually(t, func() bool { return f.pathCount(sessionPath("retry")+"/events") == 1 })
	if f.pathCount(sessionPath("retry")+"/state") != 0 {
		t.Fatal("state overtook retry")
	}
	advanceFrom(t, f.clock, 250*time.Millisecond, "retry")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if !j.flush(ctx) {
		t.Fatal("flush")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	last := ""
	for _, r := range f.records {
		last = r.path
	}
	if !strings.HasSuffix(last, "/state") {
		t.Fatal("state order", last)
	}
}
func TestJournalOversizedRowAndPostCap(t *testing.T) {
	f := newFixture(t)
	j := newJournal(f.d.x, "big")
	go j.run()
	defer func() { j.cancel(); <-j.done }()
	j.row(map[string]any{"kind": "system", "event_id": "huge", "payload": map[string]any{"text": strings.Repeat("x", maxJSON)}})
	for i := 0; i < 3; i++ {
		j.row(map[string]any{"kind": "transcript", "event_id": fmt.Sprint(i), "text": strings.Repeat("x", maxJSON/2), "payload": map[string]any{}})
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if !j.flush(ctx) {
		t.Fatal("flush")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, r := range f.records {
		if strings.HasSuffix(r.path, "/events") {
			for _, v := range r.body["events"].([]any) {
				row := object(v)
				if row["event_id"] == "huge" && object(row["payload"])["code"] != "journal_event_too_large" {
					t.Fatal(row)
				}
			}
		}
	}
}

func TestJournal4xxIsFinalIncludingState409(t *testing.T) {
	f := newFixture(t)
	calls := make(chan int, 4)
	logs := make(chan int, 4)
	server := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		status := 403
		if strings.HasSuffix(r.URL.Path, "/state") {
			status = 409
		}
		calls <- status
		w.WriteHeader(status)
	}))
	defer server.Close()
	cfg := *f.d.cfg
	cfg.exchangeURL = server.URL
	x := newExchange(&cfg, f.clock, func(_ string, fields map[string]any) {
		if len(fields) != 1 {
			t.Error("refusal log contains more than HTTP code")
		}
		logs <- fields["status"].(int)
	})
	x.client.Transport = testTransport()
	j := newJournal(x, "final")
	go j.run()
	defer func() { j.cancel(); <-j.done }()
	j.row(map[string]any{"kind": "dtmf"})
	j.state(map[string]any{"status": "ended"})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if !j.flush(ctx) {
		t.Fatal("final refusal retried")
	}
	if len(calls) != 2 || len(logs) != 2 {
		t.Fatal(len(calls), len(logs))
	}
}
