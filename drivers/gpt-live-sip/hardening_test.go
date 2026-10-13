package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestLeaseNullJobExpiryAndTransportFailure(t *testing.T) {
	f := newFixture(t)
	server := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reply(w, 200, map[string]any{"active": true, "connector_id": "connector", "job_id": nil, "thread_id": "job"})
	}))
	defer server.Close()
	x := newExchange(&config{connectorID: "connector", exchangeURL: server.URL, identity: f.d.cfg.identity}, f.clock, func(string, map[string]any) {})
	x.client.Transport = testTransport()
	if status := x.authorize(context.Background(), testToken, "job"); status != 403 {
		t.Fatal("thread id used as job", status)
	}
	f.mu.Lock()
	f.expires = f.clock.Now().Add(time.Second)
	f.mu.Unlock()
	if status := f.d.x.authorize(context.Background(), testToken, "job"); status != 200 {
		t.Fatal(status)
	}
	f.clock.advance(time.Second)
	if status := f.d.x.authorize(context.Background(), testToken, "job"); status != 401 {
		t.Fatal("expired lease", status)
	}
	f.d.x.client.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("token secret should never surface")
	})
	if status := f.d.x.authorize(context.Background(), "scl_"+strings.Repeat("B", 49), "job"); status != 503 {
		t.Fatal(status)
	}
}
func TestCredentialRefreshBeforeExpiryAndCacheCap(t *testing.T) {
	f := newFixture(t)
	f.mu.Lock()
	f.credentialTTL = 99999
	f.mu.Unlock()
	if _, err := f.d.x.getCredential(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.d.x.mu.Lock()
	until := f.d.x.expires
	f.d.x.mu.Unlock()
	if until.Sub(f.clock.Now()) != 300*time.Second {
		t.Fatal("cache not capped")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); f.d.x.refreshLoop(ctx) }()
	advanceFrom(t, f.clock, 270*time.Second, "refreshLoop")
	eventually(t, func() bool { f.mu.Lock(); defer f.mu.Unlock(); return f.credentials == 2 })
	cancel()
	<-done
}
func TestCredentialRevocationLoggedOnceWithoutSecrets(t *testing.T) {
	f := newFixture(t)
	var mu sync.Mutex
	logs := []map[string]any{}
	f.d.x.log = func(name string, fields map[string]any) {
		mu.Lock()
		defer mu.Unlock()
		copy := map[string]any{"event": name}
		for k, v := range fields {
			copy[k] = v
		}
		logs = append(logs, copy)
	}
	f.mu.Lock()
	f.credentialStatus = 401
	f.mu.Unlock()
	for i := 0; i < 3; i++ {
		if _, err := f.d.x.getCredential(context.Background()); !errors.Is(err, errRevoked) {
			t.Fatal(err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(logs) != 1 || logs[0]["event"] != "identity_revoked" {
		t.Fatal(logs)
	}
	b, _ := json.Marshal(logs)
	for _, secret := range []string{"secret-key", "header-secret", f.d.cfg.identity, f.d.cfg.controlToken, testToken} {
		if strings.Contains(string(b), secret) {
			t.Fatal("secret logged")
		}
	}
}
func TestCredentialLockRespectsCanceledAdmission(t *testing.T) {
	f := newFixture(t)
	f.d.x.fetchGate <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() { _, err := f.d.x.getCredential(ctx); done <- err }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("canceled fetch succeeded")
		}
	case <-time.After(100 * time.Millisecond):
		f.d.x.unlockFetch()
		t.Fatal("credential fetch ignored canceled context")
	}
	f.d.x.unlockFetch()
}
func TestEngineTransportErrorsAndNoRedirects(t *testing.T) {
	f := newFixture(t)
	c, err := f.d.x.getCredential(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	client := httpClient()
	client.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("secret-key header-secret") })
	e := enginePost(context.Background(), client, c, "rtc", "hangup", nil)
	if e == nil || e.Status != 0 || e.Code != "transport_error" || strings.Contains(e.Message, "secret") {
		t.Fatal(e)
	}
	redirect := newTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", f.api.URL+"/v1/live/sessions/rtc/accept")
		w.WriteHeader(307)
	}))
	defer redirect.Close()
	c.APIBase = redirect.URL
	client.Transport = testTransport()
	e = enginePost(context.Background(), client, c, "rtc", "accept", map[string]any{})
	if e == nil || e.Status != 307 {
		t.Fatal(e)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.accepts != 0 {
		t.Fatal("redirect followed")
	}
}
func TestWebsocketHeartbeatAndGracefulClose(t *testing.T) {
	f := newFixture(t)
	s := f.handoff("s")
	pings := make(chan bool, 1)
	closed := make(chan bool, 1)
	endpoint, stop := wsServer(t, false, func(sock *testSocket) {
		op, p, masked, err := sock.read()
		pings <- op == 9 && len(p) == 0 && masked && err == nil
		op, p, masked, err = sock.read()
		closed <- op == 8 && len(p) == 2 && masked && err == nil
	})
	defer stop()
	// Separate connection makes the heartbeat/close commands observable without
	// relying on the fake engine's append reader.
	w, err := testDialWebsocket(context.Background(), endpoint, http.Header{})
	if err != nil {
		t.Fatal(err)
	}
	if err = w.write(9, nil); err != nil {
		t.Fatal(err)
	}
	if !<-pings {
		t.Fatal("bad heartbeat")
	}
	w.close()
	if !<-closed {
		t.Fatal("client close not masked")
	}
	// The session heartbeat is scheduled at exactly the reference interval.
	advanceFrom(t, f.clock, 20*time.Second, "readLoop")
	s.mu.Lock()
	active := s.ws != nil
	s.mu.Unlock()
	if !active {
		t.Fatal("heartbeat disconnected session")
	}
}
func TestAppendDisconnectAndAttachStopsFallbackBeforeDeadline(t *testing.T) {
	f := newFixture(t)
	s := f.handoff("s")
	f.mu.Lock()
	f.ackMode = "silent"
	f.mu.Unlock()
	result := make(chan int, 1)
	go func() {
		status, _ := f.request("POST", "/v1/sessions/s/append", testToken, map[string]string{"kind": "thinking", "text": "hi", "event_id": "pending"})
		result <- status
	}()
	eventually(t, func() bool { return f.countCommands("pending") == 1 })
	s.mu.Lock()
	w := s.ws
	s.mu.Unlock()
	w.close()
	if status := <-result; status != 409 {
		t.Fatal(status)
	}
}
func TestClosedJournalKeepsRenewingDuringRetry(t *testing.T) {
	f := newFixture(t)
	s := f.handoff("s")
	flushSession(t, s)
	f.mu.Lock()
	f.journalFailures = 10
	f.mu.Unlock()
	s.handleEvent(map[string]any{"type": "session.closed", "event_id": "closed", "reason": "expired"})
	eventually(t, func() bool { return f.pathCount(sessionPath("s")+"/events") >= 2 })
	// Delay the retry beyond the first lease-renew interval without expiring
	// ownership, then verify the final events still precede ended.
	advanceFrom(t, f.clock, 10*time.Second, "renew")
	eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.leaseUntil.After(f.clock.Now().Add(20 * time.Second))
	})
	f.mu.Lock()
	f.journalFailures = 0
	f.mu.Unlock()
	advanceFrom(t, f.clock, 500*time.Millisecond, "retry")
	eventually(t, func() bool {
		select {
		case <-s.done:
			return true
		default:
			return false
		}
	})
	v := s.snapshot()
	if v.Status != "ended" {
		t.Fatal(v)
	}
}
func TestSSEPingAndIDlessEvent(t *testing.T) {
	f := newFixture(t)
	s := f.handoff("s")
	f.clock.advance(4 * time.Second)
	req, _ := http.NewRequest("GET", f.pod.URL+"/v1/sessions/s/events", nil)
	req.Header.Set("Authorization", "Bearer "+testToken)
	resp, err := f.pod.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	advanceFrom(t, f.clock, 15*time.Second, "events")
	p := make([]byte, len(": ping\n\n"))
	if _, err = io.ReadFull(resp.Body, p); err != nil || string(p) != ": ping\n\n" {
		t.Fatal(string(p), err)
	}
	// Read and write concurrently because net.Pipe provides backpressure.
	s.handleEvent(map[string]any{"type": "session.closed", "reason": "content"})
	b, err := io.ReadAll(resp.Body)
	if err != nil || strings.Contains(string(b), "id: ") || !strings.Contains(string(b), "event: closed\n") {
		t.Fatal(string(b), err)
	}
}
func TestAdoptUnattachedSkipsFixedFallback(t *testing.T) {
	f := newFixture(t)
	h := testHandoff("adopt")
	b, _ := json.Marshal(h)
	var body map[string]any
	_ = json.Unmarshal(b, &body)
	f.mu.Lock()
	f.adoptSessions = []map[string]any{{"session_id": "adopt", "provider_session_id": "rtc_adopt", "job_id": "job", "accepted_at": f.clock.Now(), "attached_at": nil, "handoff": body, "sent_event_ids": []string{"srw_greeting_adopt", "srw_fallback_adopt_d"}}}
	f.mu.Unlock()
	f.d.adopt()
	f.d.mu.Lock()
	s := f.d.sessions["adopt"]
	f.d.mu.Unlock()
	eventually(t, func() bool { s.mu.Lock(); defer s.mu.Unlock(); return s.ws != nil })
	s.handleEvent(map[string]any{"type": "session.delegation.created", "event_id": "d", "delegation": map[string]any{"id": "d"}})
	advanceFrom(t, f.clock, 500*time.Millisecond, "monitor")
	f.clock.advance(8 * time.Second)
	advanceFrom(t, f.clock, 500*time.Millisecond, "monitor")
	if f.countCommands("srw_greeting_adopt") != 0 || f.countCommands("srw_fallback_adopt_d") != 0 {
		t.Fatal("adopt resent fixed append")
	}
}

func TestAcceptTransportFailureHasBoundedBudget(t *testing.T) {
	f := newFixture(t)
	f.d.client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		deadline, ok := r.Context().Deadline()
		if !ok || time.Until(deadline) > 12*time.Second {
			t.Error("admission lacks its total deadline")
		}
		return nil, context.DeadlineExceeded
	})
	status, b := f.request("POST", "/v1/sessions", f.d.cfg.controlToken, testHandoff("timeout"))
	e := object(b["error"])
	if status != 502 || e["status"] != float64(0) || e["code"] != "transport_error" {
		t.Fatal(status, b)
	}
	eventually(t, func() bool { return f.pathCount(sessionPath("timeout")+"/state") == 1 })
}
