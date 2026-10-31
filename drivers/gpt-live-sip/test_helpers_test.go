package main

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

type timerWait struct {
	at    time.Time
	ch    chan time.Time
	owner string
}
type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []timerWait
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)}
}
func (c *fakeClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *fakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	pcs := make([]uintptr, 12)
	n := runtime.Callers(2, pcs)
	frames := runtime.CallersFrames(pcs[:n])
	owner := ""
	for {
		frame, more := frames.Next()
		owner += frame.Function + " "
		if !more {
			break
		}
	}
	c.timers = append(c.timers, timerWait{c.now.Add(d), ch, owner})
	return ch
}
func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	remaining := c.timers[:0]
	for _, w := range c.timers {
		if !c.now.Before(w.at) {
			w.ch <- c.now
		} else {
			remaining = append(remaining, w)
		}
	}
	c.timers = remaining
}
func (c *fakeClock) waiting(d time.Duration) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, w := range c.timers {
		if w.at.Equal(c.now.Add(d)) {
			return true
		}
	}
	return false
}
func eventually(t *testing.T, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !f() {
		if time.Now().After(deadline) {
			t.Fatal("condition did not become true")
		}
		time.Sleep(time.Millisecond)
	}
}
func advanceTick(t *testing.T, c *fakeClock, d time.Duration) {
	t.Helper()
	eventually(t, func() bool { return c.waiting(d) })
	c.advance(d)
}

type testSocket struct {
	conn net.Conn
	r    *bufio.Reader
	mu   sync.Mutex
}

func upgrade(w http.ResponseWriter, r *http.Request, bad bool) (*testSocket, error) {
	conn, rw, err := w.(http.Hijacker).Hijack()
	if err != nil {
		return nil, err
	}
	sum := sha1.Sum([]byte(r.Header.Get("Sec-WebSocket-Key") + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	accept := base64.StdEncoding.EncodeToString(sum[:])
	if bad {
		accept = "wrong"
	}
	_, err = io.WriteString(rw, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: "+accept+"\r\n\r\n")
	if err == nil {
		err = rw.Flush()
	}
	return &testSocket{conn: conn, r: rw.Reader}, err
}
func serverFrame(op byte, fin bool, p []byte) []byte {
	first := op
	if fin {
		first |= 128
	}
	h := []byte{first}
	n := len(p)
	switch {
	case n < 126:
		h = append(h, byte(n))
	case n <= 65535:
		h = append(h, 126, byte(n>>8), byte(n))
	default:
		h = append(h, 127)
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(n))
		h = append(h, b[:]...)
	}
	return append(h, p...)
}
func (s *testSocket) sendFrame(op byte, fin bool, p []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.conn.Write(serverFrame(op, fin, p))
	return err
}
func (s *testSocket) send(e map[string]any) error {
	b, _ := json.Marshal(e)
	return s.sendFrame(1, true, b)
}
func (s *testSocket) read() (byte, []byte, bool, error) {
	var h [2]byte
	if _, err := io.ReadFull(s.r, h[:]); err != nil {
		return 0, nil, false, err
	}
	masked := h[1]&128 != 0
	n := uint64(h[1] & 127)
	if n == 126 {
		var b [2]byte
		if _, err := io.ReadFull(s.r, b[:]); err != nil {
			return 0, nil, false, err
		}
		n = uint64(binary.BigEndian.Uint16(b[:]))
	}
	if n == 127 {
		var b [8]byte
		if _, err := io.ReadFull(s.r, b[:]); err != nil {
			return 0, nil, false, err
		}
		n = binary.BigEndian.Uint64(b[:])
	}
	var mask [4]byte
	if masked {
		if _, err := io.ReadFull(s.r, mask[:]); err != nil {
			return 0, nil, false, err
		}
	}
	if n > maxMessage {
		return 0, nil, masked, errWSCap
	}
	p := make([]byte, int(n))
	if _, err := io.ReadFull(s.r, p); err != nil {
		return 0, nil, masked, err
	}
	if masked {
		for i := range p {
			p[i] ^= mask[i%4]
		}
	}
	return h[0] & 15, p, masked, nil
}
func testConfig() *config {
	return &config{connectorID: "connector", identity: "sdi_" + strings.Repeat("A", 49), controlToken: strings.Repeat("C", 32), port: "8080", limits: limits{900, 8}, transferTarget: "sip:default@example"}
}
func testHandoff(id string) handoff {
	h := handoff{SessionID: id, ProviderID: "rtc_" + id, JobID: "job", Model: "gpt-live-1", Greeting: "Greet now"}
	h.Accept.Instructions = "Instructions"
	h.Accept.Voice = "marin"
	h.Texts.Fallback = "Please wait"
	return h
}

const testToken = "scl_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

type recordedRequest struct {
	path string
	body map[string]any
}
type fixture struct {
	t                                                      *testing.T
	d                                                      *driver
	clock                                                  *fakeClock
	api, exchange, pod                                     *httptest.Server
	mu                                                     sync.Mutex
	records                                                []recordedRequest
	commands                                               []map[string]any
	sockets                                                []*testSocket
	active                                                 bool
	authStatus                                             int
	connector, job                                         string
	expires                                                time.Time
	introspects, credentials, adoptions, accepts, attaches int
	owned                                                  bool
	engineStatus, attachStatus, credentialStatus           int
	credentialTTL                                          int
	ackMode                                                string
	adoptSessions                                          []map[string]any
	journalFailures                                        int
	hangupFailures                                         int
	closeOnHangup                                          bool
	attachTimes                                            []time.Time
	replay                                                 []map[string]any // sent on every attach
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{t: t, clock: newFakeClock(), active: true, authStatus: 200, connector: "connector", job: "job", owned: true, credentialTTL: 300, ackMode: "ack"}
	f.api = newTestServer(http.HandlerFunc(f.engine))
	f.exchange = newTestServer(http.HandlerFunc(f.exchangeHandler))
	cfg := testConfig()
	cfg.exchangeURL = f.exchange.URL
	f.d = newDriver(cfg, f.clock, func(string, map[string]any) {})
	f.d.client.Transport = testTransport()
	f.d.x.client.Transport = testTransport()
	f.d.dialWS = testDialWebsocket
	f.pod = newTestServer(f.d)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		f.d.shutdown(ctx)
		f.mu.Lock()
		for _, s := range f.sockets {
			s.conn.Close()
		}
		f.mu.Unlock()
		f.pod.Close()
		f.api.Close()
		f.exchange.Close()
	})
	return f
}
func (f *fixture) exchangeHandler(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "Bearer "+f.d.cfg.identity {
		f.t.Errorf("wrong exchange identity")
	}
	raw, err := io.ReadAll(r.Body)
	if err != nil || len(raw) > maxJSON {
		f.t.Error("invalid exchange body")
	}
	var b map[string]any
	_ = json.Unmarshal(raw, &b)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.records = append(f.records, recordedRequest{r.URL.Path, b})
	switch {
	case r.URL.Path == "/v1/live/credential":
		f.credentials++
		if f.credentialStatus != 0 {
			w.WriteHeader(f.credentialStatus)
			return
		}
		reply(w, 200, credential{APIKey: "secret-key", APIBase: f.api.URL + "/v1", WSBase: "ws" + strings.TrimPrefix(f.api.URL, "http") + "/v1", Headers: map[string]string{"X-Test": "header-secret"}, CacheSeconds: f.credentialTTL})
	case r.URL.Path == "/v1/leases/introspect":
		f.introspects++
		if f.authStatus != 200 {
			w.WriteHeader(f.authStatus)
			return
		}
		var exp any
		if !f.expires.IsZero() {
			exp = f.expires
		}
		reply(w, 200, map[string]any{"active": f.active, "connector_id": f.connector, "job_id": f.job, "thread_id": "thread", "expires_at": exp})
	case r.URL.Path == "/v1/live/sessions/adopt":
		f.adoptions++
		reply(w, 200, map[string]any{"sessions": f.adoptSessions})
	case strings.HasSuffix(r.URL.Path, "/lease"):
		reply(w, 200, map[string]any{"owned": f.owned, "lease_until": f.clock.Now().Add(30 * time.Second)})
	case strings.HasSuffix(r.URL.Path, "/events"):
		if f.journalFailures > 0 {
			f.journalFailures--
			w.WriteHeader(500)
			return
		}
		reply(w, 200, map[string]int{"inserted": 1})
	default:
		reply(w, 200, map[string]string{"status": "active"})
	}
}
func (f *fixture) engine(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "Bearer secret-key" || r.Header.Get("X-Test") != "header-secret" {
		f.t.Error("engine credentials not supplied")
	}
	if strings.HasSuffix(r.URL.Path, "/attach") {
		f.mu.Lock()
		f.attaches++
		f.attachTimes = append(f.attachTimes, f.clock.Now())
		status := f.attachStatus
		f.mu.Unlock()
		if status != 0 {
			w.WriteHeader(status)
			return
		}
		sock, err := upgrade(w, r, false)
		if err != nil {
			return
		}
		defer sock.conn.Close()
		f.mu.Lock()
		f.sockets = append(f.sockets, sock)
		replay := append([]map[string]any(nil), f.replay...)
		f.mu.Unlock()
		for _, e := range replay {
			_ = sock.send(e)
		}
		for {
			op, p, masked, err := sock.read()
			if err != nil {
				return
			}
			if !masked {
				f.t.Error("client frame unmasked")
			}
			if op == 9 {
				_ = sock.sendFrame(10, true, p)
				continue
			}
			if op == 8 {
				return
			}
			if op != 1 {
				continue
			}
			var cmd map[string]any
			_ = json.Unmarshal(p, &cmd)
			f.mu.Lock()
			f.commands = append(f.commands, cmd)
			mode := f.ackMode
			f.mu.Unlock()
			kind := strings.Split(str(cmd["type"]), ".")[1]
			id := str(cmd["event_id"])
			if mode == "ack" {
				_ = sock.send(map[string]any{"type": "session." + kind + ".appended", "event_id": "ack_" + id, "client_event_id": id})
			}
			if mode == "error" {
				_ = sock.send(map[string]any{"type": "error", "event_id": "err_" + id, "error": map[string]any{"code": "refused", "message": "refusal", "client_event_id": id}})
			}
		}
	}
	var b map[string]any
	if r.ContentLength != 0 {
		_ = json.NewDecoder(r.Body).Decode(&b)
	}
	f.mu.Lock()
	f.records = append(f.records, recordedRequest{r.URL.Path, b})
	status := f.engineStatus
	if strings.HasSuffix(r.URL.Path, "/accept") {
		f.accepts++
	}
	if strings.HasSuffix(r.URL.Path, "/hangup") && f.hangupFailures > 0 {
		f.hangupFailures--
		status = 500
	}
	closeCall := strings.HasSuffix(r.URL.Path, "/hangup") && f.closeOnHangup && status == 0
	f.mu.Unlock()
	if status != 0 {
		reply(w, status, map[string]any{"error": map[string]any{"code": "decision_already_made", "message": "secret-key header-secret Instructions"}})
		return
	}
	if closeCall {
		f.emit(map[string]any{"type": "session.closed", "event_id": "close_hangup", "reason": "close_requested", "usage": map[string]any{"seconds": 42.0}})
	}
	w.WriteHeader(200)
}
func (f *fixture) request(method, path, token string, body any) (int, map[string]any) {
	f.t.Helper()
	var reader io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		reader = strings.NewReader(string(b))
	}
	req, _ := http.NewRequest(method, f.pod.URL+path, reader)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := f.pod.Client().Do(req)
	if err != nil {
		f.t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}
func (f *fixture) handoff(id string) *session {
	f.t.Helper()
	status, out := f.request("POST", "/v1/sessions", f.d.cfg.controlToken, testHandoff(id))
	if status != 200 {
		f.t.Fatalf("handoff %d %v", status, out)
	}
	f.d.mu.Lock()
	s := f.d.sessions[id]
	f.d.mu.Unlock()
	eventually(f.t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.ws != nil && s.seen.seen["ack_srw_greeting_"+id]
	})
	return s
}
func (f *fixture) emit(raw map[string]any) {
	f.mu.Lock()
	ss := append([]*testSocket(nil), f.sockets...)
	f.mu.Unlock()
	for _, s := range ss {
		_ = s.send(raw)
	}
}
func (f *fixture) countCommands(id string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.commands {
		if c["event_id"] == id {
			n++
		}
	}
	return n
}
func (f *fixture) pathCount(path string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, r := range f.records {
		if r.path == path {
			n++
		}
	}
	return n
}
func flushSession(t *testing.T, s *session) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if !s.journal.flush(ctx) {
		t.Fatal("journal flush failed")
	}
}

// httptest over net.Pipe exercises the complete HTTP parser, hijacking and
// WebSocket framing without requiring a kernel listening socket in CI sandboxes.
type pipeAddr string

func (a pipeAddr) Network() string { return "tcp" }
func (a pipeAddr) String() string  { return string(a) }

type pipeListener struct {
	address pipeAddr
	conns   chan net.Conn
	done    chan struct{}
	once    sync.Once
}

func (l *pipeListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}
func (l *pipeListener) Close() error   { l.once.Do(func() { close(l.done) }); return nil }
func (l *pipeListener) Addr() net.Addr { return l.address }

var pipeRegistry = struct {
	sync.Mutex
	next    int
	servers map[string]*pipeListener
}{servers: map[string]*pipeListener{}}

func newTestServer(h http.Handler) *httptest.Server {
	pipeRegistry.Lock()
	pipeRegistry.next++
	addr := fmt.Sprintf("127.0.0.1:%d", 10000+pipeRegistry.next)
	l := &pipeListener{address: pipeAddr(addr), conns: make(chan net.Conn), done: make(chan struct{})}
	pipeRegistry.servers[addr] = l
	pipeRegistry.Unlock()
	s := &httptest.Server{Listener: l, Config: &http.Server{Handler: h}}
	s.Start()
	s.Client().Transport = testTransport()
	return s
}
func testDial(ctx context.Context, network, addr string) (net.Conn, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	pipeRegistry.Lock()
	l := pipeRegistry.servers[addr]
	pipeRegistry.Unlock()
	if l == nil {
		return nil, errors.New("unknown test server")
	}
	client, server := net.Pipe()
	select {
	case l.conns <- server:
		return client, nil
	case <-ctx.Done():
		client.Close()
		server.Close()
		return nil, ctx.Err()
	case <-l.done:
		client.Close()
		server.Close()
		return nil, net.ErrClosed
	}
}
func testTransport() *http.Transport {
	return &http.Transport{DialContext: testDial, DisableKeepAlives: true}
}
func testDialWebsocket(ctx context.Context, endpoint string, headers http.Header) (*websocket, error) {
	return dialWebsocketWithDial(ctx, endpoint, headers, testDial)
}

// Wait for this operation's timer registration before advancing the shared
// clock: a concurrent batch/heartbeat timer must not satisfy that precondition.
func advanceFrom(t *testing.T, c *fakeClock, d time.Duration, owner string) {
	t.Helper()
	waitTimer(t, c, d, owner)
	c.advance(d)
}
func waitTimer(t *testing.T, c *fakeClock, d time.Duration, owner string) {
	t.Helper()
	eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, w := range c.timers {
			if w.at.Equal(c.now.Add(d)) && strings.Contains(w.owner, "."+owner) {
				return true
			}
		}
		return false
	})
}
