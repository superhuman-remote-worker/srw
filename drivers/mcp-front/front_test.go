package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	connectorID = "66666666-7777-4888-8999-aaaaaaaaaaaa"
	credential  = "upstream-token-0123456789"
)

var (
	tokenA    = "scl_" + strings.Repeat("A", 49)
	tokenB    = "scl_" + strings.Repeat("B", 49)
	tokenRO   = "scl_" + strings.Repeat("C", 49)
	tokenDead = "scl_" + strings.Repeat("D", 49)
	tokenElse = "scl_" + strings.Repeat("E", 49)
	identity  = "sdi_" + strings.Repeat("Z", 49)
)

// fakeAuthority is the lease exchange: per token, the lease introspection
// returns and whether it is refused.
type fakeAuthority struct {
	mu          sync.Mutex
	leases      map[string]lease
	introspects int
	exchanges   []string
	down        bool
}

func newFakeAuthority() *fakeAuthority {
	expires := time.Now().Add(15 * time.Minute)
	return &fakeAuthority{leases: map[string]lease{
		tokenA:  {active: true, id: "lease-a", connectorID: connectorID, access: "ReadWrite", expires: expires},
		tokenB:  {active: true, id: "lease-b", connectorID: connectorID, access: "ReadWrite", expires: expires},
		tokenRO: {active: true, id: "lease-ro", connectorID: connectorID, access: "ReadOnly", expires: expires},
		// A live lease, but of another connector.
		tokenElse: {active: true, id: "lease-e", connectorID: "11111111-2222-4333-8444-555555555555", access: "ReadWrite", expires: expires},
	}}
}

func (a *fakeAuthority) introspect(_ context.Context, token string) (lease, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.introspects++
	if a.down {
		return lease{}, errUnavailable
	}
	return a.leases[token], nil
}

func (a *fakeAuthority) exchange(_ context.Context, token, operation string) (grant, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.exchanges = append(a.exchanges, operation)
	if a.down {
		return grant{}, errUnavailable
	}
	found := a.leases[token]
	if !found.active {
		return grant{status: http.StatusUnauthorized, reason: "unknown_lease"}, nil
	}
	if operation == classWrite && found.access != "ReadWrite" {
		return grant{status: http.StatusForbidden, reason: "operation_not_allowed"}, nil
	}
	return grant{credential: credential, status: http.StatusOK, cache: 30 * time.Second}, nil
}

// fakeServer is a stock MCP server on loopback: it records what reached
// it and answers initialize, tools/list (JSON or a stream) and tools/call.
type fakeServer struct {
	mu        sync.Mutex
	seen      []*http.Request
	bodies    []string
	stream    bool
	status    int
	toolNames []string
	// The GET stream's events (data payloads), and whether it stays open
	// until the caller goes.
	getEvents []string
	holdGet   bool
	// A streamed tools/call sends a progress event and then holds, never
	// answering, until the caller goes.
	holdCall bool
	// A tools/call answered with this body (a JSON-RPC message), framed
	// as an event when stream is set.
	callAnswer string
}

func (s *fakeServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	s.mu.Lock()
	s.seen = append(s.seen, r.Clone(context.Background()))
	s.bodies = append(s.bodies, string(body))
	stream, status := s.stream, s.status
	getEvents, holdGet, holdCall := s.getEvents, s.holdGet, s.holdCall
	callAnswer := s.callAnswer
	s.mu.Unlock()
	if status != 0 {
		w.WriteHeader(status)
		return
	}
	if r.Method == http.MethodDelete {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method == http.MethodGet {
		w.Header().Set("Content-Type", "text/event-stream")
		if len(getEvents) == 0 {
			fmt.Fprintf(w, "data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/message\",\"params\":{\"data\":%q}}\n\n", r.Header.Get("Authorization"))
		}
		for _, event := range getEvents {
			fmt.Fprintf(w, "%s\n\n", event)
		}
		if holdGet {
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		}
		return
	}
	var message map[string]any
	json.Unmarshal(body, &message)
	method, _ := message["method"].(string)
	id := message["id"]
	result := map[string]any{}
	switch method {
	case "initialize":
		w.Header().Set("Mcp-Session-Id", "session-1")
		result = map[string]any{"protocolVersion": "2025-06-18", "capabilities": map[string]any{"tools": map[string]any{}}, "serverInfo": map[string]any{"name": "fake"}}
	case "notifications/initialized":
		w.WriteHeader(http.StatusAccepted)
		return
	case "tools/list":
		tools := []any{}
		for _, name := range s.toolNames {
			tools = append(tools, map[string]any{"name": name, "description": "<" + name + ">", "inputSchema": map[string]any{"type": "object"}})
		}
		result = map[string]any{"tools": tools}
	case "tools/call":
		params, _ := message["params"].(map[string]any)
		name, _ := params["name"].(string)
		text := "called " + name
		if name == "leak_credential" {
			text = "I hold " + r.Header.Get("Authorization")
		}
		result = map[string]any{"content": []any{map[string]any{"type": "text", "text": text}}}
	}
	answer, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
	if callAnswer != "" && method == "tools/call" {
		answer = []byte(callAnswer)
	}
	if stream && holdCall && method == "tools/call" {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "event: message\ndata: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\",\"params\":{\"progressToken\":1,\"progress\":1}}\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		return
	}
	if stream {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(w, ": comment\n\nevent: message\ndata: %s\n\n", answer)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(answer)
}

func (s *fakeServer) last() (*http.Request, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.seen) == 0 {
		return nil, ""
	}
	return s.seen[len(s.seen)-1], s.bodies[len(s.bodies)-1]
}

func (s *fakeServer) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.seen)
}

type harness struct {
	front     *front
	authority *fakeAuthority
	server    *fakeServer
	logs      *strings.Builder
	logMu     *sync.Mutex
	clock     *testClock
}

func testConfig(t *testing.T, upstream string) *config {
	t.Helper()
	request := requestFile{ProtocolVersion: "1.0", Plane: "service", Driver: "srw.mcp-test/v1"}
	request.Connector.ID = connectorID
	request.Service.Port = 8080
	request.Exchange.URL = "http://exchange.srw.svc:8088"
	request.MCP = &mcpBlock{
		Upstream:              upstream,
		Access:                map[string][]string{"ReadOnly": {"read"}, "ReadWrite": {"read", "write"}},
		Credential:            &credentialRule{Header: "Authorization", Scheme: "Bearer"},
		MaxInFlightPerBinding: 2,
	}
	request.MCP.Tools.Read = []string{"whoami", "notes_read", "leak_*"}
	cfg, err := parseConfig(request, identity)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	server := &fakeServer{toolNames: []string{"whoami", "notes_read", "notes_write", "leak_credential", "delete_everything"}}
	upstream := httptest.NewServer(server)
	t.Cleanup(upstream.Close)
	cfg := testConfig(t, upstream.URL+"/mcp")
	now := time.Now()
	clock := &testClock{now: now}
	logs := &strings.Builder{}
	logMu := &sync.Mutex{}
	logf := func(format string, a ...any) {
		logMu.Lock()
		defer logMu.Unlock()
		fmt.Fprintf(logs, format+"\n", a...)
	}
	authority := newFakeAuthority()
	f := newFront(cfg, authority, newUpstreamClient(), logf, clock.Now)
	return &harness{front: f, authority: authority, server: server, logs: logs, logMu: logMu, clock: clock}
}

func (h *harness) logText() string {
	h.logMu.Lock()
	defer h.logMu.Unlock()
	return h.logs.String()
}

func (h *harness) do(t *testing.T, method, token, body string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, "/mcp", strings.NewReader(body))
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json, text/event-stream")
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	recorder := httptest.NewRecorder()
	h.front.ServeHTTP(recorder, request)
	return recorder
}

func rpc(id int, method string, params any) string {
	message := map[string]any{"jsonrpc": "2.0", "id": id, "method": method}
	if params != nil {
		message["params"] = params
	}
	raw, _ := json.Marshal(message)
	return string(raw)
}

func call(name string) string {
	return rpc(7, "tools/call", map[string]any{"name": name, "arguments": map[string]any{"secret_arg": "argument-value"}})
}

// listed reads the tool names of a tools/list answer, JSON or a stream.
func listed(t *testing.T, recorder *httptest.ResponseRecorder) []string {
	t.Helper()
	body := recorder.Body.String()
	if strings.HasPrefix(recorder.Header().Get("Content-Type"), "text/event-stream") {
		scanner := bufio.NewScanner(strings.NewReader(body))
		for scanner.Scan() {
			if data, ok := strings.CutPrefix(scanner.Text(), "data: "); ok {
				body = data
			}
		}
	}
	var answer struct {
		Result struct {
			Tools []struct {
				Name string `json:"name"`
			} `json:"tools"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(body), &answer); err != nil {
		t.Fatalf("not a tools/list answer: %v: %s", err, body)
	}
	names := []string{}
	for _, tool := range answer.Result.Tools {
		names = append(names, tool.Name)
	}
	return names
}

func TestARequestWithoutALeaseIs401AndNeverReachesTheServer(t *testing.T) {
	h := newHarness(t)
	for _, token := range []string{"", "not-a-lease", "srw_" + strings.Repeat("A", 49)} {
		response := h.do(t, http.MethodPost, token, rpc(1, "initialize", nil), nil)
		if response.Code != http.StatusUnauthorized {
			t.Fatalf("token %q: status %d", token, response.Code)
		}
		if !strings.HasPrefix(response.Header().Get("WWW-Authenticate"), "Bearer") {
			t.Fatal("no WWW-Authenticate challenge")
		}
	}
	if h.server.count() != 0 || h.authority.introspects != 0 {
		t.Fatal("a malformed token reached the server or the exchange")
	}
}

func TestADeadLeaseOrAnotherConnectorsLeaseIs401(t *testing.T) {
	h := newHarness(t)
	for _, token := range []string{tokenDead, tokenElse} {
		if code := h.do(t, http.MethodPost, token, rpc(1, "initialize", nil), nil).Code; code != http.StatusUnauthorized {
			t.Fatalf("status %d", code)
		}
	}
	if h.server.count() != 0 {
		t.Fatal("a refused lease reached the server")
	}
}

func TestARefusalIsRememberedBrieflyAndADecisionForTheRevocationLag(t *testing.T) {
	h := newHarness(t)
	h.do(t, http.MethodPost, tokenDead, rpc(1, "initialize", nil), nil)
	h.do(t, http.MethodPost, tokenDead, rpc(1, "initialize", nil), nil)
	if h.authority.introspects != 1 {
		t.Fatalf("introspected %d times", h.authority.introspects)
	}
	h.advance(6 * time.Second)
	h.do(t, http.MethodPost, tokenDead, rpc(1, "initialize", nil), nil)
	if h.authority.introspects != 2 {
		t.Fatal("a refusal was remembered past its window")
	}
	h.do(t, http.MethodPost, tokenA, rpc(1, "initialize", nil), nil)
	h.advance(29 * time.Second)
	h.do(t, http.MethodPost, tokenA, rpc(1, "tools/list", nil), nil)
	if h.authority.introspects != 3 {
		t.Fatal("a live lease was introspected again within the revocation lag")
	}
	h.advance(2 * time.Second)
	h.do(t, http.MethodPost, tokenA, rpc(1, "tools/list", nil), nil)
	if h.authority.introspects != 4 {
		t.Fatal("a live lease was reused past the revocation lag")
	}
}

func TestAnExchangeOutageIs503(t *testing.T) {
	h := newHarness(t)
	h.authority.down = true
	if code := h.do(t, http.MethodPost, tokenA, rpc(1, "initialize", nil), nil).Code; code != http.StatusServiceUnavailable {
		t.Fatalf("status %d", code)
	}
}

func TestARequestWithAnOriginIsRefused(t *testing.T) {
	h := newHarness(t)
	response := h.do(t, http.MethodPost, tokenA, rpc(1, "initialize", nil), map[string]string{"Origin": "https://evil.example"})
	if response.Code != http.StatusForbidden || h.server.count() != 0 {
		t.Fatalf("status %d, server reached %d times", response.Code, h.server.count())
	}
}

func TestTheServerGetsTheCredentialNeverTheLeaseOrTheCallersHeaders(t *testing.T) {
	h := newHarness(t)
	response := h.do(t, http.MethodPost, tokenA, call("whoami"), map[string]string{
		"Cookie":          "session=abc",
		"X-Forwarded-For": "10.0.0.1",
	})
	if response.Code != http.StatusOK {
		t.Fatalf("status %d: %s", response.Code, response.Body)
	}
	seen, body := h.server.last()
	if got := seen.Header.Get("Authorization"); got != "Bearer "+credential {
		t.Fatalf("the server got Authorization %q", got)
	}
	for _, header := range []string{"Cookie", "X-Forwarded-For", "Origin"} {
		if seen.Header.Get(header) != "" {
			t.Fatalf("%s reached the server", header)
		}
	}
	for name, values := range seen.Header {
		for _, value := range values {
			if strings.Contains(value, tokenA) {
				t.Fatalf("the lease token reached the server in %s", name)
			}
		}
	}
	if strings.Contains(body, tokenA) {
		t.Fatal("the lease token reached the server's body")
	}
	if seen.Host != h.front.cfg.upstream.Host {
		t.Fatalf("the server saw Host %q", seen.Host)
	}
	if h.authority.exchanges[0] != classRead {
		t.Fatalf("a read tool exchanged for %q", h.authority.exchanges[0])
	}
	h.do(t, http.MethodPost, tokenA, call("notes_write"), nil)
	if h.authority.exchanges[len(h.authority.exchanges)-1] != classWrite {
		t.Fatal("a write tool did not exchange for write")
	}
}

func TestReadOnlyHidesWriteToolsAndRefusesThem(t *testing.T) {
	for _, stream := range []bool{false, true} {
		h := newHarness(t)
		h.server.stream = stream
		readOnly := listed(t, h.do(t, http.MethodPost, tokenRO, rpc(3, "tools/list", nil), nil))
		if strings.Join(readOnly, ",") != "whoami,notes_read,leak_credential" {
			t.Fatalf("stream=%v: ReadOnly sees %v", stream, readOnly)
		}
		readWrite := listed(t, h.do(t, http.MethodPost, tokenA, rpc(3, "tools/list", nil), nil))
		if len(readWrite) != 5 {
			t.Fatalf("stream=%v: ReadWrite sees %v", stream, readWrite)
		}
		before := h.server.count()
		if code := h.do(t, http.MethodPost, tokenRO, call(""), nil).Code; code != http.StatusBadRequest {
			t.Fatalf("a call without a tool name: %d", code)
		}
		for _, name := range []string{"notes_write", "delete_everything"} {
			response := h.do(t, http.MethodPost, tokenRO, call(name), nil)
			var answer struct {
				ID    int `json:"id"`
				Error struct {
					Code int `json:"code"`
				} `json:"error"`
			}
			json.Unmarshal(response.Body.Bytes(), &answer)
			if response.Code != http.StatusOK || answer.ID != 7 || answer.Error.Code != -32602 {
				t.Fatalf("%q at ReadOnly: %d %s", name, response.Code, response.Body)
			}
		}
		if h.server.count() != before {
			t.Fatal("a refused call reached the server")
		}
		for _, operation := range h.authority.exchanges {
			if operation == classWrite {
				t.Fatal("a refused call exchanged for write")
			}
		}
		if !strings.Contains(h.logText(), `tool="delete_everything"`) {
			t.Fatal("the refusal was not logged")
		}
	}
}

func TestAFilteredListKeepsEverythingElseOfTheAnswer(t *testing.T) {
	h := newHarness(t)
	response := h.do(t, http.MethodPost, tokenRO, rpc(3, "tools/list", nil), nil)
	var answer struct {
		JSONRPC string `json:"jsonrpc"`
		ID      int    `json:"id"`
		Result  struct {
			Tools []struct {
				Description string         `json:"description"`
				InputSchema map[string]any `json:"inputSchema"`
			} `json:"tools"`
		} `json:"result"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &answer); err != nil {
		t.Fatal(err)
	}
	tool := answer.Result.Tools[0]
	if answer.JSONRPC != "2.0" || answer.ID != 3 || tool.Description != "<whoami>" || tool.InputSchema["type"] != "object" {
		t.Fatalf("the answer was rewritten: %s", response.Body)
	}
	if response.Header().Get("Content-Length") != fmt.Sprint(response.Body.Len()) {
		t.Fatal("Content-Length does not match the filtered body")
	}
}

func TestTheCredentialIsScrubbedFromEveryAnswer(t *testing.T) {
	for _, stream := range []bool{false, true} {
		h := newHarness(t)
		h.server.stream = stream
		response := h.do(t, http.MethodPost, tokenRO, call("leak_credential"), nil)
		if strings.Contains(response.Body.String(), credential) {
			t.Fatalf("stream=%v: the credential reached the caller: %s", stream, response.Body)
		}
		if !strings.Contains(response.Body.String(), "I hold Bearer "+redacted) {
			t.Fatalf("stream=%v: %s", stream, response.Body)
		}
		get := h.do(t, http.MethodGet, tokenRO, "", nil)
		if strings.Contains(get.Body.String(), credential) || !strings.Contains(get.Body.String(), redacted) {
			t.Fatalf("stream=%v: the event stream leaked: %s", stream, get.Body)
		}
	}
}

func TestNothingSecretIsLogged(t *testing.T) {
	h := newHarness(t)
	h.do(t, http.MethodPost, tokenA, call("whoami"), nil)
	h.do(t, http.MethodPost, tokenRO, call("notes_write"), nil)
	h.do(t, http.MethodPost, tokenDead, call("whoami"), nil)
	h.server.status = http.StatusUnauthorized
	h.do(t, http.MethodPost, tokenA, call("whoami"), nil)
	logs := h.logText()
	for _, secret := range []string{tokenA, tokenRO, tokenDead, credential, identity, "argument-value"} {
		if strings.Contains(logs, secret) {
			t.Fatalf("the log holds a secret: %s", logs)
		}
	}
	if !strings.Contains(logs, `call lease=lease-a tool="whoami" class=read status=200`) {
		t.Fatalf("no audit line: %s", logs)
	}
}

func TestTheServerRefusingTheCredentialIs502WithoutAChallenge(t *testing.T) {
	h := newHarness(t)
	h.server.status = http.StatusUnauthorized
	response := h.do(t, http.MethodPost, tokenA, call("whoami"), nil)
	if response.Code != http.StatusBadGateway || response.Header().Get("WWW-Authenticate") != "" {
		t.Fatalf("status %d", response.Code)
	}
}

func TestASessionBelongsToTheLeaseThatOpenedIt(t *testing.T) {
	h := newHarness(t)
	opened := h.do(t, http.MethodPost, tokenA, rpc(1, "initialize", nil), nil)
	session := opened.Header().Get("Mcp-Session-Id")
	if session != "session-1" {
		t.Fatalf("no session: %q", session)
	}
	other := h.do(t, http.MethodPost, tokenB, rpc(2, "tools/list", nil), map[string]string{"Mcp-Session-Id": session})
	if other.Code != http.StatusNotFound {
		t.Fatalf("another lease used the session: %d", other.Code)
	}
	if h.do(t, http.MethodPost, tokenA, rpc(2, "tools/list", nil), map[string]string{"Mcp-Session-Id": session}).Code != http.StatusOK {
		t.Fatal("the owner could not use its session")
	}
	h.do(t, http.MethodDelete, tokenA, "", map[string]string{"Mcp-Session-Id": session})
	if h.front.sessions.mayUse(session, "lease-a") {
		t.Fatal("a closed session stayed open")
	}
	if code := h.do(t, http.MethodPost, tokenA, rpc(3, "tools/list", nil), map[string]string{"Mcp-Session-Id": session}).Code; code != http.StatusNotFound {
		t.Fatalf("a closed session answered %d", code)
	}
}

func TestTooManyCallsInFlightIs429(t *testing.T) {
	h := newHarness(t)
	release1, _ := h.front.inflight.acquire("lease-a", 2)
	release2, _ := h.front.inflight.acquire("lease-a", 2)
	if code := h.do(t, http.MethodPost, tokenA, call("whoami"), nil).Code; code != http.StatusTooManyRequests {
		t.Fatalf("status %d", code)
	}
	if code := h.do(t, http.MethodPost, tokenB, call("whoami"), nil).Code; code != http.StatusOK {
		t.Fatalf("another binding was capped: %d", code)
	}
	release1()
	release2()
	if code := h.do(t, http.MethodPost, tokenA, call("whoami"), nil).Code; code != http.StatusOK {
		t.Fatalf("status %d after release", code)
	}
}

func TestBatchesAndDisagreeingRoutingHeadersAreRefused(t *testing.T) {
	h := newHarness(t)
	batch := "[" + call("whoami") + "]"
	if code := h.do(t, http.MethodPost, tokenA, batch, nil).Code; code != http.StatusBadRequest {
		t.Fatalf("batch: %d", code)
	}
	mismatch := h.do(t, http.MethodPost, tokenRO, call("whoami"), map[string]string{"Mcp-Method": "tools/call", "Mcp-Name": "notes_write"})
	if mismatch.Code != http.StatusBadRequest {
		t.Fatalf("mismatch: %d", mismatch.Code)
	}
	if h.server.count() != 0 {
		t.Fatal("a refused request reached the server")
	}
}

func TestOtherPathsAndMethodsAreRefused(t *testing.T) {
	h := newHarness(t)
	request := httptest.NewRequest(http.MethodPost, "/admin", nil)
	recorder := httptest.NewRecorder()
	h.front.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status %d", recorder.Code)
	}
	if code := h.do(t, http.MethodPut, tokenA, "", nil).Code; code != http.StatusMethodNotAllowed {
		t.Fatalf("PUT: %d", code)
	}
}

func TestReadinessIsARealMCPProbeAndPinsTheToolList(t *testing.T) {
	h := newHarness(t)
	ready := func() (int, string) {
		recorder := httptest.NewRecorder()
		h.front.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		return recorder.Code, recorder.Body.String()
	}
	code, body := ready()
	if code != http.StatusOK || !strings.Contains(body, `"tools":5`) {
		t.Fatalf("readyz: %d %s", code, body)
	}
	var methods []string
	for _, seen := range h.server.seen {
		methods = append(methods, seen.Method)
		if seen.Header.Get("Authorization") != "" {
			t.Fatal("the probe carried a credential")
		}
	}
	if strings.Join(methods, ",") != "POST,POST,POST,DELETE" {
		t.Fatalf("probe requests: %v", methods)
	}
	// Cached for a few seconds.
	ready()
	if h.server.count() != 4 {
		t.Fatal("the probe was not cached")
	}
	// The tool list changes under the image: warn keeps it ready...
	h.server.toolNames = append(h.server.toolNames, "new_tool")
	h.advance(6 * time.Second)
	if code, _ := ready(); code != http.StatusOK {
		t.Fatal("warn made the pod unready")
	}
	if !strings.Contains(h.logText(), "tool list changed") {
		t.Fatal("the change was not logged")
	}
	// ...block does not.
	h.front.cfg.toolPinning = "block"
	h.advance(6 * time.Second)
	if code, body := ready(); code != http.StatusServiceUnavailable || !strings.Contains(body, "tool list changed") {
		t.Fatalf("block: %d %s", code, body)
	}
	// A server that does not answer is not ready.
	h.server.status = http.StatusInternalServerError
	h.advance(6 * time.Second)
	if code, _ := ready(); code != http.StatusServiceUnavailable {
		t.Fatal("a failing server was ready")
	}
	recorder := httptest.NewRecorder()
	h.front.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/livez", nil))
	if recorder.Code != http.StatusOK {
		t.Fatal("livez")
	}
}

func TestToolClassesFollowTheSharedPatternVectors(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "pattern_vectors.json"))
	if err != nil {
		t.Fatal(err)
	}
	var vectors struct {
		Cases []struct {
			Pattern string `json:"pattern"`
			Name    string `json:"name"`
			Match   bool   `json:"match"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(raw, &vectors); err != nil {
		t.Fatal(err)
	}
	for _, c := range vectors.Cases {
		if got := patternMatches(c.Pattern, c.Name); got != c.Match {
			t.Errorf("%q ~ %q = %v, want %v", c.Pattern, c.Name, got, c.Match)
		}
	}
}

func TestAnAccessLevelTheSpecDoesNotNameSeesNothing(t *testing.T) {
	cfg := testConfig(t, "http://127.0.0.1:8091/mcp")
	if cfg.allowed("whoami", "Admin") || cfg.allowed("whoami", "") {
		t.Fatal("an unknown access level saw a tool")
	}
	if !cfg.allowed("whoami", "ReadOnly") || cfg.allowed("notes_write", "ReadOnly") || !cfg.allowed("notes_write", "ReadWrite") {
		t.Fatal("tool classes")
	}
}

func TestTheConfigOnlyForwardsToTheServerBesideTheFront(t *testing.T) {
	base := func() requestFile {
		request := requestFile{ProtocolVersion: "1.0", Plane: "service"}
		request.Connector.ID = connectorID
		request.Service.Port = 8080
		request.Exchange.URL = "http://exchange:8088"
		request.MCP = &mcpBlock{Upstream: "http://127.0.0.1:8091/mcp", Access: map[string][]string{"ReadWrite": {"read", "write"}}}
		return request
	}
	if _, err := parseConfig(base(), identity); err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(*requestFile, *string){
		"remote upstream":  func(r *requestFile, _ *string) { r.MCP.Upstream = "http://evil.example:8091/mcp" },
		"https upstream":   func(r *requestFile, _ *string) { r.MCP.Upstream = "https://127.0.0.1:8091/mcp" },
		"no upstream port": func(r *requestFile, _ *string) { r.MCP.Upstream = "http://127.0.0.1/mcp" },
		"same port":        func(r *requestFile, _ *string) { r.MCP.Upstream = "http://127.0.0.1:8080/mcp" },
		"no mcp block":     func(r *requestFile, _ *string) { r.MCP = nil },
		"protocol 2":       func(r *requestFile, _ *string) { r.ProtocolVersion = "2.0" },
		"bad identity":     func(_ *requestFile, id *string) { *id = "sdi_short" },
		"bad pattern":      func(r *requestFile, _ *string) { r.MCP.Tools.Read = []string{"get_[a]"} },
		"unknown class":    func(r *requestFile, _ *string) { r.MCP.Access["ReadOnly"] = []string{"admin"} },
		"no access":        func(r *requestFile, _ *string) { r.MCP.Access = nil },
		"bad header":       func(r *requestFile, _ *string) { r.MCP.Credential = &credentialRule{Header: "Bad Header"} },
		"no connector":     func(r *requestFile, _ *string) { r.Connector.ID = "" },
		"no exchange":      func(r *requestFile, _ *string) { r.Exchange.URL = "" },
	}
	for name, mutate := range cases {
		request, id := base(), identity
		mutate(&request, &id)
		if _, err := parseConfig(request, id); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestLoadConfigReadsTheRequestAndIdentityFiles(t *testing.T) {
	dir := t.TempDir()
	request := map[string]any{
		"protocol_version": "1.0",
		"plane":            "service",
		"driver":           "srw.gitea-mcp/v1",
		"connector":        map[string]any{"id": strings.ToUpper(connectorID), "config": map[string]any{}},
		"credentials":      map[string]any{},
		"service":          map[string]any{"port": 8080, "port_name": "srw-driver"},
		"exchange":         map[string]any{"url": "http://srw-orchestrator.srw.svc:8088", "identity_file": "/run/srw/identity"},
		"mcp": map[string]any{
			"upstream": "http://127.0.0.1:8091/mcp", "protocol": "legacy",
			"tools":                     map[string]any{"read": []string{"get_me"}},
			"access":                    map[string]any{"ReadOnly": []string{"read"}, "ReadWrite": []string{"read", "write"}},
			"credential":                map[string]any{"header": "Authorization", "scheme": "Bearer"},
			"max_in_flight_per_binding": 4, "tool_pinning": "warn",
		},
	}
	raw, _ := json.Marshal(request)
	os.WriteFile(filepath.Join(dir, "request.json"), raw, 0o600)
	os.WriteFile(filepath.Join(dir, "identity"), []byte(identity+"\n"), 0o600)
	env := map[string]string{
		"SRW_REQUEST_FILE":         filepath.Join(dir, "request.json"),
		"SRW_DRIVER_IDENTITY_FILE": filepath.Join(dir, "identity"),
		"SRW_DRIVER_PORT":          "8080",
	}
	cfg, err := loadConfig(func(name string) string { return env[name] })
	if err != nil {
		t.Fatal(err)
	}
	if cfg.connectorID != connectorID || cfg.identity != identity || cfg.port != "8080" {
		t.Fatalf("config %+v", cfg)
	}
	if cfg.exchangeURL != "http://srw-orchestrator.srw.svc:8088" || cfg.upstream.String() != "http://127.0.0.1:8091/mcp" {
		t.Fatalf("config %+v", cfg)
	}
}

func TestTheHTTPAuthorityAuthenticatesWithTheIdentityAndMapsAnswers(t *testing.T) {
	var seen []*http.Request
	var bodies []map[string]string
	answers := map[string]func(http.ResponseWriter){}
	exchange := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]string
		json.NewDecoder(r.Body).Decode(&body)
		seen = append(seen, r)
		bodies = append(bodies, body)
		answers[r.URL.Path](w)
	}))
	defer exchange.Close()
	a := newHTTPAuthority(exchange.URL, identity)
	answers[introspectPath] = func(w http.ResponseWriter) {
		writeJSON(w, 200, map[string]any{"active": true, "lease_id": "l1", "connector_id": connectorID, "access": "ReadOnly", "expires_at": "2026-10-08T12:15:00.123456+00:00"})
	}
	found, err := a.introspect(context.Background(), tokenA)
	if err != nil || !found.active || found.access != "ReadOnly" || found.expires.IsZero() {
		t.Fatalf("%+v %v", found, err)
	}
	if seen[0].Header.Get("Authorization") != "Bearer "+identity || bodies[0]["lease_token"] != tokenA {
		t.Fatal("introspection is not authenticated with the identity")
	}
	answers[introspectPath] = func(w http.ResponseWriter) { writeJSON(w, 200, map[string]any{"active": false}) }
	if found, _ := a.introspect(context.Background(), tokenA); found.active {
		t.Fatal("an inactive lease")
	}
	answers[introspectPath] = func(w http.ResponseWriter) {
		writeJSON(w, 403, map[string]any{"error": "driver_identity_of_another_connector"})
	}
	if found, err := a.introspect(context.Background(), tokenA); found.active || err != nil {
		t.Fatal("another connector's lease")
	}
	answers[introspectPath] = func(w http.ResponseWriter) { writeJSON(w, 401, map[string]any{"error": "driver_identity_revoked"}) }
	if _, err := a.introspect(context.Background(), tokenA); err != errFrontRevoked {
		t.Fatal("a revoked identity")
	}
	answers[exchangePath] = func(w http.ResponseWriter) {
		writeJSON(w, 200, map[string]any{"credential": credential, "max_cache_seconds": 30})
	}
	issued, err := a.exchange(context.Background(), tokenA, classWrite)
	if err != nil || issued.credential != credential || issued.cache != 30*time.Second {
		t.Fatalf("%+v %v", issued, err)
	}
	if bodies[len(bodies)-1]["operation"] != classWrite {
		t.Fatal("the operation was not sent")
	}
	for reason, status := range map[string]int{"operation_not_allowed": 403, "lease_revoked": 401, "lease_expired": 401} {
		answers[exchangePath] = func(w http.ResponseWriter) { writeJSON(w, 403, map[string]any{"error": reason}) }
		if issued, _ := a.exchange(context.Background(), tokenA, classWrite); issued.status != status {
			t.Fatalf("%s: %d", reason, issued.status)
		}
	}
	exchange.Close()
	if _, err := a.introspect(context.Background(), tokenA); err != errUnavailable {
		t.Fatal("an unreachable exchange")
	}
}
