package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

// In front of a stdio server (D5b) the upstream is SRW's stdio bridge: the
// front names each request's binding, hands over its credential, ends a
// binding's process with its lease, and probes readiness in the
// background. Every check of the front stays as it is.

const bridgeUpstream = "http://" + bridgeHost + "/mcp"

func bridgeConfig(t *testing.T, socket string) *config {
	t.Helper()
	request := requestFile{ProtocolVersion: "1.0", Plane: "service", Driver: "srw.mcp-stdio-test/v1"}
	request.Connector.ID = connectorID
	request.Service.Port = 8080
	request.Exchange.URL = "http://exchange.srw.svc:8088"
	request.MCP = &mcpBlock{
		Transport:             "stdio",
		Upstream:              bridgeUpstream,
		Socket:                socket,
		Access:                map[string][]string{"ReadOnly": {"read"}, "ReadWrite": {"read", "write"}},
		Credential:            &credentialRule{Env: "MCP_TOKEN"},
		MaxInFlightPerBinding: 2,
	}
	request.MCP.Tools.Read = []string{"whoami", "notes_read", "leak_*"}
	cfg, err := parseConfig(request, identity)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

// newBridgeHarness is the front in front of a fake bridge that serves a
// unix socket, as the real one does.
func newBridgeHarness(t *testing.T) *harness {
	t.Helper()
	h := newHarness(t)
	socket := filepath.Join(t.TempDir(), "bridge.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	bridge := httptest.NewUnstartedServer(h.server)
	bridge.Listener.Close()
	bridge.Listener = listener
	bridge.Start()
	t.Cleanup(bridge.Close)
	cfg := bridgeConfig(t, socket)
	h.front = newFront(cfg, h.authority, newUpstreamClient(socket), func(format string, a ...any) {
		h.logMu.Lock()
		defer h.logMu.Unlock()
		fmt.Fprintf(h.logs, format+"\n", a...)
	}, h.clock.Now)
	return h
}

// requests the bridge saw, by path.
func (s *fakeServer) at(path string) []*http.Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*http.Request
	for _, seen := range s.seen {
		if seen.URL.Path == path {
			out = append(out, seen)
		}
	}
	return out
}

func TestAStdioServersConfigNamesAnEnvironmentVariable(t *testing.T) {
	cfg := bridgeConfig(t, "/srw/bridge/bridge.sock")
	if !cfg.bridge || cfg.credential.Env != "MCP_TOKEN" || cfg.socket != "/srw/bridge/bridge.sock" {
		t.Fatalf("%+v", cfg)
	}
	for _, test := range []struct {
		transport  string
		credential *credentialRule
	}{
		{"stdio", &credentialRule{Header: "Authorization"}},
		{"stdio", &credentialRule{Env: "MCP_TOKEN", Scheme: "Bearer"}},
		{"stdio", &credentialRule{}},
		{"http", &credentialRule{Header: "Authorization", Env: "MCP_TOKEN"}},
		{"http", &credentialRule{Header: bridgeBindingHeader}},
		{"http", &credentialRule{Header: "srw-bridge-credential"}},
		{"sse", nil},
	} {
		request := requestFile{ProtocolVersion: "1.0", Plane: "service"}
		request.Connector.ID = connectorID
		request.Service.Port = 8080
		request.Exchange.URL = "http://exchange:8088"
		request.MCP = &mcpBlock{
			Transport:  test.transport,
			Upstream:   "http://127.0.0.1:8091/mcp",
			Access:     map[string][]string{"ReadWrite": {"read", "write"}},
			Credential: test.credential,
		}
		if test.transport == "stdio" {
			request.MCP.Upstream, request.MCP.Socket = bridgeUpstream, "/srw/bridge/bridge.sock"
		}
		if _, err := parseConfig(request, identity); err == nil {
			t.Errorf("%s %+v was accepted", test.transport, test.credential)
		}
	}
	// A stdio server that takes no credential.
	request := requestFile{ProtocolVersion: "1.0", Plane: "service"}
	request.Connector.ID = connectorID
	request.Service.Port = 8080
	request.Exchange.URL = "http://exchange:8088"
	request.MCP = &mcpBlock{Transport: "stdio", Upstream: bridgeUpstream, Socket: "/srw/bridge/bridge.sock", Access: map[string][]string{"ReadWrite": {"read"}}}
	if cfg, err := parseConfig(request, identity); err != nil || !cfg.bridge || cfg.credential != nil {
		t.Fatalf("%+v %v", cfg, err)
	}
}

func TestTheBridgeIsReachedOnItsSocketOnly(t *testing.T) {
	for name, block := range map[string]mcpBlock{
		"stdio on a port":         {Transport: "stdio", Upstream: "http://127.0.0.1:8091/mcp"},
		"stdio on a port, socket": {Transport: "stdio", Upstream: "http://127.0.0.1:8091/mcp", Socket: "/srw/bridge/bridge.sock"},
		"stdio, relative socket":  {Transport: "stdio", Upstream: bridgeUpstream, Socket: "bridge.sock"},
		"stdio, unclean socket":   {Transport: "stdio", Upstream: bridgeUpstream, Socket: "/srw/../bridge.sock"},
		"stdio, control path":     {Transport: "stdio", Upstream: "http://" + bridgeHost + "/srw/status", Socket: "/srw/bridge/bridge.sock"},
		"stdio, other host":       {Transport: "stdio", Upstream: "http://evil.example/mcp", Socket: "/srw/bridge/bridge.sock"},
		"http with a socket":      {Transport: "http", Upstream: "http://127.0.0.1:8091/mcp", Socket: "/srw/bridge/bridge.sock"},
	} {
		request := requestFile{ProtocolVersion: "1.0", Plane: "service"}
		request.Connector.ID = connectorID
		request.Service.Port = 8080
		request.Exchange.URL = "http://exchange:8088"
		block.Access = map[string][]string{"ReadWrite": {"read"}}
		request.MCP = &block
		if _, err := parseConfig(request, identity); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	// The front dials the socket whatever the upstream's host says.
	h := newBridgeHarness(t)
	if code := h.do(t, http.MethodPost, tokenA, call("whoami"), nil).Code; code != http.StatusOK {
		t.Fatalf("through the socket: %d", code)
	}
	if seen, _ := h.server.last(); seen == nil || seen.Host != bridgeHost {
		t.Fatalf("the bridge saw %+v", seen)
	}
}

func TestEachRequestNamesItsBindingAndCarriesItsCredential(t *testing.T) {
	h := newBridgeHarness(t)
	forged := map[string]string{
		bridgeBindingHeader:    "lease-b",
		bridgeCredentialHeader: base64.StdEncoding.EncodeToString([]byte("forged")),
	}
	if code := h.do(t, http.MethodPost, tokenA, call("whoami"), forged).Code; code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	seen, _ := h.server.last()
	if seen.Header.Get(bridgeBindingHeader) != "lease-a" {
		t.Fatalf("binding %q", seen.Header.Get(bridgeBindingHeader))
	}
	got, _ := base64.StdEncoding.DecodeString(seen.Header.Get(bridgeCredentialHeader))
	if string(got) != credential {
		t.Fatalf("credential %q", got)
	}
	if seen.Header.Get("Authorization") != "" {
		t.Fatal("a header credential reached the bridge")
	}
	// A ReadOnly lease is its own binding, refused its write tools here.
	h.do(t, http.MethodPost, tokenRO, call("whoami"), nil)
	if seen, _ := h.server.last(); seen.Header.Get(bridgeBindingHeader) != "lease-ro" {
		t.Fatal("the ReadOnly lease's binding")
	}
	before := h.server.count()
	if answer := h.do(t, http.MethodPost, tokenRO, call("notes_write"), nil).Body.String(); !strings.Contains(answer, "Unknown tool") {
		t.Fatalf("%s", answer)
	}
	if h.server.count() != before {
		t.Fatal("a refused write reached the bridge")
	}
}

func TestTheSweepEndsTheProcessOfABindingWhoseLeaseEnded(t *testing.T) {
	h := newBridgeHarness(t)
	for _, token := range []string{tokenA, tokenB} {
		if code := h.do(t, http.MethodPost, token, rpc(1, "initialize", map[string]any{}), nil).Code; code != http.StatusOK {
			t.Fatalf("initialize: %d", code)
		}
	}
	if tracked := h.front.bindings.snapshot(); len(tracked) != 2 || tracked["lease-b"] != tokenB {
		t.Fatalf("tracked %v", len(tracked))
	}
	h.authority.mu.Lock()
	delete(h.authority.leases, tokenB)
	h.authority.mu.Unlock()
	h.advance(31 * time.Second) // past the cached decision
	h.front.sweepOnce(context.Background())
	ended := h.server.at("/srw/bindings/lease-b")
	if len(ended) != 1 || ended[0].Method != http.MethodDelete {
		t.Fatalf("the bridge was not told: %d", len(ended))
	}
	if len(h.server.at("/srw/bindings/lease-a")) != 0 {
		t.Fatal("a live binding was ended")
	}
	if tracked := h.front.bindings.snapshot(); len(tracked) != 1 {
		t.Fatalf("%d tracked", len(tracked))
	}
	if !strings.Contains(h.logText(), "lease=lease-b ended: its process stops") {
		t.Fatalf("%s", h.logText())
	}
	// The exchange down: nothing is ended on a guess.
	h.authority.mu.Lock()
	h.authority.down = true
	h.authority.mu.Unlock()
	h.advance(31 * time.Second)
	h.front.sweepOnce(context.Background())
	if len(h.server.at("/srw/bindings/lease-a")) != 0 {
		t.Fatal("ended while the exchange was down")
	}
}

func TestARefusedLeaseEndsItsBindingsProcessAtOnce(t *testing.T) {
	h := newBridgeHarness(t)
	if code := h.do(t, http.MethodPost, tokenB, rpc(1, "initialize", map[string]any{}), nil).Code; code != http.StatusOK {
		t.Fatalf("initialize: %d", code)
	}
	h.authority.mu.Lock()
	delete(h.authority.leases, tokenB)
	h.authority.mu.Unlock()
	h.advance(31 * time.Second)
	if code := h.do(t, http.MethodPost, tokenB, call("whoami"), nil).Code; code != http.StatusUnauthorized {
		t.Fatalf("a dead lease: %d", code)
	}
	deadline := time.Now().Add(5 * time.Second)
	for len(h.server.at("/srw/bindings/lease-b")) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the bridge was not told the binding ended")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// A token that never had a process ends nothing.
	h.do(t, http.MethodPost, tokenDead, call("whoami"), nil)
	time.Sleep(50 * time.Millisecond)
	if count := len(h.server.at("/srw/bindings/lease-b")); count != 1 {
		t.Fatalf("%d binding ends", count)
	}
	if h.front.bindings.leaseOf(tokenB) != "" {
		t.Fatal("the ended binding is still tracked")
	}
}

func TestAStreamWhoseLeaseEndsStopsItsBindingsProcess(t *testing.T) {
	previous := streamRecheck
	streamRecheck = 20 * time.Millisecond
	defer func() { streamRecheck = previous }()
	h := newBridgeHarness(t)
	h.server.holdGet = true
	h.server.getEvents = []string{`data: {"jsonrpc":"2.0","method":"notifications/message","params":{}}`}
	finished := make(chan int)
	go func() { finished <- h.do(t, http.MethodGet, tokenB, "", nil).Code }()
	time.Sleep(100 * time.Millisecond)
	h.authority.mu.Lock()
	delete(h.authority.leases, tokenB)
	h.authority.mu.Unlock()
	h.advance(31 * time.Second)
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("the stream outlived its lease")
	}
	deadline := time.Now().Add(5 * time.Second)
	for len(h.server.at("/srw/bindings/lease-b")) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the bridge was not told the binding ended")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestAStdioServersReadinessIsProbedInTheBackground(t *testing.T) {
	h := newBridgeHarness(t)
	ready := func() int {
		recorder := httptest.NewRecorder()
		h.front.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		return recorder.Code
	}
	// The kubelet's probe never starts a process itself.
	if code := ready(); code != http.StatusServiceUnavailable || h.server.count() != 0 {
		t.Fatalf("before a probe: %d, bridge reached %d times", code, h.server.count())
	}
	if !h.front.probe.probeOnce(context.Background()) {
		t.Fatal("the probe failed")
	}
	initialize := h.server.at("/mcp")[0]
	if initialize.Header.Get(bridgeBindingHeader) != "" || initialize.Header.Get(bridgeCredentialHeader) != "" {
		t.Fatal("the probe named a binding or carried a credential")
	}
	// Its messages are in the form the front forwards (the bridge accepts
	// no other).
	h.server.mu.Lock()
	sent := append([]string(nil), h.server.bodies...)
	h.server.mu.Unlock()
	for _, body := range sent {
		if body == "" {
			continue
		}
		if _, forwarded, err := parseMessage([]byte(body)); err != nil || string(forwarded) != body {
			t.Fatalf("the probe sent %s", body)
		}
	}
	if !strings.HasPrefix(sent[0], `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{`) {
		t.Fatalf("initialize: %s", sent[0])
	}
	probes := h.server.count()
	if code := ready(); code != http.StatusOK {
		t.Fatalf("after a probe: %d", code)
	}
	// /readyz asks only whether the bridge lives.
	if h.server.count() != probes+1 || len(h.server.at("/srw/livez")) != 1 {
		t.Fatalf("/readyz reached the bridge %d times", h.server.count()-probes)
	}
	h.server.mu.Lock()
	h.server.status = http.StatusServiceUnavailable
	h.server.mu.Unlock()
	if code := ready(); code != http.StatusServiceUnavailable {
		t.Fatalf("with the bridge down: %d", code)
	}
	if h.front.probe.probeOnce(context.Background()) {
		t.Fatal("a failed probe counted as ready")
	}
}

// The shared corpus (testdata/bypass_corpus.json, read by the stdio
// bridge's tests, its integration test and the D5b gate) is exactly the
// review's corpus above.
func TestTheSharedCorpusIsTheReviewCorpus(t *testing.T) {
	raw, err := os.ReadFile("testdata/bypass_corpus.json")
	if err != nil {
		t.Fatal(err)
	}
	var shared struct {
		WriteTool    string   `json:"write_tool"`
		ReadTool     string   `json:"read_tool"`
		Bodies       []string `json:"bodies"`
		BodiesBase64 []string `json:"bodies_base64"`
	}
	if err := json.Unmarshal(raw, &shared); err != nil {
		t.Fatal(err)
	}
	got := append([]string(nil), shared.Bodies...)
	for _, encoded := range shared.BodiesBase64 {
		body, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, string(body))
	}
	want := append(append([]string(nil), reviewersBodies...), keyVariantBodies()...)
	sort.Strings(got)
	sort.Strings(want)
	if len(got) != len(want) || shared.WriteTool != "notes_write" || shared.ReadTool != "whoami" {
		t.Fatalf("the shared corpus drifted from the review's: %d bodies, want %d", len(got), len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("the shared corpus drifted from the review's: %q, want %q", got[i], want[i])
		}
	}
}

func TestNoCorpusBodyLetsAReadOnlyBindingRunAWriteToolBehindTheBridge(t *testing.T) {
	for _, body := range append(append([]string(nil), reviewersBodies...), keyVariantBodies()...) {
		h := newBridgeHarness(t)
		answer := h.do(t, http.MethodPost, tokenRO, body, nil).Body.String()
		if strings.Contains(answer, "called notes_write") {
			t.Fatalf("BYPASS: %s ran notes_write: %s", body, answer)
		}
		seen, forwarded := h.server.last()
		if seen == nil {
			continue
		}
		if seen.Header.Get(bridgeBindingHeader) != "lease-ro" {
			t.Fatalf("%s was forwarded for %q", body, seen.Header.Get(bridgeBindingHeader))
		}
		for _, view := range views(t, forwarded) {
			if view != "tools/call/whoami" {
				t.Fatalf("%s was forwarded as %s", body, forwarded)
			}
		}
	}
}
