package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	testConnector  = "0d6f3a52-8b1c-4e8e-9a8f-1f2e3d4c5b6a"
	otherConnector = "9d6f3a52-8b1c-4e8e-9a8f-1f2e3d4c5b6a"
	testCredential = "ghp_TheRealForgeToken0123456789abcdefABCD"
)

var (
	readWriteLease = "scl_" + strings.Repeat("W", 49)
	readOnlyLease  = "scl_" + strings.Repeat("R", 49)
	otherLease     = "scl_" + strings.Repeat("O", 49)
	deadLease      = "scl_" + strings.Repeat("D", 49)
)

// fakeAuthority is the lease exchange: three live leases (ReadWrite,
// ReadOnly, and one of another connector) and one dead.
type fakeAuthority struct {
	mu           sync.Mutex
	operations   []string
	allowed      []string
	revoked      bool
	ended        bool // introspection: every lease has ended
	down         bool // introspection: the exchange does not answer
	introspected int
}

func (f *fakeAuthority) introspect(_ context.Context, token string) (lease, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.introspected++
	if f.down {
		return lease{}, errUnavailable
	}
	if f.ended {
		return lease{}, nil
	}
	switch token {
	case readWriteLease:
		return lease{active: true, id: "lease-rw", connectorID: testConnector, access: "ReadWrite"}, nil
	case readOnlyLease:
		return lease{active: true, id: "lease-ro", connectorID: strings.ToUpper(testConnector), access: "ReadOnly"}, nil
	case otherLease:
		return lease{active: true, id: "lease-other", connectorID: otherConnector, access: "ReadWrite"}, nil
	}
	return lease{}, nil
}

func (f *fakeAuthority) exchange(_ context.Context, token, operation string) (grant, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.operations = append(f.operations, operation)
	if f.revoked {
		return grant{status: http.StatusUnauthorized, reason: "lease_revoked"}, nil
	}
	if token == readOnlyLease && operation == "write" {
		return grant{status: http.StatusForbidden, reason: "operation_not_allowed"}, nil
	}
	allowed := f.allowed
	if allowed == nil {
		allowed = []string{"https://example.com/o/r.git"}
	}
	return grant{credential: testCredential, allowed: allowed, status: http.StatusOK, cache: 30 * time.Second}, nil
}

func (f *fakeAuthority) asked() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.operations...)
}

// upstreamCall is what the fake git server received.
type upstreamCall struct {
	method, path, query string
	header              http.Header
	body                []byte
}

type harness struct {
	t        *testing.T
	auth     *fakeAuthority
	driver   *driver
	server   *httptest.Server
	upstream *httptest.Server
	mu       sync.Mutex
	calls    []upstreamCall
	handle   func(w http.ResponseWriter, r *http.Request, body []byte)
	stream   http.HandlerFunc // replaces handle for streaming tests
	logMu    sync.Mutex
	logs     bytes.Buffer
}

func (h *harness) logText() string {
	h.logMu.Lock()
	defer h.logMu.Unlock()
	return h.logs.String()
}

func bufioReader(text string) *bufio.Reader {
	return bufio.NewReader(strings.NewReader(text))
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{t: t, auth: &fakeAuthority{}}
	h.upstream = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if h.stream != nil {
			h.stream(w, r)
			return
		}
		body, _ := io.ReadAll(r.Body)
		h.mu.Lock()
		h.calls = append(h.calls, upstreamCall{r.Method, r.URL.Path, r.URL.RawQuery, r.Header.Clone(), body})
		h.mu.Unlock()
		h.handle(w, r, body)
	}))
	t.Cleanup(h.upstream.Close)
	h.handle = func(w http.ResponseWriter, r *http.Request, _ []byte) {
		service := r.URL.Query().Get("service")
		if strings.HasSuffix(r.URL.Path, "/info/refs") {
			w.Header().Set("Content-Type", "application/x-"+service+"-advertisement")
			w.Write([]byte(push("# service="+service+"\n", flushPkt, oldID+" refs/heads/main\x00report-status\n", flushPkt)))
			return
		}
		name := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
		w.Header().Set("Content-Type", "application/x-"+name+"-result")
		w.Write([]byte(push("unpack ok\n", "ok refs/heads/main\n", flushPkt)))
	}
	// The driver's upstream client: example.com:443 is the fake server.
	pool := x509.NewCertPool()
	pool.AddCert(h.upstream.Certificate())
	address := h.upstream.Listener.Addr().String()
	client := newUpstreamClient()
	transport := client.Transport.(*http.Transport)
	transport.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	transport.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}
	served, err := parseUpstream("https://example.com/o/r.git")
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config{driver: "srw.git-swap/v1", connectorID: testConnector, upstream: served, port: "8443"}
	logf := func(format string, a ...any) {
		h.logMu.Lock()
		defer h.logMu.Unlock()
		fmt.Fprintf(&h.logs, format+"\n", a...)
	}
	h.driver = newDriver(cfg, h.auth, client, logf, time.Now)
	h.server = httptest.NewServer(h.driver)
	t.Cleanup(h.server.Close)
	return h
}

func (h *harness) upstreamCalls() []upstreamCall {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]upstreamCall(nil), h.calls...)
}

func basic(token string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte("srw-lease:"+token))
}

const repoPath = "/" + testConnector + "/o/r.git"

func (h *harness) do(method, target, lease string, body io.Reader, header map[string]string) *http.Response {
	h.t.Helper()
	request, err := http.NewRequest(method, h.server.URL+target, body)
	if err != nil {
		h.t.Fatal(err)
	}
	if lease != "" {
		request.Header.Set("Authorization", basic(lease))
	}
	for key, value := range header {
		request.Header.Set(key, value)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() { response.Body.Close() })
	return response
}

func readAll(t *testing.T, r io.Reader) string {
	t.Helper()
	raw, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func rpc(service string) map[string]string {
	return map[string]string{"Content-Type": "application/x-" + service + "-request"}
}

func TestRoutesAreOnlyTheSmartHTTPEndpoints(t *testing.T) {
	h := newHarness(t)
	cases := []struct {
		method, target string
		status         int
	}{
		{"GET", repoPath + "/info/refs?service=git-upload-pack", 200},
		{"GET", "/" + testConnector + "/o/r/info/refs?service=git-upload-pack", 200},
		{"GET", repoPath + "/info/refs", 403},
		{"GET", repoPath + "/info/refs?service=git-upload-pack&x=1", 403},
		{"GET", repoPath + "/info/refs?service=git-upload-pack&service=git-receive-pack", 403},
		{"GET", repoPath + "/info/refs?service=git-upload-pac%6B", 403},
		{"POST", repoPath + "/info/refs?service=git-upload-pack", 405},
		{"GET", repoPath + "/git-upload-pack", 405},
		{"GET", repoPath + "/HEAD", 404},
		{"GET", repoPath + "/objects/info/packs", 404},
		{"GET", repoPath, 404},
		{"GET", "/" + testConnector + "/o/other.git/info/refs?service=git-upload-pack", 404},
		{"GET", "/" + testConnector + "/o/r.git.git/info/refs?service=git-upload-pack", 404},
		{"GET", "/" + otherConnector + "/o/r.git/info/refs?service=git-upload-pack", 404},
		{"GET", "/" + strings.ToUpper(testConnector) + "/o/r.git/info/refs?service=git-upload-pack", 404},
		{"GET", "/" + testConnector + "/o/%72.git/info/refs?service=git-upload-pack", 404},
		{"GET", "/" + testConnector + "/o/x/../r.git/info/refs?service=git-upload-pack", 404},
		{"GET", "/" + testConnector + "/o//r.git/info/refs?service=git-upload-pack", 404},
		{"GET", "/" + testConnector + "/./o/r.git/info/refs?service=git-upload-pack", 404},
		{"GET", "/api/v1/repos/o/r", 404},
		{"GET", "/", 404},
		{"PUT", repoPath + "/git-upload-pack", 405},
	}
	for _, c := range cases {
		request, _ := http.NewRequest(c.method, h.server.URL+c.target, nil)
		// The raw target, escapes and dot segments as written.
		request.URL.Opaque = strings.SplitN(c.target, "?", 2)[0]
		request.Header.Set("Authorization", basic(readWriteLease))
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != c.status {
			t.Errorf("%s %s: %d, want %d", c.method, c.target, response.StatusCode, c.status)
		}
	}
	for _, call := range h.upstreamCalls() {
		if call.path != "/o/r.git/info/refs" || call.query != "service=git-upload-pack" {
			t.Errorf("forwarded %s %s?%s", call.method, call.path, call.query)
		}
	}
}

func TestAuthenticationAsksGitForItsHelper(t *testing.T) {
	h := newHarness(t)
	target := repoPath + "/info/refs?service=git-upload-pack"
	for name, header := range map[string]string{
		"none":          "",
		"not a lease":   basic("hunter2"),
		"dead lease":    basic(deadLease),
		"other lease":   basic(otherLease),
		"bearer typo":   "Bearer " + readWriteLease + "x",
		"no password":   "Basic " + base64.StdEncoding.EncodeToString([]byte(readWriteLease)),
		"unknown kind":  "Digest " + readWriteLease,
		"broken base64": "Basic !!!",
	} {
		request, _ := http.NewRequest("GET", h.server.URL+target, nil)
		if header != "" {
			request.Header.Set("Authorization", header)
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		body := readAll(t, response.Body)
		response.Body.Close()
		if response.StatusCode != 401 || !strings.HasPrefix(response.Header.Get("WWW-Authenticate"), "Basic ") {
			t.Errorf("%s: %d %q", name, response.StatusCode, response.Header.Get("WWW-Authenticate"))
		}
		if !strings.Contains(body, "lease") {
			t.Errorf("%s: %q", name, body)
		}
	}
	if len(h.upstreamCalls()) != 0 || len(h.auth.asked()) != 0 {
		t.Fatal("an unauthenticated request reached the exchange or the upstream")
	}
	// A Bearer lease works too (an http.extraHeader client).
	response := h.do("GET", target, "", nil, map[string]string{"Authorization": "Bearer " + readWriteLease})
	if response.StatusCode != 200 {
		t.Fatalf("bearer: %d", response.StatusCode)
	}
}

func TestReadOnlyRefusesBothReceivePackFormsByPath(t *testing.T) {
	h := newHarness(t)
	pushBody := push(oldID+" "+newID+" refs/heads/main\x00report-status\n", flushPkt) + "PACK"
	for _, target := range []string{
		repoPath + "/info/refs?service=git-receive-pack",
		repoPath + "/git-receive-pack",
		repoPath + "/git-receive-pack?service=git-upload-pack",
		repoPath + "/git-receive-pack?service=git-receive-pack",
		repoPath + "/git-receive-pack?",
		repoPath + "/git-receive-pack?service=git-upload-pack&service=git-upload-pack",
		"/" + testConnector + "/o/r/git-receive-pack?service=git-upload-pack",
	} {
		method := "POST"
		var body io.Reader = strings.NewReader(pushBody)
		if strings.Contains(target, "info/refs") {
			method, body = "GET", nil
		}
		response := h.do(method, target, readOnlyLease, body, rpc("git-receive-pack"))
		text := readAll(t, response.Body)
		if response.StatusCode != 403 || !strings.Contains(text, "read-only") {
			t.Errorf("%s %s: %d %q", method, target, response.StatusCode, text)
		}
		if response.Header.Get("Content-Type") != "text/plain; charset=utf-8" {
			t.Errorf("%s: git shows a refusal only from text/plain", target)
		}
	}
	// The probe is a push too.
	if response := h.do("POST", repoPath+"/git-receive-pack", readOnlyLease, strings.NewReader(flushPkt), rpc("git-receive-pack")); response.StatusCode != 403 {
		t.Fatalf("read-only probe: %d", response.StatusCode)
	}
	if len(h.upstreamCalls()) != 0 {
		t.Fatalf("a read-only push reached the upstream: %+v", h.upstreamCalls())
	}
	for _, operation := range h.auth.asked() {
		if operation == "write" {
			t.Fatal("a read-only push was exchanged for the write credential")
		}
	}
	// Reads still work, and an upload-pack POST naming receive-pack in its
	// query is no push (it is refused for its query, not as a write).
	if response := h.do("GET", repoPath+"/info/refs?service=git-upload-pack", readOnlyLease, nil, nil); response.StatusCode != 200 {
		t.Fatalf("read-only fetch: %d", response.StatusCode)
	}
	response := h.do("POST", repoPath+"/git-upload-pack?service=git-receive-pack", readOnlyLease, strings.NewReader(flushPkt), rpc("git-upload-pack"))
	if text := readAll(t, response.Body); response.StatusCode != 400 || strings.Contains(text, "read-only") {
		t.Fatalf("upload-pack with a query: %d %q", response.StatusCode, text)
	}
}

func TestReadWritePushesBranchesOnly(t *testing.T) {
	h := newHarness(t)
	cases := map[string]string{
		"delete": oldID + " " + zero40 + " refs/heads/main",
		"tag":    zero40 + " " + newID + " refs/tags/v1",
		"notes":  zero40 + " " + newID + " refs/notes/commits",
		"dotdot": zero40 + " " + newID + " refs/heads/../tags/v1",
	}
	for name, command := range cases {
		body := push(oldID+" "+newID2+" refs/heads/ok\x00report-status side-band-64k\n", command+"\n", flushPkt) + "PACK" + strings.Repeat("p", 1<<16)
		response := h.do("POST", repoPath+"/git-receive-pack", readWriteLease, strings.NewReader(body), rpc("git-receive-pack"))
		text := readAll(t, response.Body)
		if response.StatusCode != 200 || response.Header.Get("Content-Type") != "application/x-git-receive-pack-result" {
			t.Fatalf("%s: %d %q", name, response.StatusCode, response.Header.Get("Content-Type"))
		}
		ref := strings.Fields(command)[2]
		if !strings.Contains(text, "ng "+ref+" ") || !strings.Contains(text, "ng refs/heads/ok not pushed") {
			t.Errorf("%s: report %q", name, text)
		}
	}
	if len(h.upstreamCalls()) != 0 {
		t.Fatal("a refused push reached the upstream")
	}
	if !strings.Contains(h.logText(), `ref="refs/tags/v1"`) {
		t.Fatalf("the push audit is missing a ref: %s", h.logText())
	}
}

func TestReadWriteBranchPushForwardsTheCheckedHeadAndThePack(t *testing.T) {
	h := newHarness(t)
	line := oldID + " " + newID + " refs/heads/main\x00report-status\n"
	head := fmt.Sprintf("%04X", len(line)+4) + line + flushPkt
	pack := "PACK\x00\x00\x00\x02" + strings.Repeat("\xff", 4096)
	response := h.do("POST", repoPath+"/git-receive-pack", readWriteLease, strings.NewReader(head+pack), rpc("git-receive-pack"))
	if text := readAll(t, response.Body); response.StatusCode != 200 || !strings.Contains(text, "ok refs/heads/main") {
		t.Fatalf("%d %q", response.StatusCode, text)
	}
	calls := h.upstreamCalls()
	if len(calls) != 1 {
		t.Fatalf("calls %+v", calls)
	}
	call := calls[0]
	if call.method != "POST" || call.path != "/o/r.git/git-receive-pack" || call.query != "" {
		t.Fatalf("forwarded %s %s?%s", call.method, call.path, call.query)
	}
	if string(call.body) != strings.ToLower(head[:4])+head[4:]+pack {
		t.Fatal("the upstream got other bytes than the driver checked")
	}
	if got := call.header.Get("Authorization"); got != "Basic "+base64.StdEncoding.EncodeToString([]byte("oauth2:"+testCredential)) {
		t.Fatalf("upstream Authorization %q", got)
	}
	ops := h.auth.asked()
	if len(ops) != 1 || ops[0] != "write" {
		t.Fatalf("exchanged for %v", ops)
	}
}

func TestTheLargePushProbePasses(t *testing.T) {
	h := newHarness(t)
	h.handle = func(w http.ResponseWriter, _ *http.Request, body []byte) {
		w.Header().Set("Content-Type", "application/x-git-receive-pack-result")
		if string(body) != flushPkt {
			w.WriteHeader(500)
		}
	}
	response := h.do("POST", repoPath+"/git-receive-pack", readWriteLease, strings.NewReader(flushPkt), rpc("git-receive-pack"))
	if response.StatusCode != 200 {
		t.Fatalf("probe: %d %q", response.StatusCode, readAll(t, response.Body))
	}
}

func TestHeadersThatPassAndHeadersThatStay(t *testing.T) {
	h := newHarness(t)
	response := h.do("GET", repoPath+"/info/refs?service=git-upload-pack", readWriteLease, nil, map[string]string{
		"Git-Protocol":    "version=2",
		"User-Agent":      "git/2.43.0",
		"Cookie":          "session=abc",
		"Origin":          "https://evil.example",
		"X-Forwarded-For": "1.2.3.4",
	})
	readAll(t, response.Body)
	gz := map[string]string{"Content-Type": "application/x-git-upload-pack-request", "Content-Encoding": "gzip"}
	h.do("POST", repoPath+"/git-upload-pack", readWriteLease, strings.NewReader("\x1f\x8bgzipped"), gz)
	calls := h.upstreamCalls()
	if len(calls) != 2 {
		t.Fatalf("calls %d", len(calls))
	}
	discovery, fetch := calls[0].header, calls[1].header
	if discovery.Get("Git-Protocol") != "version=2" || discovery.Get("User-Agent") != "git/2.43.0" {
		t.Fatalf("discovery headers %v", discovery)
	}
	for _, name := range []string{"Cookie", "Origin", "X-Forwarded-For", "Accept-Encoding"} {
		if discovery.Get(name) != "" {
			t.Errorf("%s reached the upstream", name)
		}
	}
	if strings.Contains(discovery.Get("Authorization"), readWriteLease) {
		t.Fatal("the lease token reached the upstream")
	}
	if fetch.Get("Content-Encoding") != "gzip" || string(calls[1].body) != "\x1f\x8bgzipped" {
		t.Fatalf("a compressed fetch was changed: %v %q", fetch, calls[1].body)
	}
	// A compressed push cannot be checked: refused.
	push := h.do("POST", repoPath+"/git-receive-pack", readWriteLease, strings.NewReader("\x1f\x8b"),
		map[string]string{"Content-Type": "application/x-git-receive-pack-request", "Content-Encoding": "gzip"})
	if push.StatusCode != 415 {
		t.Fatalf("compressed push: %d", push.StatusCode)
	}
	wrong := h.do("POST", repoPath+"/git-upload-pack", readWriteLease, strings.NewReader(flushPkt), rpc("git-receive-pack"))
	if wrong.StatusCode != 415 {
		t.Fatalf("a receive-pack body to upload-pack: %d", wrong.StatusCode)
	}
}

func TestV2CapabilitiesAreStrippedThroughTheDriver(t *testing.T) {
	h := newHarness(t)
	h.handle = func(w http.ResponseWriter, r *http.Request, _ []byte) {
		w.Header().Set("Content-Type", "application/x-git-upload-pack-advertisement")
		w.Write([]byte(push(serviceLine, flushPkt, versionTwoLine, "fetch=shallow packfile-uris\n", "bundle-uri\n", "ls-refs\n", flushPkt)))
	}
	response := h.do("GET", repoPath+"/info/refs?service=git-upload-pack", readWriteLease, nil, map[string]string{"Git-Protocol": "version=2"})
	got := readAll(t, response.Body)
	if want := push(serviceLine, flushPkt, versionTwoLine, "fetch=shallow\n", "ls-refs\n", flushPkt); got != want {
		t.Fatalf("got %q", got)
	}
	if response.Header.Get("Content-Type") != "application/x-git-upload-pack-advertisement" || response.Header.Get("Cache-Control") == "" {
		t.Fatalf("headers %v", response.Header)
	}
}

func TestUpstreamRedirectsAreNeverFollowed(t *testing.T) {
	h := newHarness(t)
	h.handle = func(w http.ResponseWriter, r *http.Request, _ []byte) {
		http.Redirect(w, r, "https://evil.example/steal"+r.URL.Path, http.StatusFound)
	}
	response := h.do("GET", repoPath+"/info/refs?service=git-upload-pack", readWriteLease, nil, nil)
	text := readAll(t, response.Body)
	if response.StatusCode != 502 || !strings.Contains(text, "evil.example") || !strings.Contains(text, "never follows a redirect") {
		t.Fatalf("%d %q", response.StatusCode, text)
	}
	if response.Header.Get("Location") != "" {
		t.Fatal("the redirect was passed on")
	}
	// Every dial goes to the fake server: one request means none followed.
	if calls := h.upstreamCalls(); len(calls) != 1 {
		t.Fatalf("%d upstream requests", len(calls))
	}
}

func TestUpstreamRefusalsAreTheDriversOwn(t *testing.T) {
	for status, want := range map[int]int{401: 502, 403: 403, 404: 404, 500: 502, 503: 502} {
		h := newHarness(t)
		h.handle = func(w http.ResponseWriter, _ *http.Request, _ []byte) {
			w.Header().Set("WWW-Authenticate", `Basic realm="forge"`)
			w.Header().Set("Set-Cookie", "forge=1")
			w.WriteHeader(status)
			io.WriteString(w, "<html>forge page with "+testCredential+"</html>")
		}
		response := h.do("GET", repoPath+"/info/refs?service=git-upload-pack", readWriteLease, nil, nil)
		text := readAll(t, response.Body)
		if response.StatusCode != want || strings.Contains(text, "forge page") || strings.Contains(text, testCredential) {
			t.Errorf("upstream %d: %d %q", status, response.StatusCode, text)
		}
		if response.Header.Get("WWW-Authenticate") != "" || response.Header.Get("Set-Cookie") != "" {
			t.Errorf("upstream %d: a header passed through", status)
		}
	}
	h := newHarness(t)
	h.handle = func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		w.Header().Set("Content-Type", "text/html")
		io.WriteString(w, "dumb")
	}
	if response := h.do("GET", repoPath+"/info/refs?service=git-upload-pack", readWriteLease, nil, nil); response.StatusCode != 502 {
		t.Fatalf("a non-smart answer: %d", response.StatusCode)
	}
}

func TestTheCredentialIsScrubbedFromAnswers(t *testing.T) {
	h := newHarness(t)
	h.handle = func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		w.Header().Set("Content-Type", "application/x-git-upload-pack-result")
		message := "\x02remote: echo " + testCredential + " and " + basicValue(testCredential) + "\n"
		w.Write([]byte(push("NAK\n", message, flushPkt)))
	}
	response := h.do("POST", repoPath+"/git-upload-pack", readWriteLease, strings.NewReader(flushPkt), rpc("git-upload-pack"))
	text := readAll(t, response.Body)
	if strings.Contains(text, testCredential) || strings.Contains(text, basicValue(testCredential)) {
		t.Fatalf("leaked: %q", text)
	}
	// Same length: the pkt-line framing still parses.
	r := newPktReader(bufioReader(text), 1<<20)
	for {
		_, flush, err := r.read()
		if err != nil {
			t.Fatalf("the scrubbed answer no longer parses: %v", err)
		}
		if flush {
			break
		}
	}
	for _, line := range strings.Split(h.logText(), "\n") {
		if strings.Contains(line, testCredential) || strings.Contains(line, readWriteLease) {
			t.Fatalf("a log line holds a secret: %q", line)
		}
	}
}

func TestLFSIsRefusedWithAMessage(t *testing.T) {
	h := newHarness(t)
	for _, target := range []string{
		repoPath + "/info/lfs/objects/batch",
		repoPath + "/info/lfs/locks/verify",
		"/" + testConnector + "/o/r/info/lfs/objects/batch",
	} {
		response := h.do("POST", target, "", strings.NewReader(`{"operation":"upload"}`), map[string]string{"Content-Type": "application/vnd.git-lfs+json"})
		var answer map[string]string
		if err := json.NewDecoder(response.Body).Decode(&answer); err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != 501 || !strings.Contains(answer["message"], "Git LFS is not supported") {
			t.Errorf("%s: %d %v", target, response.StatusCode, answer)
		}
	}
	if len(h.upstreamCalls()) != 0 {
		t.Fatal("an LFS request reached the upstream")
	}
}

func TestTheExchangeDecidesTooAndCachesAtMost30Seconds(t *testing.T) {
	h := newHarness(t)
	target := repoPath + "/info/refs?service=git-upload-pack"
	h.do("GET", target, readWriteLease, nil, nil)
	h.do("GET", target, readWriteLease, nil, nil)
	if ops := h.auth.asked(); len(ops) != 1 {
		t.Fatalf("exchanged %d times within the cache window", len(ops))
	}
	// The connector's repository changed: the exchange no longer allows it.
	h2 := newHarness(t)
	h2.auth.allowed = []string{"https://example.com/o/other.git"}
	if response := h2.do("GET", target, readWriteLease, nil, nil); response.StatusCode != 403 {
		t.Fatalf("another upstream: %d", response.StatusCode)
	}
	if len(h2.upstreamCalls()) != 0 {
		t.Fatal("forwarded to an upstream the exchange did not allow")
	}
	// Revoked between introspection and exchange: 401, so git re-asks.
	h3 := newHarness(t)
	h3.auth.revoked = true
	if response := h3.do("GET", target, readWriteLease, nil, nil); response.StatusCode != 401 {
		t.Fatalf("revoked: %d", response.StatusCode)
	}
}

func TestStreamsBothWaysWithoutBuffering(t *testing.T) {
	h := newHarness(t)
	firstIn := make(chan string, 1)
	release := make(chan struct{})
	h.stream = func(w http.ResponseWriter, r *http.Request) {
		// The request streams: the first part arrives before the client
		// has sent the rest.
		buf := make([]byte, 4)
		io.ReadFull(r.Body, buf)
		firstIn <- string(buf)
		<-release
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/x-git-upload-pack-result")
		w.Write([]byte(pkt("NAK\n")))
		w.(http.Flusher).Flush()
		// The answer streams: the client reads the first packet while
		// the upstream still holds the rest.
		<-release
		w.Write([]byte(flushPkt))
	}
	reader, writer := io.Pipe()
	request, _ := http.NewRequest("POST", h.server.URL+repoPath+"/git-upload-pack", reader)
	request.Header.Set("Authorization", basic(readWriteLease))
	request.Header.Set("Content-Type", "application/x-git-upload-pack-request")
	done := make(chan *http.Response, 1)
	go func() {
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Error(err)
		}
		done <- response
	}()
	writer.Write([]byte("0032want "))
	select {
	case got := <-firstIn:
		if got != "0032" {
			t.Fatalf("first bytes %q", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the request body was buffered")
	}
	release <- struct{}{}
	writer.Write([]byte(strings.Repeat("x", 100)))
	writer.Close()
	var response *http.Response
	select {
	case response = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the answer's head was buffered")
	}
	defer response.Body.Close()
	first := make([]byte, len(pkt("NAK\n")))
	if _, err := io.ReadFull(response.Body, first); err != nil || string(first) != pkt("NAK\n") {
		t.Fatalf("first packet %q %v", first, err)
	}
	release <- struct{}{}
	if rest := readAll(t, response.Body); rest != flushPkt {
		t.Fatalf("rest %q", rest)
	}
}

func TestAnIdleTransferIsCut(t *testing.T) {
	h := newHarness(t)
	h.driver.idle = 200 * time.Millisecond
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	h.stream = func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-git-upload-pack-result")
		w.Write([]byte(pkt("NAK\n")))
		w.(http.Flusher).Flush()
		select {
		case <-stop:
		case <-r.Context().Done():
		}
	}
	started := time.Now()
	response := h.do("POST", repoPath+"/git-upload-pack", readWriteLease, strings.NewReader(flushPkt), rpc("git-upload-pack"))
	_, err := io.ReadAll(response.Body)
	if err == nil {
		t.Fatal("an idle transfer ended as if complete")
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("cut after %s", elapsed)
	}
}

func TestALeaseThatEndsDuringATransferCutsIt(t *testing.T) {
	for name, end := range map[string]func(*fakeAuthority){
		"revoked":     func(f *fakeAuthority) { f.ended = true },
		"unconfirmed": func(f *fakeAuthority) { f.down = true },
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			h.driver.recheck = 100 * time.Millisecond
			stop := make(chan struct{})
			t.Cleanup(func() { close(stop) })
			sent := make(chan struct{})
			h.stream = func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/x-git-upload-pack-result")
				w.Write([]byte(pkt("NAK\n")))
				w.(http.Flusher).Flush()
				close(sent)
				// A long clone keeps its bytes moving: never idle.
				tick := time.NewTicker(20 * time.Millisecond)
				defer tick.Stop()
				for {
					select {
					case <-stop:
						return
					case <-r.Context().Done():
						return
					case <-tick.C:
						if _, err := w.Write([]byte(pkt("\x02progress\n"))); err != nil {
							return
						}
						w.(http.Flusher).Flush()
					}
				}
			}
			response := h.do("POST", repoPath+"/git-upload-pack", readWriteLease, strings.NewReader(flushPkt), rpc("git-upload-pack"))
			<-sent
			// The lease lives through several checks: the transfer goes on.
			time.Sleep(350 * time.Millisecond)
			h.auth.mu.Lock()
			checks := h.auth.introspected
			end(h.auth)
			h.auth.mu.Unlock()
			if checks < 3 {
				t.Fatalf("the lease was asked about %d times during the transfer", checks)
			}
			started := time.Now()
			_, err := io.ReadAll(response.Body)
			if err == nil {
				t.Fatal("the transfer ended as if complete")
			}
			if elapsed := time.Since(started); elapsed > 2*time.Second {
				t.Fatalf("cut after %s", elapsed)
			}
			if !strings.Contains(h.logText(), "cut") {
				t.Fatalf("log %q", h.logText())
			}
		})
	}
}

func TestTheDefaultIdleTimeoutIsAtLeastTenMinutes(t *testing.T) {
	if defaultIdleTimeout < 10*time.Minute {
		t.Fatal(defaultIdleTimeout)
	}
	server := newServer(":0", http.NotFoundHandler())
	if server.ReadTimeout != 0 || server.WriteTimeout != 0 {
		t.Fatal("a transfer has a total time limit")
	}
	if server.TLSNextProto == nil || len(server.TLSNextProto) != 0 {
		t.Fatal("HTTP/2 is on")
	}
}

func TestPerLeaseInFlightCap(t *testing.T) {
	in := &inflight{perLease: map[string]int{}}
	var releases []func()
	for i := 0; i < maxInFlightPerLease; i++ {
		release, ok := in.acquire("a")
		if !ok {
			t.Fatal("refused under the cap")
		}
		releases = append(releases, release)
	}
	if _, ok := in.acquire("a"); ok {
		t.Fatal("admitted over the cap")
	}
	if release, ok := in.acquire("b"); !ok {
		t.Fatal("another lease was capped")
	} else {
		release()
	}
	for _, release := range releases {
		release()
	}
	if len(in.perLease) != 0 || in.total != 0 {
		t.Fatalf("leaked %v %d", in.perLease, in.total)
	}
}
