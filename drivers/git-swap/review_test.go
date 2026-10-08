package main

// The C3 review's cases: mutations the first suite let survive, the
// upstream probe at start, the connector's upstream CA, short tokens and
// the masking log.

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// A cache answer longer than the revocation lag is cut to it: a credential
// is never reused past 30 s, whatever the exchange says.
func TestTheCredentialCacheNeverOutlivesTheLag(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	clock := func() time.Time { return now }
	authority := &fakeAuthority{cache: 300 * time.Second}
	cache := newAuthCache(authority, clock)
	ctx := context.Background()
	for _, step := range []time.Duration{0, 29 * time.Second} {
		now = time.Unix(1_000_000, 0).Add(step)
		if _, err := cache.credential(ctx, readWriteLease, "read"); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(authority.asked()); got != 1 {
		t.Fatalf("exchanged %d times within the lag", got)
	}
	now = time.Unix(1_000_000, 0).Add(31 * time.Second)
	if _, err := cache.credential(ctx, readWriteLease, "read"); err != nil {
		t.Fatal(err)
	}
	if got := len(authority.asked()); got != 2 {
		t.Fatalf("a credential was reused past 30 s (%d exchanges)", got)
	}
}

// A lease decision is never kept past the lease's own expiry, even inside
// the 30 s window.
func TestTheLeaseCacheNeverOutlivesTheLeasesExpiry(t *testing.T) {
	now := time.Unix(2_000_000, 0)
	clock := func() time.Time { return now }
	authority := &fakeAuthority{expires: now.Add(5 * time.Second)}
	cache := newAuthCache(authority, clock)
	ctx := context.Background()
	if _, err := cache.lease(ctx, readWriteLease); err != nil {
		t.Fatal(err)
	}
	now = now.Add(4 * time.Second)
	cache.lease(ctx, readWriteLease)
	if authority.introspected != 1 {
		t.Fatalf("introspected %d times before the expiry", authority.introspected)
	}
	now = now.Add(2 * time.Second) // past the expiry, inside 30 s
	cache.lease(ctx, readWriteLease)
	if authority.introspected != 2 {
		t.Fatal("a lease decision outlived the lease's expiry")
	}
}

func TestACompressedAdvertisementIsRefused(t *testing.T) {
	h := newHarness(t)
	h.handle = func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		w.Header().Set("Content-Type", "application/x-git-upload-pack-advertisement")
		w.Header().Set("Content-Encoding", "gzip")
		w.Write([]byte("\x1f\x8bcompressed"))
	}
	response := h.do("GET", repoPath+"/info/refs?service=git-upload-pack", readWriteLease, nil, nil)
	text := readAll(t, response.Body)
	if response.StatusCode != 502 || !strings.Contains(text, "compressed its ref advertisement") {
		t.Fatalf("%d %q", response.StatusCode, text)
	}
}

func TestOnlyAWellFormedGitProtocolHeaderPasses(t *testing.T) {
	h := newHarness(t)
	for _, value := range []string{
		"version=2<script>",
		"version=2;" + strings.Repeat("x", 300),
		"version=2\tx",
		"version=2,\"quoted\"",
	} {
		response := h.do("GET", repoPath+"/info/refs?service=git-upload-pack", readWriteLease, nil, map[string]string{"Git-Protocol": value})
		readAll(t, response.Body)
	}
	h.do("GET", repoPath+"/info/refs?service=git-upload-pack", readWriteLease, nil, map[string]string{"Git-Protocol": "version=2:object-format=sha1"})
	calls := h.upstreamCalls()
	if len(calls) != 5 {
		t.Fatalf("%d calls", len(calls))
	}
	for _, call := range calls[:4] {
		if got := call.header.Get("Git-Protocol"); got != "" {
			t.Errorf("a malformed Git-Protocol passed: %q", got)
		}
	}
	if got := calls[4].header.Get("Git-Protocol"); got != "version=2:object-format=sha1" {
		t.Fatalf("a valid Git-Protocol was dropped: %q", got)
	}
}

// countingReader yields n zero bytes and counts what was read.
type countingReader struct {
	left int64
	read atomic.Int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	if c.left <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > c.left {
		p = p[:c.left]
	}
	for i := range p {
		p[i] = 0
	}
	c.left -= int64(len(p))
	c.read.Add(int64(len(p)))
	return len(p), nil
}

// A refused push is read to its end before the report: git sends its whole
// pack before it reads an answer, and a report sent early is lost when the
// server closes the connection on the unread rest.
func TestARefusedPushIsDrainedBeforeItsReport(t *testing.T) {
	h := newHarness(t)
	const packBytes = 32 << 20
	head := push(zero40+" "+newID+" refs/tags/v1\x00report-status\n", flushPkt) + "PACK"
	pack := &countingReader{left: packBytes}
	body := io.MultiReader(strings.NewReader(head), pack)
	response := h.do("POST", repoPath+"/git-receive-pack", readWriteLease, body, rpc("git-receive-pack"))
	text := readAll(t, response.Body)
	if response.StatusCode != 200 || !strings.Contains(text, "ng refs/tags/v1") {
		t.Fatalf("%d %q", response.StatusCode, text)
	}
	if got := pack.read.Load(); got != packBytes {
		t.Fatalf("the driver answered after reading %d of %d pack bytes", got, packBytes)
	}
	if len(h.upstreamCalls()) != 0 {
		t.Fatal("a refused push reached the upstream")
	}
}

func TestAShortForgeTokenIsNotUsed(t *testing.T) {
	h := newHarness(t)
	h.auth.credential = "short-token"
	response := h.do("GET", repoPath+"/info/refs?service=git-upload-pack", readWriteLease, nil, nil)
	text := readAll(t, response.Body)
	if response.StatusCode != 502 || !strings.Contains(text, "shorter than 16 characters") {
		t.Fatalf("%d %q", response.StatusCode, text)
	}
	if len(h.upstreamCalls()) != 0 || strings.Contains(h.logText(), "short-token") {
		t.Fatal("a short token was used or logged")
	}
}

func TestMaskingTheCredentialIsLogged(t *testing.T) {
	h := newHarness(t)
	h.handle = func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		w.Header().Set("Content-Type", "application/x-git-upload-pack-result")
		w.Write([]byte(push("NAK\n", "\x02remote: "+testCredential+"\n", flushPkt)))
	}
	response := h.do("POST", repoPath+"/git-upload-pack", readWriteLease, strings.NewReader(flushPkt), rpc("git-upload-pack"))
	readAll(t, response.Body)
	if !strings.Contains(h.logText(), "masked 1 occurrence(s) of the connector's credential") {
		t.Fatalf("log %q", h.logText())
	}
	clean := newHarness(t)
	readAll(t, clean.do("POST", repoPath+"/git-upload-pack", readWriteLease, strings.NewReader(flushPkt), rpc("git-upload-pack")).Body)
	if strings.Contains(clean.logText(), "masked") {
		t.Fatal("an answer without the credential was reported as masked")
	}
}

// The probe at start: any HTTP answer of a trusted upstream will do; an
// untrusted certificate is final at once; a dead address is retried.
func TestTheUpstreamProbeAtStart(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			t.Error("the probe sent a credential")
		}
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()
	served, err := parseUpstream("https://example.com/o/r.git")
	if err != nil {
		t.Fatal(err)
	}
	address := server.Listener.Addr().String()
	client := func(roots *x509.CertPool, to string) *http.Client {
		c := newUpstreamClient(roots)
		transport := c.Transport.(*http.Transport)
		transport.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, to)
		}
		return c
	}
	trusted := x509.NewCertPool()
	trusted.AddCert(server.Certificate())
	if problem := probeUpstream(context.Background(), client(trusted, address), served, 3, time.Millisecond); problem != "" {
		t.Fatalf("a trusted upstream: %q", problem)
	}
	started := time.Now()
	problem := probeUpstream(context.Background(), client(nil, address), served, 3, time.Second)
	if !strings.Contains(problem, "does not verify") || time.Since(started) > 900*time.Millisecond {
		t.Fatalf("an untrusted upstream: %q after %s", problem, time.Since(started))
	}
	dead, _ := net.Listen("tcp", "127.0.0.1:0")
	deadAddress := dead.Addr().String()
	dead.Close()
	problem = probeUpstream(context.Background(), client(trusted, deadAddress), served, 2, time.Millisecond)
	if !strings.Contains(problem, "is unreachable") || len(problem) > maxProbeReport {
		t.Fatalf("an unreachable upstream: %q", problem)
	}
	log := filepath.Join(t.TempDir(), "termination-log")
	writeTermination(log, problem)
	if raw, _ := os.ReadFile(log); strings.TrimSpace(string(raw)) != problem {
		t.Fatalf("termination message %q", raw)
	}
	if upstreamExitCode != 78 {
		t.Fatal("the exit code is the reconciler's UPSTREAM_EXIT_CODE")
	}
}

func TestTheConnectorsUpstreamCAIsTheOnlyRoots(t *testing.T) {
	server := httptest.NewTLSServer(http.NotFoundHandler())
	defer server.Close()
	certificate := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}))
	roots, err := upstreamRoots(certificate + "\n" + certificate)
	if err != nil || roots == nil {
		t.Fatalf("a PEM CA: %v", err)
	}
	if none, err := upstreamRoots("  \n"); none != nil || err != nil {
		t.Fatal("no CA means the public roots")
	}
	for name, text := range map[string]string{
		"a key":        "-----BEGIN PRIVATE KEY-----\nMIIB\n-----END PRIVATE KEY-----\n",
		"garbage":      "not pem",
		"a bad cert":   "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n",
		"text after":   certificate + "trailing",
		"cert and key": certificate + "-----BEGIN RSA PRIVATE KEY-----\nMIIB\n-----END RSA PRIVATE KEY-----\n",
	} {
		if _, err := upstreamRoots(text); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	// Through the request file: the driver's client verifies against it.
	cfg, err := parseConfig(requestWithCA(certificate), "sdi_"+repeat("A", 49))
	if err != nil {
		t.Fatal(err)
	}
	served, _ := parseUpstream("https://example.com/o/r.git")
	c := newUpstreamClient(cfg.upstreamRoots)
	c.Transport.(*http.Transport).DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
	}
	if problem := probeUpstream(context.Background(), c, served, 1, time.Millisecond); problem != "" {
		t.Fatalf("the connector's CA was not used: %q", problem)
	}
}

func requestWithCA(ca string) requestFile {
	var r requestFile
	r.ProtocolVersion = "1.0"
	r.Plane = "service"
	r.Driver = "srw.git-swap/v1"
	r.Connector.ID = testConnector
	r.Connector.Config.Upstream = "https://example.com/o/r.git"
	r.Connector.Config.Host = "example.com"
	r.Connector.Config.UpstreamCA = ca
	r.Service.Port = 8443
	r.Exchange.URL = "http://srw-exchange.srw.svc:8088"
	r.TLS = &struct {
		CertFile string `json:"cert_file"`
		KeyFile  string `json:"key_file"`
	}{"/run/srw/tls.crt", "/run/srw/tls.key"}
	return r
}
