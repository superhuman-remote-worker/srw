package api

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/superhuman-remote-worker/srw/cli/internal/exit"
	"github.com/superhuman-remote-worker/srw/cli/internal/target"
)

const token = "ak_secret_token"

func clientFor(t *testing.T, server string, options Options) *Client {
	t.Helper()
	base, err := url.Parse(server)
	if err != nil {
		t.Fatal(err)
	}
	if options.Pauses == nil {
		options.Pauses = []time.Duration{0, 0}
	}
	client, err := New(&target.Target{Server: base, Token: token}, "1.2.3", options)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func coded(t *testing.T, err error) *exit.Error {
	t.Helper()
	var failure *exit.Error
	if !errors.As(err, &failure) {
		t.Fatalf("not an exit error: %v", err)
	}
	return failure
}

func TestARequestCarriesTheTokenAndSaysWhoSendsIt(t *testing.T) {
	var seen *http.Request
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.Clone(context.Background())
		w.Write([]byte(`{"ok": true}`))
	}))
	defer server.Close()
	var out struct{ OK bool }
	err := clientFor(t, server.URL+"/base/", Options{}).JSON(context.Background(),
		Request{Method: "GET", Path: "/api/jobs", Query: url.Values{"status": {"failed"}}}, &out)
	if err != nil || !out.OK {
		t.Fatal(err)
	}
	if seen.Header.Get("Authorization") != "Bearer "+token || seen.Header.Get("User-Agent") != "srw/1.2.3" {
		t.Errorf("headers: %v", seen.Header)
	}
	if seen.URL.Path != "/base/api/jobs" || seen.URL.RawQuery != "status=failed" {
		t.Errorf("url: %s", seen.URL)
	}
}

func TestOnlyAPathOnTheConfiguredServerIsAccepted(t *testing.T) {
	client := clientFor(t, "https://api.example.com", Options{})
	for _, path := range []string{"https://evil.example.com/api/jobs", "api/jobs", "//evil.example.com/x://y"} {
		if _, err := client.Do(context.Background(), Request{Method: "GET", Path: path}); err == nil || coded(t, err).Code != exit.Usage {
			t.Errorf("%q: %v", path, err)
		}
	}
}

// flaky fails the first calls with a connection error, as a dropped
// connection does, then hands over to the real transport.
type flaky struct {
	failures int32
	calls    atomic.Int32
	next     http.RoundTripper
}

func (f *flaky) RoundTrip(request *http.Request) (*http.Response, error) {
	if f.calls.Add(1) <= f.failures {
		return nil, &net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset by peer")}
	}
	return f.next.RoundTrip(request)
}

func TestAGetIsTriedThreeTimesOnAConnectionErrorAndOnNothingElse(t *testing.T) {
	var served atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		served.Add(1)
		if r.URL.Path == "/broken" {
			w.WriteHeader(500)
		}
	}))
	defer server.Close()

	recovering := &flaky{failures: 2, next: http.DefaultTransport}
	if _, err := clientFor(t, server.URL, Options{Transport: recovering}).Do(context.Background(), Request{Method: "GET", Path: "/ok"}); err != nil {
		t.Fatalf("the third try must succeed: %v", err)
	}
	if recovering.calls.Load() != 3 {
		t.Errorf("tries: %d", recovering.calls.Load())
	}

	dead := &flaky{failures: 99, next: http.DefaultTransport}
	_, err := clientFor(t, server.URL, Options{Transport: dead}).Do(context.Background(), Request{Method: "GET", Path: "/ok"})
	if coded(t, err).Code != exit.Unreachable || dead.calls.Load() != 3 {
		t.Errorf("after three tries: %v, tries %d", err, dead.calls.Load())
	}

	// A server error is an answer, not a connection error: one try.
	served.Store(0)
	response, err := clientFor(t, server.URL, Options{}).Do(context.Background(), Request{Method: "GET", Path: "/broken"})
	if err != nil || response.Status != 500 || served.Load() != 1 {
		t.Errorf("a 500: %v %v, served %d", response, err, served.Load())
	}
}

func TestAMutationIsNeverRepeated(t *testing.T) {
	for _, method := range []string{"POST", "PUT", "DELETE", "PATCH"} {
		transport := &flaky{failures: 99, next: http.DefaultTransport}
		_, err := clientFor(t, "https://api.example.com", Options{Transport: transport}).Do(context.Background(), Request{Method: method, Path: "/api/jobs"})
		if err == nil || transport.calls.Load() != 1 {
			t.Errorf("%s: tries %d, %v", method, transport.calls.Load(), err)
		}
	}
}

func TestALostResponseToAMutationIsOutcomeUnknown(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The request arrived; the connection dies before any answer.
		connection, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			connection.Close()
		}
	}))
	defer server.Close()
	client := clientFor(t, server.URL, Options{})
	_, err := client.Do(context.Background(), Request{Method: "POST", Path: "/api/jobs", Body: map[string]string{"description": "x"}})
	if failure := coded(t, err); failure.Code != exit.Unknown || !strings.Contains(failure.Hint, "check before sending it again") {
		t.Errorf("%+v", failure)
	}
	// The same loss on a read is only "not reachable": nothing can have changed.
	_, err = client.Do(context.Background(), Request{Method: "GET", Path: "/api/jobs"})
	if coded(t, err).Code != exit.Unreachable {
		t.Errorf("a lost read: %v", err)
	}
}

func TestARequestThatNeverLeftIsNotOutcomeUnknown(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	_, err = clientFor(t, "http://"+address, Options{}).Do(context.Background(), Request{Method: "POST", Path: "/api/jobs"})
	if coded(t, err).Code != exit.Unreachable {
		t.Errorf("a refused connection: %v", err)
	}
}

func TestARedirectIsReportedAndNotFollowed(t *testing.T) {
	var followed atomic.Bool
	elsewhere := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { followed.Store(true) }))
	defer elsewhere.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, elsewhere.URL+"/login?next="+token, http.StatusFound)
	}))
	defer server.Close()
	client := clientFor(t, server.URL, Options{})
	_, err := client.Send(context.Background(), Request{Method: "GET", Path: "/api/jobs"})
	failure := coded(t, err)
	if failure.Code != exit.Usage || followed.Load() {
		t.Errorf("%+v, followed %v", failure, followed.Load())
	}
	if strings.Contains(failure.Message, token) {
		t.Errorf("the token is in the message: %q", failure.Message)
	}
}

func TestAnUntrustedCertificateIsUnreachableWithAHint(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()
	_, err := clientFor(t, server.URL, Options{}).Do(context.Background(), Request{Method: "POST", Path: "/api/jobs"})
	if failure := coded(t, err); failure.Code != exit.Unreachable || !strings.Contains(failure.Hint, "--ca-file") {
		t.Errorf("%+v", failure)
	}
}

func TestACAFileReplacesTheSystemRootsAndMustHoldACertificate(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("{}")) }))
	defer server.Close()
	directory := t.TempDir()
	good := filepath.Join(directory, "ca.pem")
	if err := os.WriteFile(good, pemOf(server.Certificate().Raw), 0o600); err != nil {
		t.Fatal(err)
	}
	base, _ := url.Parse(server.URL)
	client, err := New(&target.Target{Server: base, Token: token, CAFile: good}, "1.2.3", Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Send(context.Background(), Request{Method: "GET", Path: "/api/health"}); err != nil {
		t.Errorf("the server's own certificate in --ca-file must be trusted: %v", err)
	}
	empty := filepath.Join(directory, "empty.pem")
	os.WriteFile(empty, []byte("not a certificate\n"), 0o600)
	if _, err := New(&target.Target{Server: base, CAFile: empty}, "1.2.3", Options{}); err == nil || coded(t, err).Code != exit.Usage {
		t.Errorf("a CA file without a certificate: %v", err)
	}
}

func TestTheProxyFunctionCanBeReplaced(t *testing.T) {
	var viaProxy atomic.Bool
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		viaProxy.Store(true)
		w.Write([]byte("{}"))
	}))
	defer proxy.Close()
	proxyURL, _ := url.Parse(proxy.URL)
	client := clientFor(t, "http://api.internal.example", Options{Proxy: http.ProxyURL(proxyURL)})
	if _, err := client.Send(context.Background(), Request{Method: "GET", Path: "/api/health"}); err != nil || !viaProxy.Load() {
		t.Errorf("the request did not go through the proxy: %v", err)
	}
}

func TestStatusErrors(t *testing.T) {
	cases := []struct {
		name    string
		status  int
		body    string
		code    int
		message string
		hint    string
	}{
		{"unknown token", 401, `{"detail":"Invalid token"}`, exit.Auth, "the token is unknown, revoked or expired", "srw login"},
		{"a missing scope names it", 403, `{"detail":"Insufficient token scope: requires jobs:write, projects:write"}`, exit.Auth,
			"Insufficient token scope: requires jobs:write, projects:write", "also has `jobs:write` and `projects:write`"},
		{"a route no token may use", 403, `{"detail":"Insufficient token scope: personal access tokens cannot use this endpoint"}`, exit.Auth, "this route is not available to tokens", ""},
		{"another refusal is the server's line", 403, `{"detail":"Access denied by MCP token scope"}`, exit.Auth, "Access denied by MCP token scope", ""},
		{"a route the server does not have", 404, `{"detail":"Not Found"}`, exit.NotFound, "this server does not have GET /api/x", ""},
		{"an object that is not there", 404, `{"detail":"Job 'abc' not found"}`, exit.NotFound, "Job 'abc' not found", ""},
		{"conflict", 409, `{"detail":"Resource version changed; read the current resource and retry."}`, exit.Conflict, "Resource version changed; read the current resource and retry.", ""},
		{"a server error without a body", 502, `<html>bad gateway</html>`, exit.Failed, "GET /api/x answered 502 Bad Gateway", ""},
		{"a validation list never echoes the input", 422,
			`{"detail":[{"type":"missing","loc":["body","source"],"msg":"Field required","input":{"sourec":"ak_secret_token PRIVATE"}}]}`,
			exit.Failed, "the request was not accepted: source: Field required", ""},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			client := clientFor(t, "https://api.example.com", Options{})
			request := Request{Method: "GET", Path: "/api/x"}
			failure := coded(t, client.StatusError(request, &Response{Status: test.status, Body: []byte(test.body)}))
			if failure.Code != test.code || failure.Message != test.message || failure.Status != test.status {
				t.Errorf("%+v", failure)
			}
			if test.hint != "" && !strings.Contains(failure.Hint, test.hint) {
				t.Errorf("hint: %q", failure.Hint)
			}
			if strings.Contains(failure.Message+failure.Hint, "PRIVATE") {
				t.Errorf("the request body is in the error: %+v", failure)
			}
		})
	}
	client := clientFor(t, "https://api.example.com", Options{})
	if err := client.StatusError(Request{}, &Response{Status: 204}); err != nil {
		t.Errorf("a 204 is a success: %v", err)
	}
}

func TestAMissingRouteOfSrwsOwnSaysTheServerMayBeOlder(t *testing.T) {
	client := clientFor(t, "https://api.example.com", Options{})
	request := Request{Method: "GET", Path: "/api/version", Known: true}
	failure := coded(t, client.StatusError(request, &Response{Status: 404, Body: []byte(`{"detail":"Not Found"}`)}))
	if failure.Message != "this server does not have GET /api/version; it may be older than this srw" {
		t.Errorf("%q", failure.Message)
	}
}

func TestAManifestErrorKeepsItsLocation(t *testing.T) {
	client := clientFor(t, "https://api.example.com", Options{})
	body := `{"detail":{"code":"DuplicateKey","message":"Duplicate object key.","document":2,"path":"/spec","source":"experts/b.yaml"}}`
	failure := coded(t, client.StatusError(Request{Method: "POST", Path: "/api/manifests/validate"}, &Response{Status: 422, Body: []byte(body)}))
	if failure.ErrCode != "DuplicateKey" || failure.Message != "Duplicate object key." || failure.Document != 2 ||
		failure.Path != "/spec" || failure.File != "experts/b.yaml" || failure.Code != exit.Failed {
		t.Errorf("%+v", failure)
	}
}

func TestTheTokenIsRemovedFromEverythingPrinted(t *testing.T) {
	client := clientFor(t, "https://api.example.com", Options{})
	if got := client.Redact("dial failed for Bearer " + token); strings.Contains(got, token) || !strings.Contains(got, "<token>") {
		t.Errorf("%q", got)
	}
	failure := coded(t, client.StatusError(Request{}, &Response{Status: 400, Body: []byte(`{"detail":"bad header ` + token + `"}`)}))
	if strings.Contains(failure.Message, token) {
		t.Errorf("%q", failure.Message)
	}
}

func TestDeadlines(t *testing.T) {
	client := clientFor(t, "https://api.example.com", Options{})
	if client.timeout("GET") != 30*time.Second || client.timeout("PUT") != 60*time.Second {
		t.Errorf("reads get 30 s and mutations 60 s")
	}
}
