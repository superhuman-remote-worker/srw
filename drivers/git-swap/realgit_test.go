package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/cgi"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// The driver between a real git client and a real smart-HTTP server (git
// http-backend), wired as SRW wires a workspace: the remote is the clean
// upstream URL, insteadOf points it at the driver, a credential helper
// scoped to the driver's URL answers with the lease, and the driver's
// authority is trusted for its URL only.
type realGit struct {
	t         *testing.T
	git       string
	dir       string
	home      string
	leaseFile string
	upstream  string // the bare repository
	env       []string
	injected  atomic.Int64
	chunked   atomic.Int64
}

func newRealGit(t *testing.T) *realGit {
	t.Helper()
	git, err := exec.LookPath("git")
	if err != nil {
		t.Skip("git is not installed")
	}
	g := &realGit{t: t, git: git, dir: t.TempDir()}
	g.home = filepath.Join(g.dir, "home")
	g.upstream = filepath.Join(g.dir, "upstream", "o", "r.git")
	g.leaseFile = filepath.Join(g.dir, "lease")
	os.MkdirAll(g.home, 0o700)
	g.env = []string{
		"HOME=" + g.home,
		"PATH=" + os.Getenv("PATH"),
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_TERMINAL_PROMPT=0",
		"GIT_ASKPASS=",
		"GIT_AUTHOR_NAME=Agent", "GIT_AUTHOR_EMAIL=agent@srw.local",
		"GIT_COMMITTER_NAME=Agent", "GIT_COMMITTER_EMAIL=agent@srw.local",
	}
	g.run("", "init", "--bare", "-b", "main", g.upstream)
	g.run(g.upstream, "config", "http.receivepack", "true")
	seed := filepath.Join(g.dir, "seed")
	g.run("", "init", "-b", "main", seed)
	os.WriteFile(filepath.Join(seed, "README"), []byte("hello\n"), 0o644)
	g.run(seed, "add", "README")
	g.run(seed, "commit", "-m", "seed")
	g.run(seed, "push", g.upstream, "main")

	// The upstream: git http-backend, which only answers the injected token.
	backend := &cgi.Handler{
		Path: git,
		Args: []string{"http-backend"},
		Env: []string{
			"GIT_PROJECT_ROOT=" + filepath.Join(g.dir, "upstream"),
			"GIT_HTTP_EXPORT_ALL=1",
			"GIT_CONFIG_NOSYSTEM=1",
			"HOME=" + g.dir,
		},
	}
	want := "Basic " + base64.StdEncoding.EncodeToString([]byte("oauth2:"+testCredential))
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != want {
			w.Header().Set("WWW-Authenticate", `Basic realm="forge"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		g.injected.Add(1)
		if len(r.TransferEncoding) > 0 {
			// The driver passed git's chunked body on; Go's CGI host takes
			// none, so this test server reads it whole first (a forge does
			// not need to).
			g.chunked.Add(1)
			body, err := io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
			r.ContentLength = int64(len(body))
			r.TransferEncoding = nil
		}
		backend.ServeHTTP(w, r)
	}))
	t.Cleanup(upstream.Close)
	pool := x509.NewCertPool()
	pool.AddCert(upstream.Certificate())
	client := newUpstreamClient()
	transport := client.Transport.(*http.Transport)
	transport.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	address := upstream.Listener.Addr().String()
	transport.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}
	served, _ := parseUpstream("https://example.com/o/r.git")
	cfg := &config{driver: "srw.git-swap/v1", connectorID: testConnector, upstream: served, port: "8443"}
	front := httptest.NewUnstartedServer(newDriver(cfg, &fakeAuthority{}, client, t.Logf, time.Now))
	front.TLS = newServer("", nil).TLSConfig
	front.StartTLS()
	t.Cleanup(front.Close)

	// The workspace's wiring.
	ca := filepath.Join(g.dir, "driver-ca.pem")
	os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: front.Certificate().Raw}), 0o600)
	helper := filepath.Join(g.dir, "credential-helper")
	os.WriteFile(helper, []byte(fmt.Sprintf(
		"#!/bin/sh\n[ \"$1\" = get ] || exit 0\nprintf 'username=srw-lease\\npassword=%%s\\n' \"$(cat %s)\"\n", g.leaseFile)), 0o700)
	driverURL := front.URL // https://127.0.0.1:port
	include := filepath.Join(g.home, "srw-git-swap.gitconfig")
	os.WriteFile(include, []byte(fmt.Sprintf(`[url "%s/%s/o/r"]
	insteadOf = https://example.com/o/r
[credential "%s"]
	helper =
	helper = %s
	useHttpPath = true
[http "%s/"]
	sslCAInfo = %s
`, driverURL, testConnector, driverURL, helper, driverURL, ca)), 0o600)
	g.run("", "config", "--global", "--add", "include.path", include)
	g.lease(readWriteLease)
	return g
}

func (g *realGit) lease(token string) {
	os.WriteFile(g.leaseFile, []byte(token), 0o600)
}

func (g *realGit) git2(dir string, args ...string) (string, error) {
	command := exec.Command(g.git, args...)
	command.Dir = dir
	if dir == "" {
		command.Dir = g.dir
	}
	command.Env = g.env
	out, err := command.CombinedOutput()
	return string(out), err
}

func (g *realGit) run(dir string, args ...string) string {
	g.t.Helper()
	out, err := g.git2(dir, args...)
	if err != nil {
		g.t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return out
}

func TestRealGitClonesAndPushesThroughTheDriver(t *testing.T) {
	g := newRealGit(t)
	work := filepath.Join(g.dir, "work")
	g.run("", "clone", "https://example.com/o/r.git", work)
	// Stored clean; "git remote -v" shows it with insteadOf applied (the
	// driver's URL), which holds no token either.
	if remote := strings.TrimSpace(g.run(work, "config", "--get", "remote.origin.url")); remote != "https://example.com/o/r.git" {
		t.Fatalf("the remote is %q, not the clean upstream URL", remote)
	}
	if shown := g.run(work, "remote", "-v"); !strings.Contains(shown, "/"+testConnector+"/o/r.git") || strings.Contains(shown, "@") {
		t.Fatalf("git remote -v shows %q", shown)
	}
	config, _ := os.ReadFile(filepath.Join(work, ".git", "config"))
	if strings.Contains(string(config), testCredential) || strings.Contains(string(config), readWriteLease) {
		t.Fatal(".git/config holds a token")
	}
	// A branch push, past git's post buffer: the probe (body 0000), then a
	// chunked body.
	os.WriteFile(filepath.Join(work, "big"), []byte(strings.Repeat("random-ish data\n", 200000)), 0o644)
	g.run(work, "checkout", "-b", "feature")
	g.run(work, "add", "big")
	g.run(work, "commit", "-m", "big")
	g.run(work, "-c", "http.postBuffer=1024", "push", "origin", "feature")
	if out := g.run(g.upstream, "rev-parse", "refs/heads/feature"); len(strings.TrimSpace(out)) != 40 {
		t.Fatalf("the branch did not arrive: %q", out)
	}
	if g.chunked.Load() == 0 {
		t.Fatal("the push's chunked body did not reach the upstream chunked")
	}
	// Protocol v2 and v0 fetches.
	g.run(work, "-c", "protocol.version=2", "fetch", "origin")
	g.run(work, "-c", "protocol.version=0", "ls-remote", "origin")
	if g.injected.Load() == 0 {
		t.Fatal("the upstream never saw the injected credential")
	}
}

func TestRealGitSeesWhyATagOrADeleteIsRefused(t *testing.T) {
	g := newRealGit(t)
	work := filepath.Join(g.dir, "work")
	g.run("", "clone", "https://example.com/o/r.git", work)
	g.run(work, "tag", "v1")
	out, err := g.git2(work, "push", "origin", "v1")
	if err == nil || !strings.Contains(out, "only branches (refs/heads/*) may be pushed") {
		t.Fatalf("tag push: %v\n%s", err, out)
	}
	out, err = g.git2(work, "push", "origin", "--delete", "main")
	if err == nil || !strings.Contains(out, "deleting a ref is not allowed") {
		t.Fatalf("delete: %v\n%s", err, out)
	}
	if out := g.run(g.upstream, "for-each-ref"); strings.Contains(out, "refs/tags/v1") || !strings.Contains(out, "refs/heads/main") {
		t.Fatalf("the upstream changed: %s", out)
	}
}

func TestRealGitReadOnlyAndDeadLeases(t *testing.T) {
	g := newRealGit(t)
	work := filepath.Join(g.dir, "work")
	g.lease(readOnlyLease)
	g.run("", "clone", "https://example.com/o/r.git", work)
	g.run(work, "commit", "--allow-empty", "-m", "x")
	out, err := g.git2(work, "push", "origin", "HEAD:refs/heads/ro")
	if err == nil || !strings.Contains(out, "read-only") {
		t.Fatalf("read-only push: %v\n%s", err, out)
	}
	g.lease(deadLease)
	out, err = g.git2(work, "fetch", "origin")
	if err == nil || !strings.Contains(out, "Authentication failed") {
		t.Fatalf("dead lease: %v\n%s", err, out)
	}
}
