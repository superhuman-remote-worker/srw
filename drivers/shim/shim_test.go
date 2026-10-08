package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const testIdentity = "sdi_" + "0123456789ABCDEFGHIJabcdefghij0123456789ABCDEFGHI"

func init() {
	if len(testIdentity) != 53 {
		panic("the test identity has the wrong length")
	}
}

// ---------------------------------------------------------------- install

func TestInstallCopiesAnExecutableReadableByEveryone(t *testing.T) {
	source := filepath.Join(t.TempDir(), "shim")
	if err := os.WriteFile(source, []byte("#!/bin/true\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := copyExecutable(source, dir); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(dir, installName))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o555 {
		t.Fatalf("mode %v, want 0555", info.Mode().Perm())
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		t.Fatalf("a temporary file was left behind: %v", entries)
	}
	if err := copyExecutable(source, filepath.Join(dir, installName)); err == nil {
		t.Fatal("installing into a file must fail")
	}
}

// ------------------------------------------------------------ canary-wait

type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time        { return c.now }
func (c *fakeClock) Sleep(d time.Duration) { c.now = c.now.Add(d) }

// scripted answers reachability per target from a list, then its last value.
type scripted map[string][]bool

func (s scripted) probe(calls *[]string) probe {
	return func(addr string) bool {
		*calls = append(*calls, addr)
		answers := s[addr]
		if len(answers) == 0 {
			return false
		}
		answer := answers[0]
		if len(answers) > 1 {
			s[addr] = answers[1:]
		}
		return answer
	}
}

func config() canaryConfig {
	return canaryConfig{
		deny:          []string{"10.43.0.20:8085"},
		allow:         []string{"10.43.0.20:8088"},
		expect:        []string{"1.1.1.1:443"},
		consecutive:   3,
		interval:      time.Second,
		timeout:       30 * time.Second,
		expectTimeout: 5 * time.Second,
		dialTimeout:   time.Second,
	}
}

func silent(string, ...any) {}

func TestCanaryWaitsForConsecutiveRefusalsThenTheAllows(t *testing.T) {
	var calls []string
	// Unprotected for two probes, then denied; one refusal in between is
	// followed by a reachable probe, which resets the count.
	targets := scripted{
		"10.43.0.20:8085": {true, true, false, true, false, false, false},
		"10.43.0.20:8088": {false, true},
		"1.1.1.1:443":     {true},
	}
	clock := &fakeClock{now: time.Unix(0, 0)}
	if err := canaryWait(config(), targets.probe(&calls), clock, silent); err != nil {
		t.Fatal(err)
	}
	denyProbes := 0
	firstAllow := -1
	for i, call := range calls {
		if call == "10.43.0.20:8085" {
			denyProbes++
		}
		if call == "10.43.0.20:8088" && firstAllow < 0 {
			firstAllow = i
		}
	}
	if denyProbes != 7 {
		t.Fatalf("deny probed %d times, want 7: %v", denyProbes, calls)
	}
	// No allow probe before the deny held: an unprotected pod reaches it too.
	for _, call := range calls[:firstAllow] {
		if call != "10.43.0.20:8085" {
			t.Fatalf("probed %s before the deny was enforced", call)
		}
	}
}

func TestCanaryFailsWhenTheDenyNeverHolds(t *testing.T) {
	var calls []string
	targets := scripted{"10.43.0.20:8085": {true}}
	clock := &fakeClock{now: time.Unix(0, 0)}
	err := canaryWait(config(), targets.probe(&calls), clock, silent)
	if err == nil || !strings.Contains(err.Error(), "default deny is not enforced") {
		t.Fatalf("err = %v", err)
	}
}

func TestCanaryFailsWhenTheAllowNeverAnswers(t *testing.T) {
	var calls []string
	targets := scripted{"10.43.0.20:8085": {false}, "10.43.0.20:8088": {false}}
	clock := &fakeClock{now: time.Unix(0, 0)}
	err := canaryWait(config(), targets.probe(&calls), clock, silent)
	if err == nil || !strings.Contains(err.Error(), "egress policy is not enforced") {
		t.Fatalf("err = %v", err)
	}
}

func TestAnUnreachableUpstreamIsLoggedNotFatal(t *testing.T) {
	var calls []string
	var logged []string
	targets := scripted{"10.43.0.20:8085": {false}, "10.43.0.20:8088": {true}, "1.1.1.1:443": {false}}
	clock := &fakeClock{now: time.Unix(0, 0)}
	logf := func(format string, a ...any) { logged = append(logged, fmt.Sprintf(format, a...)) }
	if err := canaryWait(config(), targets.probe(&calls), clock, logf); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logged[len(logged)-1], "1.1.1.1:443 did not answer") {
		t.Fatalf("logged %v", logged)
	}
}

func TestTheSystemProbeConnectsAndIsRefused(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			conn.Close()
		}
	}()
	reach := systemProbe(time.Second)
	if !reach(listener.Addr().String()) {
		t.Fatal("an open port must be reachable")
	}
	addr := listener.Addr().String()
	listener.Close()
	if reach(addr) {
		t.Fatal("a closed port must be refused")
	}
}

func TestCanaryFlags(t *testing.T) {
	cfg, err := parseCanaryFlags([]string{
		"--deny", "10.43.0.20:8085", "--allow", "10.43.0.20:8088",
		"--expect", "1.1.1.1:443", "--consecutive", "5", "--timeout", "90s",
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.consecutive != 5 || cfg.timeout != 90*time.Second || len(cfg.expect) != 1 {
		t.Fatalf("cfg = %+v", cfg)
	}
	for _, bad := range [][]string{
		{"--allow", "a:1"},
		{"--deny", "a:1"},
		{"--deny", "nohost", "--allow", "a:1"},
		{"--deny", "a:1", "--allow", "a:1", "--consecutive", "0"},
		{"--deny", "a:1", "--allow", "a:1", "extra"},
	} {
		if _, err := parseCanaryFlags(bad); err == nil {
			t.Fatalf("%v must be refused", bad)
		}
	}
}

// ------------------------------------------------------------------ serve

func writeFiles(t *testing.T, request any, identity string) func(string) string {
	t.Helper()
	dir := t.TempDir()
	requestPath := filepath.Join(dir, "request.json")
	identityPath := filepath.Join(dir, "identity")
	raw, _ := json.Marshal(request)
	if err := os.WriteFile(requestPath, raw, 0o444); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(identityPath, []byte(identity), 0o444); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{
		"SRW_REQUEST_FILE":         requestPath,
		"SRW_DRIVER_IDENTITY_FILE": identityPath,
	}
	return func(name string) string { return env[name] }
}

func serviceRequest() map[string]any {
	return map[string]any{"protocol_version": "1.0", "plane": "service", "driver": "srw.echo-service/v1"}
}

func TestServeExecsTheDriverWithItsRequestAndIdentity(t *testing.T) {
	getenv := writeFiles(t, serviceRequest(), testIdentity+"\n")
	var gotPath string
	var gotArgv, gotEnv []string
	err := serve([]string{"/bin/sh", "-c", "true"}, getenv, func(path string, argv, env []string) error {
		gotPath, gotArgv, gotEnv = path, argv, env
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if gotPath != "/bin/sh" || strings.Join(gotArgv, " ") != "/bin/sh -c true" {
		t.Fatalf("exec %s %v", gotPath, gotArgv)
	}
	found := 0
	for _, entry := range gotEnv {
		if strings.HasPrefix(entry, "SRW_REQUEST_FILE=") || strings.HasPrefix(entry, "SRW_DRIVER_IDENTITY_FILE=") {
			found++
		}
	}
	if found != 2 {
		t.Fatalf("env %v", gotEnv)
	}
}

func TestServeRefusesWhatItCannotStart(t *testing.T) {
	never := func(string, []string, []string) error { t.Fatal("must not exec"); return nil }
	bindTime := serviceRequest()
	bindTime["plane"] = "bind_time"
	newer := serviceRequest()
	newer["protocol_version"] = "2.0"
	cases := map[string]func(string) string{
		"wrong plane":     writeFiles(t, bindTime, testIdentity),
		"newer protocol":  writeFiles(t, newer, testIdentity),
		"no identity":     writeFiles(t, serviceRequest(), "not-a-token"),
		"missing request": func(name string) string { return "/nonexistent/" + name },
	}
	for name, getenv := range cases {
		if err := serve([]string{"/bin/sh"}, getenv, never); err == nil {
			t.Fatalf("%s: serve must refuse", name)
		}
	}
	if err := serve([]string{"no-such-driver-program"}, writeFiles(t, serviceRequest(), testIdentity), never); err == nil {
		t.Fatal("a missing program must be refused")
	}
}

func TestDispatchNeedsTheDashes(t *testing.T) {
	if code := dispatch([]string{"serve", "/bin/true"}); code != exitUsage {
		t.Fatalf("code %d", code)
	}
	if code := dispatch(nil); code != exitUsage {
		t.Fatalf("code %d", code)
	}
}

// -------------------------------------------------------------------- run

func TestReadLinesKeepsTypedLinesAndStopsAtTheFirstBadOne(t *testing.T) {
	lines, problem := readLines(strings.NewReader(
		`{"type":"log","level":"info","message":"hi"}` + "\n\n" +
			`{"type":"result","result":{"status":"SUCCEEDED"}}` + "\n"))
	if problem != "" || len(lines) != 2 {
		t.Fatalf("lines %v problem %q", lines, problem)
	}
	lines, problem = readLines(strings.NewReader(`{"type":"log","level":"info","message":"a"}` + "\nnot json\n"))
	if len(lines) != 1 || problem != "line 2 is not a typed JSON object" {
		t.Fatalf("lines %v problem %q", lines, problem)
	}
	_, problem = readLines(strings.NewReader(`{"type":"surprise"}`))
	if problem == "" {
		t.Fatal("an unknown type must be a protocol error")
	}
	big := strings.Repeat(`{"type":"log","level":"info","message":"`+strings.Repeat("x", 1000)+`"}`+"\n", 1100)
	_, problem = readLines(strings.NewReader(big))
	if !strings.Contains(problem, "exceeds") {
		t.Fatalf("problem %q", problem)
	}
}

func TestRunPostsTheOutcomeWithTheIdentity(t *testing.T) {
	var got outcome
	var auth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth = r.Header.Get("Authorization")
		raw, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Error(err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	request := map[string]any{"protocol_version": "1.0", "operation": "bind"}
	files := writeFiles(t, request, testIdentity)
	getenv := func(name string) string {
		if name == "SRW_RESULT_URL" {
			return server.URL + "/v1/drivers/outcome"
		}
		return files(name)
	}
	script := `echo '{"type":"result","result":{"binding":{}}}'; echo oops >&2; exit 0`
	code := runWith([]string{"/bin/sh", "-c", script}, getenv, silent, server.Client())
	if code != 0 {
		t.Fatalf("code %d", code)
	}
	if auth != "Bearer "+testIdentity {
		t.Fatalf("auth %q", auth)
	}
	if got.Operation != "bind" || got.ExitCode != 0 || len(got.Lines) != 1 || got.ProtocolError != "" {
		t.Fatalf("outcome %+v", got)
	}
	// A failing driver's code is the shim's, after its error line is posted.
	script = `echo '{"type":"error","error":{"class":"config","message":"no"}}'; exit 3`
	if code := runWith([]string{"/bin/sh", "-c", script}, getenv, silent, server.Client()); code != 3 {
		t.Fatalf("code %d", code)
	}
	if got.ExitCode != 3 || len(got.Lines) != 1 {
		t.Fatalf("outcome %+v", got)
	}
	// Junk on stdout is posted as a protocol error and fails the shim.
	if code := runWith([]string{"/bin/sh", "-c", "echo junk"}, getenv, silent, server.Client()); code != exitSoftware {
		t.Fatalf("code %d", code)
	}
	if got.ProtocolError == "" {
		t.Fatalf("outcome %+v", got)
	}
}

func TestRunRefusesWithoutAResultURL(t *testing.T) {
	files := writeFiles(t, map[string]any{"protocol_version": "1.0", "operation": "bind"}, testIdentity)
	if code := runWith([]string{"/bin/true"}, files, silent, http.DefaultClient); code != exitUsage {
		t.Fatalf("code %d", code)
	}
}

func TestAClientErrorIsNotRetried(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()
	if err := post(server.URL, testIdentity, []byte("{}"), server.Client()); err == nil {
		t.Fatal("a 401 must fail")
	}
	if calls != 1 {
		t.Fatalf("calls %d", calls)
	}
}
