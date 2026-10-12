package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
)

// testClock is the bridge's clock in a test: idle stops are decided on it.
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

type logBuffer struct {
	mu    sync.Mutex
	lines []string
}

func (l *logBuffer) printf(format string, a ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(format, a...))
}

func (l *logBuffer) text() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.lines, "\n")
}

type harness struct {
	t      *testing.T
	bridge *bridge
	server *httptest.Server
	clock  *testClock
	logs   *logBuffer
	logDir string
}

func newHarness(t *testing.T, mutate func(*options)) *harness {
	t.Helper()
	return newHarnessEnv(t, mutate)
}

// newHarnessEnv is newHarness with more of the container's environment.
func newHarnessEnv(t *testing.T, mutate func(*options), extra ...string) *harness {
	t.Helper()
	h := &harness{
		t:      t,
		clock:  &testClock{now: time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)},
		logs:   &logBuffer{},
		logDir: t.TempDir(),
	}
	opts := options{
		socket:        filepath.Join(t.TempDir(), "bridge.sock"),
		socketGroup:   -1,
		path:          "/mcp",
		credentialEnv: fakeTokenEnv,
		maxProcesses:  4,
		idle:          time.Minute,
		stopGrace:     2 * time.Second,
		homeRoot:      t.TempDir(),
		processLimit:  256,
		program:       []string{os.Args[0]},
	}
	if mutate != nil {
		mutate(&opts)
	}
	// The container's environment: the image's and the spec's, plus what a
	// process must never see (SRW's own, and the credential's name set on
	// the pod).
	env := append(os.Environ(),
		fakeServerEnv+"=1",
		fakeLogEnv+"="+h.logDir,
		"SRW_POD_VALUE=never-in-a-process",
		fakeTokenEnv+"=a-value-from-the-pod",
	)
	env = append(env, extra...)
	h.bridge = newBridge(opts, env, h.logs.printf, h.clock.Now)
	h.server = httptest.NewServer(h.bridge)
	t.Cleanup(func() {
		h.server.Close()
		h.bridge.close("the test is over")
	})
	return h
}

const initializeBody = `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"bridge-test","version":"1"}}}`

func callBody(id int, tool string) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"tools/call","params":{"name":%q,"arguments":{}}}`, id, tool)
}

// do sends one request as the front would: the binding and its credential
// in SRW's headers.
func (h *harness) do(method, binding, credential, session, body string) (*http.Response, string) {
	h.t.Helper()
	var reader io.Reader = http.NoBody
	if body != "" {
		reader = strings.NewReader(body)
	}
	request, err := http.NewRequest(method, h.server.URL+"/mcp", reader)
	if err != nil {
		h.t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json, text/event-stream")
	if binding != "" {
		request.Header.Set(bindingHeader, binding)
	}
	if credential != "" {
		request.Header.Set(credentialHeader, base64.StdEncoding.EncodeToString([]byte(credential)))
	}
	if session != "" {
		request.Header.Set(sessionHeader, session)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		h.t.Fatal(err)
	}
	defer response.Body.Close()
	raw, _ := io.ReadAll(response.Body)
	return response, string(raw)
}

// open initializes a session for a binding and returns its id.
func (h *harness) open(binding, credential string) string {
	h.t.Helper()
	response, body := h.do(http.MethodPost, binding, credential, "", initializeBody)
	session := response.Header.Get(sessionHeader)
	if response.StatusCode != http.StatusOK || session == "" || !strings.Contains(body, `"serverInfo"`) {
		h.t.Fatalf("initialize %s: %d %s", binding, response.StatusCode, body)
	}
	notified, _ := h.do(http.MethodPost, binding, credential, session, `{"jsonrpc":"2.0","method":"notifications/initialized"}`)
	if notified.StatusCode != http.StatusAccepted {
		h.t.Fatalf("initialized: %d", notified.StatusCode)
	}
	return session
}

// answer is the JSON-RPC answer in a body (JSON or the first event of a
// stream that carries one).
func answer(t *testing.T, body string) map[string]json.RawMessage {
	t.Helper()
	candidates := []string{body}
	scanner := bufio.NewScanner(strings.NewReader(body))
	for scanner.Scan() {
		if data, ok := strings.CutPrefix(scanner.Text(), "data: "); ok {
			candidates = append(candidates, data)
		}
	}
	for _, candidate := range candidates {
		var message map[string]json.RawMessage
		if json.Unmarshal([]byte(candidate), &message) == nil {
			if _, ok := message["result"]; ok {
				return message
			}
			if _, ok := message["error"]; ok {
				return message
			}
		}
	}
	t.Fatalf("no answer in %q", body)
	return nil
}

// call runs one tool and returns its text.
func (h *harness) call(binding, credential, session, tool string) string {
	h.t.Helper()
	response, body := h.do(http.MethodPost, binding, credential, session, callBody(7, tool))
	if response.StatusCode != http.StatusOK {
		h.t.Fatalf("%s: %d %s", tool, response.StatusCode, body)
	}
	var result struct {
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
	}
	message := answer(h.t, body)
	if json.Unmarshal(message["result"], &result) != nil || len(result.Content) == 0 {
		h.t.Fatalf("%s: no result in %s", tool, body)
	}
	return result.Content[0].Text
}

type whoami struct {
	PID        int      `json:"pid"`
	Credential string   `json:"credential_sha256"`
	Calls      int      `json:"calls"`
	Env        []string `json:"env"`
}

func (h *harness) whoami(binding, credential, session string) whoami {
	h.t.Helper()
	var found whoami
	if err := json.Unmarshal([]byte(h.call(binding, credential, session, "whoami")), &found); err != nil {
		h.t.Fatal(err)
	}
	return found
}

func (h *harness) status() bridgeStatus {
	h.t.Helper()
	response, err := http.Get(h.server.URL + "/srw/status")
	if err != nil {
		h.t.Fatal(err)
	}
	defer response.Body.Close()
	var found bridgeStatus
	if err := json.NewDecoder(response.Body).Decode(&found); err != nil {
		h.t.Fatal(err)
	}
	return found
}

func digest(text string) string {
	sum := sha256.Sum256([]byte(text))
	return hex.EncodeToString(sum[:])
}

// gone reports whether a process exited (no longer listed, or a zombie its
// parent did not wait for yet).
func gone(pid int) bool {
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return true
	}
	end := bytes.LastIndexByte(raw, ')')
	fields := bytes.Fields(raw[end+1:])
	return len(fields) > 0 && string(fields[0]) == "Z"
}

func waitGone(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !gone(pid) {
		if time.Now().After(deadline) {
			t.Fatalf("process %d still runs", pid)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func received(t *testing.T, dir string, pid int) []string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, fmt.Sprintf("%d.log", pid)))
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
}

// =============================================================================
// Process lifecycle and routing
// =============================================================================

func TestEachBindingGetsAProcessOfItsOwnWithItsOwnCredential(t *testing.T) {
	h := newHarness(t, nil)
	sessionA := h.open("lease-a", "credential-a")
	sessionB := h.open("lease-b", "credential-b")
	a := h.whoami("lease-a", "credential-a", sessionA)
	b := h.whoami("lease-b", "credential-b", sessionB)
	if a.PID == b.PID || a.PID == 0 {
		t.Fatalf("one process for two bindings: %d %d", a.PID, b.PID)
	}
	if a.Credential != digest("credential-a") || b.Credential != digest("credential-b") {
		t.Fatal("a process got another binding's credential")
	}
	// Each process keeps its own state.
	if again := h.whoami("lease-a", "credential-a", sessionA); again.PID != a.PID || again.Calls != 2 {
		t.Fatalf("binding a's second call: %+v", again)
	}
	if h.whoami("lease-b", "credential-b", sessionB).Calls != 2 {
		t.Fatal("binding b's process saw binding a's call")
	}
	listed := h.status()
	if len(listed.Processes) != 2 || listed.MaxProcesses != 4 || listed.CredentialEnv != fakeTokenEnv {
		t.Fatalf("status: %+v", listed)
	}
	for _, process := range listed.Processes {
		want := map[string]int{"lease-a": a.PID, "lease-b": b.PID}[process.Binding]
		if process.PID != want || !process.Credential || process.Probe {
			t.Fatalf("status lists %+v", process)
		}
	}
	if strings.Contains(h.logs.text(), "credential-a") {
		t.Fatal("the bridge logged a credential")
	}
}

func TestAProcessSeesTheCredentialAndNothingOfSRWs(t *testing.T) {
	h := newHarness(t, nil)
	session := h.open("lease-a", "credential-a")
	found := h.whoami("lease-a", "credential-a", session)
	if found.Credential != digest("credential-a") {
		t.Fatal("the pod's own value of the credential's name reached the process")
	}
	for _, name := range found.Env {
		if strings.HasPrefix(name, "SRW_") {
			t.Fatalf("the process sees %s", name)
		}
	}
	// The probe (no binding) gets no credential, not even the pod's: only
	// the placeholder, whatever the front sends.
	probe := h.open("", "credential-a")
	if h.whoami("", "credential-a", probe).Credential != digest(probePlaceholder) {
		t.Fatal("the probe's process got a credential")
	}
	for _, process := range h.status().Processes {
		if process.Probe && process.Credential {
			t.Fatalf("status says the probe holds a credential: %+v", process)
		}
	}
}

func TestTheProbeStartsAServerThatExitsWithoutItsCredential(t *testing.T) {
	// mcp/brave-search and mcp/slack exit at startup without their
	// variable: the probe's process gets the placeholder in it.
	h := newHarnessEnv(t, nil, requireTokenEnv+"=1")
	probe := h.open("", "")
	if found := h.whoami("", "", probe); found.Credential != digest(probePlaceholder) {
		t.Fatalf("the probe's process saw %+v", found)
	}
	session := h.open("lease-a", "credential-a")
	if h.whoami("lease-a", "credential-a", session).Credential != digest("credential-a") {
		t.Fatal("a binding's process did not get its own credential")
	}
	// A server that takes no credential gets no placeholder either.
	none := newHarnessEnv(t, func(o *options) { o.credentialEnv = "" })
	if none.whoami("", "", none.open("", "")).Credential != digest("a-value-from-the-pod") {
		t.Fatal("the probe's environment changed for a server that takes no credential")
	}
}

func TestASessionReachesOnlyItsOwnBindingsProcess(t *testing.T) {
	h := newHarness(t, nil)
	sessionA := h.open("lease-a", "credential-a")
	h.open("lease-b", "credential-b")
	for _, try := range []struct{ binding, session string }{
		{"lease-b", sessionA},     // another binding's session
		{"", sessionA},            // the probe's view of it
		{"lease-a", "not-a-real"}, // an unknown session
	} {
		response, _ := h.do(http.MethodPost, try.binding, "credential-b", try.session, callBody(3, "whoami"))
		if response.StatusCode != http.StatusNotFound {
			t.Fatalf("%+v: %d", try, response.StatusCode)
		}
	}
	// Only initialize opens a session.
	response, _ := h.do(http.MethodPost, "lease-a", "credential-a", "", callBody(4, "whoami"))
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("a call outside a session: %d", response.StatusCode)
	}
	for _, method := range []string{http.MethodGet, http.MethodDelete} {
		if response, _ := h.do(method, "lease-a", "", "", ""); response.StatusCode != http.StatusNotFound {
			t.Fatalf("%s without a session: %d", method, response.StatusCode)
		}
	}
}

func TestANewSessionOfABindingTakesItsProcessOver(t *testing.T) {
	h := newHarness(t, nil)
	first := h.open("lease-a", "credential-a")
	before := h.whoami("lease-a", "credential-a", first)
	// The agent opens a session each time it attaches the connector: the
	// binding's process, and its state, serve the next one.
	second := h.open("lease-a", "credential-a")
	after := h.whoami("lease-a", "credential-a", second)
	if after.PID != before.PID || after.Calls != 2 {
		t.Fatalf("the new session did not keep the binding's process: %+v then %+v", before, after)
	}
	if response, _ := h.do(http.MethodPost, "lease-a", "credential-a", first, callBody(5, "whoami")); response.StatusCode != http.StatusNotFound {
		t.Fatalf("the session taken over answered %d", response.StatusCode)
	}
	// The process was initialized once: the second session's initialize
	// was answered with its first answer, its initialized stayed with the
	// bridge (a Go SDK server refuses either twice).
	lines := received(t, h.logDir, before.PID)
	initializes := 0
	for _, line := range lines {
		if strings.Contains(line, `"method":"initialize"`) || strings.Contains(line, `"method":"notifications/initialized"`) {
			initializes++
		}
	}
	if initializes != 2 {
		t.Fatalf("the process read %d initialize messages: %v", initializes, lines)
	}
	if listed := h.status(); len(listed.Processes) != 1 || listed.Processes[0].PID != before.PID || !listed.Processes[0].Session {
		t.Fatalf("status: %+v", listed)
	}
}

func TestAProcessWithACallUnansweredIsRestartedForANewSession(t *testing.T) {
	h := newHarness(t, nil)
	first := h.open("lease-a", "credential-a")
	before := h.whoami("lease-a", "credential-a", first).PID
	answers := make(chan string, 1)
	go func() {
		_, body := h.do(http.MethodPost, "lease-a", "credential-a", first, callBody(11, "slow"))
		answers <- body
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		h.bridge.mu.Lock()
		p := h.bridge.processes["lease-a"]
		h.bridge.mu.Unlock()
		p.mu.Lock()
		waiting := len(p.pending)
		p.mu.Unlock()
		if waiting == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the slow call never reached the process")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Its late answer must never reach the new session's call 11.
	second := h.open("lease-a", "credential-a")
	if after := h.whoami("lease-a", "credential-a", second); after.PID == before || after.Calls != 1 {
		t.Fatalf("a process with a call unanswered was taken over: %+v", after)
	}
	waitGone(t, before)
	select {
	case body := <-answers:
		if failure, failed := answer(t, body)["error"]; !failed || !strings.Contains(string(failure), `"code":-32000`) {
			t.Fatalf("the call in flight got %s", body)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the call in flight was never answered")
	}
}

func TestACallReadBeforeATakeoverNeverTakesTheNewSessionsAnswer(t *testing.T) {
	// The old session reads its call 7, then a new session takes the
	// process over (nothing is pending yet), then the old call would be
	// registered: it is dropped, so the process never answers it to the new
	// session's call 7.
	h := newHarness(t, nil)
	first := h.open("lease-a", "credential-a")
	before := h.whoami("lease-a", "credential-a", first)
	held, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	hook := func(message jsonrpc.Message) {
		if request, ok := message.(*jsonrpc.Request); ok && request.Method == "tools/call" && strings.Contains(string(request.Params), `"slow"`) {
			once.Do(func() { close(held) })
			<-release
		}
	}
	testHookRead.Store(&hook)
	t.Cleanup(func() { testHookRead.Store(nil) })
	oldCall := make(chan struct{})
	go func() {
		defer close(oldCall)
		request, _ := http.NewRequest(http.MethodPost, h.server.URL+"/mcp", strings.NewReader(callBody(7, "slow")))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Accept", "application/json, text/event-stream")
		request.Header.Set(bindingHeader, "lease-a")
		request.Header.Set(credentialHeader, base64.StdEncoding.EncodeToString([]byte("credential-a")))
		request.Header.Set(sessionHeader, first)
		if response, err := http.DefaultClient.Do(request); err == nil {
			io.Copy(io.Discard, response.Body)
			response.Body.Close()
		}
	}()
	<-held
	second := h.open("lease-a", "credential-a")
	close(release)
	after := h.whoami("lease-a", "credential-a", second) // its id is 7 too
	if after.PID != before.PID {
		t.Fatalf("the new session did not take the process over: %+v", after)
	}
	if after.Calls != 2 {
		t.Fatalf("the taken-over session's call reached the process: %+v", after)
	}
	<-oldCall
	for _, line := range received(t, h.logDir, before.PID) {
		if strings.Contains(line, `"slow"`) {
			t.Fatalf("the process read the old session's call: %s", line)
		}
	}
}

func TestOrphansAreReapedWhileTheirBindingLives(t *testing.T) {
	if err := becomeSubreaper(); err != nil {
		t.Skipf("no subreaper here: %v", err)
	}
	h := newHarness(t, nil)
	session := h.open("lease-a", "credential-a")
	pid := h.whoami("lease-a", "credential-a", session).PID
	if text := h.call("lease-a", "credential-a", session, "orphans"); text != fmt.Sprint(orphanCount) {
		t.Fatalf("orphans: %s", text)
	}
	// The orphans exit soon after: zombies the bridge adopted, in the group
	// of a process that still runs.
	deadline := time.Now().Add(10 * time.Second)
	for adoptedZombies(h.bridge) < orphanCount {
		if time.Now().After(deadline) {
			t.Fatalf("%d orphans became zombies", adoptedZombies(h.bridge))
		}
		time.Sleep(20 * time.Millisecond)
	}
	h.bridge.reapOrphans()
	if left := adoptedZombies(h.bridge); left != 0 {
		t.Fatalf("%d zombies left while the binding lives", left)
	}
	if gone(pid) {
		t.Fatal("the binding's own process was reaped")
	}
	if h.whoami("lease-a", "credential-a", session).PID != pid {
		t.Fatal("the binding lost its process")
	}
}

// adoptedZombies counts the zombies this process adopted, the bridge's own
// processes excepted.
func adoptedZombies(b *bridge) int {
	b.mu.Lock()
	mains := maps.Clone(b.mains)
	b.mu.Unlock()
	entries, _ := os.ReadDir("/proc")
	count := 0
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || mains[pid] {
			continue
		}
		if state, parent, ok := procState(pid); ok && state == "Z" && parent == os.Getpid() {
			count++
		}
	}
	return count
}

func TestAProcessEndsWithItsBinding(t *testing.T) {
	h := newHarness(t, nil)
	sessionA := h.open("lease-a", "credential-a")
	sessionB := h.open("lease-b", "credential-b")
	a := h.whoami("lease-a", "credential-a", sessionA)
	b := h.whoami("lease-b", "credential-b", sessionB)
	request, _ := http.NewRequest(http.MethodDelete, h.server.URL+"/srw/bindings/lease-a", nil)
	response, err := http.DefaultClient.Do(request)
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("binding end: %v %v", err, response)
	}
	response.Body.Close()
	waitGone(t, a.PID)
	if gone(b.PID) {
		t.Fatal("another binding's process stopped with it")
	}
	if response, _ := h.do(http.MethodPost, "lease-a", "credential-a", sessionA, callBody(6, "whoami")); response.StatusCode != http.StatusNotFound {
		t.Fatalf("the ended binding's session answered %d", response.StatusCode)
	}
	if listed := h.status(); len(listed.Processes) != 1 || listed.Processes[0].Binding != "lease-b" {
		t.Fatalf("status: %+v", listed)
	}
	h.bridge.stopping.Wait()
	if !strings.Contains(h.logs.text(), "binding=lease-a: process") || !strings.Contains(h.logs.text(), "its binding ended") {
		t.Fatalf("the stop was not logged: %s", h.logs.text())
	}
	// A binding that has no process is fine to end.
	request, _ = http.NewRequest(http.MethodDelete, h.server.URL+"/srw/bindings/lease-zzz", nil)
	if response, err := http.DefaultClient.Do(request); err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("ending an unknown binding: %v", err)
	}
}

func TestACallInFlightWhenItsBindingEndsIsAnsweredAtOnce(t *testing.T) {
	h := newHarness(t, nil)
	session := h.open("lease-a", "credential-a")
	answers := make(chan string, 1)
	go func() {
		_, body := h.do(http.MethodPost, "lease-a", "credential-a", session, callBody(9, "slow"))
		answers <- body
	}()
	time.Sleep(50 * time.Millisecond)
	h.bridge.endBinding("lease-a", "its binding ended")
	select {
	case body := <-answers:
		// Either the process answered first, or the call learned its
		// session ended; never no answer.
		message := answer(t, body)
		if failure, failed := message["error"]; failed && !strings.Contains(string(failure), `"code":-32000`) {
			t.Fatalf("%s", body)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the call in flight was never answered")
	}
}

func TestTheClientEndingItsSessionLeavesTheProcessToItsBinding(t *testing.T) {
	h := newHarness(t, nil)
	session := h.open("lease-a", "credential-a")
	pid := h.whoami("lease-a", "credential-a", session).PID
	if response, _ := h.do(http.MethodDelete, "lease-a", "", session, ""); response.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE: %d", response.StatusCode)
	}
	if response, _ := h.do(http.MethodPost, "lease-a", "credential-a", session, callBody(5, "whoami")); response.StatusCode != http.StatusNotFound {
		t.Fatalf("the ended session answered %d", response.StatusCode)
	}
	if gone(pid) || h.status().Processes[0].Session {
		t.Fatal("the process did not outlive its session")
	}
	again := h.open("lease-a", "credential-a")
	if h.whoami("lease-a", "credential-a", again).PID != pid {
		t.Fatal("the binding's next session did not get its process")
	}
	h.bridge.endBinding("lease-a", "its binding ended")
	waitGone(t, pid)
	// The probe's process serves one probe.
	probe := h.open("", "")
	probePID := h.whoami("", "", probe).PID
	h.do(http.MethodDelete, "", "", probe, "")
	waitGone(t, probePID)
}

func TestAProcessThatExitsEndsItsSession(t *testing.T) {
	h := newHarness(t, nil)
	session := h.open("lease-a", "credential-a")
	pid := h.whoami("lease-a", "credential-a", session).PID
	// The call the process died on is answered at once, as a session that
	// ended (never left to its caller's timeout).
	_, crashed := h.do(http.MethodPost, "lease-a", "credential-a", session, callBody(8, "crash"))
	if failure := answer(t, crashed)["error"]; !strings.Contains(string(failure), `"code":-32000`) || !strings.Contains(string(failure), "process stopped") {
		t.Fatalf("the crashed call got %s", crashed)
	}
	waitGone(t, pid)
	deadline := time.Now().Add(10 * time.Second)
	for len(h.status().Processes) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("the exited process is still listed")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if response, _ := h.do(http.MethodPost, "lease-a", "credential-a", session, callBody(9, "whoami")); response.StatusCode != http.StatusNotFound {
		t.Fatalf("the session outlived its process: %d", response.StatusCode)
	}
	// The client initializes again and gets a new process.
	again := h.open("lease-a", "credential-a")
	if h.whoami("lease-a", "credential-a", again).PID == pid {
		t.Fatal("no new process")
	}
}

func TestAnIdleProcessStopsAndAnOpenStreamKeepsOneAlive(t *testing.T) {
	h := newHarness(t, func(o *options) { o.idle = time.Minute })
	idleSession := h.open("lease-a", "credential-a")
	idle := h.whoami("lease-a", "credential-a", idleSession)
	streaming := h.open("lease-b", "credential-b")
	held := h.whoami("lease-b", "credential-b", streaming)
	// The client holds a stream open, as SRW's does.
	request, _ := http.NewRequest(http.MethodGet, h.server.URL+"/mcp", nil)
	request.Header.Set(bindingHeader, "lease-b")
	request.Header.Set(sessionHeader, streaming)
	request.Header.Set("Accept", "text/event-stream")
	stream, err := http.DefaultClient.Do(request)
	if err != nil || stream.StatusCode != http.StatusOK {
		t.Fatalf("stream: %v %v", err, stream)
	}
	defer stream.Body.Close()
	h.clock.advance(59 * time.Second)
	h.bridge.stopIdle()
	if gone(idle.PID) {
		t.Fatal("stopped before its idle time")
	}
	h.clock.advance(2 * time.Second)
	h.bridge.stopIdle()
	waitGone(t, idle.PID)
	if gone(held.PID) {
		t.Fatal("a process with an open stream was stopped as idle")
	}
	h.bridge.stopping.Wait()
	if !strings.Contains(h.logs.text(), "(idle)") {
		t.Fatal("the idle stop was not logged")
	}
}

func TestAForgottenProbeProcessStopsSooner(t *testing.T) {
	h := newHarness(t, func(o *options) { o.idle = time.Hour })
	probe := h.open("", "")
	pid := h.whoami("", "", probe).PID
	h.clock.advance(probeIdle + time.Second)
	h.bridge.stopIdle()
	waitGone(t, pid)
}

func TestThePodRunsAtMostItsCapOfBindingProcesses(t *testing.T) {
	h := newHarness(t, func(o *options) { o.maxProcesses = 2 })
	sessionA := h.open("lease-a", "credential-a")
	h.open("lease-b", "credential-b")
	response, body := h.do(http.MethodPost, "lease-c", "credential-c", "", initializeBody)
	if response.StatusCode != http.StatusServiceUnavailable || !strings.Contains(body, "at most 2") {
		t.Fatalf("past the cap: %d %s", response.StatusCode, body)
	}
	// A binding replacing its own process is no new one; the probe is
	// never counted.
	h.open("lease-a", "credential-a")
	h.open("", "")
	if count := len(h.status().Processes); count != 3 {
		t.Fatalf("%d processes", count)
	}
	_ = sessionA
	request, _ := http.NewRequest(http.MethodDelete, h.server.URL+"/srw/bindings/lease-b", nil)
	http.DefaultClient.Do(request)
	h.open("lease-c", "credential-c")
}

func TestABindingWithoutItsCredentialIsRefused(t *testing.T) {
	h := newHarness(t, nil)
	response, _ := h.do(http.MethodPost, "lease-a", "", "", initializeBody)
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("no credential: %d", response.StatusCode)
	}
	for _, header := range []string{"not base64!", base64.StdEncoding.EncodeToString([]byte("a\x00b"))} {
		request, _ := http.NewRequest(http.MethodPost, h.server.URL+"/mcp", strings.NewReader(initializeBody))
		request.Header.Set(bindingHeader, "lease-a")
		request.Header.Set(credentialHeader, header)
		response, err := http.DefaultClient.Do(request)
		if err != nil || response.StatusCode != http.StatusBadRequest {
			t.Fatalf("%q: %v %v", header, err, response)
		}
		response.Body.Close()
	}
	if response, _ := h.do(http.MethodPost, "lease/../x", "c", "", initializeBody); response.StatusCode != http.StatusBadRequest {
		t.Fatalf("a malformed binding: %d", response.StatusCode)
	}
	if len(h.status().Processes) != 0 {
		t.Fatal("a refused request started a process")
	}
}

func TestAServerThatTakesNoCredentialGetsNone(t *testing.T) {
	h := newHarness(t, func(o *options) { o.credentialEnv = "" })
	session := h.open("lease-a", "")
	found := h.whoami("lease-a", "", session)
	// The pod's own value of a variable is the container's, untouched.
	if found.Credential != digest("a-value-from-the-pod") {
		t.Fatal("the process's environment changed")
	}
	// A credential the front sends anyway is not delivered.
	other := h.open("lease-b", "credential-b")
	if h.whoami("lease-b", "credential-b", other).Credential == digest("credential-b") {
		t.Fatal("a credential reached a server that takes none")
	}
}

func TestTheCredentialIsScrubbedFromTheProcessesStderr(t *testing.T) {
	h := newHarness(t, nil)
	secret := "very-secret-credential-0123"
	session := h.open("lease-a", secret)
	h.call("lease-a", secret, session, "leak_credential")
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(h.logs.text(), "my token is") {
		if time.Now().After(deadline) {
			t.Fatal("the process's stderr was not logged")
		}
		time.Sleep(20 * time.Millisecond)
	}
	logs := h.logs.text()
	if strings.Contains(logs, secret) || !strings.Contains(logs, "my token is "+redacted) {
		t.Fatalf("the credential reached the log: %s", logs)
	}
	if !strings.Contains(logs, "binding=lease-a stderr:") {
		t.Fatal("stderr is not labelled with its binding")
	}
}

func TestAStoppedProcessTakesItsChildrenWithIt(t *testing.T) {
	h := newHarness(t, nil)
	session := h.open("lease-a", "credential-a")
	var sleeper int
	fmt.Sscan(h.call("lease-a", "credential-a", session, "spawn_sleeper"), &sleeper)
	if sleeper == 0 || gone(sleeper) {
		t.Fatal("no child process")
	}
	h.bridge.endBinding("lease-a", "its binding ended")
	waitGone(t, sleeper)
}

func TestANotificationReachesTheSessionsStream(t *testing.T) {
	h := newHarness(t, nil)
	session := h.open("lease-a", "credential-a")
	request, _ := http.NewRequest(http.MethodGet, h.server.URL+"/mcp", nil)
	request.Header.Set(bindingHeader, "lease-a")
	request.Header.Set(sessionHeader, session)
	stream, err := http.DefaultClient.Do(request)
	if err != nil || stream.StatusCode != http.StatusOK {
		t.Fatalf("stream: %v", err)
	}
	defer stream.Body.Close()
	events := make(chan string, 4)
	go func() {
		scanner := bufio.NewScanner(stream.Body)
		for scanner.Scan() {
			if data, ok := strings.CutPrefix(scanner.Text(), "data: "); ok {
				events <- data
			}
		}
	}()
	if text := h.call("lease-a", "credential-a", session, "notify"); text != "notified" {
		t.Fatalf("notify: %s", text)
	}
	select {
	case event := <-events:
		if !strings.Contains(event, "notifications/message") {
			t.Fatalf("event %s", event)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the notification never reached the stream")
	}
}

func TestConcurrentBindingsKeepToTheirOwnProcesses(t *testing.T) {
	h := newHarness(t, func(o *options) { o.maxProcesses = 8 })
	var wg sync.WaitGroup
	pids := make([]int, 8)
	for i := range pids {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			binding, credential := fmt.Sprintf("lease-%d", i), fmt.Sprintf("credential-%d", i)
			session := h.open(binding, credential)
			for call := 0; call < 3; call++ {
				found := h.whoami(binding, credential, session)
				if found.Credential != digest(credential) || (pids[i] != 0 && found.PID != pids[i]) {
					t.Errorf("%s reached another process", binding)
				}
				pids[i] = found.PID
			}
		}(i)
	}
	wg.Wait()
	seen := map[int]bool{}
	for _, pid := range pids {
		if seen[pid] {
			t.Fatal("two bindings shared a process")
		}
		seen[pid] = true
	}
}

// =============================================================================
// Control routes, flags and install
// =============================================================================

func TestTheControlRoutes(t *testing.T) {
	h := newHarness(t, nil)
	for path, want := range map[string]int{"/srw/livez": 200, "/srw/status": 200, "/elsewhere": 404} {
		response, err := http.Get(h.server.URL + path)
		if err != nil || response.StatusCode != want {
			t.Fatalf("%s: %v %v", path, err, response)
		}
		response.Body.Close()
	}
	request, _ := http.NewRequest(http.MethodDelete, h.server.URL+"/srw/bindings/", nil)
	if response, _ := http.DefaultClient.Do(request); response.StatusCode != http.StatusBadRequest {
		t.Fatalf("no binding id: %d", response.StatusCode)
	}
	// The SDK never sees a method the front does not forward.
	if response, _ := h.do(http.MethodPut, "lease-a", "", "", ""); response.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("PUT: %d", response.StatusCode)
	}
}

func TestServeRefusesWhatWouldExposeTheBridge(t *testing.T) {
	base := []string{"--socket", "/srw/bridge/bridge.sock", "--home-root", "/srw/home"}
	good := append(append([]string(nil), base...), "--socket-group", "65532", "--uid-base", "20000", "--credential-env", "GITHUB_TOKEN", "--", "node", "dist/index.js")
	opts, err := parseServe(good)
	if err != nil || opts.program[1] != "dist/index.js" || opts.maxProcesses != 8 || opts.uidBase != 20000 || opts.socketGroup != 65532 || opts.processLimit != 256 || opts.addressSpaceMB != 0 {
		t.Fatalf("%+v %v", opts, err)
	}
	if strings.Join(opts.sweepDirs, ",") != "/tmp,/dev/shm" {
		t.Fatalf("sweep dirs %v", opts.sweepDirs)
	}
	with := func(extra ...string) []string { return append(append([]string(nil), base...), extra...) }
	for _, args := range [][]string{
		with(),
		with("--"),
		{"--home-root", "/srw/home", "--", "x"}, // no socket
		{"--socket", "bridge.sock", "--home-root", "/h", "--", "x"}, // relative
		{"--socket", "/srw/../tmp/b.sock", "--home-root", "/h", "--", "x"},
		{"--socket", "/" + strings.Repeat("s", 100), "--home-root", "/h", "--", "x"},
		{"--socket", "/srw/bridge/bridge.sock", "--", "x"}, // no home root
		with("--socket-group", "0", "--", "x"),
		with("--path", "/srw/status", "--", "x"),
		with("--path", "mcp", "--", "x"),
		with("--max-processes", "0", "--", "x"),
		with("--max-processes", "65", "--", "x"),
		with("--idle", "0s", "--", "x"),
		with("--credential-env", "NODE_OPTIONS", "--", "x"),
		with("--credential-env", "LD_PRELOAD", "--", "x"),
		with("--uid-base", "500", "--", "x"),
		with("--uid-base", "65520", "--", "x"),
		with("--uid-base", "20000", "--socket-group", "20003", "--", "x"),
		with("--sweep-dir", "tmp", "--", "x"),
		with("--process-limit", "8", "--", "x"),
		with("--process-limit", "5000", "--", "x"),
		with("--address-space-mb", "10", "--", "x"),
		with("stray", "--", "x"),
		with("--", ""),
	} {
		if _, err := parseServe(args); err == nil {
			t.Errorf("%q was accepted", args)
		}
	}
}

func TestTheSocketIsTheBridgesAndTheFrontsAlone(t *testing.T) {
	dir := t.TempDir()
	os.Chmod(dir, 0o777)
	path := filepath.Join(dir, "bridge.sock")
	os.WriteFile(path, []byte("left by the last run"), 0o600)
	listener, err := listenSocket(path, -1)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	for target, want := range map[string]os.FileMode{dir: 0o700, path: 0o600} {
		if info, err := os.Stat(target); err != nil || info.Mode().Perm() != want {
			t.Fatalf("%s: %v %v", target, info.Mode(), err)
		}
	}
	if info, _ := os.Stat(path); info.Mode()&os.ModeSocket == 0 {
		t.Fatal("not a socket")
	}
}

func TestCredentialEnvNames(t *testing.T) {
	for _, name := range []string{"GITHUB_TOKEN", "API_KEY", "_x1"} {
		if err := checkEnvName(name); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	for _, name := range []string{
		"", "1ABC", "A-B", "A B", "SRW_TOKEN", "srw_x", "LD_PRELOAD", "DYLD_INSERT_LIBRARIES",
		"NODE_OPTIONS", "node_options", "PYTHONPATH", "PATH", "BASH_ENV", "JAVA_TOOL_OPTIONS",
		"GIT_SSH_COMMAND", "npm_config_node_options", "PIP_INDEX_URL", "UV_INDEX_URL",
		"GLIBC_TUNABLES", "BROWSER", "OPENSSL_CONF", "PYTHONWARNINGS", "TMPDIR", "HOME",
	} {
		if checkEnvName(name) == nil {
			t.Errorf("%q was accepted", name)
		}
	}
}

func TestChildEnvironment(t *testing.T) {
	base := []string{"PATH=/bin", "SRW_REQUEST_FILE=/x", "srw_lower=1", "TOKEN=from-pod", "=broken", "NOEQUALS"}
	got := strings.Join(childEnv(base, "TOKEN", "from-binding"), " ")
	if got != "PATH=/bin TOKEN=from-binding" {
		t.Fatalf("%s", got)
	}
	if got := strings.Join(childEnv(base, "TOKEN", ""), " "); got != "PATH=/bin" {
		t.Fatalf("no credential: %s", got)
	}
	if got := strings.Join(childEnv(base, "", "x"), " "); got != "PATH=/bin TOKEN=from-pod" {
		t.Fatalf("no credential name: %s", got)
	}
}

func TestALongLineIsScrubbedBeforeItIsCut(t *testing.T) {
	logs := &logBuffer{}
	secret := "s3cr3t-credential-value"
	logger := &lineLogger{logf: logs.printf, label: "binding=x", scrub: newScrubber(secret)}
	// The credential straddles the cut, then a line far longer than kept.
	line := strings.Repeat("a", maxLogLine-5) + secret + strings.Repeat("b", 10) + "\n"
	logger.Write([]byte(line))
	logger.Write([]byte(strings.Repeat("c", maxKept+100) + secret + "\nend"))
	logger.flush()
	text := logs.text()
	if strings.Contains(text, secret) || strings.Contains(text, secret[:5]+"\n") {
		t.Fatal("a credential survived")
	}
	for i := 4; i < len(secret); i++ {
		if strings.Contains(text, secret[:i]+"...") {
			t.Fatalf("a cut left %q", secret[:i])
		}
	}
	if len(logs.lines) != 3 || !strings.HasSuffix(logs.lines[2], "stderr: end") {
		t.Fatalf("%d lines", len(logs.lines))
	}
	for _, form := range []string{base64.StdEncoding.EncodeToString([]byte(secret)), base64.RawURLEncoding.EncodeToString([]byte(secret))} {
		if strings.Contains(string(newScrubber(secret).apply([]byte("x "+form))), form) {
			t.Fatalf("%s survived", form)
		}
	}
}

func TestInstallCopiesAnExecutableBridge(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(t.TempDir(), "bridge")
	os.WriteFile(source, []byte("#!/bin/sh\n"), 0o600)
	if err := copyExecutable(source, dir); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(dir, installName))
	if err != nil || info.Mode().Perm() != 0o555 {
		t.Fatalf("%v %v", info, err)
	}
	if err := copyExecutable(source, filepath.Join(dir, installName)); err == nil {
		t.Fatal("installed into a file")
	}
}

func TestDispatch(t *testing.T) {
	var out, errs bytes.Buffer
	if dispatch([]string{"version"}, &out, &errs) != 0 || strings.TrimSpace(out.String()) != version {
		t.Fatal("version")
	}
	if dispatch(nil, &out, &errs) != 2 || dispatch([]string{"nope"}, &out, &errs) != 2 {
		t.Fatal("usage")
	}
	if dispatch([]string{"status", "--socket", "relative.sock"}, &out, &errs) != 2 {
		t.Fatal("status of a relative socket")
	}
	h := newHarness(t, nil)
	h.open("lease-a", "credential-a")
	listener, err := listenSocket(h.bridge.opts.socket, -1)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: h.bridge}
	go server.Serve(listener)
	defer server.Close()
	out.Reset()
	if dispatch([]string{"status", "--socket", h.bridge.opts.socket}, &out, &errs) != 0 || !strings.Contains(out.String(), `"binding":"lease-a"`) {
		t.Fatalf("status: %s %s", out.String(), errs.String())
	}
	if strings.Contains(out.String(), "credential-a") {
		t.Fatal("status printed a credential")
	}
}
