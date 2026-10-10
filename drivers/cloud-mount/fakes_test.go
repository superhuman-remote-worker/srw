package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// result is what a fake rclone call answers.
type result struct {
	out      string
	code     int
	stderr   string
	timedOut bool
}

// fakeRunner answers rclone calls by their subcommand and hands out
// fakeChild mounts.
type fakeRunner struct {
	mu       sync.Mutex
	results  map[string]result // keyed by args[0]: lsjson, cat
	calls    [][]string
	started  [][]string
	children []*fakeChild
	// startErr fails a start; childFor shapes each new child.
	startErr error
	childFor func(n int) *fakeChild
}

func (f *fakeRunner) run(_ context.Context, args []string, _ time.Duration) ([]byte, int, string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, args)
	r := f.results[args[0]]
	return []byte(r.out), r.code, r.stderr, r.timedOut
}

func (f *fakeRunner) start(args []string, _ string) (child, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.startErr != nil {
		return nil, f.startErr
	}
	f.started = append(f.started, args)
	c := &fakeChild{exited: make(chan struct{})}
	if f.childFor != nil {
		c = f.childFor(len(f.children))
	}
	f.children = append(f.children, c)
	return c, nil
}

func (f *fakeRunner) startCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.started)
}

func (f *fakeRunner) child(n int) *fakeChild {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.children[n]
}

func (f *fakeRunner) callCount(sub string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, call := range f.calls {
		if call[0] == sub {
			n++
		}
	}
	return n
}

// fakeChild is one rclone. It exits on SIGTERM unless it hangs, and always
// on SIGKILL.
type fakeChild struct {
	mu      sync.Mutex
	exited  chan struct{}
	code    int
	lines   string
	hang    bool
	signals []syscall.Signal
	closed  bool
}

func (c *fakeChild) done() <-chan struct{} { return c.exited }

func (c *fakeChild) exitCode() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.code
}

func (c *fakeChild) tail() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lines
}

func (c *fakeChild) signal(sig syscall.Signal) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.signals = append(c.signals, sig)
	if sig == syscall.SIGKILL || !c.hang {
		c.exitLocked(137)
	}
	return nil
}

func (c *fakeChild) exit(code int, lines string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lines = lines
	c.exitLocked(code)
}

func (c *fakeChild) exitLocked(code int) {
	if !c.closed {
		c.code = code
		c.closed = true
		close(c.exited)
	}
}

func (c *fakeChild) signalled() []syscall.Signal {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]syscall.Signal(nil), c.signals...)
}

// fakeMounts says which targets answer as live mounts.
type fakeMounts struct {
	mu   sync.Mutex
	live map[string]bool
}

func (m *fakeMounts) check(target string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.live[target] {
		return nil
	}
	return errNotMounted
}

func (m *fakeMounts) set(target string, live bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.live == nil {
		m.live = map[string]bool{}
	}
	m.live[target] = live
}

// fakeRC scripts rclone's remote control per socket.
type fakeRC struct {
	mu      sync.Mutex
	pending map[string][]int // vfs/stats answers in turn; the last one repeats
	queue   map[string][]map[string]any
	fail    map[string]bool
	calls   []string // "socket method params"
}

func (r *fakeRC) call(_ context.Context, socket, method string, params map[string]any) (map[string]any, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, fmt.Sprintf("%s %s %v", filepath.Base(socket), method, params))
	if r.fail[socket] {
		return nil, errors.New("connection refused")
	}
	switch method {
	case "vfs/stats":
		answers := r.pending[socket]
		n := 0
		if len(answers) > 0 {
			n = answers[0]
			if len(answers) > 1 {
				r.pending[socket] = answers[1:]
			}
		}
		return map[string]any{"diskCache": map[string]any{"uploadsInProgress": float64(n), "uploadsQueued": float64(0)}}, nil
	case "vfs/queue":
		items := make([]any, 0, len(r.queue[socket]))
		for _, item := range r.queue[socket] {
			items = append(items, item)
		}
		return map[string]any{"queue": items}, nil
	}
	return map[string]any{}, nil
}

func (r *fakeRC) seen(fragment string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, call := range r.calls {
		if strings.Contains(call, fragment) {
			return true
		}
	}
	return false
}

func testPlan() *Plan {
	return &Plan{
		Version:      1,
		UID:          1000,
		GID:          1000,
		DrainSeconds: 1,
		Mounts: []Mount{
			{Index: 0, Name: "project", Remote: "m0:", Target: "/srw/cloud/project", Flags: []string{"--vfs-cache-mode", "full"}},
			{Index: 1, Name: "home", Remote: "m1:Shared/x", Target: "/srw/cloud/home", ReadOnly: true},
		},
	}
}

// harness builds a supervisor over fakes with short timings.
type harness struct {
	s      *supervisor
	runner *fakeRunner
	mounts *fakeMounts
	rc     *fakeRC
	dir    string
}

func newHarness(t *testing.T, plan *Plan) *harness {
	t.Helper()
	dir := t.TempDir()
	for _, sub := range []string{"status", "control", "cache", "run"} {
		if err := os.Mkdir(filepath.Join(dir, sub), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	config := filepath.Join(dir, "rclone.conf")
	if err := os.WriteFile(config, []byte("[m0]\ntype = webdav\n"), 0o444); err != nil {
		t.Fatal(err)
	}
	runner := &fakeRunner{results: map[string]result{}}
	mounts := &fakeMounts{}
	rc := &fakeRC{pending: map[string][]int{}, queue: map[string][]map[string]any{}, fail: map[string]bool{}}
	board := newStatusBoard(filepath.Join(dir, "status"), plan, time.Now)
	s := newSupervisor(plan, board, runner, rc, io.Discard)
	s.configPath = config
	s.cacheDir = filepath.Join(dir, "cache")
	s.runDir = filepath.Join(dir, "run")
	s.controlDir = filepath.Join(dir, "control")
	s.mounted = mounts.check
	s.backoffMin, s.backoffMax = 10*time.Millisecond, 40*time.Millisecond
	s.configWait, s.mountWait = 200*time.Millisecond, 300*time.Millisecond
	s.healthEvery, s.controlEvery, s.drainEvery = 20*time.Millisecond, 10*time.Millisecond, 5*time.Millisecond
	s.stopWait = 50 * time.Millisecond
	return &harness{s: s, runner: runner, mounts: mounts, rc: rc, dir: dir}
}

// statusFile reads what the workspace would read.
func (h *harness) statusFile(t *testing.T, index int) Status {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(h.dir, "status", fmt.Sprintf("%d.json", index)))
	if err != nil {
		return Status{}
	}
	var status Status
	if err := jsonUnmarshal(raw, &status); err != nil {
		t.Fatalf("status %d: %v", index, err)
	}
	return status
}

// eventually polls cond for up to two seconds.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
