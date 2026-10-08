//go:build linux

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
)

// The isolation tests need root (the bridge starts each process as a user
// of its own); as anyone else they are skipped. CI runs them with sudo. On a
// workstation, run them as root of a user namespace that maps a range of
// users (your /etc/subuid range):
//
//	unshare --map-users=0:$(id -u):1 --map-users=1:SUBUID:65536 \
//	    --map-groups=0:$(id -g):1 --map-groups=1:SUBGID:65536 \
//	    --setuid=0 --setgid=0 --pid --fork --mount-proc \
//	    go test -race -run Isolation ./...

const isolatedBase = 30000

type isolated struct {
	*harness
	shared string // the pod's /tmp
	socket string
}

func newIsolatedHarness(t *testing.T, mutate func(*options)) *isolated {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("needs root: run go test -run Isolation as root (CI does)")
	}
	if err := restrictProcess(); err != nil {
		t.Fatal(err)
	}
	if err := becomeSubreaper(); err != nil {
		t.Fatal(err)
	}
	// A base every user may enter (t.TempDir's parents are 0700): the
	// test binary every process runs, the log directory, the pod's /tmp,
	// the home root and the socket's directory.
	base, err := os.MkdirTemp("", "bridge-isolation-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(base) })
	os.Chmod(base, 0o755)
	binary := filepath.Join(base, "fake-server")
	copyFile(t, os.Args[0], binary, 0o755)
	shared := filepath.Join(base, "tmp")
	os.Mkdir(shared, 0o777)
	os.Chmod(shared, 0o777) // made sticky by prepareUsers
	logs := filepath.Join(base, "logs")
	os.Mkdir(logs, 0o777)
	os.Chmod(logs, 0o777)
	sockets := filepath.Join(base, "bridge")
	os.Mkdir(sockets, 0o777)
	h := newHarnessEnv(t, func(o *options) {
		o.uidBase = isolatedBase
		o.homeRoot = filepath.Join(base, "home")
		o.sweepDirs = []string{shared}
		o.socket = filepath.Join(sockets, "bridge.sock")
		o.socketGroup = frontUser
		o.program = []string{binary, "--data=" + homeToken + "/data"}
		if mutate != nil {
			mutate(o)
		}
	}, homeValueEnv+"="+homeToken+"/memory.json")
	h.logDir = logs
	for i, entry := range h.bridge.env {
		if strings.HasPrefix(entry, fakeLogEnv+"=") {
			h.bridge.env[i] = fakeLogEnv + "=" + logs
		}
	}
	if err := prepareUsers(h.bridge.opts); err != nil {
		t.Fatal(err)
	}
	listener, err := listenSocket(h.bridge.opts.socket, frontUser)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: h.bridge}
	go server.Serve(listener)
	t.Cleanup(func() { server.Close() })
	return &isolated{harness: h, shared: shared, socket: h.bridge.opts.socket}
}

func copyFile(t *testing.T, from, to string, mode os.FileMode) {
	t.Helper()
	in, err := os.Open(from)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	out, err := os.OpenFile(to, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(out, in); err != nil {
		t.Fatal(err)
	}
	out.Close()
	os.Chmod(to, mode)
}

func argsBody(id int, tool string, args map[string]string) string {
	raw, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": id, "method": "tools/call",
		"params": map[string]any{"name": tool, "arguments": args},
	})
	// The bridge accepts the form the front forwards: jsonrpc, id, method,
	// params (encoding/json sorts the keys).
	var message struct {
		Params json.RawMessage `json:"params"`
	}
	json.Unmarshal(raw, &message)
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"tools/call","params":%s}`, id, message.Params)
}

// tool runs one of the fake server's tools with arguments.
func (h *isolated) tool(binding, session, tool string, args map[string]string) string {
	h.t.Helper()
	response, body := h.do(http.MethodPost, binding, "credential-"+binding, session, argsBody(8, tool, args))
	if response.StatusCode != http.StatusOK {
		h.t.Fatalf("%s: %d %s", tool, response.StatusCode, body)
	}
	var result struct {
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
	}
	if json.Unmarshal(answer(h.t, body)["result"], &result) != nil || len(result.Content) == 0 {
		h.t.Fatalf("%s: %s", tool, body)
	}
	return result.Content[0].Text
}

func (h *isolated) self(binding, session string) selfInfo {
	h.t.Helper()
	var info selfInfo
	if err := json.Unmarshal([]byte(h.tool(binding, session, "self", nil)), &info); err != nil {
		h.t.Fatal(err)
	}
	return info
}

func refused(t *testing.T, what, outcome string) {
	t.Helper()
	if !strings.Contains(outcome, "permission denied") && !strings.Contains(outcome, "operation not permitted") {
		t.Fatalf("%s: %s", what, outcome)
	}
}

func TestIsolationEachProcessRunsAsAUserOfItsOwnWithoutPrivilege(t *testing.T) {
	h := newIsolatedHarness(t, nil)
	sessionA := h.open("lease-a", "credential-lease-a")
	sessionB := h.open("lease-b", "credential-lease-b")
	probe := h.open("", "")
	a, b, p := h.self("lease-a", sessionA), h.self("lease-b", sessionB), h.self("", probe)
	pool := poolSize(h.bridge.opts.maxProcesses)
	for _, info := range []selfInfo{a, b, p} {
		if info.UID < isolatedBase || info.UID >= isolatedBase+pool || info.GID != info.UID || len(info.Groups) != 0 {
			t.Fatalf("user %d group %d groups %v", info.UID, info.GID, info.Groups)
		}
		for _, set := range []string{"CapPrm", "CapEff", "CapAmb"} {
			if strings.Trim(info.Status[set], "0") != "" {
				t.Fatalf("user %d keeps capabilities: %v", info.UID, info.Status)
			}
		}
		if info.Status["NoNewPrivs"] != "1" {
			t.Fatalf("user %d may gain privileges: %v", info.UID, info.Status)
		}
		home := filepath.Join(h.bridge.opts.homeRoot, fmt.Sprint(info.UID))
		if info.Home != home || info.TempDir != home || info.HomeEnv != home+"/memory.json" || !slices.Equal(info.Args, []string{"--data=" + home + "/data"}) {
			t.Fatalf("user %d: %+v", info.UID, info)
		}
		stat, err := os.Stat(home)
		if err != nil || stat.Mode().Perm() != 0o700 || stat.Sys().(*syscall.Stat_t).Uid != uint32(info.UID) {
			t.Fatalf("%s: %v %v", home, stat.Mode(), err)
		}
		if info.FileMode != "0600" {
			t.Fatalf("a new file is %s, not private", info.FileMode)
		}
	}
	if a.UID == b.UID || a.UID == p.UID || b.UID == p.UID {
		t.Fatalf("two processes share a user: %d %d %d", a.UID, b.UID, p.UID)
	}
	for _, process := range h.status().Processes {
		if process.UID == 0 {
			t.Fatalf("status lists a process without its user: %+v", process)
		}
	}
}

func TestIsolationABindingsProcessCannotReachAnothersOrTheBridge(t *testing.T) {
	h := newIsolatedHarness(t, nil)
	sessionA := h.open("lease-a", "credential-lease-a")
	sessionB := h.open("lease-b", "credential-lease-b")
	b := h.self("lease-b", sessionB)
	if out := h.tool("lease-b", sessionB, "write", map[string]string{"path": b.Home + "/secret"}); out != "ok" {
		t.Fatal(out)
	}
	for what, outcome := range map[string]string{
		"B's environment":  h.tool("lease-a", sessionA, "read", map[string]string{"path": fmt.Sprintf("/proc/%d/environ", b.PID)}),
		"B's directory":    h.tool("lease-a", sessionA, "list", map[string]string{"path": b.Home}),
		"B's file":         h.tool("lease-a", sessionA, "read", map[string]string{"path": b.Home + "/secret"}),
		"B's process":      h.tool("lease-a", sessionA, "signal", map[string]string{"pid": fmt.Sprint(b.PID)}),
		"the home root":    h.tool("lease-a", sessionA, "list", map[string]string{"path": h.bridge.opts.homeRoot}),
		"the bridge":       h.tool("lease-a", sessionA, "connect", map[string]string{"path": h.socket}),
		"the bridge's env": h.tool("lease-a", sessionA, "read", map[string]string{"path": fmt.Sprintf("/proc/%d/environ", os.Getpid())}),
	} {
		refused(t, what, outcome)
	}
	// A file A leaves in the shared /tmp is A's alone.
	shared := filepath.Join(h.shared, "from-a")
	if out := h.tool("lease-a", sessionA, "write", map[string]string{"path": shared}); out != "ok" {
		t.Fatal(out)
	}
	refused(t, "A's file in /tmp", h.tool("lease-b", sessionB, "read", map[string]string{"path": shared}))
	// B's process still serves B.
	if h.self("lease-b", sessionB).PID != b.PID {
		t.Fatal("B lost its process")
	}
}

func TestIsolationAnEndedBindingLeavesNoProcessOrFile(t *testing.T) {
	h := newIsolatedHarness(t, func(o *options) { o.maxProcesses = 1 })
	session := h.open("lease-a", "credential-lease-a")
	a := h.self("lease-a", session)
	var escaped int
	fmt.Sscan(h.tool("lease-a", session, "escape", nil), &escaped)
	if escaped == 0 || gone(escaped) {
		t.Fatal("no escaped process")
	}
	shared := filepath.Join(h.shared, "left-by-a")
	if out := h.tool("lease-a", session, "write", map[string]string{"path": shared}); out != "ok" {
		t.Fatal(out)
	}
	h.bridge.endBinding("lease-a", "its binding ended")
	h.bridge.stopping.Wait()
	// The sleeper left its group and its session: it goes with its user.
	waitGone(t, escaped)
	for _, path := range []string{a.Home, shared} {
		if _, err := os.Lstat(path); err == nil {
			t.Fatalf("%s outlived its binding", path)
		}
	}
	h.bridge.users.mu.Lock()
	back := slices.Contains(h.bridge.users.free, a.UID)
	h.bridge.users.mu.Unlock()
	if !back {
		t.Fatalf("user %d did not go back to the pool", a.UID)
	}
	// The pool cycles; the next binding finds nothing of the last one.
	for round := range poolSize(1) {
		binding := fmt.Sprintf("lease-%d", round)
		next := h.open(binding, "credential-"+binding)
		info := h.self(binding, next)
		if entries, _ := os.ReadDir(info.Home); len(entries) != 0 {
			t.Fatalf("user %d's directory holds %d entries", info.UID, len(entries))
		}
		h.bridge.endBinding(binding, "its binding ended")
		h.bridge.stopping.Wait()
	}
}

func TestIsolationTheStartClearsWhatAnEarlierRunLeft(t *testing.T) {
	h := newIsolatedHarness(t, nil)
	left := filepath.Join(h.shared, "earlier-run")
	kept := filepath.Join(h.shared, "roots")
	os.WriteFile(left, []byte("x"), 0o600)
	os.Chown(left, isolatedBase+1, isolatedBase+1)
	os.WriteFile(kept, []byte("x"), 0o600)
	os.Mkdir(filepath.Join(h.bridge.opts.homeRoot, "stale"), 0o700)
	if err := prepareUsers(h.bridge.opts); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(left); err == nil {
		t.Fatal("a pool user's file survived the start")
	}
	if _, err := os.Lstat(kept); err != nil {
		t.Fatal("another user's file was removed")
	}
	if entries, _ := os.ReadDir(h.bridge.opts.homeRoot); len(entries) != 0 {
		t.Fatal("the home root was not cleared")
	}
	info, _ := os.Stat(h.shared)
	if info.Mode()&os.ModeSticky == 0 {
		t.Fatal("the shared directory is not sticky")
	}
	root, _ := os.Stat(h.bridge.opts.homeRoot)
	if root.Mode().Perm() != 0o711 {
		t.Fatalf("the home root is %v", root.Mode())
	}
	dir, _ := os.Stat(filepath.Dir(h.socket))
	if dir.Mode().Perm() != 0o750 || dir.Sys().(*syscall.Stat_t).Gid != frontUser {
		t.Fatalf("the socket's directory is %v", dir.Mode())
	}
}
