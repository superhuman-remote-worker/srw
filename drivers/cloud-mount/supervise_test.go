package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func jsonUnmarshal(raw []byte, v any) error { return json.Unmarshal(raw, v) }

func TestAPlanIsCheckedBeforeAnythingRuns(t *testing.T) {
	if err := testPlan().check(); err != nil {
		t.Fatalf("a good plan: %v", err)
	}
	unicode := testPlan()
	unicode.Mounts[0].Name = "projekt_müller"
	if err := unicode.check(); err != nil {
		t.Fatalf("a slug with a Unicode letter: %v", err)
	}
	broken := map[string]func(*Plan){
		"version":         func(p *Plan) { p.Version = 2 },
		"root uid":        func(p *Plan) { p.UID = 0 },
		"no mounts":       func(p *Plan) { p.Mounts = nil },
		"index":           func(p *Plan) { p.Mounts[1].Index = 5 },
		"hidden name":     func(p *Plan) { p.Mounts[0].Name = ".srw" },
		"slash in name":   func(p *Plan) { p.Mounts[0].Name = "a/b" },
		"same target":     func(p *Plan) { p.Mounts[1].Target = p.Mounts[0].Target },
		"remote":          func(p *Plan) { p.Mounts[0].Remote = "cloud:" },
		"relative target": func(p *Plan) { p.Mounts[0].Target = "srw/cloud/x" },
		"rc flag":         func(p *Plan) { p.Mounts[0].Flags = []string{"--rc-addr", ":5572"} },
		"config flag":     func(p *Plan) { p.Mounts[0].Flags = []string{"--config=/tmp/x"} },
		"daemon flag":     func(p *Plan) { p.Mounts[0].Flags = []string{"--daemon"} },
		"lone value":      func(p *Plan) { p.Mounts[0].Flags = []string{"full"} },
		"two values":      func(p *Plan) { p.Mounts[0].Flags = []string{"--vfs-cache-mode", "full", "extra"} },
		"drain":           func(p *Plan) { p.DrainSeconds = -1 },
	}
	for what, breakIt := range broken {
		plan := testPlan()
		breakIt(plan)
		if err := plan.check(); err == nil {
			t.Fatalf("%s: a broken plan passed", what)
		}
	}
}

func TestLoadPlanRefusesUnknownFields(t *testing.T) {
	dir := t.TempDir()
	good, _ := json.Marshal(testPlan())
	path := filepath.Join(dir, "plan.json")
	os.WriteFile(path, good, 0o644)
	if _, err := loadPlan(path); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(path, []byte(`{"version":1,"uid":1000,"gid":1000,"mounts":[],"surprise":true}`), 0o644)
	if _, err := loadPlan(path); err == nil {
		t.Fatal("an unknown field was accepted")
	}
}

func TestClassifyNamesOneClosedReason(t *testing.T) {
	cases := []struct {
		code     int
		stderr   string
		timedOut bool
		want     string
	}{
		{1, "Failed to lsjson: couldn't list files: 401 Unauthorized: 401 Unauthorized", false, reasonCredentialRejected},
		{1, "couldn't list files: 403 Forbidden", false, reasonCredentialRejected},
		{3, "Failed to lsjson: directory not found", false, reasonNotFound},
		{1, `Propfind "http://x/": dial tcp: lookup x: no such host`, false, reasonUnreachable},
		{1, "context deadline exceeded", false, reasonTimeout},
		{-1, "", true, reasonTimeout},
		{1, "500 Internal Server Error", false, reasonUnreachable},
	}
	for _, c := range cases {
		if got := classify(c.code, c.stderr, c.timedOut); got != c.want {
			t.Fatalf("%d %q: %s, want %s", c.code, c.stderr, got, c.want)
		}
	}
}

func TestCloudignoreCompilesLikeTheWorkspaceManager(t *testing.T) {
	// The same lines through shared/runtime/services/cloud_mount's
	// compile_cloudignore (awk) gave exactly this; tests/test_cloud_mount_plan.py
	// keeps the awk side to the same vectors.
	lines := []string{
		"# a comment", "", "!keep.txt", "../escape", "a/../b", "/build/", "node_modules",
		"*.log", "docs/tmp", "/", "dir//", "  spaced  \r", "\tcache/", "/root.txt", "x[0-9]", "sub/dir/",
	}
	want := []string{
		"build/**", "**/build/**", "node_modules", "**/node_modules", "*.log", "docs/tmp",
		"dir/**", "**/dir/**", "spaced", "**/spaced", "cache/**", "**/cache/**", "root.txt",
		"**/root.txt", "x[0-9]", "sub/dir/**",
	}
	if got := compileCloudignore(lines); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %q\nwant %q", got, want)
	}
	if got := filterRules([]string{"b", "a"}, []string{"a"}); !reflect.DeepEqual(got, []string{"**/a", "**/b", "a", "b"}) {
		t.Fatalf("rules are sorted and unique: %q", got)
	}
}

func TestMountArgsKeepTheRemoteControlOffTheNetwork(t *testing.T) {
	plan := testPlan()
	args := mountArgs(plan, plan.Mounts[1], "/etc/srw-cloud/rclone.conf", "/srw/cloud-cache/1", "/tmp/srw/rc-1.sock", "/tmp/srw/filter-1.txt")
	joined := strings.Join(args, " ")
	for _, want := range []string{
		"mount2 m1:Shared/x /srw/cloud/home",
		"--config /etc/srw-cloud/rclone.conf",
		"--rc --rc-addr unix:///tmp/srw/rc-1.sock --rc-no-auth",
		"--uid 1000 --gid 1000 --umask 022",
		"--read-only",
		"--exclude-from /tmp/srw/filter-1.txt",
		"--allow-non-empty",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("%q lacks %q", joined, want)
		}
	}
	rw := strings.Join(mountArgs(plan, plan.Mounts[0], "c", "d", "/s", ""), " ")
	if strings.Contains(rw, "--read-only") || strings.Contains(rw, "--exclude-from") || !strings.HasSuffix(rw, "--vfs-cache-mode full") {
		t.Fatalf("read-write args %q", rw)
	}
	if strings.Contains(rw, "-vv") || strings.Contains(rw, "DEBUG") {
		t.Fatal("debug logging prints credentials rclone reads")
	}
}

func TestAMountComesUpAndIsReported(t *testing.T) {
	h := newHarness(t, testPlan())
	h.mounts.set("/srw/cloud/project", true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.s.worker(ctx, h.s.plan.Mounts[0])
	eventually(t, "mounted", func() bool { return h.statusFile(t, 0).State == stateMounted })
	status := h.statusFile(t, 0)
	if status.Name != "project" || status.Reason != "" || status.Attempts != 1 || status.Since == "" {
		t.Fatalf("status %+v", status)
	}
	if h.runner.callCount("lsjson") != 1 {
		t.Fatal("the remote was not tested before mounting")
	}
	started := strings.Join(h.runner.started[0], " ")
	if !strings.Contains(started, "--cache-dir "+filepath.Join(h.dir, "cache", "0")) {
		t.Fatalf("started %s", started)
	}
	if info, err := os.Stat(filepath.Join(h.dir, "cache", "0")); err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("each mount's cache is private: %v", err)
	}
}

func TestALostMountIsRemounted(t *testing.T) {
	h := newHarness(t, testPlan())
	h.mounts.set("/srw/cloud/project", true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.s.worker(ctx, h.s.plan.Mounts[0])
	eventually(t, "first mount", func() bool { return h.runner.startCount() == 1 && h.statusFile(t, 0).State == stateMounted })
	h.runner.child(0).exit(137, "")
	eventually(t, "a second rclone", func() bool { return h.runner.startCount() == 2 })
	eventually(t, "mounted again", func() bool { return h.statusFile(t, 0).State == stateMounted })
	if h.statusFile(t, 0).Attempts != 2 {
		t.Fatalf("attempts %d", h.statusFile(t, 0).Attempts)
	}
}

func TestABadCredentialIsReportedAndNeverMounted(t *testing.T) {
	h := newHarness(t, testPlan())
	h.runner.results["lsjson"] = result{code: 1, stderr: "couldn't list files: 401 Unauthorized"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.s.worker(ctx, h.s.plan.Mounts[0])
	eventually(t, "unavailable", func() bool { return h.statusFile(t, 0).State == stateUnavailable })
	if status := h.statusFile(t, 0); status.Reason != reasonCredentialRejected {
		t.Fatalf("status %+v", status)
	}
	raw, _ := os.ReadFile(filepath.Join(h.dir, "status", "0.json"))
	if strings.Contains(string(raw), "401") || strings.Contains(string(raw), "Unauthorized") {
		t.Fatalf("rclone's words reached the status file: %s", raw)
	}
	eventually(t, "a retry", func() bool { return h.runner.callCount("lsjson") >= 2 })
	if h.runner.startCount() != 0 {
		t.Fatal("rclone mounted with a refused credential")
	}
	// Retried, it keeps its last reason rather than flipping back to pending.
	eventually(t, "attempts counted", func() bool { return h.statusFile(t, 0).Attempts >= 2 })
	if status := h.statusFile(t, 0); status.State != stateUnavailable || status.Reason != reasonCredentialRejected {
		t.Fatalf("a retried mount lost its reason: %+v", status)
	}
	// The credential is fixed (say the remote recovered): the next try mounts.
	h.runner.mu.Lock()
	h.runner.results["lsjson"] = result{}
	h.runner.mu.Unlock()
	h.mounts.set("/srw/cloud/project", true)
	eventually(t, "mounted after recovery", func() bool { return h.statusFile(t, 0).State == stateMounted })
}

func TestAMissingCredentialFileIsReported(t *testing.T) {
	h := newHarness(t, testPlan())
	os.Remove(h.s.configPath)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.s.worker(ctx, h.s.plan.Mounts[0])
	eventually(t, "config_missing", func() bool { return h.statusFile(t, 0).Reason == reasonConfigMissing })
	if h.runner.callCount("lsjson") != 0 {
		t.Fatal("tested the remote without a credential")
	}
}

func TestAMountThatNeverAnswersTimesOut(t *testing.T) {
	h := newHarness(t, testPlan())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.s.worker(ctx, h.s.plan.Mounts[0])
	eventually(t, "timeout", func() bool { return h.statusFile(t, 0).Reason == reasonTimeout })
	if signals := h.runner.child(0).signalled(); len(signals) == 0 || signals[0] != syscall.SIGTERM {
		t.Fatalf("the silent rclone was not stopped: %v", signals)
	}
}

func TestAnOpenerRefusalIsAMountFailure(t *testing.T) {
	h := newHarness(t, testPlan())
	h.runner.childFor = func(int) *fakeChild {
		c := &fakeChild{exited: make(chan struct{})}
		c.exit(1, "fusermount3 (srw): opener refused: option \"suid\" is not allowed")
		return c
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.s.worker(ctx, h.s.plan.Mounts[0])
	eventually(t, "mount_failed", func() bool { return h.statusFile(t, 0).Reason == reasonMountFailed })
}

func TestAHungMountIsKilledAndRemounted(t *testing.T) {
	h := newHarness(t, testPlan())
	h.runner.childFor = func(int) *fakeChild { return &fakeChild{exited: make(chan struct{}), hang: true} }
	h.mounts.set("/srw/cloud/project", true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.s.worker(ctx, h.s.plan.Mounts[0])
	eventually(t, "mounted", func() bool { return h.statusFile(t, 0).State == stateMounted })
	h.mounts.set("/srw/cloud/project", false)
	eventually(t, "killed", func() bool {
		signals := h.runner.child(0).signalled()
		return len(signals) == 2 && signals[1] == syscall.SIGKILL
	})
	h.mounts.set("/srw/cloud/project", true)
	eventually(t, "remounted", func() bool { return h.runner.startCount() >= 2 && h.statusFile(t, 0).State == stateMounted })
}

func TestTheFoldersCloudignoreBecomesAnExcludeFile(t *testing.T) {
	plan := testPlan()
	plan.Mounts[0].Cloudignore = true
	plan.Mounts[0].Ignore = []string{"*.tmp"}
	h := newHarness(t, plan)
	h.runner.results["cat"] = result{out: "node_modules\n# x\n"}
	h.mounts.set("/srw/cloud/project", true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.s.worker(ctx, h.s.plan.Mounts[0])
	eventually(t, "mounted", func() bool { return h.statusFile(t, 0).State == stateMounted })
	filter, err := os.ReadFile(filepath.Join(h.dir, "run", "filter-0.txt"))
	if err != nil || string(filter) != "**/node_modules\n*.tmp\nnode_modules\n" {
		t.Fatalf("filter %q, %v", filter, err)
	}
	if !strings.Contains(strings.Join(h.runner.calls[1], " "), "m0:.cloudignore") {
		t.Fatalf("calls %v", h.runner.calls)
	}
}

func TestADrainRequestFlushesAndIsAnswered(t *testing.T) {
	h := newHarness(t, testPlan())
	h.mounts.set("/srw/cloud/project", true)
	h.mounts.set("/srw/cloud/home", true)
	socket := h.s.socket(h.s.plan.Mounts[0])
	h.rc.pending[socket] = []int{2, 1, 0}
	h.rc.queue[socket] = []map[string]any{
		{"id": float64(7), "expiry": 4.5, "uploading": false},
		{"id": float64(8), "expiry": 0.5, "uploading": true},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, m := range h.s.plan.Mounts {
		go h.s.worker(ctx, m)
	}
	eventually(t, "both mounted", func() bool {
		return h.statusFile(t, 0).State == stateMounted && h.statusFile(t, 1).State == stateMounted
	})
	go h.s.control(ctx)
	os.WriteFile(filepath.Join(h.dir, "control", "drain"), []byte("n-1\n"), 0o644)
	eventually(t, "drain acks", func() bool {
		a, b := h.statusFile(t, 0).Drain, h.statusFile(t, 1).Drain
		return a != nil && b != nil && a.Nonce == "n-1" && a.State == "drained" && b.State == "drained"
	})
	if !h.rc.seen("rc-0.sock vfs/queue-set-expiry map[expiry:-1e+09 id:7]") {
		t.Fatalf("the queued upload was not expedited: %v", h.rc.calls)
	}
	if h.rc.seen("id:8") {
		t.Fatal("an upload already running was touched")
	}
	if h.rc.seen("rc-1.sock") {
		t.Fatal("a read-only mount was drained")
	}
	// The same nonce again is not a new request; a malformed one is ignored.
	calls := len(h.rc.calls)
	os.WriteFile(filepath.Join(h.dir, "control", "drain"), []byte("n-1"), 0o644)
	time.Sleep(60 * time.Millisecond)
	os.WriteFile(filepath.Join(h.dir, "control", "drain"), []byte("$(reboot)"), 0o644)
	time.Sleep(60 * time.Millisecond)
	h.rc.mu.Lock()
	again := len(h.rc.calls)
	h.rc.mu.Unlock()
	if again != calls {
		t.Fatal("an answered or malformed nonce was acted on")
	}
}

func TestADrainThatRunsOutOfTimeSaysWhatIsLeft(t *testing.T) {
	plan := testPlan()
	plan.DrainSeconds = 0
	h := newHarness(t, plan)
	h.mounts.set("/srw/cloud/project", true)
	h.rc.pending[h.s.socket(plan.Mounts[0])] = []int{3}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.s.worker(ctx, plan.Mounts[0])
	eventually(t, "mounted", func() bool { return h.statusFile(t, 0).State == stateMounted })
	pending, complete := h.s.drainAll(ctx, "n-2", time.Now())
	if complete || pending != 3 {
		t.Fatalf("pending %d complete %v", pending, complete)
	}
	if ack := h.statusFile(t, 0).Drain; ack == nil || ack.State != "incomplete" || ack.Pending != 3 {
		t.Fatalf("ack %+v", ack)
	}
	// A mount that is restarting cannot say: unknown, not drained.
	h.s.board.set(0, statePending, "")
	if pending, complete := h.s.drainAll(ctx, "n-3", time.Now()); complete || pending != -1 {
		t.Fatalf("pending %d complete %v", pending, complete)
	}
}

func TestARefreshRequestRereadsMountedFolders(t *testing.T) {
	h := newHarness(t, testPlan())
	h.mounts.set("/srw/cloud/project", true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.s.worker(ctx, h.s.plan.Mounts[0])
	eventually(t, "mounted", func() bool { return h.statusFile(t, 0).State == stateMounted })
	go h.s.control(ctx)
	os.WriteFile(filepath.Join(h.dir, "control", "refresh"), []byte("r-1"), 0o644)
	eventually(t, "refresh acks", func() bool {
		a, b := h.statusFile(t, 0).Refresh, h.statusFile(t, 1).Refresh
		return a != nil && b != nil && a.State == "done" && b.State == "failed"
	})
	if !h.rc.seen("rc-0.sock vfs/refresh map[recursive:true]") {
		t.Fatalf("calls %v", h.rc.calls)
	}
}

func TestShutdownDrainsThenStopsEveryRclone(t *testing.T) {
	h := newHarness(t, testPlan())
	h.mounts.set("/srw/cloud/project", true)
	h.mounts.set("/srw/cloud/home", true)
	h.rc.pending[h.s.socket(h.s.plan.Mounts[0])] = []int{1, 0}
	ctx, cancel := context.WithCancel(context.Background())
	var workers sync.WaitGroup
	for _, m := range h.s.plan.Mounts {
		workers.Add(1)
		go func(m Mount) { defer workers.Done(); h.s.worker(ctx, m) }(m)
	}
	eventually(t, "both mounted", func() bool {
		return h.statusFile(t, 0).State == stateMounted && h.statusFile(t, 1).State == stateMounted
	})
	pending, complete := h.s.shutdown(cancel, &workers)
	if !complete || pending != 0 {
		t.Fatalf("pending %d complete %v", pending, complete)
	}
	for i := 0; i < 2; i++ {
		if signals := h.runner.child(i).signalled(); len(signals) != 1 || signals[0] != syscall.SIGTERM {
			t.Fatalf("child %d got %v: each rclone unmounts on SIGTERM", i, signals)
		}
	}
	if h.runner.startCount() != 2 {
		t.Fatal("a mount restarted during shutdown")
	}
}

func TestWaitForPlanGivesWayToAStop(t *testing.T) {
	signals := make(chan os.Signal, 1)
	signals <- syscall.SIGTERM
	var stderr strings.Builder
	if plan := waitForPlan(filepath.Join(t.TempDir(), "absent.json"), signals, &stderr); plan != nil {
		t.Fatal("a plan out of nothing")
	}
	if !strings.Contains(stderr.String(), "no usable plan yet") {
		t.Fatalf("stderr %q", stderr.String())
	}
}

func TestAControlRequestIsOnlyEverASmallRegularFile(t *testing.T) {
	dir := t.TempDir()
	plain := filepath.Join(dir, "plain")
	os.WriteFile(plain, []byte("n-1\n"), 0o644)
	if got, err := readControl(plain); err != nil || got != "n-1\n" {
		t.Fatalf("a plain request: %q %v", got, err)
	}
	secret := filepath.Join(dir, "secret")
	os.WriteFile(secret, []byte("n-secret"), 0o600)
	link := filepath.Join(dir, "link")
	if err := os.Symlink(secret, link); err != nil {
		t.Fatal(err)
	}
	fifo := filepath.Join(dir, "fifo")
	if err := syscall.Mkfifo(fifo, 0o644); err != nil {
		t.Fatal(err)
	}
	big := filepath.Join(dir, "big")
	os.WriteFile(big, []byte(strings.Repeat("a", maxControlBytes+1)), 0o644)
	sub := filepath.Join(dir, "sub")
	os.Mkdir(sub, 0o755)
	for _, path := range []string{link, fifo, big, sub} {
		done := make(chan error, 1)
		go func(path string) {
			_, err := readControl(path)
			done <- err
		}(path)
		select {
		case err := <-done:
			if err == nil {
				t.Fatalf("%s was read as a request", filepath.Base(path))
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("%s blocked the read", filepath.Base(path))
		}
	}
}

func TestAFifoInTheControlDirectoryNeverStallsTheLoop(t *testing.T) {
	h := newHarness(t, testPlan())
	h.mounts.set("/srw/cloud/project", true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.s.worker(ctx, h.s.plan.Mounts[0])
	eventually(t, "mounted", func() bool { return h.statusFile(t, 0).State == stateMounted })
	if err := syscall.Mkfifo(filepath.Join(h.dir, "control", "drain"), 0o644); err != nil {
		t.Fatal(err)
	}
	go h.s.control(ctx)
	time.Sleep(50 * time.Millisecond)
	os.WriteFile(filepath.Join(h.dir, "control", "refresh"), []byte("r-9"), 0o644)
	eventually(t, "the refresh is still answered", func() bool {
		ack := h.statusFile(t, 0).Refresh
		return ack != nil && ack.Nonce == "r-9"
	})
}
