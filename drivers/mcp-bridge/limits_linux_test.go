//go:build linux

package main

import (
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"
)

var limitLine = regexp.MustCompile(`^(.+?)\s{2,}(\S+)\s+(\S+)`)

// softLimits reads /proc/<pid>/limits' soft limits by name.
func softLimits(t *testing.T, text string) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, line := range strings.Split(text, "\n")[1:] {
		if found := limitLine.FindStringSubmatch(line); found != nil {
			out[found[1]] = found[2]
		}
	}
	return out
}

func TestLaunchSetsItsLimitsAndBecomesTheProgram(t *testing.T) {
	// As anyone: lowering a limit needs no privilege, and cat forks none.
	out, err := exec.Command(os.Args[0], "launch", "--process-limit", "64", "--address-space-mb", "4096", "--", "cat", "/proc/self/limits").CombinedOutput()
	if err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	limits := softLimits(t, string(out))
	for name, want := range map[string]string{
		"Max processes":      "64",
		"Max core file size": "0",
		"Max address space":  fmt.Sprint(4096 << 20),
	} {
		if limits[name] != want {
			t.Fatalf("%s: %q, want %q (%s)", name, limits[name], want, out)
		}
	}
	// No address space limit unless asked.
	out, _ = exec.Command(os.Args[0], "launch", "--process-limit", "64", "--", "cat", "/proc/self/limits").CombinedOutput()
	if softLimits(t, string(out))["Max address space"] != "unlimited" {
		t.Fatalf("an address space limit nobody asked for: %s", out)
	}
	// Limits it refuses, and a program it cannot start.
	for _, args := range [][]string{
		{"launch", "--process-limit", "8", "--", "true"},
		{"launch", "--process-limit", "64", "--address-space-mb", "10", "--", "true"},
		{"launch", "--process-limit", "64"},
	} {
		if code := exitCode(exec.Command(os.Args[0], args...).Run()); code != 2 {
			t.Errorf("%q exited %d", args, code)
		}
	}
	if code := exitCode(exec.Command(os.Args[0], "launch", "--process-limit", "64", "--", "/no/such/program").Run()); code != 127 {
		t.Fatalf("a missing program exited %d", code)
	}
}

func exitCode(err error) int {
	if exit, ok := err.(*exec.ExitError); ok {
		return exit.ExitCode()
	}
	if err != nil {
		return -1
	}
	return 0
}

func TestIsolationAProcessStartsUnderItsLimits(t *testing.T) {
	h := newIsolatedHarness(t, func(o *options) { o.processLimit = 48 })
	session := h.open("lease-a", "credential-lease-a")
	a := h.self("lease-a", session)
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/limits", a.PID))
	if err != nil {
		t.Fatal(err)
	}
	limits := softLimits(t, string(raw))
	if limits["Max processes"] != "48" || limits["Max core file size"] != "0" {
		t.Fatalf("limits %v", limits)
	}
	// The launcher became the program: same process, the program's argv.
	if !slices.Equal(a.Args, []string{"--data=" + h.bridge.opts.homeRoot + fmt.Sprintf("/%d/data", a.UID)}) {
		t.Fatalf("args %v", a.Args)
	}
	cmdline, _ := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", a.PID))
	if strings.Contains(string(cmdline), "launch") {
		t.Fatalf("the launcher is still running: %q", cmdline)
	}
}

func TestIsolationAForkBurstStaysUnderItsLimitAndItsUserComesBack(t *testing.T) {
	const limit = 32
	h := newIsolatedHarness(t, func(o *options) {
		o.processLimit = limit
		o.maxProcesses = 1
	})
	session := h.open("lease-a", "credential-lease-a")
	a := h.self("lease-a", session)
	h.tool("lease-a", session, "fork_burst", nil)
	// The burst fills its user's limit and keeps trying; it never passes it.
	deadline := time.Now().Add(15 * time.Second)
	for {
		pids, _ := userProcesses(a.UID)
		if len(pids) > limit {
			t.Fatalf("%d processes past the limit of %d", len(pids), limit)
		}
		if len(pids) >= limit-10 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the burst made only %d processes", len(pids))
		}
		time.Sleep(20 * time.Millisecond)
	}
	for range 25 {
		if pids, _ := userProcesses(a.UID); len(pids) > limit {
			t.Fatalf("%d processes past the limit of %d", len(pids), limit)
		}
		time.Sleep(20 * time.Millisecond)
	}
	// The binding ends (its own process may have died of the limit
	// already): every process of its user goes, and the user comes back.
	h.bridge.endBinding("lease-a", "its binding ended")
	h.bridge.stopping.Wait()
	reclaimed(t, h, a.UID)
}

func TestIsolationAUserWhoseKillDidNotConvergeIsHeldUntilItDoes(t *testing.T) {
	previous := killWithin
	killWithin = 0 // the first kill gives up at once
	t.Cleanup(func() { killWithin = previous })
	h := newIsolatedHarness(t, nil)
	session := h.open("lease-a", "credential-lease-a")
	a := h.self("lease-a", session)
	var escaped int
	fmt.Sscan(h.tool("lease-a", session, "escape", nil), &escaped)
	h.bridge.endBinding("lease-a", "its binding ended")
	h.bridge.stopping.Wait()
	if held := h.status().HeldUsers; !slices.Contains(held, a.UID) {
		t.Fatalf("user %d is not held: %v", a.UID, held)
	}
	if inPool(h, a.UID) {
		t.Fatal("a user with a live process went back to the pool")
	}
	reclaimed(t, h, a.UID)
	waitGone(t, escaped)
	if held := h.status().HeldUsers; len(held) != 0 {
		t.Fatalf("still held: %v", held)
	}
}

func inPool(h *isolated, uid int) bool {
	h.bridge.users.mu.Lock()
	defer h.bridge.users.mu.Unlock()
	return slices.Contains(h.bridge.users.free, uid)
}

// reclaimed waits, housekeeping as the bridge does, until a user has no
// process left and is back in the pool.
func reclaimed(t *testing.T, h *isolated, uid int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !inPool(h, uid) {
		if time.Now().After(deadline) {
			pids, _ := userProcesses(uid)
			t.Fatalf("user %d never came back (%d processes left)", uid, len(pids))
		}
		h.bridge.retryHeld()
		time.Sleep(50 * time.Millisecond)
	}
	if pids, _ := userProcesses(uid); len(pids) != 0 {
		t.Fatalf("user %d is back with %d processes", uid, len(pids))
	}
}
