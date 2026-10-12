//go:build linux

package main

import (
	"bytes"
	"os"
	"os/exec"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"
)

// ownGroup runs a binding's process in a process group of its own (so it
// is stopped with everything it started) and kills it if the bridge dies.
func ownGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
}

// killGroup kills what is left of a stopped process's group.
func killGroup(pid int) {
	if pid > 0 {
		syscall.Kill(-pid, syscall.SIGKILL)
	}
}

// reapGroup waits for the bridge's children left in a group: as the
// container's first process the bridge inherits whatever a stopped process
// left behind.
func reapGroup(pid int, within time.Duration) {
	if pid <= 0 {
		return
	}
	deadline := time.Now().Add(within)
	for {
		var status syscall.WaitStatus
		found, err := syscall.Wait4(-pid, &status, syscall.WNOHANG, nil)
		switch {
		case err == syscall.EINTR:
			continue
		case err != nil:
			return // none of the bridge's children is left in the group
		case found > 0:
			continue
		}
		if time.Now().After(deadline) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

const prSetChildSubreaper = 36

// subreaper is set once the bridge adopts its processes' orphans.
var subreaper atomic.Bool

// becomeSubreaper makes the bridge the parent of every orphan its processes
// leave (PR_SET_CHILD_SUBREAPER), as the container's first process is
// anyway, so it reaps them whatever its process id.
func becomeSubreaper() error {
	if _, _, errno := syscall.RawSyscall(syscall.SYS_PRCTL, prSetChildSubreaper, 1, 0); errno != 0 {
		return errno
	}
	subreaper.Store(true)
	return nil
}

// reapOrphans waits for every zombie the bridge adopted (an orphan of a
// process, in its group or not), while the process lives too. It never
// waits for a process the bridge started (mains): that process's own
// command waits for it.
func reapOrphans(mains map[int]bool) {
	self := os.Getpid()
	if self != 1 && !subreaper.Load() {
		return
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return
	}
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid == self || mains[pid] {
			continue
		}
		state, parent, ok := procState(pid)
		if !ok || state != "Z" || parent != self {
			continue
		}
		var status syscall.WaitStatus
		syscall.Wait4(pid, &status, syscall.WNOHANG, nil)
	}
}

// procState is a process's state letter and parent's process id, from
// /proc/PID/stat.
func procState(pid int) (string, int, bool) {
	raw, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return "", 0, false
	}
	// pid (comm) state ppid ...: comm may hold spaces and ')'.
	end := bytes.LastIndexByte(raw, ')')
	if end < 0 {
		return "", 0, false
	}
	fields := bytes.Fields(raw[end+1:])
	if len(fields) < 2 {
		return "", 0, false
	}
	parent, err := strconv.Atoi(string(fields[1]))
	if err != nil {
		return "", 0, false
	}
	return string(fields[0]), parent, true
}
