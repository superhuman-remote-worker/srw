//go:build linux

package main

import (
	"bufio"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const prSetNoNewPrivs = 38

var (
	spawnerOnce sync.Once
	spawnerErr  error
	// The jobs of the thread every process is started from, once there is
	// one.
	spawnJobs atomic.Pointer[chan func()]
)

// restrictProcess makes every process the bridge starts from now on create
// private files (umask 077, the process's) and unable to gain a privilege
// on exec (no_new_privs, as allowPrivilegeEscalation: false sets it for the
// whole container already). no_new_privs is a thread's, and a child
// inherits the thread that forked it, so every process is started from one
// thread that set it (it also owns each process's parent-death signal).
func restrictProcess() error {
	syscall.Umask(0o077)
	spawnerOnce.Do(func() {
		ready := make(chan error)
		jobs := make(chan func())
		go func() {
			// Never unlocked: the thread keeps its no_new_privs.
			runtime.LockOSThread()
			if _, _, errno := syscall.RawSyscall(syscall.SYS_PRCTL, prSetNoNewPrivs, 1, 0); errno != 0 {
				ready <- fmt.Errorf("no_new_privs: %w", errno)
				return
			}
			ready <- nil
			for job := range jobs {
				job()
			}
		}()
		if spawnerErr = <-ready; spawnerErr == nil {
			spawnJobs.Store(&jobs)
		}
	})
	return spawnerErr
}

// onSpawner runs start on the thread processes are started from, if the
// bridge has one.
func onSpawner(start func()) {
	jobs := spawnJobs.Load()
	if jobs == nil {
		start()
		return
	}
	done := make(chan struct{})
	*jobs <- func() {
		defer close(done)
		start()
	}
	<-done
}

// runAs starts a process as its user: that user's group alone (no
// supplementary group). The bridge is root, so the kernel clears every
// capability with the switch.
func runAs(attr *syscall.SysProcAttr, uid int) {
	attr.Credential = &syscall.Credential{Uid: uint32(uid), Gid: uint32(uid), Groups: []uint32{}}
}

// prepareUsers readies the pool at start: the home root is the bridge's
// alone (0711: a process reaches its own directory by name and lists no
// other) and empty, /tmp-like directories are sticky (no process removes
// another's file), and nothing of a pool user is left from an earlier run
// of the container (emptyDirs outlive it).
func prepareUsers(opts options) error {
	if os.Geteuid() != 0 {
		return errors.New("--uid-base needs the bridge to run as root, with the SETUID, SETGID, KILL, CHOWN, DAC_OVERRIDE and FOWNER capabilities: it starts each binding's process as a user of its own")
	}
	if err := os.MkdirAll(opts.homeRoot, 0o711); err != nil {
		return err
	}
	if err := os.Chown(opts.homeRoot, 0, 0); err != nil {
		return err
	}
	if err := os.Chmod(opts.homeRoot, 0o711); err != nil {
		return err
	}
	if err := clearDir(opts.homeRoot); err != nil {
		return err
	}
	for _, dir := range opts.sweepDirs {
		info, err := os.Stat(dir)
		if err != nil {
			continue // not in this container
		}
		if info.IsDir() && info.Mode().Perm()&0o002 != 0 && info.Mode()&fs.ModeSticky == 0 {
			os.Chmod(dir, info.Mode().Perm()|fs.ModeSticky)
		}
	}
	size := poolSize(opts.maxProcesses)
	for uid := opts.uidBase; uid < opts.uidBase+size; uid++ {
		killUser(uid, nil, reapWithin)
		sweepUser(opts.sweepDirs, uid)
	}
	return nil
}

// procUser is a process's real user and state, from /proc/PID/status.
func procUser(pid int) (int, string, bool) {
	file, err := os.Open("/proc/" + strconv.Itoa(pid) + "/status")
	if err != nil {
		return 0, "", false
	}
	defer file.Close()
	uid, state, found := 0, "", 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() && found < 2 {
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, "State:"):
			if fields := strings.Fields(line); len(fields) > 1 {
				state = fields[1]
			}
			found++
		case strings.HasPrefix(line, "Uid:"):
			fields := strings.Fields(line)
			if len(fields) < 2 {
				return 0, "", false
			}
			if uid, err = strconv.Atoi(fields[1]); err != nil {
				return 0, "", false
			}
			found++
		}
	}
	return uid, state, found == 2
}

// userProcesses lists a user's processes that are not zombies, and how many
// of them still run (are not stopped).
func userProcesses(uid int) (pids []int, running int) {
	entries, _ := os.ReadDir("/proc")
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		owner, state, ok := procUser(pid)
		if !ok || owner != uid || state == "Z" || state == "X" {
			continue
		}
		pids = append(pids, pid)
		if state != "T" && state != "t" {
			running++
		}
	}
	return pids, running
}

// killUser kills every process of a user (a process that left its group or
// its session included) and reaps the ones the bridge adopted, until none
// is left or the time runs out; it reports whether none is left. It first
// stops them all (SIGSTOP, which nothing ignores: a stopped process forks
// no more, so a fork burst cannot outrun it), then kills them.
func killUser(uid int, mains map[int]bool, within time.Duration) bool {
	if uid <= 0 {
		return true
	}
	deadline := time.Now().Add(within)
	for {
		pids, running := userProcesses(uid)
		if len(pids) == 0 {
			reapOrphans(mains)
			return true
		}
		signal := syscall.SIGKILL
		if running > 0 {
			signal = syscall.SIGSTOP
		}
		for _, pid := range pids {
			syscall.Kill(pid, signal)
		}
		reapOrphans(mains)
		if time.Now().After(deadline) {
			return false
		}
		if running == 0 {
			// The killed ones go; their adopted zombies are reaped above.
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// rlimitNproc is RLIMIT_NPROC on Linux (amd64 and arm64).
const rlimitNproc = 6

// launch is the start of a binding's process, already running as the
// binding's user: it caps the user's processes (RLIMIT_NPROC counts every
// process and thread of a real user, so of this binding alone), takes core
// dumps away and, when asked, caps the address space, then becomes the
// server's program. Go cannot run code between fork and exec, so the
// bridge starts itself in this mode.
func launch(limits launchLimits, program []string) error {
	nproc := uint64(limits.processes)
	if err := syscall.Setrlimit(rlimitNproc, &syscall.Rlimit{Cur: nproc, Max: nproc}); err != nil {
		return fmt.Errorf("the process limit: %w", err)
	}
	if err := syscall.Setrlimit(syscall.RLIMIT_CORE, &syscall.Rlimit{}); err != nil {
		return fmt.Errorf("the core limit: %w", err)
	}
	if limits.addressSpace > 0 {
		bytes := uint64(limits.addressSpace)
		if err := syscall.Setrlimit(syscall.RLIMIT_AS, &syscall.Rlimit{Cur: bytes, Max: bytes}); err != nil {
			return fmt.Errorf("the address space limit: %w", err)
		}
	}
	path, err := exec.LookPath(program[0])
	if err != nil {
		return err
	}
	return syscall.Exec(path, program, os.Environ())
}

// sweepUser removes what a user left in the shared writable directories.
func sweepUser(dirs []string, uid int) {
	for _, dir := range dirs {
		filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
			if err != nil || path == dir {
				return nil
			}
			info, err := entry.Info()
			if err != nil {
				return nil
			}
			if stat, ok := info.Sys().(*syscall.Stat_t); ok && int(stat.Uid) == uid {
				os.RemoveAll(path)
				if entry.IsDir() {
					return filepath.SkipDir
				}
			}
			return nil
		})
	}
}
