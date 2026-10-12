//go:build linux

package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
)

// isolationTool runs the fake server's tools that look around the pod from
// inside a binding's process: what a compromised server could try.
//
//	self          its user, groups, capabilities, no_new_privs, HOME, TMPDIR,
//	              its arguments, and the mode of a file it creates
//	read  path    reads a file; list path lists a directory
//	connect path  connects to a unix socket
//	signal pid    sends signal 0 to a process
//	write path    writes a file
//	escape        starts a sleeper in a session of its own (as browsers are
//	              started): outside its process group
//	fork_burst    starts a shell of its own that forks sleepers without end
func isolationTool(name string, args map[string]string) (string, bool) {
	outcome := func(err error) string {
		if err != nil {
			return "error: " + err.Error()
		}
		return "ok"
	}
	switch name {
	case "self":
		return selfReport(), true
	case "read":
		_, err := os.ReadFile(args["path"])
		return outcome(err), true
	case "list":
		_, err := os.ReadDir(args["path"])
		return outcome(err), true
	case "connect":
		conn, err := net.Dial("unix", args["path"])
		if err == nil {
			conn.Close()
		}
		return outcome(err), true
	case "signal":
		pid, _ := strconv.Atoi(args["pid"])
		return outcome(syscall.Kill(pid, 0)), true
	case "write":
		return outcome(os.WriteFile(args["path"], []byte("left behind"), 0o666)), true
	case "escape":
		sleeper := exec.Command(os.Args[0])
		sleeper.Env = append(os.Environ(), sleeperEnv+"=1", fakeServerEnv+"=0")
		sleeper.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		if err := sleeper.Start(); err != nil {
			return outcome(err), true
		}
		return fmt.Sprint(sleeper.Process.Pid), true
	case "fork_burst":
		// A shell in a session of its own that forks sleepers without end
		// (each fork it is refused, it tries again): a sustained burst.
		burst := exec.Command("sh", "-c", "trap '' TERM; while :; do sleep 60 & done")
		burst.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		if err := burst.Start(); err != nil {
			return outcome(err), true
		}
		return fmt.Sprint(burst.Process.Pid), true
	}
	return "", false
}

type selfInfo struct {
	PID      int               `json:"pid"`
	UID      int               `json:"uid"`
	GID      int               `json:"gid"`
	Groups   []int             `json:"groups"`
	Status   map[string]string `json:"status"`
	Home     string            `json:"home"`
	TempDir  string            `json:"tmpdir"`
	Args     []string          `json:"args"`
	HomeEnv  string            `json:"home_env"`
	FileMode string            `json:"file_mode"`
}

func selfReport() string {
	groups, _ := os.Getgroups()
	info := selfInfo{
		PID:     os.Getpid(),
		UID:     os.Getuid(),
		GID:     os.Getgid(),
		Groups:  groups,
		Status:  map[string]string{},
		Home:    os.Getenv("HOME"),
		TempDir: os.Getenv("TMPDIR"),
		Args:    os.Args[1:],
		HomeEnv: os.Getenv(homeValueEnv),
	}
	raw, _ := os.ReadFile("/proc/self/status")
	for _, line := range strings.Split(string(raw), "\n") {
		key, value, ok := strings.Cut(line, ":")
		switch key {
		case "CapInh", "CapPrm", "CapEff", "CapAmb", "NoNewPrivs":
			if ok {
				info.Status[key] = strings.TrimSpace(value)
			}
		}
	}
	probe := info.Home + "/mode-probe"
	if err := os.WriteFile(probe, []byte("x"), 0o666); err == nil {
		if stat, err := os.Stat(probe); err == nil {
			info.FileMode = fmt.Sprintf("%#o", stat.Mode().Perm())
		}
		os.Remove(probe)
	}
	out, _ := json.Marshal(info)
	return string(out)
}
