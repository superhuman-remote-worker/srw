//go:build linux

package main

import (
	"bytes"
	"os"
	"os/exec"
	"strconv"
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

// reapOrphans waits for zombies the bridge inherited as the container's
// first process that belong to no process group it still manages (one that
// left its group and outlived it). It never waits for a live session's
// process, which its own command waits for.
func reapOrphans(live map[int]bool) {
	if os.Getpid() != 1 {
		return
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return
	}
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid == 1 {
			continue
		}
		raw, err := os.ReadFile("/proc/" + entry.Name() + "/stat")
		if err != nil {
			continue
		}
		// pid (comm) state ppid pgrp ...: comm may hold spaces and ')'.
		end := bytes.LastIndexByte(raw, ')')
		if end < 0 {
			continue
		}
		fields := bytes.Fields(raw[end+1:])
		if len(fields) < 3 || string(fields[0]) != "Z" || string(fields[1]) != "1" {
			continue
		}
		group, err := strconv.Atoi(string(fields[2]))
		if err != nil || live[group] {
			continue
		}
		var status syscall.WaitStatus
		syscall.Wait4(pid, &status, syscall.WNOHANG, nil)
	}
}
