//go:build !linux

package main

import (
	"os/exec"
	"time"
)

// The bridge runs in Linux containers; elsewhere (a developer's machine) a
// process is stopped by itself.

func ownGroup(*exec.Cmd)           {}
func killGroup(int)                {}
func reapGroup(int, time.Duration) {}
func reapOrphans(map[int]bool)     {}
func becomeSubreaper() error       { return nil }

func procState(int) (string, int, bool) { return "", 0, false }
