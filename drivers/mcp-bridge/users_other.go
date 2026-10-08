//go:build !linux

package main

import (
	"errors"
	"syscall"
	"time"
)

// Users of their own need Linux: elsewhere (a developer's machine) the
// bridge runs every process as itself (--uid-base 0).

func restrictProcess() error                         { return nil }
func onSpawner(start func())                         { start() }
func runAs(*syscall.SysProcAttr, int)                {}
func prepareUsers(options) error                     { return errors.New("--uid-base needs Linux") }
func killUser(int, map[int]bool, time.Duration) bool { return true }
func sweepUser([]string, int)                        {}
