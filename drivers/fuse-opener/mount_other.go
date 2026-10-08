//go:build !linux

package main

import (
	"errors"
	"net"
)

var errLinuxOnly = errors.New("srw-fuse-opener mounts on Linux only")

type systemMounter struct{}

func (systemMounter) Mount(string, mountSpec, policy, int, int) (int, error) {
	return -1, errLinuxOnly
}

func (systemMounter) Detach(string) (int, error) { return 0, errLinuxOnly }

func checkMount(string, string) error { return errLinuxOnly }

func peerCredentials(*net.UnixConn) (int, int, error) { return -1, -1, errLinuxOnly }

func placeAt(int, int) error { return errLinuxOnly }
