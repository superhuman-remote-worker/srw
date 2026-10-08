//go:build linux

package main

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"syscall"
)

type systemMounter struct{}

func (systemMounter) Mount(target string, spec mountSpec, p policy, uid, gid int) (int, error) {
	// mount(2) follows a symlink at the target; whoever can write the
	// emptyDir must not be able to point the mount elsewhere.
	var st syscall.Stat_t
	if err := syscall.Lstat(target, &st); err != nil {
		return -1, fmt.Errorf("stat %s: %w", target, err)
	}
	if st.Mode&syscall.S_IFMT != syscall.S_IFDIR {
		return -1, fmt.Errorf("%s is not a directory", target)
	}
	fd, err := syscall.Open("/dev/fuse", syscall.O_RDWR|syscall.O_CLOEXEC, 0)
	if err != nil {
		return -1, fmt.Errorf("open /dev/fuse: %w", err)
	}
	data := spec.data(p, fd, st.Mode, uid, gid)
	if err := syscall.Mount(spec.Source, target, spec.FSType, spec.Flags, data); err != nil {
		syscall.Close(fd)
		return -1, fmt.Errorf("mount %s: %w", target, err)
	}
	return fd, nil
}

func (systemMounter) Detach(target string) (int, error) {
	// Mounts can be stacked; a bounded loop removes them top down.
	detached := 0
	for range 16 {
		mounts, err := mountsAt("/proc/self/mountinfo", target)
		if err != nil {
			return detached, err
		}
		if len(mounts) == 0 {
			return detached, nil
		}
		top := mounts[len(mounts)-1]
		if !strings.HasPrefix(top, "fuse") {
			return detached, fmt.Errorf("%s holds a %s mount, not FUSE", target, top)
		}
		if err := syscall.Unmount(target, syscall.MNT_DETACH); err != nil && !errors.Is(err, syscall.EINVAL) {
			return detached, fmt.Errorf("unmount %s: %w", target, err)
		}
		detached++
	}
	return detached, fmt.Errorf("%s: too many stacked mounts", target)
}

// checkMount is the daemon side's startup probe: the top mount at target is
// fuse.<subtype> and answers (a mount whose daemon died answers ENOTCONN).
func checkMount(target, subtype string) error {
	mounts, err := mountsAt("/proc/self/mountinfo", target)
	if err != nil {
		return err
	}
	if len(mounts) == 0 || mounts[len(mounts)-1] != "fuse."+subtype {
		return fmt.Errorf("%s is not mounted as fuse.%s", target, subtype)
	}
	var st syscall.Stat_t
	if err := syscall.Stat(target, &st); err != nil {
		return fmt.Errorf("%s does not answer: %w", target, err)
	}
	return nil
}

// mountsAt lists the filesystem types mounted at target, bottom first.
func mountsAt(mountinfo, target string) ([]string, error) {
	file, err := os.Open(mountinfo)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return parseMountinfo(bufio.NewScanner(file), target)
}

func peerCredentials(conn *net.UnixConn) (int, int, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return -1, -1, err
	}
	var cred *syscall.Ucred
	var credErr error
	if err := raw.Control(func(fd uintptr) {
		cred, credErr = syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	}); err != nil {
		return -1, -1, err
	}
	if credErr != nil {
		return -1, -1, credErr
	}
	return int(cred.Uid), int(cred.Gid), nil
}

// placeAt moves fd to slot without close-on-exec, for an exec'd daemon.
func placeAt(fd, slot int) error {
	if fd == slot {
		_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, uintptr(fd), syscall.F_SETFD, 0)
		if errno != 0 {
			return errno
		}
		return nil
	}
	if err := syscall.Dup3(fd, slot, 0); err != nil {
		return err
	}
	return syscall.Close(fd)
}
