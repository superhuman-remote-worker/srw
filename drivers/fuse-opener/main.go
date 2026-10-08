// Command srw-fuse-opener is the D7 spike prototype of the in-pod plane's
// privileged half (connector drivers, "Three planes", In-pod). It is not a
// product component yet.
//
// One binary, three roles:
//
//	srw-fuse-opener serve --socket S --target DIR --client-uid N [--read-only]
//	    The privileged native sidecar. It owns one FUSE mountpoint, DIR, in an
//	    emptyDir it mounts with Bidirectional propagation. For each request on
//	    the unix socket S (a volume only the rclone sidecar shares) it detaches
//	    a stale mount at DIR, opens /dev/fuse, mounts it at DIR with options it
//	    chooses (nosuid, nodev, allow_other, ro when --read-only), and passes
//	    the descriptor back over SCM_RIGHTS. It opens no network listener and
//	    never reads what the filesystem serves. On SIGTERM it unmounts DIR.
//
//	fusermount3 [-u] [-z] [-q] [-o OPTS] MOUNTPOINT   (argv[0] or subcommand)
//	    The client in the unprivileged rclone sidecar, installed ahead of the
//	    real fusermount3 on PATH. It speaks libfuse's _FUSE_COMMFD protocol to
//	    its caller (go-fuse, so rclone mount2) and forwards the mount or
//	    unmount to the opener, so rclone runs with no capability and no
//	    /dev/fuse.
//
//	srw-fuse-opener exec --socket S -- PROG ARGS...
//	    The alternative client: it fetches a mounted descriptor, places it at
//	    fd 3 and execs PROG, which serves the magic /dev/fd/3 mountpoint
//	    (rclone mount2 ... /dev/fd/3 --allow-non-empty).
//
//	srw-fuse-opener ping --socket S
//	    The opener's startup probe.
//
//	srw-fuse-opener check --target DIR [--subtype rclone]
//	    The daemon sidecar's startup probe: DIR is a live fuse.<subtype> mount.
//
// See docker/Dockerfile.in-pod-mount and scripts/k3d-in-pod-plane-spike.py.
package main

import (
	"fmt"
	"os"
	"path/filepath"
)

func main() {
	os.Exit(run(os.Args, os.Stderr))
}

func run(args []string, stderr *os.File) int {
	if filepath.Base(args[0]) == "fusermount3" || filepath.Base(args[0]) == "fusermount" {
		return fusermountMain(args[1:], os.Getenv, stderr)
	}
	if len(args) < 2 {
		fmt.Fprintln(stderr, "usage: srw-fuse-opener serve|exec|ping|fusermount3 ...")
		return 2
	}
	switch args[1] {
	case "serve":
		return serveMain(args[2:], stderr)
	case "exec":
		return execMain(args[2:], stderr)
	case "ping":
		return pingMain(args[2:], stderr)
	case "check":
		return checkMain(args[2:], stderr)
	case "fusermount3":
		return fusermountMain(args[2:], os.Getenv, stderr)
	}
	fmt.Fprintf(stderr, "srw-fuse-opener: unknown command %q\n", args[1])
	return 2
}
