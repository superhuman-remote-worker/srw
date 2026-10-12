package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// defaultSocket is where the client looks without SRW_FUSE_OPENER_SOCKET.
// go-fuse runs fusermount3 with _FUSE_COMMFD as its only variable, so the
// mount path cannot rely on the environment.
const defaultSocket = "/run/srw-fuse/opener.sock"

const clientTimeout = 15 * time.Second

func socketFrom(getenv func(string) string) string {
	if path := getenv("SRW_FUSE_OPENER_SOCKET"); path != "" {
		return path
	}
	return defaultSocket
}

type fusermountArgs struct {
	unmount    bool
	quiet      bool
	options    string
	mountpoint string
}

// parseFusermount reads the subset of fusermount3's command line that
// libfuse and go-fuse use: [-u] [-z] [-q] [-o OPTS] [--] MOUNTPOINT.
func parseFusermount(args []string) (fusermountArgs, error) {
	var parsed fusermountArgs
	var options []string
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "--":
			if i+1 < len(args) {
				parsed.mountpoint = args[i+1]
			}
			i = len(args)
		case arg == "-u":
			parsed.unmount = true
		case arg == "-z":
			// Lazy unmount: the opener always detaches lazily.
		case arg == "-q":
			parsed.quiet = true
		case arg == "-o":
			if i+1 >= len(args) {
				return parsed, errors.New("-o needs a value")
			}
			i++
			options = append(options, args[i])
		case strings.HasPrefix(arg, "-o"):
			options = append(options, arg[2:])
		case strings.HasPrefix(arg, "-"):
			return parsed, fmt.Errorf("unsupported option %q", arg)
		default:
			parsed.mountpoint = arg
		}
	}
	if parsed.mountpoint == "" {
		return parsed, errors.New("no mountpoint")
	}
	parsed.options = strings.Join(options, ",")
	return parsed, nil
}

// fusermountMain stands in for fusermount3: the opener mounts, and the
// descriptor goes back to the FUSE library over _FUSE_COMMFD.
func fusermountMain(args []string, getenv func(string) string, stderr io.Writer) int {
	parsed, err := parseFusermount(args)
	if err != nil {
		fmt.Fprintf(stderr, "fusermount3 (srw): %v\n", err)
		return 1
	}
	mountpoint, err := filepath.Abs(parsed.mountpoint)
	if err != nil {
		fmt.Fprintf(stderr, "fusermount3 (srw): %v\n", err)
		return 1
	}
	socket := socketFrom(getenv)
	if parsed.unmount {
		if _, err := call(socket, request{Op: "unmount", Mountpoint: mountpoint}, clientTimeout); err != nil {
			if !parsed.quiet {
				fmt.Fprintf(stderr, "fusermount3 (srw): %v\n", err)
			}
			return 1
		}
		return 0
	}
	commfd, err := strconv.Atoi(getenv("_FUSE_COMMFD"))
	if err != nil || commfd < 0 {
		fmt.Fprintln(stderr, "fusermount3 (srw): _FUSE_COMMFD is not set; only a FUSE library may call this")
		return 1
	}
	fd, err := call(socket, request{Op: "mount", Mountpoint: mountpoint, Options: parsed.options}, clientTimeout)
	if err != nil {
		fmt.Fprintf(stderr, "fusermount3 (srw): %v\n", err)
		return 1
	}
	defer syscall.Close(fd)
	// libfuse's send_fd: one zero byte with the descriptor as SCM_RIGHTS.
	if err := syscall.Sendmsg(commfd, []byte{0}, syscall.UnixRights(fd), nil, 0); err != nil {
		fmt.Fprintf(stderr, "fusermount3 (srw): send the descriptor: %v\n", err)
		return 1
	}
	return 0
}

// execMain fetches a mounted descriptor, puts it at fd 3 and execs the
// daemon, which serves the magic /dev/fd/3 mountpoint.
func execMain(args []string, stderr io.Writer) int {
	flags := flag.NewFlagSet("exec", flag.ContinueOnError)
	flags.SetOutput(stderr)
	socket := flags.String("socket", socketFrom(os.Getenv), "the opener's socket")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	argv := flags.Args()
	if len(argv) == 0 {
		fmt.Fprintln(stderr, "exec needs a program after --")
		return 2
	}
	program, err := exec.LookPath(argv[0])
	if err != nil {
		fmt.Fprintf(stderr, "exec: %v\n", err)
		return 1
	}
	fd, err := call(*socket, request{Op: "mount"}, clientTimeout)
	if err != nil {
		fmt.Fprintf(stderr, "exec: %v\n", err)
		return 1
	}
	if err := placeAt(fd, 3); err != nil {
		fmt.Fprintf(stderr, "exec: place the descriptor: %v\n", err)
		return 1
	}
	err = syscall.Exec(program, argv, os.Environ())
	fmt.Fprintf(stderr, "exec: %v\n", err)
	return 1
}

// parseMountinfo lists the filesystem types mounted at target, in
// /proc/self/mountinfo order (bottom first).
func parseMountinfo(scanner *bufio.Scanner, target string) ([]string, error) {
	var types []string
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 7 || unescapeMountinfo(fields[4]) != target {
			continue
		}
		for i := 6; i < len(fields)-1; i++ {
			if fields[i] == "-" {
				types = append(types, fields[i+1])
				break
			}
		}
	}
	return types, scanner.Err()
}

// unescapeMountinfo undoes the kernel's octal escapes (\040 for a space).
func unescapeMountinfo(field string) string {
	if !strings.Contains(field, `\`) {
		return field
	}
	var out strings.Builder
	for i := 0; i < len(field); i++ {
		if field[i] == '\\' && i+4 <= len(field) {
			if n, err := strconv.ParseUint(field[i+1:i+4], 8, 8); err == nil {
				out.WriteByte(byte(n))
				i += 3
				continue
			}
		}
		out.WriteByte(field[i])
	}
	return out.String()
}
