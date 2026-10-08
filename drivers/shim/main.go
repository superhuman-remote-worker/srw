// Command srw-driver-shim is SRW's static helper inside connector driver pods
// (connector drivers D5, "The driver contract": image driver transport).
//
//	srw-driver-shim install DIR          copy itself into DIR (an emptyDir)
//	srw-driver-shim canary-wait FLAGS    block until the pod's network policy is enforced
//	srw-driver-shim serve -- PROGRAM...  exec a service driver with SRW's request and identity
//	srw-driver-shim run -- PROGRAM...    run a bind-time driver and post its typed JSON lines
//
// An init container copies the shim into the pod, and it becomes the driver
// container's command with the image's own entrypoint as its arguments, so
// stock images need no rebuild. It is static (CGO off), uses only the Go
// standard library, and never reads stdin.
package main

import (
	"fmt"
	"os"
)

const usage = `usage:
  srw-driver-shim install DIR
  srw-driver-shim canary-wait --deny HOST:PORT --allow HOST:PORT [--expect HOST:PORT]
                              [--consecutive N] [--interval D] [--timeout D]
  srw-driver-shim serve -- PROGRAM [ARGS...]
  srw-driver-shim run -- PROGRAM [ARGS...]
`

// Exit codes: 0 success, 2 usage or setup errors, 70 a protocol or delivery
// failure (EX_SOFTWARE); a bind-time driver's own exit code otherwise.
const (
	exitUsage    = 2
	exitSoftware = 70
)

func main() {
	os.Exit(dispatch(os.Args[1:]))
}

func dispatch(args []string) int {
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, usage)
		return exitUsage
	}
	logf := func(format string, a ...any) {
		fmt.Fprintf(os.Stderr, "srw-driver-shim: "+format+"\n", a...)
	}
	switch args[0] {
	case "install":
		if len(args) != 2 {
			fmt.Fprint(os.Stderr, usage)
			return exitUsage
		}
		if err := install(args[1]); err != nil {
			logf("install: %v", err)
			return exitUsage
		}
		return 0
	case "canary-wait":
		cfg, err := parseCanaryFlags(args[1:])
		if err != nil {
			logf("canary-wait: %v", err)
			return exitUsage
		}
		if err := canaryWait(cfg, systemProbe(cfg.dialTimeout), systemClock{}, logf); err != nil {
			logf("canary-wait: %v", err)
			return 1
		}
		return 0
	case "serve":
		program, err := afterDashes(args[1:])
		if err != nil {
			logf("serve: %v", err)
			return exitUsage
		}
		if err := serve(program, os.Getenv, systemExec); err != nil {
			logf("serve: %v", err)
			return exitUsage
		}
		return 0 // unreachable: exec replaced the process
	case "run":
		program, err := afterDashes(args[1:])
		if err != nil {
			logf("run: %v", err)
			return exitUsage
		}
		return run(program, os.Getenv, logf)
	case "version", "--version":
		fmt.Println(version)
		return 0
	}
	fmt.Fprint(os.Stderr, usage)
	return exitUsage
}

// version is set at build time (-ldflags "-X main.version=...").
var version = "dev"

func afterDashes(args []string) ([]string, error) {
	if len(args) < 2 || args[0] != "--" {
		return nil, fmt.Errorf("expected -- PROGRAM [ARGS...]")
	}
	return args[1:], nil
}
