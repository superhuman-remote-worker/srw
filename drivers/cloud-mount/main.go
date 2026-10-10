// Command srw-cloud-mount runs a workspace Pod's cloud mounts in the
// unprivileged rclone sidecar (connector drivers, "Three planes", In-pod;
// slice D7).
//
//	srw-cloud-mount --plan P --config C --status-dir S [--control-dir K]
//	                --cache-dir D --run-dir R [--rclone rclone]
//
// It reads the plan P (the non-secret half, from the Pod's ConfigMap) and
// keeps each mount up with one rclone mount2 child, which asks the
// privileged srw-fuse-opener to mount through its fusermount3 client. The
// credentials stay in the rclone config C (from the Pod's Secret, never the
// environment). For each mount it:
//
//   - tests the remote before mounting (rclone builds a WebDAV remote
//     without contacting it, so a wrong password would mount and fail every
//     read) and reports a failure as one closed reason: credential_rejected,
//     not_found, unreachable, timeout, mount_failed or config_missing;
//   - retries a failed or lost mount with backoff, and kills an rclone whose
//     mount stops answering;
//   - writes S/<index>.json, which the workspace reads.
//
// It never stops the workspace: it has no probe the kubelet waits on, and a
// mount that does not come up is only reported. Each rclone's remote
// control listens on a unix socket in R, which the workspace cannot reach.
// The workspace may drop "drain" or "refresh" (holding a nonce) into K to
// ask for a flush of pending uploads or a directory refresh; the answer
// lands in every status file. On SIGTERM, which the kubelet sends after the
// workspace has stopped, it drains for up to the plan's drain_seconds, stops
// every rclone (each unmounts through the opener, which stops last), and
// writes what could not be flushed to the termination message.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stderr))
}

func run(args []string, stderr io.Writer) int {
	flags := flag.NewFlagSet("srw-cloud-mount", flag.ContinueOnError)
	flags.SetOutput(stderr)
	planPath := flags.String("plan", "", "the mount plan (JSON)")
	configPath := flags.String("config", "", "the rclone config holding the credentials")
	statusDir := flags.String("status-dir", "", "where each mount's status file goes")
	controlDir := flags.String("control-dir", "", "where the workspace drops drain and refresh requests")
	cacheDir := flags.String("cache-dir", "", "rclone's VFS cache, a directory per mount")
	runDir := flags.String("run-dir", "", "private directory for sockets and filters")
	binary := flags.String("rclone", "rclone", "the rclone binary")
	terminationLog := flags.String("termination-log", "/dev/termination-log", "where an incomplete flush is reported at stop")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *planPath == "" || *configPath == "" || *statusDir == "" || *cacheDir == "" || *runDir == "" {
		fmt.Fprintln(stderr, "srw-cloud-mount needs --plan, --config, --status-dir, --cache-dir and --run-dir")
		return 2
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)

	plan := waitForPlan(*planPath, signals, stderr)
	if plan == nil {
		return 0
	}
	if err := os.MkdirAll(*runDir, 0o700); err != nil {
		fmt.Fprintf(stderr, "srw-cloud-mount: %v\n", err)
	}
	board := newStatusBoard(*statusDir, plan, time.Now)
	board.publishAll()
	s := newSupervisor(plan, board, execRunner{binary: *binary, log: stderr}, unixRC{}, stderr)
	s.configPath, s.cacheDir, s.runDir, s.controlDir = *configPath, *cacheDir, *runDir, *controlDir

	ctx, cancel := context.WithCancel(context.Background())
	var workers sync.WaitGroup
	for _, m := range plan.Mounts {
		workers.Add(1)
		go func(m Mount) {
			defer workers.Done()
			s.worker(ctx, m)
		}(m)
	}
	controlCtx, stopControl := context.WithCancel(context.Background())
	controlDone := make(chan struct{})
	go func() { s.control(controlCtx); close(controlDone) }()

	<-signals
	s.logf("stopping: draining for up to %ds", plan.DrainSeconds)
	stopControl()
	// A request being answered ends with its context; never let a stuck one
	// eat the grace period the shutdown drain needs.
	select {
	case <-controlDone:
	case <-time.After(controlStopWait):
		s.logf("the control loop did not stop in %s; going on", controlStopWait)
	}
	pending, complete := s.shutdown(cancel, &workers)
	if complete {
		s.logf("stopped: every upload flushed")
		return 0
	}
	message := fmt.Sprintf("flush incomplete: %d upload(s) pending", pending)
	if pending < 0 {
		message = "flush incomplete: rclone did not answer"
	}
	s.logf("stopped: %s", message)
	_ = os.WriteFile(*terminationLog, []byte(message+"\n"), 0o644)
	return 0
}

// controlStopWait bounds the wait for the control loop at shutdown.
const controlStopWait = 5 * time.Second

// waitForPlan reads the plan. The ConfigMap volume is not optional, so a
// missing or broken plan is one the kubelet has not finished writing, or a
// bad one: it is logged and waited out, never a crash loop; nil means a stop
// signal came first.
func waitForPlan(path string, signals <-chan os.Signal, stderr io.Writer) *Plan {
	logged := ""
	for {
		plan, err := loadPlan(path)
		if err == nil {
			return plan
		}
		if err.Error() != logged {
			fmt.Fprintf(stderr, "srw-cloud-mount: no usable plan yet: %v\n", err)
			logged = err.Error()
		}
		select {
		case <-signals:
			return nil
		case <-time.After(10 * time.Second):
		}
	}
}
