package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"
)

// runner starts rclone. Tests replace it.
type runner interface {
	// run executes rclone to completion within timeout and returns its
	// stdout, exit code and stderr (code -1 when it could not start).
	run(ctx context.Context, args []string, timeout time.Duration) (out []byte, code int, stderr string, timedOut bool)
	// start launches a long-running rclone whose stderr lines are logged
	// with prefix.
	start(args []string, prefix string) (child, error)
}

// child is one long-running rclone.
type child interface {
	done() <-chan struct{} // closed when it exited
	exitCode() int
	tail() string // its last stderr lines
	signal(sig syscall.Signal) error
}

type execRunner struct {
	binary string
	log    io.Writer
}

func (r execRunner) run(ctx context.Context, args []string, timeout time.Duration) ([]byte, int, string, bool) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, r.binary, args...)
	cmd.Env = rcloneEnv()
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &limitedWriter{buf: &stdout, max: 1 << 20}
	cmd.Stderr = &limitedWriter{buf: &stderr, max: 64 << 10}
	err := cmd.Run()
	timedOut := errors.Is(ctx.Err(), context.DeadlineExceeded)
	code := 0
	if err != nil {
		code = -1
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			code = exitErr.ExitCode()
		}
	}
	return stdout.Bytes(), code, stderr.String(), timedOut
}

func (r execRunner) start(args []string, prefix string) (child, error) {
	cmd := exec.Command(r.binary, args...)
	cmd.Env = rcloneEnv()
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	cmd.Stdout = nil
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	c := &execChild{cmd: cmd, exited: make(chan struct{})}
	lines := make(chan struct{})
	go func() {
		defer close(lines)
		scanner := bufio.NewScanner(stderr)
		scanner.Buffer(make([]byte, 64<<10), 64<<10)
		for scanner.Scan() {
			line := scanner.Text()
			c.remember(line)
			fmt.Fprintf(r.log, "%s: %s\n", prefix, line)
		}
	}()
	go func() {
		<-lines
		err := cmd.Wait()
		code := 0
		if err != nil {
			code = -1
			var exitErr *exec.ExitError
			if errors.As(err, &exitErr) {
				code = exitErr.ExitCode()
			}
		}
		c.mu.Lock()
		c.code = code
		c.mu.Unlock()
		close(c.exited)
	}()
	return c, nil
}

// rcloneEnv is the whole environment rclone gets: no credential, and a
// HOME in the sidecar's writable /tmp.
func rcloneEnv() []string {
	return []string{"HOME=/tmp", "PATH=/usr/local/bin:/usr/bin:/bin"}
}

type execChild struct {
	cmd    *exec.Cmd
	exited chan struct{}
	mu     sync.Mutex
	code   int
	lines  []string
}

func (c *execChild) done() <-chan struct{} { return c.exited }

func (c *execChild) exitCode() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.code
}

func (c *execChild) remember(line string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lines = append(c.lines, line)
	if len(c.lines) > 20 {
		c.lines = c.lines[len(c.lines)-20:]
	}
}

func (c *execChild) tail() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return strings.Join(c.lines, "\n")
}

func (c *execChild) signal(sig syscall.Signal) error {
	if c.cmd.Process == nil {
		return os.ErrProcessDone
	}
	return c.cmd.Process.Signal(sig)
}

// limitedWriter keeps the first max bytes and drops the rest.
type limitedWriter struct {
	buf *bytes.Buffer
	max int
}

func (w *limitedWriter) Write(p []byte) (int, error) {
	if room := w.max - w.buf.Len(); room > 0 {
		if len(p) > room {
			w.buf.Write(p[:room])
		} else {
			w.buf.Write(p)
		}
	}
	return len(p), nil
}
