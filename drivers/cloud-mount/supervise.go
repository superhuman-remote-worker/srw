package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// supervisor keeps every mount of a plan up, reports each one's state, and
// drains uploads when asked and when it stops.
type supervisor struct {
	plan       *Plan
	configPath string
	cacheDir   string
	runDir     string
	controlDir string
	board      *statusBoard
	runner     runner
	rc         rcClient
	mounted    func(target string) error
	log        io.Writer

	backoffMin   time.Duration // after a failure, doubling up to backoffMax
	backoffMax   time.Duration
	configWait   time.Duration // how long the credential file may take to appear
	mountWait    time.Duration // how long a started rclone may take to mount
	healthEvery  time.Duration
	controlEvery time.Duration
	drainEvery   time.Duration
	stopWait     time.Duration // between SIGTERM and SIGKILL at stop

	mu       sync.Mutex
	children map[int]child
	lost     int // under mu: uploads the last drain gave up for good
}

func newSupervisor(plan *Plan, board *statusBoard, r runner, rc rcClient, log io.Writer) *supervisor {
	return &supervisor{
		plan:         plan,
		board:        board,
		runner:       r,
		rc:           rc,
		mounted:      liveMount,
		log:          log,
		backoffMin:   2 * time.Second,
		backoffMax:   5 * time.Minute,
		configWait:   time.Minute,
		mountWait:    30 * time.Second,
		healthEvery:  30 * time.Second,
		controlEvery: time.Second,
		drainEvery:   500 * time.Millisecond,
		stopWait:     10 * time.Second,
		children:     map[int]child{},
	}
}

func (s *supervisor) logf(format string, args ...any) {
	fmt.Fprintf(s.log, "srw-cloud-mount: "+format+"\n", args...)
}

func (s *supervisor) socket(m Mount) string {
	return filepath.Join(s.runDir, "rc-"+strconv.Itoa(m.Index)+".sock")
}

func (s *supervisor) cacheOf(m Mount) string {
	return filepath.Join(s.cacheDir, strconv.Itoa(m.Index))
}

// permanentReasons are the ones a retry cannot cure: the folder is gone,
// the credential is refused, or it never reached the supervisor (its Secret
// is immutable). Uploads waiting behind them are lost, not pending.
var permanentReasons = map[string]bool{
	reasonNotFound:           true,
	reasonCredentialRejected: true,
	reasonConfigMissing:      true,
}

// maxMetaBytes bounds what is read of one VFS metadata file.
const maxMetaBytes = 64 << 10

// dirtyEntries counts the files a mount's VFS cache still has to upload.
// rclone keeps, per cached file, a JSON metadata file under vfsMeta/ whose
// "Dirty" is true until the upload succeeded; the cache outlives rclone, so
// this answers whether or not a rclone runs.
func dirtyEntries(cacheDir string) (int, error) {
	root := filepath.Join(cacheDir, "vfsMeta")
	count := 0
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if path == root && errors.Is(err, fs.ErrNotExist) {
				return fs.SkipDir
			}
			return err
		}
		if !d.Type().IsRegular() {
			return nil
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		raw, err := io.ReadAll(io.LimitReader(f, maxMetaBytes))
		f.Close()
		if err != nil {
			return err
		}
		var meta struct {
			Dirty bool `json:"Dirty"`
		}
		if json.Unmarshal(raw, &meta) == nil && meta.Dirty {
			count++
		}
		return nil
	})
	return count, err
}

// worker keeps one mount up until ctx ends. A mount that failed stays
// unavailable with its last reason while it is retried, so a reader never
// sees it flip back to pending between attempts; it is pending only before
// its first attempt and after a mount that came up was lost.
func (s *supervisor) worker(ctx context.Context, m Mount) {
	backoff := s.backoffMin
	lastReason := ""
	for ctx.Err() == nil {
		if lastReason == "" {
			s.board.set(m.Index, statePending, "")
		} else {
			s.board.retry(m.Index)
		}
		reason := s.attempt(ctx, m)
		lastReason = reason
		if ctx.Err() != nil {
			return
		}
		if reason == "" {
			// It mounted and later went away: try again soon.
			backoff = s.backoffMin
		} else {
			s.logf("%s unavailable: %s; retrying in %s", m.Name, reason, backoff)
			s.board.set(m.Index, stateUnavailable, reason)
		}
		if !sleep(ctx, backoff) {
			return
		}
		if reason != "" {
			backoff = min(backoff*2, s.backoffMax)
		}
	}
}

// attempt mounts once and watches the mount until it fails. It returns ""
// when the mount came up (and later went away), else why it did not.
func (s *supervisor) attempt(ctx context.Context, m Mount) string {
	if !s.configReady(ctx) {
		return reasonConfigMissing
	}
	_, code, stderr, timedOut := s.runner.run(ctx, preflightArgs(s.configPath, m), 40*time.Second)
	if code != 0 || timedOut {
		return classify(code, stderr, timedOut)
	}
	filter := s.writeFilter(ctx, m)
	cache := filepath.Join(s.cacheDir, strconv.Itoa(m.Index))
	if err := os.MkdirAll(cache, 0o700); err != nil {
		s.logf("%s: cache directory: %v", m.Name, err)
		return reasonMountFailed
	}
	socket := s.socket(m)
	_ = os.Remove(socket)
	c, err := s.runner.start(mountArgs(s.plan, m, s.configPath, cache, socket, filter), m.Name)
	if err != nil {
		s.logf("%s: start rclone: %v", m.Name, err)
		return reasonMountFailed
	}
	s.track(m.Index, c)
	defer s.untrack(m.Index, c)
	if reason := s.awaitMounted(ctx, m, c); reason != "" {
		s.stopChild(c)
		return reason
	}
	s.board.set(m.Index, stateMounted, "")
	s.logf("%s mounted at %s", m.Name, m.Target)
	if s.watch(ctx, m, c) {
		// Unhealthy but still running (hung): kill it so the opener can
		// detach the dead mount when the next rclone asks to mount.
		s.stopChild(c)
	}
	return ""
}

// configReady waits for the credential file. The Pod's Secret volume is
// not optional (the kubelet starts the container only once it exists), so
// this covers a file the kubelet has not finished writing.
func (s *supervisor) configReady(ctx context.Context) bool {
	deadline := time.Now().Add(s.configWait)
	for {
		if info, err := os.Stat(s.configPath); err == nil && info.Size() > 0 {
			return true
		}
		if !time.Now().Before(deadline) || !sleep(ctx, time.Second) {
			return false
		}
	}
}

// writeFilter compiles the default and the folder's own .cloudignore into
// an rclone exclude file; "" when there is nothing to exclude.
func (s *supervisor) writeFilter(ctx context.Context, m Mount) string {
	var folder []string
	if m.Cloudignore {
		out, code, _, timedOut := s.runner.run(ctx, cloudignoreArgs(s.configPath, m), 20*time.Second)
		if code == 0 && !timedOut {
			folder = strings.Split(string(out), "\n")
		}
	}
	rules := filterRules(m.Ignore, folder)
	if len(rules) == 0 {
		return ""
	}
	path := filepath.Join(s.runDir, "filter-"+strconv.Itoa(m.Index)+".txt")
	if err := os.WriteFile(path, []byte(strings.Join(rules, "\n")+"\n"), 0o600); err != nil {
		s.logf("%s: filter: %v", m.Name, err)
		return ""
	}
	return path
}

var openerRefusedRE = regexp.MustCompile(`opener refused|fusermount3 \(srw\)|failed to mount FUSE`)

// awaitMounted waits for the mount to answer; "" once it does.
func (s *supervisor) awaitMounted(ctx context.Context, m Mount, c child) string {
	deadline := time.Now().Add(s.mountWait)
	for {
		if s.mounted(m.Target) == nil {
			return ""
		}
		select {
		case <-c.done():
			tail := c.tail()
			if openerRefusedRE.MatchString(tail) {
				return reasonMountFailed
			}
			reason := classify(c.exitCode(), tail, false)
			if reason == reasonUnreachable && !strings.Contains(tail, "dial tcp") {
				return reasonMountFailed
			}
			return reason
		case <-ctx.Done():
			return reasonMountFailed
		case <-time.After(250 * time.Millisecond):
		}
		if !time.Now().Before(deadline) {
			return reasonTimeout
		}
	}
}

// watch returns when the mount stops answering (true: rclone may still run)
// or rclone exits or ctx ends (false).
func (s *supervisor) watch(ctx context.Context, m Mount, c child) bool {
	ticker := time.NewTicker(s.healthEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-c.done():
			s.logf("%s: rclone exited (code %d)", m.Name, c.exitCode())
			return false
		case <-ticker.C:
			if err := s.mounted(m.Target); err != nil {
				s.logf("%s: mount stopped answering: %v", m.Name, err)
				return true
			}
		}
	}
}

func (s *supervisor) track(index int, c child) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.children[index] = c
}

func (s *supervisor) untrack(index int, c child) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.children[index] == c {
		delete(s.children, index)
	}
}

func (s *supervisor) snapshotChildren() map[int]child {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[int]child, len(s.children))
	for index, c := range s.children {
		out[index] = c
	}
	return out
}

// stopChild asks rclone to unmount and exit, and kills it if it does not.
func (s *supervisor) stopChild(c child) {
	_ = c.signal(syscall.SIGTERM)
	select {
	case <-c.done():
		return
	case <-time.After(s.stopWait):
	}
	_ = c.signal(syscall.SIGKILL)
	select {
	case <-c.done():
	case <-time.After(2 * time.Second):
	}
}

var nonceRE = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

// maxControlBytes bounds what is read of a control request: a nonce.
const maxControlBytes = 128

var errNotARequest = errors.New("not a regular request file")

// readControl reads one request the workspace dropped. The workspace owns
// the directory, so the file may be anything: a link is never followed
// (O_NOFOLLOW), a FIFO never blocks the loop (O_NONBLOCK), and only a small
// regular file is read.
func readControl(path string) (string, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
	if err != nil {
		return "", err
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() || info.Size() > maxControlBytes {
		return "", errNotARequest
	}
	buf := make([]byte, maxControlBytes+1)
	n, err := io.ReadFull(f, buf)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		return "", err
	}
	if n > maxControlBytes {
		return "", errNotARequest
	}
	return string(buf[:n]), nil
}

// control answers requests the workspace drops into the control directory:
// a file "drain" or "refresh" holding a nonce. The workspace can only ask
// for a flush or a directory refresh, both of which rclone does by itself
// anyway; each nonce is answered once, in every mount's status.
func (s *supervisor) control(ctx context.Context) {
	if s.controlDir == "" {
		return
	}
	answered := map[string]string{}
	ticker := time.NewTicker(s.controlEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		for _, command := range []string{"drain", "refresh"} {
			raw, err := readControl(filepath.Join(s.controlDir, command))
			if err != nil {
				continue
			}
			nonce := strings.TrimSpace(raw)
			if !nonceRE.MatchString(nonce) || answered[command] == nonce {
				continue
			}
			answered[command] = nonce
			if command == "drain" {
				s.drainAll(ctx, nonce, time.Now().Add(time.Duration(s.plan.DrainSeconds)*time.Second))
			} else {
				s.refreshAll(ctx, nonce)
			}
		}
	}
}

// drainAll flushes every writable mount; it returns how many uploads are
// still pending (-1 if a mount could not say) and acknowledges nonce when
// one is given.
//
// An unavailable writable mount is drained only if its cache holds nothing
// to upload. With uploads waiting and a reason a retry may cure
// (unreachable, timeout, a failed mount), it is incomplete with their count,
// so End refuses and is retried. With a permanent reason (see
// permanentReasons) it is drained, and the uploads it gives up are counted
// as lost in its acknowledgement and logged: retrying cannot help.
func (s *supervisor) drainAll(ctx context.Context, nonce string, deadline time.Time) (int, bool) {
	total, complete, lost := 0, true, 0
	for _, m := range s.plan.Mounts {
		status := s.board.get(m.Index)
		ack := Ack{Nonce: nonce, State: "drained"}
		switch {
		case m.ReadOnly:
			// Nothing is written through it.
		case status.State == stateUnavailable:
			dirty, err := dirtyEntries(s.cacheOf(m))
			switch {
			case err != nil:
				s.logf("%s: reading its cache: %v", m.Name, err)
				ack.State, ack.Pending = "incomplete", -1
			case dirty == 0:
			case permanentReasons[status.Reason]:
				ack.Lost = dirty
				lost += dirty
				s.logf("%s: %d upload(s) lost: the folder is %s", m.Name, dirty, status.Reason)
			default:
				ack.State, ack.Pending = "incomplete", dirty
			}
		case status.State == statePending:
			ack.State, ack.Pending = "incomplete", -1
		default:
			if nonce != "" {
				s.board.ack(m.Index, false, Ack{Nonce: nonce, State: "draining"})
			}
			if pending := drainMount(ctx, s.rc, s.socket(m), deadline, s.drainEvery); pending != 0 {
				ack.State, ack.Pending = "incomplete", pending
			}
		}
		if ack.State != "drained" {
			complete = false
			if ack.Pending > 0 {
				total += ack.Pending
			}
		}
		if nonce != "" {
			s.board.ack(m.Index, false, ack)
		}
	}
	s.mu.Lock()
	s.lost = lost
	s.mu.Unlock()
	if !complete && total == 0 {
		return -1, false
	}
	return total, complete
}

// lostUploads is how many uploads the last drain gave up for good.
func (s *supervisor) lostUploads() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lost
}

// refreshAll re-reads every mounted folder, for a view that must show what
// was just written to the cloud from elsewhere (an applied review).
func (s *supervisor) refreshAll(ctx context.Context, nonce string) {
	for _, m := range s.plan.Mounts {
		ack := Ack{Nonce: nonce, State: "failed"}
		if s.board.get(m.Index).State == stateMounted {
			callCtx, cancel := context.WithTimeout(ctx, time.Minute)
			if _, err := s.rc.call(callCtx, s.socket(m), "vfs/refresh", map[string]any{"recursive": "true"}); err == nil {
				ack.State = "done"
			} else {
				s.logf("%s: refresh: %v", m.Name, err)
			}
			cancel()
		}
		s.board.ack(m.Index, true, ack)
	}
}

// shutdown drains, then stops every rclone (each unmounts through the
// opener) and returns what could not be flushed.
func (s *supervisor) shutdown(cancelWorkers context.CancelFunc, workers *sync.WaitGroup) (int, bool) {
	deadline := time.Now().Add(time.Duration(s.plan.DrainSeconds) * time.Second)
	pending, complete := s.drainAll(context.Background(), "", deadline)
	children := s.snapshotChildren()
	cancelWorkers()
	var stopping sync.WaitGroup
	for _, c := range children {
		stopping.Add(1)
		go func(c child) {
			defer stopping.Done()
			s.stopChild(c)
		}(c)
	}
	stopping.Wait()
	finished := make(chan struct{})
	go func() { workers.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
	}
	return pending, complete
}

func sleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}
