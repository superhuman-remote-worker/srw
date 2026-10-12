package main

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestRunReturnsOutputCodeAndAStoppedTimeout(t *testing.T) {
	r := execRunner{binary: "/bin/sh", log: io.Discard}
	out, code, stderr, timedOut := r.run(context.Background(), []string{"-c", "echo out; echo err >&2; exit 3"}, 5*time.Second)
	if string(out) != "out\n" || code != 3 || stderr != "err\n" || timedOut {
		t.Fatalf("out %q code %d stderr %q timedOut %v", out, code, stderr, timedOut)
	}
	if _, _, _, timedOut := r.run(context.Background(), []string{"-c", "sleep 5"}, 100*time.Millisecond); !timedOut {
		t.Fatal("a slow call did not time out")
	}
}

func TestRcloneGetsNoCredentialFromTheEnvironment(t *testing.T) {
	t.Setenv("RCLONE_WEBDAV_PASS", "must-not-leak")
	r := execRunner{binary: "/bin/sh", log: io.Discard}
	out, _, _, _ := r.run(context.Background(), []string{"-c", "env"}, 5*time.Second)
	if strings.Contains(string(out), "must-not-leak") || !strings.Contains(string(out), "HOME=/tmp") {
		t.Fatalf("environment %q", out)
	}
}

func TestAStartedChildLogsAndKeepsItsTail(t *testing.T) {
	var log strings.Builder
	r := execRunner{binary: "/bin/sh", log: &log}
	c, err := r.start([]string{"-c", "echo first >&2; echo second >&2; exit 4"}, "project")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-c.done():
	case <-time.After(5 * time.Second):
		t.Fatal("the child never exited")
	}
	if c.exitCode() != 4 || c.tail() != "first\nsecond" {
		t.Fatalf("code %d tail %q", c.exitCode(), c.tail())
	}
	if !strings.Contains(log.String(), "project: second") {
		t.Fatalf("log %q", log.String())
	}
	long, err := r.start([]string{"-c", "sleep 30"}, "x")
	if err != nil {
		t.Fatal(err)
	}
	long.signal(syscall.SIGTERM)
	select {
	case <-long.done():
	case <-time.After(5 * time.Second):
		t.Fatal("SIGTERM did not reach the child")
	}
}

func TestTheRemoteControlIsCalledOverItsUnixSocket(t *testing.T) {
	dir, err := os.MkdirTemp("", "rc")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	socket := filepath.Join(dir, "rc.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var params map[string]any
		json.NewDecoder(r.Body).Decode(&params)
		got = append(got, r.URL.Path)
		if r.URL.Path == "/vfs/broken" {
			http.Error(w, `{"error":"no"}`, http.StatusInternalServerError)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"diskCache": map[string]any{"uploadsInProgress": 1, "uploadsQueued": 2}})
	})}
	go server.Serve(listener)
	defer server.Close()
	pending, err := pendingUploads(context.Background(), unixRC{}, socket)
	if err != nil || pending != 3 {
		t.Fatalf("pending %d, %v", pending, err)
	}
	if _, err := (unixRC{}).call(context.Background(), socket, "vfs/broken", nil); err == nil {
		t.Fatal("an HTTP error was not an error")
	}
	if strings.Join(got, ",") != "/vfs/stats,/vfs/broken" {
		t.Fatalf("paths %v", got)
	}
}

func TestTopMountTypeReadsTheStackAtTheTarget(t *testing.T) {
	info := strings.Join([]string{
		"30 25 0:40 / /srw/cloud rw - tmpfs tmpfs rw",
		"31 30 0:41 / /srw/cloud/project rw,nosuid,nodev shared:10 - fuse.rclone srw-cloud rw",
		`33 30 0:43 / /srw/cloud/with\040space rw - fuse.rclone x rw`,
		"34 31 0:44 / /srw/cloud/project rw master:3 - tmpfs other rw",
	}, "\n")
	top, err := topMountType(strings.NewReader(info), "/srw/cloud/project")
	if err != nil || top != "tmpfs" {
		t.Fatalf("top %q, %v: the last entry is the top of the stack", top, err)
	}
	if top, _ := topMountType(strings.NewReader(info), "/srw/cloud/with space"); top != "fuse.rclone" {
		t.Fatalf("an escaped mountpoint: %q", top)
	}
	if top, _ := topMountType(strings.NewReader(info), "/srw/cloud/absent"); top != "" {
		t.Fatalf("nothing mounted: %q", top)
	}
}

func TestStatWithinAnswersForAPlainPath(t *testing.T) {
	if err := statWithin(t.TempDir(), time.Second); err != nil {
		t.Fatal(err)
	}
	if err := statWithin(filepath.Join(t.TempDir(), "absent"), time.Second); err == nil {
		t.Fatal("an absent path answered")
	}
	if err := liveMount(t.TempDir()); err == nil {
		t.Fatal("a plain directory is not a live mount")
	}
}
