package main

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// startMulti serves two targets, the second read-only, under a base policy
// that forces nothing.
func startMulti(t *testing.T) (string, *fakeMounter, *server) {
	t.Helper()
	socket := filepath.Join(socketDir(t), "o.sock")
	fake := &fakeMounter{}
	s := newServer(fake, 65534)
	s.policy.ReadOnly = false
	s.targets = []string{"/srw/cloud/project", "/srw/cloud/lower"}
	s.readOnly = map[string]bool{"/srw/cloud/project": false, "/srw/cloud/lower": true}
	listener, err := listen(socket, os.Getuid())
	if err != nil {
		t.Fatal(err)
	}
	stop := make(chan os.Signal, 1)
	done := make(chan struct{})
	go func() { s.serve(listener, stop); close(done) }()
	t.Cleanup(func() { stop <- os.Interrupt; <-done })
	return socket, fake, s
}

func (f *fakeMounter) targetsTouched() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return strings.Join(f.at, ",")
}

func TestEachTargetKeepsItsOwnReadOnly(t *testing.T) {
	socket, fake, _ := startMulti(t)
	for _, mountpoint := range []string{"/srw/cloud/project", "/srw/cloud/lower/"} {
		fd, err := call(socket, request{Op: "mount", Mountpoint: mountpoint, Options: "rw"}, time.Second)
		if err != nil {
			t.Fatalf("%s: %v", mountpoint, err)
		}
		syscall.Close(fd)
	}
	_, mounted, _, _ := fake.snapshot()
	if mounted[0].Flags&msReadOnly != 0 || mounted[1].Flags&msReadOnly == 0 {
		t.Fatalf("flags %#x %#x: only the second target is read-only", mounted[0].Flags, mounted[1].Flags)
	}
	want := "detach /srw/cloud/project,mount /srw/cloud/project,detach /srw/cloud/lower,mount /srw/cloud/lower"
	if got := fake.targetsTouched(); got != want {
		t.Fatalf("each request must touch only its own target: %s", got)
	}
}

func TestSeveralTargetsNeedTheMountpointNamed(t *testing.T) {
	socket, fake, _ := startMulti(t)
	if _, err := call(socket, request{Op: "mount"}, time.Second); err == nil || !strings.Contains(err.Error(), "name the mountpoint") {
		t.Fatalf("a mount without a mountpoint: %v", err)
	}
	if _, err := call(socket, request{Op: "mount", Mountpoint: "/srw/cloud/other"}, time.Second); err == nil || !strings.Contains(err.Error(), "not one of") {
		t.Fatalf("a mount elsewhere: %v", err)
	}
	if _, err := call(socket, request{Op: "unmount", Mountpoint: "/srw/cloud"}, time.Second); err == nil {
		t.Fatal("unmounted the parent")
	}
	if fake.count("mount")+fake.count("detach") != 0 {
		t.Fatal("a refused request reached the mounter")
	}
}

func TestShutdownDetachesEveryTarget(t *testing.T) {
	_, fake, s := startMulti(t)
	s.shutdown()
	if got := fake.targetsTouched(); got != "detach /srw/cloud/project,detach /srw/cloud/lower" {
		t.Fatalf("shutdown detached %s", got)
	}
}

func TestPrepareCreatesEveryTargetAndDirectory(t *testing.T) {
	root := t.TempDir()
	fake := &fakeMounter{}
	s := newServer(fake, 65534)
	s.targets = []string{filepath.Join(root, "cloud", "a"), filepath.Join(root, "cloud", "b")}
	s.dirs = []string{filepath.Join(root, "cloud", "merged")}
	if detached, err := s.prepare(); err != nil || detached != 0 {
		t.Fatalf("%d detached, %v", detached, err)
	}
	for _, path := range append(append([]string(nil), s.targets...), s.dirs...) {
		if info, err := os.Stat(path); err != nil || !info.IsDir() {
			t.Fatalf("%s: %v", path, err)
		}
	}
	if fake.count("check") != 3 {
		t.Fatalf("every path is checked: %d", fake.count("check"))
	}
	fake.mu.Lock()
	fake.checkErr = errors.New("must be a directory, not a symlink")
	fake.mu.Unlock()
	if _, err := s.prepare(); err == nil {
		t.Fatal("a refused check still prepared")
	}
}

func TestServeReadsRepeatedTargetsAndDirs(t *testing.T) {
	var targets targetFlag
	for _, value := range []string{"/srw/cloud/a", "/srw/cloud/b:ro"} {
		if err := targets.Set(value); err != nil {
			t.Fatal(err)
		}
	}
	if strings.Join(targets.paths, ",") != "/srw/cloud/a,/srw/cloud/b" || targets.readOnly["/srw/cloud/a"] || !targets.readOnly["/srw/cloud/b"] {
		t.Fatalf("targets %+v", targets)
	}
	for _, bad := range []string{"/srw/cloud/a", "relative", "/srw/../etc", "/", "/srw/cloud/c/"} {
		if err := targets.Set(bad); err == nil {
			t.Fatalf("target %q was accepted", bad)
		}
	}
	var dirs dirFlag
	if err := dirs.Set("/srw/cloud/merged"); err != nil {
		t.Fatal(err)
	}
	if err := dirs.Set("relative"); err == nil {
		t.Fatal("a relative dir was accepted")
	}
	var stderr bytes.Buffer
	code := serveMain([]string{"--socket", "/tmp/x.sock", "--client-uid", "65534", "--target", "/srw/cloud/a", "--dir", "/srw/cloud/a"}, &stderr)
	if code != 2 || !strings.Contains(stderr.String(), "both a --target and a --dir") {
		t.Fatalf("exit %d: %s", code, stderr.String())
	}
	stderr.Reset()
	if code := serveMain([]string{"--socket", "/tmp/x.sock", "--client-uid", "65534"}, &stderr); code != 2 {
		t.Fatalf("no target: exit %d", code)
	}
}
