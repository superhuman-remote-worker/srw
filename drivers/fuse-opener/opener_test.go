package main

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestPlanMountForcesNosuidNodevAndThePolicysReadOnly(t *testing.T) {
	spec, err := planMount(policy{ReadOnly: true, Source: "srw-cloud", Subtype: "rclone"}, "rw,allow_other")
	if err != nil {
		t.Fatal(err)
	}
	if spec.Flags != msNoSuid|msNoDev|msReadOnly {
		t.Fatalf("flags %#x: rw must not lift a forced ro", spec.Flags)
	}
	if spec.FSType != "fuse.rclone" || spec.Source != "srw-cloud" {
		t.Fatalf("type %q source %q", spec.FSType, spec.Source)
	}
}

func TestPlanMountHonoursOnlyWhatTakesPrivilegeAway(t *testing.T) {
	spec, err := planMount(policy{Subtype: "rclone"}, "ro,noexec,default_permissions,max_read=1048576,fsname=evil,subtype=ext4")
	if err != nil {
		t.Fatal(err)
	}
	if spec.Flags != msNoSuid|msNoDev|msReadOnly|msNoExec || spec.MaxRead != 1048576 {
		t.Fatalf("spec %+v", spec)
	}
	if spec.FSType != "fuse.rclone" {
		t.Fatalf("a client chose the type: %q", spec.FSType)
	}
	if spec, err := planMount(policy{Subtype: "rclone"}, "max_read=4294967295"); err != nil || spec.MaxRead != 4294967295 {
		t.Fatalf("the largest max_read: %+v %v", spec, err)
	}
	for _, refused := range []string{
		"suid", "dev", "blksize=4096", "max_read=0", "max_read=x", "max_read=-1",
		"max_read=4294967296", "max_read=99999999999999", "user_id=0", "fd=3", "rootmode=100000",
	} {
		if _, err := planMount(policy{Subtype: "rclone"}, refused); err == nil {
			t.Fatalf("option %q was accepted", refused)
		}
	}
}

func TestMountDataNamesTheDescriptorRootTypeAndDaemon(t *testing.T) {
	p := policy{AllowOther: true, Subtype: "rclone"}
	spec, _ := planMount(p, "max_read=4096")
	got := spec.data(p, 7, 0o40755, 65534, 65533)
	want := "fd=7,rootmode=40000,user_id=65534,group_id=65533,allow_other,default_permissions,max_read=4096"
	if got != want {
		t.Fatalf("data %q, want %q", got, want)
	}
	got = (mountSpec{}).data(policy{}, 3, 0o40000, 1, 1)
	if strings.Contains(got, "allow_other") || !strings.Contains(got, "default_permissions") {
		t.Fatalf("allow_other without the policy, or no default_permissions: %q", got)
	}
}

func TestParseFusermountReadsLibfuseAndGoFuseCommandLines(t *testing.T) {
	cases := []struct {
		args []string
		want fusermountArgs
	}{
		{[]string{"/srw/cloud/root", "-o", "allow_other,ro"}, fusermountArgs{mountpoint: "/srw/cloud/root", options: "allow_other,ro"}},
		{[]string{"-o", "ro", "-o", "max_read=1", "--", "/m"}, fusermountArgs{mountpoint: "/m", options: "ro,max_read=1"}},
		{[]string{"-oro", "/m"}, fusermountArgs{mountpoint: "/m", options: "ro"}},
		{[]string{"-u", "-q", "-z", "--", "/m"}, fusermountArgs{mountpoint: "/m", unmount: true, quiet: true}},
	}
	for _, c := range cases {
		got, err := parseFusermount(c.args)
		if err != nil || got != c.want {
			t.Fatalf("%v: got %+v, %v; want %+v", c.args, got, err, c.want)
		}
	}
	for _, bad := range [][]string{{}, {"-o"}, {"--auto-unmount", "/m"}, {"-u"}} {
		if _, err := parseFusermount(bad); err == nil {
			t.Fatalf("%v was accepted", bad)
		}
	}
}

func TestParseMountinfoListsTheStackAtTheTarget(t *testing.T) {
	info := strings.Join([]string{
		"30 25 0:40 / /srw/cloud rw,relatime shared:9 - tmpfs tmpfs rw",
		"31 30 0:41 / /srw/cloud/root ro,nosuid,nodev shared:10 - fuse.rclone srw-cloud ro,user_id=65534",
		"32 31 0:42 / /srw/cloud/root ro,nosuid,nodev shared:11 master:3 - fuse.rclone srw-cloud ro",
		`33 30 0:43 / /srw/cloud/with\040space rw - fuse.rclone x rw`,
		"34 30 0:44 / /srw/cloud/rootless rw - fuse.rclone x rw",
	}, "\n")
	got, err := parseMountinfo(bufio.NewScanner(strings.NewReader(info)), "/srw/cloud/root")
	if err != nil || strings.Join(got, ",") != "fuse.rclone,fuse.rclone" {
		t.Fatalf("got %v, %v", got, err)
	}
	got, _ = parseMountinfo(bufio.NewScanner(strings.NewReader(info)), "/srw/cloud/with space")
	if len(got) != 1 {
		t.Fatalf("an escaped mountpoint was not matched: %v", got)
	}
}

// pair returns two connected unix stream sockets.
func pair(t *testing.T) (*net.UnixConn, *net.UnixConn) {
	t.Helper()
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	conn := func(fd int) *net.UnixConn {
		file := os.NewFile(uintptr(fd), "pair")
		defer file.Close()
		c, err := net.FileConn(file)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		return c.(*net.UnixConn)
	}
	return conn(fds[0]), conn(fds[1])
}

func TestAMessageCarriesOneDescriptor(t *testing.T) {
	left, right := pair(t)
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if err := writeMessage(left, response{OK: true}, int(writer.Fd())); err != nil {
		t.Fatal(err)
	}
	writer.Close()
	var resp response
	fd, err := readMessage(right, &resp)
	if err != nil || !resp.OK || fd < 0 {
		t.Fatalf("resp %+v fd %d err %v", resp, fd, err)
	}
	received := os.NewFile(uintptr(fd), "received")
	received.WriteString("through the passed descriptor")
	received.Close()
	got, _ := io.ReadAll(reader)
	if string(got) != "through the passed descriptor" {
		t.Fatalf("read %q", got)
	}
}

// fakeMounter stands in for the kernel. The server calls it from its
// handler goroutines, so every field is behind mu.
type fakeMounter struct {
	mu       sync.Mutex
	calls    []string // "detach", "check", "mount", in order
	mounted  []mountSpec
	uids     []int
	reader   *os.File
	stale    bool     // a dead mount sits at the target until detached
	failWith error    // Detach refuses (a foreign mount at the target)
	checkErr error    // Check refuses even a live target
	at       []string // the target of each detach and mount, as "op target"
}

func (f *fakeMounter) Stale(string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stale
}

func (f *fakeMounter) Detach(target string) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "detach")
	f.at = append(f.at, "detach "+target)
	if f.failWith != nil {
		return 0, f.failWith
	}
	f.stale = false
	return 1, nil
}

func (f *fakeMounter) Check(string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "check")
	if f.stale {
		return syscall.ENOTCONN
	}
	return f.checkErr
}

func (f *fakeMounter) Mount(target string, spec mountSpec, p policy, uid, gid int) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "mount")
	f.at = append(f.at, "mount "+target)
	f.mounted = append(f.mounted, spec)
	f.uids = append(f.uids, uid)
	reader, writer, err := os.Pipe()
	if err != nil {
		return -1, err
	}
	f.reader = reader
	fd, err := syscall.Dup(int(writer.Fd()))
	writer.Close()
	return fd, err
}

// snapshot copies what the server did so far.
func (f *fakeMounter) snapshot() (calls []string, mounted []mountSpec, uids []int, reader *os.File) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...), append([]mountSpec(nil), f.mounted...), append([]int(nil), f.uids...), f.reader
}

func (f *fakeMounter) count(call string) int {
	calls, _, _, _ := f.snapshot()
	n := 0
	for _, c := range calls {
		if c == call {
			n++
		}
	}
	return n
}

func newServer(fake *fakeMounter, peerUID int) *server {
	return &server{
		targets:   []string{"/srw/cloud/root"},
		readOnly:  map[string]bool{"/srw/cloud/root": false},
		policy:    policy{ReadOnly: true, AllowOther: true, Source: "srw-cloud", Subtype: "rclone"},
		clientUID: 65534,
		dirUID:    -1,
		mounter:   fake,
		peer:      func(*net.UnixConn) (int, int, error) { return peerUID, peerUID, nil },
		logger:    log.New(io.Discard, "", 0),
	}
}

func socketDir(t *testing.T) string {
	t.Helper()
	// Short: a unix socket path has a 108-byte limit.
	dir, err := os.MkdirTemp("", "opener")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// startServer serves a fake mounter on a socket; peerUID is what
// SO_PEERCRED would report.
func startServer(t *testing.T, peerUID int) (string, *fakeMounter, *server) {
	t.Helper()
	socket := filepath.Join(socketDir(t), "o.sock")
	fake := &fakeMounter{}
	s := newServer(fake, peerUID)
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

func TestTheOpenerMountsOnlyItsTargetForItsClient(t *testing.T) {
	socket, fake, _ := startServer(t, 65534)
	fd, err := call(socket, request{Op: "mount", Mountpoint: "/srw/cloud/root/", Options: "rw"}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	syscall.Close(fd)
	calls, mounted, uids, _ := fake.snapshot()
	if len(mounted) != 1 || mounted[0].Flags&msReadOnly == 0 || uids[0] != 65534 {
		t.Fatalf("mounted %+v for %v", mounted, uids)
	}
	if strings.Join(calls, ",") != "detach,mount" {
		t.Fatalf("a stale mount was not detached first: %v", calls)
	}
	for _, req := range []request{
		{Op: "mount", Mountpoint: "/etc"},
		{Op: "mount", Mountpoint: "/srw/cloud/root/../../etc"},
		{Op: "mount", Options: "suid"},
		{Op: "format"},
	} {
		if _, err := call(socket, req, time.Second); err == nil {
			t.Fatalf("%+v was served", req)
		}
	}
	if fake.count("mount") != 1 {
		t.Fatal("a refused request mounted")
	}
}

func TestTheOpenerRefusesAnotherUIDBeforeReadingIt(t *testing.T) {
	socket, fake, _ := startServer(t, 1000)
	conn, err := net.Dial("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// Nothing is sent: the refusal must not wait for a request.
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	answer, err := io.ReadAll(conn)
	if err != nil || !strings.Contains(string(answer), "uid 1000 may not use this opener") {
		t.Fatalf("answer %q, %v", answer, err)
	}
	if _, err := call(socket, request{Op: "ping"}, time.Second); err == nil {
		t.Fatal("another uid may ping")
	}
	if calls, _, _, _ := fake.snapshot(); len(calls) != 0 {
		t.Fatalf("a refused uid reached the mounter: %v", calls)
	}
}

func TestRootMayOnlyPing(t *testing.T) {
	socket, fake, _ := startServer(t, 0)
	if _, err := call(socket, request{Op: "ping"}, time.Second); err != nil {
		t.Fatalf("the startup probe's ping: %v", err)
	}
	for _, op := range []string{"mount", "unmount"} {
		if _, err := call(socket, request{Op: op}, time.Second); err == nil || !strings.Contains(err.Error(), "may only ping") {
			t.Fatalf("%s from root: %v", op, err)
		}
	}
	if fake.count("mount")+fake.count("detach") != 0 {
		t.Fatal("root reached the mounter")
	}
}

func TestAPassedDescriptorIsDroppedAndASplitRequestIsRead(t *testing.T) {
	socket, _, _ := startServer(t, 65534)
	conn, err := net.Dial("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	unix := conn.(*net.UnixConn)
	reader, writer, _ := os.Pipe()
	defer reader.Close()
	defer writer.Close()
	// A descriptor with the first half; the server reads without a control
	// buffer, so the kernel discards it, and the request still completes.
	if _, _, err := unix.WriteMsgUnix([]byte(`{"op":`), syscall.UnixRights(int(writer.Fd())), nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	unix.Write([]byte(`"ping"}` + "\n"))
	var resp response
	unix.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := readMessage(unix, &resp); err != nil || !resp.OK {
		t.Fatalf("resp %+v, %v", resp, err)
	}
	for _, payload := range []string{`{"op":"ping"}` + "\n" + `{"op":"mount"}` + "\n", strings.Repeat("a", 5000)} {
		c, _ := net.Dial("unix", socket)
		c.Write([]byte(payload))
		c.(*net.UnixConn).CloseWrite()
		var r response
		c.SetDeadline(time.Now().Add(2 * time.Second))
		if _, err := readMessage(c.(*net.UnixConn), &r); err != nil || r.OK {
			t.Fatalf("%.20q was accepted: %+v %v", payload, r, err)
		}
		c.Close()
	}
}

func TestAnIdleClientDoesNotBlockOthers(t *testing.T) {
	socket, _, _ := startServer(t, 65534)
	idle, err := net.Dial("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer idle.Close()
	time.Sleep(50 * time.Millisecond)
	started := time.Now()
	if _, err := call(socket, request{Op: "ping"}, time.Second); err != nil {
		t.Fatalf("ping behind an idle client: %v", err)
	}
	if time.Since(started) > 500*time.Millisecond {
		t.Fatalf("ping took %v behind an idle client", time.Since(started))
	}
}

func TestTheOpenerRefusesToMountOverAStaleMountItCannotDetach(t *testing.T) {
	socket, fake, _ := startServer(t, 65534)
	fake.mu.Lock()
	fake.failWith = errors.New("/srw/cloud/root holds a tmpfs mount, not FUSE")
	fake.mu.Unlock()
	if _, err := call(socket, request{Op: "mount"}, time.Second); err == nil || !strings.Contains(err.Error(), "not FUSE") {
		t.Fatalf("mount over a foreign mount: %v", err)
	}
	if fake.count("mount") != 0 {
		t.Fatal("mounted over a foreign mount")
	}
}

func TestNothingMountsAfterTheShutdownDetach(t *testing.T) {
	socket, fake, s := startServer(t, 65534)
	s.shutdown()
	if _, err := call(socket, request{Op: "mount"}, time.Second); err == nil || !strings.Contains(err.Error(), "stopping") {
		t.Fatalf("a mount after shutdown: %v", err)
	}
	if fake.count("mount") != 0 || fake.count("detach") != 1 {
		calls, _, _, _ := fake.snapshot()
		t.Fatalf("calls %v", calls)
	}
}

func TestARestartedOpenerDetachesADeadMountBeforeTouchingTheTarget(t *testing.T) {
	// The predecessor's daemon died: a dead mount answers ENOTCONN to every
	// stat, so the target must be detached before it is created or checked.
	target := filepath.Join(t.TempDir(), "cloud", "root")
	fake := &fakeMounter{stale: true}
	if detached, err := prepareTarget(fake, target); err != nil || detached != 1 {
		t.Fatalf("prepare over a dead mount: %d detached, %v", detached, err)
	}
	calls, _, _, _ := fake.snapshot()
	if strings.Join(calls, ",") != "detach,check" {
		t.Fatalf("calls %v: the dead mount must go first", calls)
	}
	if info, err := os.Stat(target); err != nil || !info.IsDir() {
		t.Fatalf("target not created: %v", err)
	}
}

func TestARestartedOpenerLeavesALiveMountAlone(t *testing.T) {
	target := filepath.Join(t.TempDir(), "root")
	os.Mkdir(target, 0o755)
	fake := &fakeMounter{}
	if detached, err := prepareTarget(fake, target); err != nil || detached != 0 {
		t.Fatalf("%d detached, %v", detached, err)
	}
	if fake.count("detach") != 0 {
		t.Fatal("a live mount was detached at start")
	}
}

func TestStartRetriesInsteadOfExitingAndStillStops(t *testing.T) {
	fake := &fakeMounter{checkErr: errors.New("not a directory")}
	var logged bytes.Buffer
	s := newServer(fake, 65534)
	s.targets = []string{filepath.Join(t.TempDir(), "root")}
	s.logger = log.New(&logged, "", 0)
	stop := make(chan os.Signal, 1)
	result := make(chan *net.UnixListener, 1)
	go func() { result <- s.start(filepath.Join(socketDir(t), "o.sock"), stop) }()
	time.Sleep(200 * time.Millisecond)
	stop <- os.Interrupt
	select {
	case listener := <-result:
		if listener != nil {
			t.Fatal("served a target that failed its check")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("start ignored stop")
	}
	if !strings.Contains(logged.String(), "not serving yet, retrying") {
		t.Fatalf("log %q", logged.String())
	}
	s.shutdown()
	if fake.count("detach") != 1 {
		t.Fatal("a stopped opener did not detach")
	}
}

func TestTheSocketIsBornPrivateAndAPlantedLinkIsNeverFollowed(t *testing.T) {
	dir := socketDir(t)
	victim := filepath.Join(dir, "victim")
	os.WriteFile(victim, []byte("keep"), 0o644)
	socket := filepath.Join(dir, "o.sock")
	if err := os.Symlink(victim, socket); err != nil {
		t.Fatal(err)
	}
	listener, err := listen(socket, os.Getuid())
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	info, err := os.Lstat(socket)
	if err != nil || info.Mode()&os.ModeSocket == 0 || info.Mode().Perm() != 0o600 {
		t.Fatalf("socket %v %v", info.Mode(), err)
	}
	victimInfo, _ := os.Stat(victim)
	if content, _ := os.ReadFile(victim); string(content) != "keep" || victimInfo.Mode().Perm() != 0o644 {
		t.Fatal("the link's target was touched")
	}
	// A path that cannot be removed fails this attempt (start retries it).
	blocked := filepath.Join(dir, "blocked")
	os.MkdirAll(filepath.Join(blocked, "inside"), 0o755)
	if _, err := listen(blocked, os.Getuid()); err == nil {
		t.Fatal("listened over a directory")
	}
}

func TestFusermountHandsTheDescriptorBackOverCommFD(t *testing.T) {
	socket, fake, _ := startServer(t, 65534)
	// go-fuse's side: a SOCK_SEQPACKET pair, the far end at _FUSE_COMMFD.
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_SEQPACKET, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(fds[0])
	defer syscall.Close(fds[1])
	env := map[string]string{"SRW_FUSE_OPENER_SOCKET": socket, "_FUSE_COMMFD": strconv.Itoa(fds[1])}
	stderr, _ := os.CreateTemp(t.TempDir(), "stderr")
	if code := fusermountMain([]string{"/srw/cloud/root", "-o", "allow_other,ro,max_read=1048576"}, mapEnv(env), stderr); code != 0 {
		out, _ := os.ReadFile(stderr.Name())
		t.Fatalf("exit %d: %s", code, out)
	}
	data := make([]byte, 4)
	oob := make([]byte, syscall.CmsgSpace(4))
	n, oobn, _, _, err := syscall.Recvmsg(fds[0], data, oob, 0)
	if err != nil || n != 1 || data[0] != 0 {
		t.Fatalf("recvmsg n=%d data=%v err=%v", n, data[:n], err)
	}
	got, err := parseRights(oob[:oobn])
	if err != nil || len(got) != 1 {
		t.Fatalf("rights %v %v", got, err)
	}
	received := os.NewFile(uintptr(got[0]), "received")
	received.WriteString("served")
	received.Close()
	_, mounted, _, reader := fake.snapshot()
	buf, _ := io.ReadAll(reader)
	if string(buf) != "served" {
		t.Fatalf("the descriptor is not the opener's: %q", buf)
	}
	if mounted[0].MaxRead != 1048576 {
		t.Fatalf("options lost: %+v", mounted[0])
	}
}

func TestFusermountUnmountAsksTheOpener(t *testing.T) {
	socket, fake, _ := startServer(t, 65534)
	env := mapEnv(map[string]string{"SRW_FUSE_OPENER_SOCKET": socket})
	if code := fusermountMain([]string{"-u", "-z", "--", "/srw/cloud/root"}, env, os.Stderr); code != 0 {
		t.Fatalf("exit %d", code)
	}
	if fake.count("detach") != 1 || fake.count("mount") != 0 {
		calls, _, _, _ := fake.snapshot()
		t.Fatalf("calls %v", calls)
	}
	if code := fusermountMain([]string{"-u", "-q", "/elsewhere"}, env, os.Stderr); code == 0 {
		t.Fatal("unmounted a path the opener does not own")
	}
}

func TestFusermountWithoutCommFDRefusesBeforeAsking(t *testing.T) {
	socket, fake, _ := startServer(t, 65534)
	stderr, _ := os.CreateTemp(t.TempDir(), "stderr")
	env := mapEnv(map[string]string{"SRW_FUSE_OPENER_SOCKET": socket})
	if code := fusermountMain([]string{"/srw/cloud/root"}, env, stderr); code == 0 {
		t.Fatal("mounted without _FUSE_COMMFD")
	}
	if fake.count("mount") != 0 {
		t.Fatal("the opener was asked")
	}
}

func mapEnv(env map[string]string) func(string) string {
	return func(key string) string { return env[key] }
}
