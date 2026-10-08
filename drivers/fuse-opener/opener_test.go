package main

import (
	"bufio"
	"errors"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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
	if spec.Flags != msNoSuid|msNoDev|msReadOnly|msNoExec || !spec.Default || spec.MaxRead != 1048576 {
		t.Fatalf("spec %+v", spec)
	}
	if spec.FSType != "fuse.rclone" {
		t.Fatalf("a client chose the type: %q", spec.FSType)
	}
	for _, refused := range []string{"suid", "dev", "blksize=4096", "max_read=0", "max_read=x", "user_id=0", "fd=3"} {
		if _, err := planMount(policy{Subtype: "rclone"}, refused); err == nil {
			t.Fatalf("option %q was accepted", refused)
		}
	}
}

func TestMountDataNamesTheDescriptorRootTypeAndDaemon(t *testing.T) {
	p := policy{AllowOther: true, Subtype: "rclone"}
	spec, _ := planMount(p, "default_permissions,max_read=4096")
	got := spec.data(p, 7, 0o40755, 65534, 65533)
	want := "fd=7,rootmode=40000,user_id=65534,group_id=65533,allow_other,default_permissions,max_read=4096"
	if got != want {
		t.Fatalf("data %q, want %q", got, want)
	}
	if got := (mountSpec{}).data(policy{}, 3, 0o40000, 1, 1); strings.Contains(got, "allow_other") {
		t.Fatalf("allow_other without the policy: %q", got)
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

type fakeMounter struct {
	detached []string
	mounted  []mountSpec
	uids     []int
	reader   *os.File
	failWith error
}

func (f *fakeMounter) Detach(target string) (int, error) {
	f.detached = append(f.detached, target)
	return 1, f.failWith
}

func (f *fakeMounter) Mount(target string, spec mountSpec, p policy, uid, gid int) (int, error) {
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

// startServer serves a fake mounter on a socket; peerUID is what
// SO_PEERCRED would report.
func startServer(t *testing.T, peerUID int) (string, *fakeMounter) {
	t.Helper()
	dir, err := os.MkdirTemp("", "opener")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	socket := filepath.Join(dir, "o.sock")
	fake := &fakeMounter{}
	s := &server{
		target:    "/srw/cloud/root",
		policy:    policy{ReadOnly: true, AllowOther: true, Source: "srw-cloud", Subtype: "rclone"},
		clientUID: 65534,
		mounter:   fake,
		peer:      func(*net.UnixConn) (int, int, error) { return peerUID, peerUID, nil },
		logger:    log.New(io.Discard, "", 0),
	}
	listener, err := listen(socket, os.Getuid())
	if err != nil {
		t.Fatal(err)
	}
	stop := make(chan os.Signal, 1)
	done := make(chan struct{})
	go func() { s.serve(listener, stop); close(done) }()
	t.Cleanup(func() { stop <- os.Interrupt; <-done })
	return socket, fake
}

func TestTheOpenerMountsOnlyItsTargetForItsClient(t *testing.T) {
	socket, fake := startServer(t, 65534)
	fd, err := call(socket, request{Op: "mount", Mountpoint: "/srw/cloud/root/", Options: "rw"}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	syscall.Close(fd)
	if len(fake.mounted) != 1 || fake.mounted[0].Flags&msReadOnly == 0 || fake.uids[0] != 65534 {
		t.Fatalf("mounted %+v for %v", fake.mounted, fake.uids)
	}
	if len(fake.detached) != 1 {
		t.Fatalf("a stale mount was not detached first: %v", fake.detached)
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
	if len(fake.mounted) != 1 {
		t.Fatalf("a refused request mounted: %+v", fake.mounted)
	}
}

func TestTheOpenerRefusesAnotherUIDButAnswersItsPing(t *testing.T) {
	socket, fake := startServer(t, 1000)
	if _, err := call(socket, request{Op: "ping"}, time.Second); err != nil {
		t.Fatalf("ping: %v", err)
	}
	for _, op := range []string{"mount", "unmount"} {
		_, err := call(socket, request{Op: op}, time.Second)
		if err == nil || !strings.Contains(err.Error(), "uid 1000") {
			t.Fatalf("%s from uid 1000: %v", op, err)
		}
	}
	if len(fake.mounted)+len(fake.detached) != 0 {
		t.Fatal("a refused uid reached the mounter")
	}
}

func TestTheOpenerRefusesToMountOverAStaleMountItCannotDetach(t *testing.T) {
	socket, fake := startServer(t, 65534)
	fake.failWith = errors.New("/srw/cloud/root holds a tmpfs mount, not FUSE")
	if _, err := call(socket, request{Op: "mount"}, time.Second); err == nil || !strings.Contains(err.Error(), "not FUSE") {
		t.Fatalf("mount over a foreign mount: %v", err)
	}
	if len(fake.mounted) != 0 {
		t.Fatal("mounted over a foreign mount")
	}
}

func TestFusermountHandsTheDescriptorBackOverCommFD(t *testing.T) {
	socket, fake := startServer(t, 65534)
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
	buf, _ := io.ReadAll(fake.reader)
	if string(buf) != "served" {
		t.Fatalf("the descriptor is not the opener's: %q", buf)
	}
	if fake.mounted[0].MaxRead != 1048576 {
		t.Fatalf("options lost: %+v", fake.mounted[0])
	}
}

func TestFusermountUnmountAsksTheOpener(t *testing.T) {
	socket, fake := startServer(t, 65534)
	env := mapEnv(map[string]string{"SRW_FUSE_OPENER_SOCKET": socket})
	if code := fusermountMain([]string{"-u", "-z", "--", "/srw/cloud/root"}, env, os.Stderr); code != 0 {
		t.Fatalf("exit %d", code)
	}
	if len(fake.detached) != 1 || len(fake.mounted) != 0 {
		t.Fatalf("detached %v mounted %v", fake.detached, fake.mounted)
	}
	if code := fusermountMain([]string{"-u", "-q", "/elsewhere"}, env, os.Stderr); code == 0 {
		t.Fatal("unmounted a path the opener does not own")
	}
}

func TestFusermountWithoutCommFDRefusesBeforeAsking(t *testing.T) {
	socket, fake := startServer(t, 65534)
	stderr, _ := os.CreateTemp(t.TempDir(), "stderr")
	env := mapEnv(map[string]string{"SRW_FUSE_OPENER_SOCKET": socket})
	if code := fusermountMain([]string{"/srw/cloud/root"}, env, stderr); code == 0 {
		t.Fatal("mounted without _FUSE_COMMFD")
	}
	if len(fake.mounted) != 0 {
		t.Fatal("the opener was asked")
	}
}

func mapEnv(env map[string]string) func(string) string {
	return func(key string) string { return env[key] }
}
