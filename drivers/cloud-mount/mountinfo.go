package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

// topMountType names the filesystem type on top of the stack at target, or
// "" when nothing is mounted there.
func topMountType(mountinfo io.Reader, target string) (string, error) {
	scanner := bufio.NewScanner(mountinfo)
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	top := ""
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 7 || unescapeMountinfo(fields[4]) != target {
			continue
		}
		for i := 6; i < len(fields)-1; i++ {
			if fields[i] == "-" {
				top = fields[i+1]
				break
			}
		}
	}
	return top, scanner.Err()
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

// statWithin stats path, giving up after timeout: a hung FUSE daemon blocks
// stat in the kernel, and the goroutine is left to finish when it is killed.
func statWithin(path string, timeout time.Duration) error {
	result := make(chan error, 1)
	go func() {
		_, err := os.Stat(path)
		result <- err
	}()
	select {
	case err := <-result:
		return err
	case <-time.After(timeout):
		return fmt.Errorf("stat %s: no answer within %s", path, timeout)
	}
}

var errNotMounted = errors.New("not mounted")

// liveMount is the real mount check: the top mount at target is
// fuse.rclone and answers a stat.
func liveMount(target string) error {
	file, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return err
	}
	top, err := topMountType(file, target)
	file.Close()
	if err != nil {
		return err
	}
	if top != "fuse.rclone" {
		return errNotMounted
	}
	return statWithin(target, 10*time.Second)
}
