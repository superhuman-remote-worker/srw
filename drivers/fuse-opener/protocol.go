package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"syscall"
	"time"
)

// One request per connection: a JSON line from the client, a JSON line back.
// A successful mount's answer carries the /dev/fuse descriptor as
// SCM_RIGHTS on the same message.

const maxMessage = 4096

type request struct {
	Op         string `json:"op"` // mount, unmount or ping
	Mountpoint string `json:"mountpoint,omitempty"`
	Options    string `json:"options,omitempty"` // fusermount3's -o, comma separated
}

type response struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

// writeMessage sends v as one JSON line, with fd attached when fd >= 0.
func writeMessage(conn *net.UnixConn, v any, fd int) error {
	payload, err := json.Marshal(v)
	if err != nil {
		return err
	}
	payload = append(payload, '\n')
	var oob []byte
	if fd >= 0 {
		oob = syscall.UnixRights(fd)
	}
	n, oobn, err := conn.WriteMsgUnix(payload, oob, nil)
	if err != nil {
		return err
	}
	if n != len(payload) || oobn != len(oob) {
		return errors.New("short write on the opener socket")
	}
	return nil
}

// readMessage reads one JSON line into v and returns a descriptor that came
// with it, or -1. Any further descriptors are closed.
func readMessage(conn *net.UnixConn, v any) (int, error) {
	buf := make([]byte, maxMessage)
	oob := make([]byte, syscall.CmsgSpace(4*4))
	n, oobn, _, _, err := conn.ReadMsgUnix(buf, oob)
	if err != nil {
		return -1, err
	}
	fd := -1
	if oobn > 0 {
		fds, err := parseRights(oob[:oobn])
		if err != nil {
			return -1, err
		}
		for i, received := range fds {
			if i == 0 {
				fd = received
			} else {
				syscall.Close(received)
			}
		}
	}
	if n == 0 || buf[n-1] != '\n' {
		closeFD(fd)
		return -1, errors.New("malformed message on the opener socket")
	}
	if err := json.Unmarshal(buf[:n-1], v); err != nil {
		closeFD(fd)
		return -1, fmt.Errorf("malformed message on the opener socket: %w", err)
	}
	return fd, nil
}

func parseRights(oob []byte) ([]int, error) {
	messages, err := syscall.ParseSocketControlMessage(oob)
	if err != nil {
		return nil, err
	}
	var fds []int
	for _, message := range messages {
		got, err := syscall.ParseUnixRights(&message)
		if err != nil {
			continue
		}
		fds = append(fds, got...)
	}
	return fds, nil
}

func closeFD(fd int) {
	if fd >= 0 {
		syscall.Close(fd)
	}
}

// call sends req to the opener at socketPath and returns the descriptor its
// answer carried (or -1).
func call(socketPath string, req request, timeout time.Duration) (int, error) {
	conn, err := net.DialTimeout("unix", socketPath, timeout)
	if err != nil {
		return -1, err
	}
	defer conn.Close()
	unix := conn.(*net.UnixConn)
	unix.SetDeadline(time.Now().Add(timeout))
	if err := writeMessage(unix, req, -1); err != nil {
		return -1, err
	}
	var resp response
	fd, err := readMessage(unix, &resp)
	if err != nil {
		return -1, err
	}
	if !resp.OK {
		closeFD(fd)
		return -1, fmt.Errorf("opener refused: %s", resp.Error)
	}
	if req.Op == "mount" && fd < 0 {
		return -1, errors.New("opener answered a mount without a descriptor")
	}
	return fd, nil
}
