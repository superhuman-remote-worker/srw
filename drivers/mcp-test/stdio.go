package main

import (
	"bufio"
	"encoding/json"
	"io"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"syscall"
)

// With -stdio (connector drivers D5b) the server speaks MCP on stdin and
// stdout, one JSON-RPC message per line, behind SRW's stdio bridge, and
// takes its upstream credential from its environment (-credential-env).
// It then also lists the probe tools, which look around the pod from inside
// a binding's process, as a compromised server could: the D5b k3d gate
// calls them to prove one binding's process cannot reach another's or the
// bridge. They read nothing they return (only whether an attempt was
// refused), and need no credential.
//
//	self_status   this process's pid, user, group, HOME and TMPDIR, its
//	              capabilities, no_new_privs and process and core limits,
//	              and its variables' names (never a value)
//	probe_path    reads a file, or lists a directory ({"path"})
//	probe_socket  connects to a unix socket ({"path"})
//	probe_signal  sends signal 0 to a process ({"pid"})

var (
	pathProperty = map[string]any{"path": map[string]any{"type": "string"}}
	probeTools   = []tool{
		{"self_status", "Reports this process's user, group, directories, capabilities and no_new_privs.", schema(map[string]any{})},
		{"probe_path", "Reports whether this process may read a file or list a directory.", schema(pathProperty, "path")},
		{"probe_socket", "Reports whether this process may connect to a unix socket.", schema(pathProperty, "path")},
		{"probe_signal", "Reports whether this process may signal another.", schema(map[string]any{"pid": map[string]any{"type": "string"}}, "pid")},
	}
)

// serveStdio answers each request line on in with a line on out until in
// ends.
func (s *server) serveStdio(in io.Reader, out io.Writer, credential string) error {
	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 64*1024), 4<<20)
	encoder := json.NewEncoder(out)
	for scanner.Scan() {
		var request rpcRequest
		if err := json.Unmarshal(scanner.Bytes(), &request); err != nil || request.Method == "" {
			continue // an answer to nothing this server asked, or no JSON
		}
		if len(request.ID) == 0 || string(request.ID) == "null" {
			continue // a notification
		}
		var answer map[string]any
		if request.Method == "initialize" {
			answer = result(request.ID, map[string]any{
				"protocolVersion": "2025-06-18",
				"capabilities":    map[string]any{"tools": map[string]any{"listChanged": false}},
				"serverInfo":      map[string]any{"name": "srw-mcp-test", "version": "1"},
			})
		} else {
			answer = s.answer(request, credential)
		}
		if err := encoder.Encode(answer); err != nil {
			return err
		}
	}
	return scanner.Err()
}

func refusal(err error) string {
	if err != nil {
		return "refused: " + err.Error()
	}
	return "allowed"
}

// probeTool runs one of the probe tools.
func probeTool(name string, args map[string]string) (string, bool) {
	switch name {
	case "self_status":
		return selfStatus(), true
	case "probe_path":
		path := args["path"]
		info, err := os.Stat(path)
		if err == nil && info.IsDir() {
			_, err = os.ReadDir(path)
		} else if err == nil {
			_, err = os.ReadFile(path)
		}
		return refusal(err), true
	case "probe_socket":
		conn, err := net.Dial("unix", args["path"])
		if err == nil {
			conn.Close()
		}
		return refusal(err), true
	case "probe_signal":
		pid, err := strconv.Atoi(args["pid"])
		if err != nil || pid <= 0 {
			return "refused: no pid", true
		}
		return refusal(syscall.Kill(pid, 0)), true
	}
	return "", false
}

func selfStatus() string {
	// The names of its environment's variables, never a value.
	names := []string{}
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		names = append(names, name)
	}
	sort.Strings(names)
	status := map[string]any{
		"pid":       os.Getpid(),
		"uid":       os.Getuid(),
		"gid":       os.Getgid(),
		"home":      os.Getenv("HOME"),
		"tmpdir":    os.Getenv("TMPDIR"),
		"env_names": names,
	}
	raw, _ := os.ReadFile("/proc/self/status")
	for _, line := range strings.Split(string(raw), "\n") {
		key, value, ok := strings.Cut(line, ":")
		switch key {
		case "CapPrm", "CapEff", "CapAmb", "NoNewPrivs":
			if ok {
				status[key] = strings.TrimSpace(value)
			}
		}
	}
	// Its soft limits on processes and core dumps (/proc/self/limits).
	raw, _ = os.ReadFile("/proc/self/limits")
	for _, line := range strings.Split(string(raw), "\n") {
		for prefix, key := range map[string]string{"Max processes": "max_processes", "Max core file size": "max_core"} {
			if rest, ok := strings.CutPrefix(line, prefix); ok {
				if fields := strings.Fields(rest); len(fields) > 0 {
					status[key] = fields[0]
				}
			}
		}
	}
	out, _ := json.Marshal(status)
	return string(out)
}
