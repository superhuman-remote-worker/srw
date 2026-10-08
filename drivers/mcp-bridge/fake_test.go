package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// The test binary doubles as a stdio MCP server (and as a process that only
// sleeps): the bridge runs it as a binding's process. It logs every line it
// reads, so a test can compare the bytes it received with the bytes sent.
const (
	fakeServerEnv = "BRIDGE_TEST_FAKE_SERVER"
	fakeLogEnv    = "BRIDGE_TEST_LOG_DIR"
	sleeperEnv    = "BRIDGE_TEST_SLEEPER"
	fakeTokenEnv  = "FAKE_TOKEN"
	// The fake server exits at once without its credential, as
	// mcp/brave-search and mcp/slack do.
	requireTokenEnv = "BRIDGE_TEST_REQUIRE_TOKEN"
	// How many orphans the orphans tool leaves.
	orphanCount = 100
	// A variable whose value names the process's private directory.
	homeValueEnv = "BRIDGE_TEST_HOME_VALUE"
)

func TestMain(m *testing.M) {
	switch {
	case len(os.Args) > 1 && os.Args[1] == "launch":
		// The bridge starts itself in launch mode as a binding's user.
		os.Exit(dispatch(os.Args[1:], os.Stdout, os.Stderr))
	case os.Getenv(sleeperEnv) == "1":
		time.Sleep(time.Hour)
		os.Exit(0)
	case os.Getenv(fakeServerEnv) == "1":
		os.Exit(fakeServer())
	}
	os.Exit(m.Run())
}

type fakeMessage struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

var fakeTools = []string{"whoami", "notes_write", "leak_credential", "crash", "notify", "spawn_sleeper", "slow", "orphans"}

func fakeServer() int {
	if os.Getenv(requireTokenEnv) == "1" && os.Getenv(fakeTokenEnv) == "" {
		fmt.Fprintf(os.Stderr, "%s is not set\n", fakeTokenEnv)
		return 4
	}
	var logFile *os.File
	if dir := os.Getenv(fakeLogEnv); dir != "" {
		logFile, _ = os.Create(filepath.Join(dir, fmt.Sprintf("%d.log", os.Getpid())))
	}
	var out sync.Mutex
	write := func(message any) {
		raw, _ := json.Marshal(message)
		out.Lock()
		defer out.Unlock()
		os.Stdout.Write(append(raw, '\n'))
	}
	answer := func(id json.RawMessage, result any) {
		write(map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
	}
	text := func(id json.RawMessage, value string) {
		answer(id, map[string]any{"content": []any{map[string]any{"type": "text", "text": value}}})
	}
	calls := 0
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 64*1024), 8<<20)
	for scanner.Scan() {
		line := scanner.Bytes()
		if logFile != nil {
			logFile.Write(append(append([]byte(nil), line...), '\n'))
			logFile.Sync()
		}
		var message fakeMessage
		if json.Unmarshal(line, &message) != nil || message.Method == "" {
			continue // an answer to nothing this server asked
		}
		switch message.Method {
		case "initialize":
			answer(message.ID, map[string]any{
				"protocolVersion": "2025-06-18",
				"capabilities":    map[string]any{"tools": map[string]any{}},
				"serverInfo":      map[string]any{"name": "fake", "version": "1"},
			})
		case "ping":
			answer(message.ID, map[string]any{})
		case "tools/list":
			var tools []any
			for _, name := range fakeTools {
				tools = append(tools, map[string]any{"name": name, "inputSchema": map[string]any{"type": "object"}})
			}
			answer(message.ID, map[string]any{"tools": tools})
		case "tools/call":
			calls++
			var params struct {
				Name      string            `json:"name"`
				Arguments map[string]string `json:"arguments"`
			}
			json.Unmarshal(message.Params, &params)
			if found, ok := isolationTool(params.Name, params.Arguments); ok {
				text(message.ID, found)
				continue
			}
			switch params.Name {
			case "whoami":
				credential, held := os.LookupEnv(fakeTokenEnv)
				digest := ""
				if held {
					sum := sha256.Sum256([]byte(credential))
					digest = hex.EncodeToString(sum[:])
				}
				var names []string
				for _, entry := range os.Environ() {
					name, _, _ := strings.Cut(entry, "=")
					names = append(names, name)
				}
				sort.Strings(names)
				raw, _ := json.Marshal(map[string]any{
					"pid": os.Getpid(), "credential_sha256": digest, "calls": calls, "env": names,
				})
				text(message.ID, string(raw))
			case "notes_write":
				text(message.ID, "written")
			case "leak_credential":
				credential := os.Getenv(fakeTokenEnv)
				fmt.Fprintf(os.Stderr, "my token is %s\nencoded %s\n", credential, hex.EncodeToString([]byte(credential)))
				text(message.ID, credential)
			case "crash":
				os.Exit(3)
			case "notify":
				write(map[string]any{"jsonrpc": "2.0", "method": "notifications/message", "params": map[string]any{"level": "info", "data": "hello"}})
				text(message.ID, "notified")
			case "spawn_sleeper":
				sleeper := exec.Command(os.Args[0])
				sleeper.Env = append(os.Environ(), sleeperEnv+"=1", fakeServerEnv+"=0")
				sleeper.Start()
				text(message.ID, fmt.Sprint(sleeper.Process.Pid))
			case "slow":
				time.Sleep(300 * time.Millisecond)
				text(message.ID, "slow")
			case "orphans":
				// Each shell exits at once and leaves its sleep behind: an
				// orphan in this process's group, which exits soon after.
				for range orphanCount {
					exec.Command("sh", "-c", "sleep 0.05 </dev/null >/dev/null 2>&1 &").Run()
				}
				text(message.ID, fmt.Sprint(orphanCount))
			default:
				write(map[string]any{"jsonrpc": "2.0", "id": message.ID, "error": map[string]any{"code": -32602, "message": "Unknown tool: " + params.Name}})
			}
		default:
			if len(message.ID) > 0 {
				write(map[string]any{"jsonrpc": "2.0", "id": message.ID, "error": map[string]any{"code": -32601, "message": "Method not found"}})
			}
		}
	}
	return 0
}
