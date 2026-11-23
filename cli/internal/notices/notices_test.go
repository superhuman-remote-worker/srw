// Package notices holds one test: the client's third-party notices name
// every module the program is built from, at the version it is built from.
package notices

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The notices are shipped with every binary. A dependency added or bumped
// without its notice would ship a binary whose licence terms are not met.
func TestEveryModuleInGoModHasItsNotice(t *testing.T) {
	notices, err := os.ReadFile(filepath.Join("..", "..", "THIRD_PARTY_NOTICES.md"))
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(filepath.Join("..", "..", "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	modules := 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(strings.TrimPrefix(strings.TrimSpace(scanner.Text()), "require "))
		// A requirement line: "<module> <version>", perhaps "// indirect".
		if len(fields) < 2 || !strings.Contains(fields[0], "/") || !strings.HasPrefix(fields[1], "v") {
			continue
		}
		modules++
		for _, needed := range []string{
			"| `" + fields[0] + "` | " + fields[1] + " |",
			"## " + fields[0] + " " + fields[1] + "\n",
		} {
			if !strings.Contains(string(notices), needed) {
				t.Errorf("THIRD_PARTY_NOTICES.md lacks %q; add the module's licence text and its table row", needed)
			}
		}
	}
	if modules < 3 {
		t.Fatalf("read only %d modules from go.mod; its format changed", modules)
	}
}
