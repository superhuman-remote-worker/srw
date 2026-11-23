package routes

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// inventory reads policy/endpoint_inventory.txt: "METHOD PATH pat=SCOPE …".
func inventory(t *testing.T) map[string]string {
	t.Helper()
	path := filepath.Join("..", "..", "..", "policy", "endpoint_inventory.txt")
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("the server's route inventory is missing: %v", err)
	}
	defer file.Close()
	decisions := map[string]string{}
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 3 || strings.HasPrefix(fields[0], "#") {
			continue
		}
		if scope, ok := strings.CutPrefix(fields[2], "pat="); ok {
			decisions[fields[0]+" "+fields[1]] = scope
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if len(decisions) < 100 {
		t.Fatalf("read only %d routes from %s; the format changed", len(decisions), path)
	}
	return decisions
}

func TestEveryRouteExistsOnTheServerWithTheScopeTheCLIExpects(t *testing.T) {
	decisions := inventory(t)
	for _, route := range All {
		scope, ok := decisions[route.Method+" "+route.Path]
		if !ok {
			t.Errorf("%s %s is not in the server's route inventory", route.Method, route.Path)
			continue
		}
		if scope != route.Scope {
			t.Errorf("%s %s needs the scope %q on the server; the CLI expects %q",
				route.Method, route.Path, scope, route.Scope)
		}
	}
}

func TestTheTableHasNoDuplicate(t *testing.T) {
	seen := map[string]bool{}
	for _, route := range All {
		key := route.Method + " " + route.Path
		if seen[key] {
			t.Errorf("%s is listed twice", key)
		}
		seen[key] = true
	}
}
