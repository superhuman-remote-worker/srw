package cmd

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/superhuman-remote-worker/srw/cli/internal/exit"
)

// These tests cover the declarative commands against a server that answers
// what the test says. What the real server answers is covered by
// tests/test_srw_cli_wire.py in the repository root.
//
// They also are the parity table with the Python command this client
// replaces (orchestrator/operator_cli/manifest_resources.py). Each case of
// that command is kept, changed on purpose, or dropped:
//
//	kept     -f repeatable, - for stdin once, never with --token-stdin
//	kept     --format, else by extension (.json is JSON, the rest YAML)
//	kept     --scope-kind with --scope-name, only together
//	kept     preview --resolution stored|bundle, stored by default
//	kept     apply --plan | --plan-revision, --expected-versions,
//	         --expected-version IDENTITY=VERSION, --idempotency-key, with
//	         their checks (no duplicate key, a stored preview, sha256:…,
//	         positive versions, at most 128 characters)
//	kept     get UID; get [--kind K] [scope]; a UID with a scope is refused
//	kept     export UID, YAML by default, JSON with -o json
//	kept     delete UID --expected-version N
//	kept     cache list | delete ARTIFACT_ID, with a scope
//	kept     JSON on stdout, {"error": …} on stderr, the token never printed
//	kept     the 1 MiB limit for the manifests of one request
//	changed  files are sent as they are, several as sources; they are not
//	         parsed and joined on the client
//	changed  exit codes say what failed (the Python command exits 1 always)
//	changed  no implicit server (was http://localhost:8085)
//	changed  proxy variables are honoured (were ignored)
//	changed  delete without --expected-version asks, or takes --yes
//	dropped  the client-side count of 100 documents: the server counts

func (h *harness) handle(route string, handler http.HandlerFunc) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.routes == nil {
		h.routes = map[string]http.HandlerFunc{}
	}
	h.routes[route] = handler
}

// record answers a route with a fixed body and keeps what was sent.
func (h *harness) record(route string, status int, answer any) *[]map[string]any {
	var bodies []map[string]any
	h.handle(route, func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		body := map[string]any{}
		json.Unmarshal(raw, &body)
		body["_query"] = r.URL.RawQuery
		h.mu.Lock()
		bodies = append(bodies, body)
		h.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		json.NewEncoder(w).Encode(answer)
	})
	return &bodies
}

func manifestFileIn(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

const expertYAML = "apiVersion: srw/v1alpha1\nkind: Expert\nmetadata:\n  name: reviewer\nspec: {}\n"

func TestFilesAreSentAsTheyAre(t *testing.T) {
	h := newHarness(t)
	h.login()
	sent := h.record("POST /api/manifests/validate", 200, map[string]any{"valid": true, "documents": []any{}})

	yamlFile := manifestFileIn(t, "a.yaml", expertYAML)
	jsonFile := manifestFileIn(t, "b.json", `{"kind": "Expert"}`)
	if _, errOut, code := h.run("", "validate", "-f", yamlFile); code != 0 {
		t.Fatal(errOut)
	}
	// One file: {source, format}, the text unparsed. Every server takes it.
	if got := (*sent)[0]; got["source"] != expertYAML || got["format"] != "yaml" || got["sources"] != nil {
		t.Errorf("one file: %v", got)
	}
	h.run("", "validate", "-f", jsonFile)
	if got := (*sent)[1]; got["format"] != "json" {
		t.Errorf("the format follows the extension: %v", got)
	}
	h.run("", "validate", "-f", jsonFile, "--format", "yaml")
	if got := (*sent)[2]; got["format"] != "yaml" {
		t.Errorf("--format wins over the extension: %v", got)
	}
	// Several files: sources, each with its name and its own format.
	h.run(expertYAML, "validate", "-f", yamlFile, "-f", jsonFile, "-f", "-")
	sources := (*sent)[3]["sources"].([]any)
	if len(sources) != 3 || (*sent)[3]["source"] != nil {
		t.Fatalf("several files: %v", (*sent)[3])
	}
	first, last := sources[0].(map[string]any), sources[2].(map[string]any)
	if first["name"] != yamlFile || first["format"] != "yaml" || sources[1].(map[string]any)["format"] != "json" || last["name"] != "<stdin>" {
		t.Errorf("sources: %v", sources)
	}

	// A directory is its manifest files, not recursive.
	directory := t.TempDir()
	os.WriteFile(filepath.Join(directory, "1.yaml"), []byte(expertYAML), 0o600)
	os.WriteFile(filepath.Join(directory, "2.yml"), []byte(expertYAML), 0o600)
	os.WriteFile(filepath.Join(directory, "notes.txt"), []byte("x"), 0o600)
	os.Mkdir(filepath.Join(directory, "sub"), 0o700)
	os.WriteFile(filepath.Join(directory, "sub", "3.yaml"), []byte(expertYAML), 0o600)
	h.run("", "validate", "-f", directory)
	if got := (*sent)[4]["sources"].([]any); len(got) != 2 {
		t.Errorf("a directory: %v", got)
	}
}

func TestFileRules(t *testing.T) {
	h := newHarness(t)
	h.login()
	h.record("POST /api/manifests/validate", 200, map[string]any{})
	good := manifestFileIn(t, "a.yaml", expertYAML)
	big := manifestFileIn(t, "big.yaml", strings.Repeat("#", 1<<20+1))
	binary := manifestFileIn(t, "bin.yaml", "name: \xff\xfe")
	for name, args := range map[string][]string{
		"no file":                    {"validate"},
		"stdin twice":                {"validate", "-f", "-", "-f", "-"},
		"stdin and the token":        {"validate", "-f", "-", "--token-stdin"},
		"a missing file":             {"validate", "-f", "/no/such/file.yaml"},
		"the same file twice":        {"validate", "-f", good, "-f", good},
		"more than 1 MiB":            {"validate", "-f", big},
		"not UTF-8":                  {"validate", "-f", binary},
		"an unknown format":          {"validate", "-f", good, "--format", "toml"},
		"an empty directory":         {"validate", "-f", t.TempDir()},
		"half a scope":               {"preview", "-f", good, "--scope-kind", "Account"},
		"two scopes":                 {"preview", "-f", good, "--account", "--catalog"},
		"an unknown scope kind":      {"preview", "-f", good, "--scope-kind", "Team", "--scope-name", "x"},
		"an unknown resolution":      {"preview", "-f", good, "--resolution", "live"},
		"a plan and a plan revision": {"apply", "-f", good, "--plan", good, "--plan-revision", "sha256:" + strings.Repeat("a", 64)},
		"a malformed plan revision":  {"apply", "-f", good, "--plan-revision", "abc"},
		"a malformed version":        {"apply", "-f", good, "--expected-version", "Expert/x"},
		"a version below one":        {"apply", "-f", good, "--expected-version", "Expert/x=0"},
		"a version twice":            {"apply", "-f", good, "--expected-version", "a=1", "--expected-version", "a=2"},
		"a long idempotency key":     {"apply", "-f", good, "--idempotency-key", strings.Repeat("k", 129)},
		"an unknown dry run":         {"apply", "-f", good, "--dry-run=client"},
	} {
		before := len(h.requests)
		if _, errOut, code := h.run("", args...); code != exit.Usage || len(h.requests) != before {
			t.Errorf("%s: code %d, requests %d, %s", name, code, len(h.requests)-before, errOut)
		}
	}
}

func TestAnOlderServerTakesOneFilePerCall(t *testing.T) {
	h := newHarness(t)
	h.login()
	h.record("POST /api/manifests/validate", 422, map[string]any{"detail": []any{
		map[string]any{"type": "missing", "loc": []any{"body", "source"}, "msg": "Field required"},
		map[string]any{"type": "extra_forbidden", "loc": []any{"body", "sources"}, "msg": "Extra inputs are not permitted", "input": "SECRET BODY"},
	}})
	a, b := manifestFileIn(t, "a.yaml", expertYAML), manifestFileIn(t, "b.yaml", expertYAML)
	_, errOut, code := h.run("", "validate", "-f", a, "-f", b)
	if code != exit.Failed || !strings.Contains(errOut, "one manifest file per call") || strings.Contains(errOut, "SECRET BODY") {
		t.Errorf("%d %s", code, errOut)
	}
}

const (
	expertKey = "Expert/Account/11111111-1111-1111-1111-111111111111/reviewer"
	revision  = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
)

func previewAnswer(version int) map[string]any {
	answer := map[string]any{
		"operation": "preview", "resolution": "stored", "planRevision": revision,
		"documents": []any{map[string]any{"kind": "Expert", "metadata": map[string]any{
			"name": "reviewer", "scope": map[string]any{"kind": "Account", "name": "11111111-1111-1111-1111-111111111111"}}}},
		"dependencies": map[string]any{},
	}
	if version > 0 {
		answer["dependencies"] = map[string]any{
			expertKey:                                map[string]any{"uid": "u1", "resourceVersion": version},
			"Connector/Account/1111/not-in-the-file": map[string]any{"uid": "u2", "resourceVersion": 9},
		}
	}
	return answer
}

func applyAnswer(changed bool) map[string]any {
	return map[string]any{
		"operation": "apply",
		"resources": []any{map[string]any{"uid": "0b5c2f6e-0000-4000-8000-000000000001", "resourceVersion": 4, "changed": changed,
			"resource": map[string]any{"kind": "Expert", "metadata": map[string]any{"name": "reviewer"}}}},
		"executions": map[string]any{"Job/Account/x/run": "7d3f0000-0000-4000-8000-000000000002"},
	}
}

func TestApplyPreviewsFirstAndSendsWhatThePreviewSaw(t *testing.T) {
	h := newHarness(t)
	h.login()
	previews := h.record("POST /api/manifests/preview", 200, previewAnswer(3))
	applies := h.record("POST /api/manifests/apply", 200, applyAnswer(true))
	file := manifestFileIn(t, "team.yaml", expertYAML)

	out, errOut, code := h.run("", "apply", "-f", file, "--idempotency-key", "run-7")
	if code != 0 {
		t.Fatal(errOut)
	}
	if got := (*previews)[0]; got["resolution"] != "stored" || got["source"] != expertYAML {
		t.Errorf("preview: %v", got)
	}
	sent := (*applies)[0]
	// Only the versions of the documents in the file, as the preview saw them.
	versions := sent["expected_versions"].(map[string]any)
	if len(versions) != 1 || versions[expertKey] != float64(3) || sent["plan_revision"] != revision || sent["idempotency_key"] != "run-7" {
		t.Errorf("apply: %v", sent)
	}
	// The answer is the server's JSON; the summary is for the person.
	var answer map[string]any
	if json.Unmarshal([]byte(out), &answer) != nil || answer["operation"] != "apply" {
		t.Errorf("stdout: %s", out)
	}
	want(t, errOut, "Expert/reviewer changed (version 4)", "job/7d3f0000-0000-4000-8000-000000000002 created")

	out, _, _ = h.run("", "apply", "-f", file, "-o", "name")
	if out != "expert/0b5c2f6e-0000-4000-8000-000000000001\njob/7d3f0000-0000-4000-8000-000000000002\n" {
		t.Errorf("names: %q", out)
	}
}

func TestApplyWithAReviewedPlanSendsOneRequest(t *testing.T) {
	h := newHarness(t)
	h.login()
	previews := h.record("POST /api/manifests/preview", 200, previewAnswer(3))
	applies := h.record("POST /api/manifests/apply", 200, applyAnswer(false))
	file := manifestFileIn(t, "team.yaml", expertYAML)
	plan := manifestFileIn(t, "plan.json", `{"operation":"preview","resolution":"stored","planRevision":"`+revision+`"}`)
	versionsFile := manifestFileIn(t, "versions.json", `{"Expert/a": 2}`)

	_, errOut, code := h.run("", "apply", "-f", file, "--plan", plan, "--expected-versions", versionsFile, "--expected-version", "Expert/b/c=7",
		"--scope-kind", "Catalog", "--scope-name", "shared")
	if code != 0 || len(*previews) != 0 {
		t.Fatalf("code %d, previews %d: %s", code, len(*previews), errOut)
	}
	sent := (*applies)[0]
	versions := sent["expected_versions"].(map[string]any)
	scope := sent["default_scope"].(map[string]any)
	if sent["plan_revision"] != revision || versions["Expert/a"] != float64(2) || versions["Expert/b/c"] != float64(7) || scope["kind"] != "Catalog" || scope["name"] != "shared" {
		t.Errorf("apply: %v", sent)
	}
	want(t, errOut, "Expert/reviewer unchanged (version 4)")

	for name, content := range map[string]string{
		"a bundle preview":  `{"operation":"preview","resolution":"bundle","planRevision":"` + revision + `"}`,
		"no plan revision":  `{"operation":"preview","resolution":"stored"}`,
		"a key twice":       `{"operation":"preview","operation":"preview","resolution":"stored","planRevision":"` + revision + `"}`,
		"not an object":     `[1]`,
		"a version as text": ``,
	} {
		bad := manifestFileIn(t, "bad.json", content)
		args := []string{"apply", "-f", file, "--plan", bad}
		if name == "a version as text" {
			args = []string{"apply", "-f", file, "--expected-versions", manifestFileIn(t, "v.json", `{"Expert/a": "2"}`)}
		}
		if _, _, code := h.run("", args...); code != exit.Usage {
			t.Errorf("%s: code %d", name, code)
		}
	}
}

func TestApplyDryRunStopsAfterThePreview(t *testing.T) {
	h := newHarness(t)
	h.login()
	h.record("POST /api/manifests/preview", 200, previewAnswer(0))
	applies := h.record("POST /api/manifests/apply", 200, applyAnswer(true))
	out, _, code := h.run("", "apply", "-f", manifestFileIn(t, "team.yaml", expertYAML), "--dry-run")
	if code != 0 || len(*applies) != 0 || !strings.Contains(out, revision) {
		t.Errorf("code %d, applies %d: %s", code, len(*applies), out)
	}
}

func TestAChangeAfterThePreviewIsExitFiveWithAWayOut(t *testing.T) {
	h := newHarness(t)
	h.login()
	h.record("POST /api/manifests/preview", 200, previewAnswer(3))
	h.record("POST /api/manifests/apply", 409, map[string]any{"detail": map[string]any{"code": "DependencyChanged", "message": "The reviewed plan changed; preview it again."}})
	_, errOut, code := h.run("", "apply", "-f", manifestFileIn(t, "team.yaml", expertYAML))
	if code != exit.Conflict {
		t.Fatalf("code %d", code)
	}
	var failure struct {
		Error map[string]any `json:"error"`
	}
	if json.Unmarshal([]byte(errOut), &failure) != nil || failure.Error["code"] != "DependencyChanged" || !strings.Contains(failure.Error["hint"].(string), "Run the command again") {
		t.Errorf("stderr: %s", errOut)
	}
}

const (
	storedUID = "0b5c2f6e-0000-4000-8000-000000000001"
	otherUID  = "0b5c2f6e-0000-4000-8000-000000000002"
)

func storedView(uid, kindName, name string, redacted bool, config map[string]any) map[string]any {
	return map[string]any{
		"uid": uid, "resourceVersion": 4, "redacted": redacted,
		"resource": map[string]any{"apiVersion": "srw/v1alpha1", "kind": kindName,
			"metadata": map[string]any{"name": name, "scope": map[string]any{"kind": "Account", "name": "1111"}},
			"spec":     map[string]any{"runtime": map[string]any{"config": config}}},
	}
}

func TestExportGuards(t *testing.T) {
	h := newHarness(t)
	h.login()
	exports := h.record("POST /api/manifests/export", 200, map[string]any{"format": "yaml", "source": "kind: Expert\n"})

	h.record("GET /api/resources/"+storedUID, 200, storedView(storedUID, "Expert", "reviewer", false, map[string]any{"model": "x"}))
	out, errOut, code := h.run("", "export", storedUID)
	if code != 0 || out != "kind: Expert\n" || errOut != "" {
		t.Fatalf("a plain export: %d %q %q", code, out, errOut)
	}
	if sent := (*exports)[0]; sent["format"] != "json" || sent["output_format"] != "yaml" || !strings.Contains(sent["source"].(string), `"kind":"Expert"`) {
		t.Errorf("export request: %v", sent)
	}
	h.run("", "export", storedUID, "-o", "json")
	if sent := (*exports)[1]; sent["output_format"] != "json" {
		t.Errorf("-o json: %v", sent)
	}

	// An owner's export can hold credentials: a warning that names the keys.
	h.record("GET /api/resources/"+storedUID, 200, storedView(storedUID, "Expert", "reviewer", false,
		map[string]any{"llm": map[string]any{"api_key": "sk-secret", "model": "x"}, "env_keys": map[string]any{"OPENROUTER_API_KEY": "sk-2", "EMBEDDING_API_KEY": ""}}))
	_, errOut, code = h.run("", "export", storedUID)
	if code != 0 || !strings.Contains(errOut, "contains credentials (OPENROUTER_API_KEY, api_key)") || strings.Contains(errOut, "sk-") || strings.Contains(errOut, "EMBEDDING") {
		t.Errorf("the credentials warning: %d %q", code, errOut)
	}

	// A redacted view is not an export: applying it back would delete what
	// was hidden.
	h.record("GET /api/resources/"+storedUID, 200, storedView(storedUID, "Expert", "reviewer", true, map[string]any{}))
	before := len(*exports)
	_, errOut, code = h.run("", "export", storedUID)
	if code != exit.Failed || len(*exports) != before || !strings.Contains(errOut, "--allow-redacted") {
		t.Errorf("a redacted view: %d %q", code, errOut)
	}
	out, errOut, code = h.run("", "export", storedUID, "--allow-redacted")
	if code != 0 || out == "" || !strings.Contains(errOut, "secret values are removed") {
		t.Errorf("--allow-redacted: %d %q", code, errOut)
	}
}

func listsOf(h *harness) {
	h.record("GET /api/experts", 200, []any{
		map[string]any{"id": "scholar", "name": "scholar", "expert_type": "worker", "source": "bundled", "manifest_uid": nil},
		map[string]any{"id": "aaaaaaaa-0000-4000-8000-000000000001", "name": "reviewer", "expert_type": "worker", "source": "user", "manifest_uid": storedUID},
	})
	h.record("GET /api/projects", 200, []any{
		map[string]any{"id": "bbbbbbbb-0000-4000-8000-000000000001", "name": "website", "status": "active", "manifest_uid": otherUID},
		map[string]any{"id": "bbbbbbbb-0000-4000-8000-000000000002", "name": "twin", "status": "active", "manifest_uid": "x1"},
		map[string]any{"id": "bbbbbbbb-0000-4000-8000-000000000003", "name": "twin", "status": "archived", "manifest_uid": "x2"},
	})
}

func TestGetListsWhatTheUserSeesAsATable(t *testing.T) {
	h := newHarness(t)
	h.login()
	listsOf(h)
	out, errOut, code := h.run("", "get", "experts")
	if code != 0 {
		t.Fatal(errOut)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 3 || strings.Join(strings.Fields(lines[0]), " ") != "NAME TYPE SOURCE ID" ||
		strings.Join(strings.Fields(lines[2]), " ") != "reviewer worker user aaaaaaaa-0000-4000-8000-000000000001" {
		t.Errorf("table:\n%s", out)
	}
	// On a terminal an id is shortened; --no-trunc and a pipe keep it whole.
	h.stdoutTerminal = true
	if out, _, _ := h.run("", "get", "Expert", "--no-headers"); !strings.HasSuffix(strings.Join(strings.Fields(out), " "), "user aaaaaaaa") || strings.Contains(out, "NAME") {
		t.Errorf("on a terminal:\n%q", out)
	}
	if out, _, _ := h.run("", "get", "expert", "--no-trunc"); !strings.Contains(out, "aaaaaaaa-0000-4000-8000-000000000001") {
		t.Errorf("--no-trunc:\n%s", out)
	}
	h.stdoutTerminal = false

	// One object by name or by id, in every format.
	out, _, code = h.run("", "get", "project", "website", "-o", "json")
	var project map[string]any
	if code != 0 || json.Unmarshal([]byte(out), &project) != nil || project["name"] != "website" {
		t.Errorf("one project: %d %s", code, out)
	}
	if out, _, _ := h.run("", "get", "project/bbbbbbbb-0000-4000-8000-000000000001", "-o", "name"); out != "project/bbbbbbbb-0000-4000-8000-000000000001\n" {
		t.Errorf("by id: %q", out)
	}
	out, _, _ = h.run("", "get", "projects", "-o", "json")
	var list struct {
		Items []map[string]any `json:"items"`
	}
	if json.Unmarshal([]byte(out), &list) != nil || len(list.Items) != 3 {
		t.Errorf("a list is {items}: %s", out)
	}

	// A name that two objects share is an error that lists them.
	_, errOut, code = h.run("", "get", "project", "twin")
	if code != exit.Usage || !strings.Contains(errOut, "bbbbbbbb-0000-4000-8000-000000000002") || !strings.Contains(errOut, "bbbbbbbb-0000-4000-8000-000000000003") {
		t.Errorf("an ambiguous name: %d %s", code, errOut)
	}
	if _, _, code := h.run("", "get", "project", "nope"); code != exit.NotFound {
		t.Errorf("a missing name: %d", code)
	}
	if _, errOut, code := h.run("", "get", "gadgets"); code != exit.Usage || !strings.Contains(errOut, "expert, project, workspacetemplate, connector") {
		t.Errorf("an unknown kind: %d %s", code, errOut)
	}
}

func TestConnectorsArePagedAndFilteredOnTheClient(t *testing.T) {
	h := newHarness(t)
	h.login()
	var queries []string
	h.handle("GET /api/datasources/catalog", func(w http.ResponseWriter, r *http.Request) {
		queries = append(queries, r.URL.RawQuery)
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("cursor") == "" {
			// A server that ignores q returns everything.
			json.NewEncoder(w).Encode(map[string]any{"items": []any{map[string]any{"id": "c1", "name": "github-main", "type": "git"}}, "next_cursor": "page2"})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"items": []any{map[string]any{"id": "c2", "name": "github", "type": "git"}}, "next_cursor": nil})
	})
	out, _, code := h.run("", "get", "connector", "github", "-o", "name")
	if code != 0 || out != "connector/c2\n" {
		t.Errorf("%d %q", code, out)
	}
	if len(queries) != 2 || !strings.Contains(queries[0], "q=github") || !strings.Contains(queries[1], "cursor=page2") {
		t.Errorf("queries: %v", queries)
	}
}

func TestStoredLookupsNeedAServerThatFiltersByName(t *testing.T) {
	h := newHarness(t)
	h.login()
	listsOf(h)
	stored := h.record("GET /api/resources", 200, map[string]any{"truncated": false, "resources": []any{
		storedView(storedUID, "WorkspaceTemplate", "python-dev", false, nil),
		storedView(otherUID, "WorkspaceTemplate", "node-dev", false, nil),
	}})
	out, errOut, code := h.run("", "get", "wt", "--project", "website")
	if code != 0 {
		t.Fatal(errOut)
	}
	want(t, out, "python-dev  Account/1111  4        "+storedUID)
	want(t, errOut, "Stored workspacetemplates of project bbbbbbbb-0000-4000-8000-000000000001")
	if query := (*stored)[0]["_query"].(string); !strings.Contains(query, "kind=WorkspaceTemplate") || !strings.Contains(query, "scope_kind=Project") || !strings.Contains(query, "scope_name=bbbbbbbb-0000-4000-8000-000000000001") {
		t.Errorf("query: %s", query)
	}
	// The name is sent, and filtered again here.
	out, _, _ = h.run("", "get", "wt", "node-dev", "-o", "name")
	if out != "workspacetemplate/"+otherUID+"\n" || !strings.Contains((*stored)[1]["_query"].(string), "name=node-dev") {
		t.Errorf("by name: %q %v", out, (*stored)[1])
	}
	// A server without `truncated` cannot be trusted for a name.
	h.record("GET /api/resources", 200, map[string]any{"resources": []any{storedView(storedUID, "WorkspaceTemplate", "python-dev", false, nil)}})
	h.record("POST /api/manifests/export", 200, map[string]any{"source": "x"})
	_, errOut, code = h.run("", "export", "wt", "python-dev")
	if code != exit.Failed || !strings.Contains(errOut, "cannot look a stored resource up by name") {
		t.Errorf("an older server: %d %s", code, errOut)
	}
	// A cut list is said.
	h.record("GET /api/resources", 200, map[string]any{"truncated": true, "resources": []any{}})
	if _, errOut, _ := h.run("", "get", "--kind", "Expert"); !strings.Contains(errOut, "the list is cut") {
		t.Errorf("truncated: %s", errOut)
	}
}

func TestDeleteAsksOrNeedsYes(t *testing.T) {
	h := newHarness(t)
	h.login()
	listsOf(h)
	h.record("GET /api/resources/"+storedUID, 200, storedView(storedUID, "Expert", "reviewer", false, nil))
	h.record("GET /api/resources/"+otherUID, 200, storedView(otherUID, "Project", "website", false, nil))
	deleted := h.record("DELETE /api/resources/"+storedUID, 200, map[string]any{"deleted": true, "uid": storedUID})
	deletedProject := h.record("DELETE /api/resources/"+otherUID, 202, map[string]any{"deleted": false, "deleting": true, "uid": otherUID})

	// The older form: a UID and its version, no question.
	out, _, code := h.run("", "delete", storedUID, "--expected-version", "4")
	if code != 0 || !strings.Contains(out, `"deleted": true`) || (*deleted)[0]["_query"] != "expected_version=4" {
		t.Fatalf("by UID and version: %d %s %v", code, out, *deleted)
	}
	// Not on a terminal: --yes, or nothing happens.
	if _, errOut, code := h.run("", "delete", "expert", "reviewer"); code != exit.Usage || !strings.Contains(errOut, "--yes") || len(*deleted) != 1 {
		t.Errorf("without a terminal: %d %s", code, errOut)
	}
	if _, _, code := h.run("", "delete", "expert", "reviewer", "--yes"); code != 0 || (*deleted)[1]["_query"] != "expected_version=4" {
		t.Errorf("--yes: %d %v", code, *deleted)
	}
	// On a terminal the question names what goes away.
	h.terminal, h.stdoutTerminal = true, true
	_, errOut, code := h.run("n\n", "delete", "expert", "reviewer")
	if code != exit.Failed || len(*deleted) != 2 || !strings.Contains(errOut, `Delete Expert "reviewer" in Account 1111 (UID `+storedUID) {
		t.Errorf("answered no: %d %s", code, errOut)
	}
	_, errOut, code = h.run("y\n", "delete", "project", "website")
	if code != 0 || len(*deletedProject) != 1 {
		t.Fatalf("answered yes: %d %s", code, errOut)
	}
	want(t, errOut, "This deletes the whole project: its repositories, its cloud folder and its group", "continues in the background")
	// A built-in expert is not a stored resource.
	if _, errOut, code := h.run("y\n", "delete", "expert", "scholar"); code != exit.Failed || !strings.Contains(errOut, "not a stored resource") {
		t.Errorf("a bundled expert: %d %s", code, errOut)
	}
}

func TestTheOlderFormsKeepTheirMeaning(t *testing.T) {
	h := newHarness(t)
	h.login()
	h.record("GET /api/resources/"+storedUID, 200, storedView(storedUID, "Expert", "reviewer", false, nil))
	stored := h.record("GET /api/resources", 200, map[string]any{"truncated": false, "resources": []any{storedView(storedUID, "Expert", "reviewer", false, nil)}})
	cache := h.record("GET /api/workspace-cache", 200, map[string]any{"artifacts": []any{}})
	evicted := h.record("DELETE /api/workspace-cache/"+storedUID, 200, map[string]any{"deleted": true})

	out, _, code := h.run("", "get", storedUID)
	if code != 0 || !strings.Contains(out, `"resourceVersion": 4`) {
		t.Errorf("get UID: %d %s", code, out)
	}
	out, _, code = h.run("", "get", "--kind", "Expert", "--scope-kind", "Catalog", "--scope-name", "shared")
	if code != 0 || !strings.Contains(out, `"resources"`) || !strings.Contains((*stored)[0]["_query"].(string), "kind=Expert") || !strings.Contains((*stored)[0]["_query"].(string), "scope_kind=Catalog") {
		t.Errorf("get --kind: %d %s %v", code, out, *stored)
	}
	if h.run("", "get"); !strings.Contains((*stored)[1]["_query"].(string), "scope_kind=Account&scope_name=me") {
		t.Errorf("the default scope: %v", (*stored)[1])
	}
	for _, refused := range [][]string{
		{"get", storedUID, "--kind", "Expert"},
		{"get", storedUID, "--account"},
		{"get", "--kind", "expert"},
		{"export", storedUID, "--catalog"},
		{"cache", "delete", "not-a-uuid"},
	} {
		if _, _, code := h.run("", refused...); code != exit.Usage {
			t.Errorf("%v: code %d", refused, code)
		}
	}
	if out, _, code := h.run("", "cache", "list", "--catalog"); code != 0 || !strings.Contains(out, "artifacts") || (*cache)[0]["_query"] != "scope_kind=Catalog&scope_name=shared" {
		t.Errorf("cache list: %d %s", code, out)
	}
	if _, _, code := h.run("", "cache", "delete", storedUID); code != 0 || (*evicted)[0]["_query"] != "scope_kind=Account&scope_name=me" {
		t.Errorf("cache delete: %d %v", code, *evicted)
	}
	// A manifest error is JSON on stderr, with its place.
	h.record("POST /api/manifests/validate", 422, map[string]any{"detail": map[string]any{"code": "InvalidManifest", "message": "Field violates the oneOf constraint.", "document": 2, "path": "/spec"}})
	file := manifestFileIn(t, "team.yaml", expertYAML)
	_, errOut, code := h.run("", "validate", "-f", file)
	if code != exit.Failed || errOut != `{"error":{"code":"InvalidManifest","document":2,"file":"`+file+`","message":"Field violates the oneOf constraint.","path":"/spec","status":422}}`+"\n" {
		t.Errorf("a manifest error: %d %s", code, errOut)
	}
}
