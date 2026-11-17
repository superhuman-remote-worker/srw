package cmd

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/superhuman-remote-worker/srw/cli/internal/exit"
)

const (
	jobID    = "0b5c2f6e-1111-4000-8000-000000000001"
	otherJob = "0b5c2f6e-2222-4000-8000-000000000002"
)

func jobRoute(suffix string) string { return "/api/jobs/" + jobID + suffix }

// progressOf answers the progress route with these statuses in turn; the
// last one repeats. A status may carry ":held" for a job held by a person.
// progressOf scripts the answers to the progress route, one per poll; the
// last one repeats. "status:held" is held by a person. "away:<how>" is a
// server that does not answer as itself (see awayAnswer).
func progressOf(h *harness, statuses ...string) *int {
	polls := 0
	h.handle("GET "+jobRoute("/progress"), func(w http.ResponseWriter, _ *http.Request) {
		status := statuses[min(polls, len(statuses)-1)]
		polls++
		if how, away := strings.CutPrefix(status, "away:"); away {
			awayAnswer(w, how)
			return
		}
		answer := map[string]any{"status": status, "state": "running", "reasons": []string{}, "held_by": nil}
		if name, found := strings.CutSuffix(status, ":held"); found {
			answer["status"], answer["held_by"] = name, "operator"
		}
		if status == "cancelled:blocked" {
			answer["status"], answer["completion_outcome_kind"] = "cancelled", "blocked_undelivered"
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(answer)
	})
	return &polls
}

// awayAnswer answers as the things in front of a restarting server do.
func awayAnswer(w http.ResponseWriter, how string) {
	switch how {
	case "404": // an ingress without a backend
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(404)
		w.Write([]byte("404 page not found\n"))
	case "502":
		w.WriteHeader(502)
		w.Write([]byte("<html>Bad Gateway</html>"))
	case "page": // a web server that answers its app for any path
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte("<!doctype html><title>Cockpit</title>"))
	case "503": // the API itself, not ready yet
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(503)
		w.Write([]byte(`{"detail":"Database unavailable"}`))
	case "drop": // the connection dies
		if connection, _, err := w.(http.Hijacker).Hijack(); err == nil {
			connection.Close()
		}
	}
}

// An upgrade of the server must not end a wait that a pipeline sits in.
func TestWaitAsksAgainWhileTheServerIsAway(t *testing.T) {
	for _, how := range []string{"404", "502", "page", "503", "drop"} {
		t.Run(how, func(t *testing.T) {
			h := newHarness(t)
			h.login()
			// Four in a row: a GET is tried three times on a dropped
			// connection, so fewer would not fail a whole poll.
			away := "away:" + how
			progressOf(h, "processing", away, away, away, away, "processing", "completed")
			out, errOut, code := h.run("", "wait", jobID)
			if code != exit.OK || out != "job/"+jobID+" completed\n" {
				t.Fatalf("%d %q %s", code, out, errOut)
			}
			// Said once per outage, and once when it is over.
			if strings.Count(errOut, "The server stopped answering") != 1 || strings.Count(errOut, "The server answers again.") != 1 {
				t.Errorf("stderr:\n%s", errOut)
			}
			// The status did not change across the outage: not printed twice.
			if strings.Count(errOut, "job/"+jobID+" processing\n") != 1 {
				t.Errorf("stderr:\n%s", errOut)
			}
		})
	}
}

func TestWaitGivesUpOnAServerThatStaysAway(t *testing.T) {
	h := newHarness(t)
	h.login()
	progressOf(h, "processing", "away:404")
	out, errOut, code := h.run("", "wait", jobID)
	if code != exit.Unreachable || out != "" {
		t.Fatalf("%d %q %s", code, out, errOut)
	}
	want(t, errOut, "the server has not answered for 10m0s", "srw wait job/"+jobID)
	// It asked for the whole grace and not much longer.
	if waited := h.clock.Sub(now); waited < awayGrace || waited > awayGrace+time.Minute {
		t.Errorf("waited %s", waited)
	}
	// --timeout still ends the wait first, and says what it knows.
	h.clock = now
	progressOf(h, "processing", "away:502")
	_, errOut, code = h.run("", "wait", jobID, "--timeout", "1m")
	if code != exit.WaitTimeout {
		t.Fatalf("%d %s", code, errOut)
	}
	want(t, errOut, "the server stopped answering; the job was processing")
}

// Before the server has answered once, nothing says the address is right:
// the error comes at once. And an answer of the API itself is never an
// outage.
func TestWaitDoesNotAskAgainWithoutReason(t *testing.T) {
	h := newHarness(t)
	h.login()
	polls := progressOf(h, "away:404")
	_, errOut, code := h.run("", "wait", jobID)
	// "Not reached", not "not found": a proxy's 404 says nothing about the job.
	if code != exit.Unreachable || *polls != 1 {
		t.Fatalf("%d after %d polls: %s", code, *polls, errOut)
	}
	want(t, errOut, "is not answering", "from something in front of it")

	asked := h.record("GET "+jobRoute("/progress"), 404, map[string]string{"detail": "Job '" + jobID + "' not found"})
	if _, errOut, code := h.run("", "wait", jobID); code != exit.NotFound || len(*asked) != 1 {
		t.Errorf("the API's own 404: %d after %d polls: %s", code, len(*asked), errOut)
	}
}

func TestAJobIsNamedByItsIdOrTheStartOfIt(t *testing.T) {
	h := newHarness(t)
	h.login()
	progressOf(h, "completed")
	searches := h.record("GET /api/jobs", 200, map[string]any{"has_more": false, "jobs": []any{
		// The search also matches titles; only ids that start with the
		// prefix count.
		map[string]any{"id": "ffffffff-0000-4000-8000-000000000009", "title": "about 0b5c2f6e"},
		map[string]any{"id": jobID},
	}})
	for _, typed := range []string{jobID, "job/" + jobID, "JOBS/" + strings.ToUpper(jobID), "0b5c2f6e-1111", "job/0b5c2f6e-1111"} {
		if out, errOut, code := h.run("", "wait", typed); code != 0 || out != "job/"+jobID+" completed\n" {
			t.Errorf("%s: %d %q %q", typed, code, out, errOut)
		}
	}
	if query := (*searches)[0]["_query"].(string); !strings.Contains(query, "search=0b5c2f6e-1111") || !strings.Contains(query, "include_archived_projects=true") {
		t.Errorf("search: %s", query)
	}
	h.record("GET /api/jobs", 200, map[string]any{"has_more": false, "jobs": []any{map[string]any{"id": jobID}, map[string]any{"id": otherJob}}})
	if _, errOut, code := h.run("", "wait", "0b5c2f6e"); code != exit.Usage || !strings.Contains(errOut, otherJob) {
		t.Errorf("an ambiguous prefix: %d %s", code, errOut)
	}
	if _, _, code := h.run("", "wait", "aaaaaaaa"); code != exit.NotFound {
		t.Errorf("no such job: %d", code)
	}
	for _, typed := range []string{"0b5c", "not-an-id!", "expert/" + jobID} {
		if _, _, code := h.run("", "wait", typed); code != exit.Usage {
			t.Errorf("%q: %d", typed, code)
		}
	}
}

func TestWaitExitsByTheOutcome(t *testing.T) {
	cases := []struct {
		name     string
		statuses []string
		args     []string
		code     int
		last     string
	}{
		{"completed", []string{"created", "processing", "completed"}, nil, exit.OK, "completed"},
		{"failed", []string{"processing", "failed"}, nil, exit.WaitFailed, "failed"},
		{"cancelled, with its outcome", []string{"cancelled:blocked"}, nil, exit.WaitFailed, "cancelled (blocked_undelivered)"},
		{"stopped for review", []string{"processing", "pending_review"}, nil, exit.WaitNeedsPerson, "pending_review"},
		{"waiting for a reply", []string{"waiting_for_reply"}, nil, exit.WaitNeedsPerson, "waiting_for_reply"},
		{"paused by a person", []string{"processing", "paused:held"}, nil, exit.WaitNeedsPerson, "paused"},
		// A pause without a hold is the system's: the job is picked up again.
		{"paused by the system", []string{"paused", "paused", "processing", "completed"}, nil, exit.OK, "completed"},
		{"a status srw does not know", []string{"reticulating", "completed"}, nil, exit.OK, "completed"},
		{"--for reached", []string{"processing", "pending_review"}, []string{"--for", "status=pending_review"}, exit.OK, "pending_review"},
		{"--for one of two", []string{"paused"}, []string{"--for", "status=completed,paused"}, exit.OK, "paused"},
		{"--for out of reach", []string{"processing", "failed"}, []string{"--for", "status=pending_review"}, exit.WaitFailed, "failed"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			h := newHarness(t)
			h.login()
			polls := progressOf(h, test.statuses...)
			out, errOut, code := h.run("", append([]string{"wait", jobID}, test.args...)...)
			if code != test.code {
				t.Fatalf("code %d, want %d: %s", code, test.code, errOut)
			}
			// The last state line goes to stdout, progress to stderr.
			if out != "job/"+jobID+" "+test.last+"\n" {
				t.Errorf("stdout: %q", out)
			}
			if *polls != len(test.statuses) {
				t.Errorf("polls: %d, want %d", *polls, len(test.statuses))
			}
			// The status that ends the wait is printed in one place.
			if strings.Contains(errOut, strings.TrimSpace(out)+"\n") {
				t.Errorf("the last status is on stderr too: %q", errOut)
			}
		})
	}
}

func TestWaitBacksOffAndTimesOut(t *testing.T) {
	h := newHarness(t)
	h.login()
	progressOf(h, "processing")
	_, errOut, code := h.run("", "wait", jobID, "--timeout", "1m")
	if code != exit.WaitTimeout || !strings.Contains(errOut, "keeps running") {
		t.Fatalf("%d %s", code, errOut)
	}
	// From 2 s, growing, never above 15 s.
	if h.sleeps[0] != 2*time.Second || h.sleeps[1] != 3*time.Second || h.sleeps[len(h.sleeps)-1] != 15*time.Second {
		t.Errorf("pauses: %v", h.sleeps)
	}
	h.sleeps, h.clock = nil, now
	h.run("", "wait", jobID, "--timeout", "20s", "--interval", "5s")
	if len(h.sleeps) != 4 || h.sleeps[3] != 5*time.Second {
		t.Errorf("a fixed interval: %v", h.sleeps)
	}
	if _, _, code := h.run("", "wait", jobID, "--for", "pending_review"); code != exit.Usage {
		t.Errorf("--for without status=: %d", code)
	}
}

func TestRunSendsWhatWasAskedAndNothingElse(t *testing.T) {
	h := newHarness(t)
	h.login()
	listsOf(h)
	created := h.record("POST /api/jobs", 200, map[string]any{"id": jobID, "status": "created", "execution_lane": "pinned", "project_id": nil})

	out, errOut, code := h.run("", "run", "Summarise the open issues")
	if code != 0 || out != "job/"+jobID+" created\n" {
		t.Fatalf("%d %q %s", code, out, errOut)
	}
	// Only the task, and the connector default the Cockpit's form uses.
	sent := (*created)[0]
	delete(sent, "_query")
	if len(sent) != 2 || sent["description"] != "Summarise the open issues" || sent["use_datasource_defaults"] != true {
		t.Errorf("a bare run: %v", sent)
	}

	h.run("", "run", "Task", "--expert", "scholar", "-p", "website", "--model", "gpt-x", "--autonomy", "full",
		"--phases", "--lane", "stateless", "--priority", "8", "--connector", "c1", "--connector", "c2")
	sent = (*created)[1]
	override := sent["config_override"].(map[string]any)
	if sent["expert"] != "scholar" || sent["project_id"] != "bbbbbbbb-0000-4000-8000-000000000001" || sent["phases"] != true ||
		sent["execution_lane"] != "stateless" || sent["priority"] != float64(8) || override["autonomy"] != "full" ||
		override["llm"].(map[string]any)["model"] != "gpt-x" || len(sent["datasource_ids"].([]any)) != 2 || sent["use_datasource_defaults"] != nil {
		t.Errorf("every flag: %v", sent)
	}
	// phases goes into its own field, never into config_override.
	if _, inOverride := override["phases"]; inOverride {
		t.Errorf("phases in config_override: %v", override)
	}
	h.run("", "run", "Task", "--phases=false", "--no-connectors")
	sent = (*created)[2]
	if sent["phases"] != false || len(sent["datasource_ids"].([]any)) != 0 {
		t.Errorf("--phases=false and --no-connectors: %v", sent)
	}

	// The task from a file and from standard input.
	file := manifestFileIn(t, "task.md", "\n  From a file\nsecond line\n")
	h.run("", "run", "-f", file)
	h.run("From standard input\n", "run")
	if (*created)[3]["description"] != "From a file\nsecond line" || (*created)[4]["description"] != "From standard input" {
		t.Errorf("task sources: %v %v", (*created)[3]["description"], (*created)[4]["description"])
	}
	if out, _, _ := h.run("", "run", "Task", "-o", "name"); out != "job/"+jobID+"\n" {
		t.Errorf("-o name: %q", out)
	}
	// The server may assign another lane than the one asked for.
	if _, errOut, _ := h.run("", "run", "Task", "--lane", "stateless"); !strings.Contains(errOut, "runs on the pinned lane") {
		t.Errorf("the lane note: %q", errOut)
	}
	before := len(*created)
	for name, args := range map[string][]string{
		"an unknown autonomy":         {"run", "Task", "--autonomy", "careful"},
		"an unknown lane":             {"run", "Task", "--lane", "fast"},
		"a priority out of range":     {"run", "Task", "--priority", "11"},
		"both connector flags":        {"run", "Task", "--connector", "c1", "--no-connectors"},
		"a timeout without --wait":    {"run", "Task", "--timeout", "1m"},
		"a task twice":                {"run", "Task", "-f", file},
		"two arguments":               {"run", "Two", "words"},
		"an empty task":               {"run", "   "},
		"the task and token on stdin": {"run", "-f", "-", "--token-stdin"},
	} {
		if _, _, code := h.run("", args...); code != exit.Usage {
			t.Errorf("%s: %d", name, code)
		}
	}
	h.terminal = true
	if _, _, code := h.run("", "run"); code != exit.Usage || len(*created) != before {
		t.Errorf("no task on a terminal: %d", code)
	}
}

func TestRunNamesAStoredExpertByTheNameItWasAppliedUnder(t *testing.T) {
	h := newHarness(t)
	h.login()
	// The server lists an expert stored from a manifest as "<name>-<uid>".
	experts := h.record("GET /api/experts", 200, []any{
		map[string]any{"id": "scholar", "name": "scholar", "display_name": "Scholar", "manifest_uid": "17f941d4-6fd9-437f-a44d-c1211c060af1"},
		map[string]any{"id": "c282e3f4-0000-4000-8000-000000000001", "name": "reviewer-fc05181c152241e0b3e6749fd1a92d40", "display_name": "Code reviewer", "manifest_uid": "fc05181c-1522-41e0-b3e6-749fd1a92d40"},
	})
	created := h.record("POST /api/jobs", 200, map[string]any{"id": jobID})
	for typed, sent := range map[string]string{
		"reviewer":      "c282e3f4-0000-4000-8000-000000000001",
		"Code reviewer": "c282e3f4-0000-4000-8000-000000000001",
		"scholar":       "scholar",
		// Unknown here: sent as typed, the server decides.
		"brand-new": "brand-new",
	} {
		h.run("", "run", "Task", "--expert", typed)
		if got := (*created)[len(*created)-1]["expert"]; got != sent {
			t.Errorf("--expert %s sent %v, want %s", typed, got, sent)
		}
	}
	// An id needs no lookup, for the expert or for the project.
	before := len(*experts)
	h.run("", "run", "Task", "--expert", "c282e3f4-0000-4000-8000-000000000001", "-p", "BBBBBBBB-0000-4000-8000-000000000001")
	sent := (*created)[len(*created)-1]
	if len(*experts) != before || sent["project_id"] != "bbbbbbbb-0000-4000-8000-000000000001" {
		t.Errorf("ids were looked up: %v", sent)
	}
	// The table shows the name a person can type, and the lookup takes it.
	out, _, _ := h.run("", "get", "experts", "--no-headers")
	if !strings.Contains(out, "reviewer ") || strings.Contains(out, "fc05181c152241e0") {
		t.Errorf("table:\n%s", out)
	}
	if out, _, _ := h.run("", "get", "expert", "reviewer", "-o", "name"); out != "expert/c282e3f4-0000-4000-8000-000000000001\n" {
		t.Errorf("by its applied name: %q", out)
	}
	h.record("GET /api/resources/fc05181c-1522-41e0-b3e6-749fd1a92d40", 200, storedView("fc05181c-1522-41e0-b3e6-749fd1a92d40", "Expert", "reviewer", false, nil))
	h.record("POST /api/manifests/export", 200, map[string]any{"source": "kind: Expert\n"})
	if out, _, code := h.run("", "export", "expert", "reviewer"); code != 0 || out != "kind: Expert\n" {
		t.Errorf("export by its applied name: %d %q", code, out)
	}
}

func TestRunWaitExitsByTheOutcomeAndPrintsTheIdFirst(t *testing.T) {
	h := newHarness(t)
	h.login()
	h.record("POST /api/jobs", 200, map[string]any{"id": jobID, "status": "created"})
	progressOf(h, "created", "processing", "pending_review")
	out, errOut, code := h.run("", "run", "Task", "--wait")
	if code != exit.WaitNeedsPerson {
		t.Fatalf("%d %s", code, errOut)
	}
	if out != "job/"+jobID+" created\njob/"+jobID+" pending_review\n" {
		t.Errorf("stdout: %q", out)
	}
	want(t, errOut, "srw approve job/"+jobID)

	// The start was answered, so the server is known: an outage that begins
	// before the first poll is ridden out too.
	progressOf(h, "away:404", "away:404", "completed")
	if out, errOut, code := h.run("", "run", "Task", "--wait"); code != exit.OK || !strings.HasSuffix(out, "job/"+jobID+" completed\n") {
		t.Errorf("%d %q %s", code, out, errOut)
	}
}

func TestALostAnswerToRunIsOutcomeUnknown(t *testing.T) {
	h := newHarness(t)
	h.login()
	h.handle("POST /api/jobs", func(w http.ResponseWriter, _ *http.Request) {
		if connection, _, err := w.(http.Hijacker).Hijack(); err == nil {
			connection.Close()
		}
	})
	_, errOut, code := h.run("", "run", "Task")
	if code != exit.Unknown || !strings.Contains(errOut, "srw get jobs") {
		t.Errorf("%d %s", code, errOut)
	}
}

func TestLifecycleVerbsPrintTheServersWord(t *testing.T) {
	h := newHarness(t)
	h.login()
	cancel := h.record("PUT "+jobRoute("/cancel"), 200, map[string]any{"status": "cancelled"})
	pause := h.record("PUT "+jobRoute("/pause"), 200, map[string]any{"status": "paused", "job_id": jobID})
	resume := h.record("POST "+jobRoute("/resume"), 200, map[string]any{"status": "queued", "message": "Job queued for an agent"})
	approve := h.record("POST "+jobRoute("/approve"), 200, map[string]any{"status": "approved_continue"})

	if out, _, code := h.run("", "cancel", "job/"+jobID); code != 0 || out != "job/"+jobID+" cancelled\n" || len(*cancel) != 1 {
		t.Errorf("cancel: %d %q", code, out)
	}
	if out, _, _ := h.run("", "pause", jobID); out != "job/"+jobID+" paused\n" || len(*pause) != 1 {
		t.Errorf("pause: %q", out)
	}
	if out, _, _ := h.run("", "resume", jobID, "--feedback", "Use the second source"); out != "job/"+jobID+" queued: Job queued for an agent\n" || (*resume)[0]["feedback"] != "Use the second source" {
		t.Errorf("resume: %q %v", out, *resume)
	}
	if out, _, _ := h.run("", "approve", jobID, "--notes", "Looks right", "-o", "json"); !strings.Contains(out, "approved_continue") || (*approve)[0]["notes"] != "Looks right" {
		t.Errorf("approve: %q %v", out, *approve)
	}
	h.record("PUT "+jobRoute("/pause"), 409, map[string]any{"detail": "Only a processing job can be paused"})
	if _, errOut, code := h.run("", "pause", jobID); code != exit.Conflict || !strings.Contains(errOut, "Only a processing job can be paused") {
		t.Errorf("a refused pause: %d %s", code, errOut)
	}
}

func TestDeleteJobNeedsAnEndedJobAndAYes(t *testing.T) {
	h := newHarness(t)
	h.login()
	deleted := h.record("DELETE /api/jobs/"+jobID, 200, map[string]any{"status": "deleted"})
	progressOf(h, "processing")
	if _, errOut, code := h.run("", "delete", "job", jobID, "--yes"); code != exit.Conflict || !strings.Contains(errOut, "srw cancel job/"+jobID) || len(*deleted) != 0 {
		t.Errorf("a running job: %d %s", code, errOut)
	}
	progressOf(h, "failed")
	if _, _, code := h.run("", "delete", "job/"+jobID); code != exit.Usage || len(*deleted) != 0 {
		t.Errorf("without --yes: %d", code)
	}
	if out, _, code := h.run("", "delete", "job", jobID, "--yes"); code != 0 || !strings.Contains(out, "deleted") || len(*deleted) != 1 {
		t.Errorf("--yes: %d %s", code, out)
	}
	if _, _, code := h.run("", "delete", "job", jobID, "--expected-version", "2"); code != exit.Usage {
		t.Errorf("a version for a job: %d", code)
	}
}

func jobRow(id, status, title string, root bool) map[string]any {
	return map[string]any{
		"id": id, "status": status, "title": title, "description": "The task\nwith more", "expert_display_name": "Scholar",
		"project_name": "website", "created_at": "2026-11-17T09:00:00Z", "is_display_root": root,
	}
}

func TestGetJobs(t *testing.T) {
	h := newHarness(t)
	h.login()
	listsOf(h)
	var queries []string
	h.handle("GET /api/jobs", func(w http.ResponseWriter, r *http.Request) {
		queries = append(queries, r.URL.RawQuery)
		w.Header().Set("Content-Type", "application/json")
		if offset := r.URL.Query().Get("offset"); offset == "0" || offset == "" {
			json.NewEncoder(w).Encode(map[string]any{"has_more": true, "jobs": []any{
				jobRow(jobID, "completed", "", true), jobRow(otherJob, "failed", "A child", false)}})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"has_more": false, "jobs": []any{jobRow("0b5c2f6e-3333-4000-8000-000000000003", "processing", "Third", true)}})
	})
	out, errOut, code := h.run("", "get", "jobs", "--status", "failed", "--status", "completed", "-p", "website", "--search", "task", "--limit", "2")
	if code != 0 {
		t.Fatal(errOut)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	// Root jobs only; a job without a title shows the first line of its task.
	if len(lines) != 2 || strings.Join(strings.Fields(lines[0]), " ") != "ID STATUS EXPERT PROJECT CREATED TASK" ||
		strings.Join(strings.Fields(lines[1]), " ") != jobID+" completed Scholar website 2026-11-17T09:00:00Z The task" {
		t.Errorf("table:\n%s", out)
	}
	want(t, errOut, "There are more jobs")
	if query := queries[0]; !strings.Contains(query, "status=failed&status=completed") || !strings.Contains(query, "project_id=bbbbbbbb-0000-4000-8000-000000000001") ||
		!strings.Contains(query, "search=task") || !strings.Contains(query, "limit=2") {
		t.Errorf("query: %s", query)
	}
	out, _, _ = h.run("", "get", "jobs", "--all", "--subjobs", "--limit", "2", "-o", "name")
	if out != "job/"+jobID+"\njob/"+otherJob+"\njob/0b5c2f6e-3333-4000-8000-000000000003\n" {
		t.Errorf("--all --subjobs: %q", out)
	}
	h.stdoutTerminal = true
	if out, _, _ := h.run("", "get", "jobs", "--limit", "2"); !strings.Contains(out, "0b5c2f6e  completed  Scholar  website  3h ago") {
		t.Errorf("on a terminal:\n%s", out)
	}
	h.stdoutTerminal = false
	// One job: the server's object as JSON; in the table the names come
	// from the list row, because the detail has neither.
	detail := jobRow(jobID, "completed", "One job", true)
	delete(detail, "expert_display_name")
	delete(detail, "project_name")
	detail["config_name"] = "worker_base"
	h.record("GET /api/jobs/"+jobID, 200, detail)
	if out, _, _ := h.run("", "get", "job", jobID, "-o", "json"); !strings.Contains(out, `"title": "One job"`) || strings.Contains(out, "Scholar") {
		t.Errorf("one job: %s", out)
	}
	if out, _, _ := h.run("", "get", "job", jobID, "--no-headers"); strings.Join(strings.Fields(out), " ") != jobID+" completed Scholar website 2026-11-17T09:00:00Z The task" {
		t.Errorf("one job as a row: %q", out)
	}
	for _, refused := range [][]string{{"get", "jobs", "--limit", "0"}, {"get", "jobs", "--catalog"}, {"get", "jobs", "--stored"}, {"get", "sessions", "x"}} {
		if _, _, code := h.run("", refused...); code != exit.Usage {
			t.Errorf("%v: %d", refused, code)
		}
	}
	h.record("GET /api/persistent/threads", 200, map[string]any{"threads": []any{map[string]any{"id": "t1", "status": "active", "title": "Planning"}}})
	if out, _, _ := h.run("", "get", "sessions"); !strings.Contains(out, "t1  active  Planning") {
		t.Errorf("sessions:\n%s", out)
	}
}

func TestDescribeJob(t *testing.T) {
	h := newHarness(t)
	h.login()
	h.record("GET /api/jobs/"+jobID, 200, map[string]any{"id": jobID, "title": "Summary", "description": "Summarise\nthe issues", "config_name": "scholar",
		"execution_lane": "pinned", "phases": false, "created_at": "2026-11-17T09:00:00Z"})
	h.record("GET "+jobRoute("/progress"), 200, map[string]any{"status": "paused", "state": "waiting", "reasons": []string{"paused by control plane"}, "held_by": "operator"})
	h.record("GET "+jobRoute("/todos"), 200, map[string]any{"current": []any{map[string]any{"content": "Read the issues", "status": "completed"}, map[string]any{"content": "Write the summary", "status": "pending"}}, "archives": []any{1}})
	h.record("GET "+jobRoute("/frozen"), 200, map[string]any{"summary": "Half done", "deliverables": []string{"output/summary.md"}})
	// No completion report yet: a 404 that means "none".
	h.record("GET "+jobRoute("/completion-report"), 404, map[string]any{"detail": "No completion report"})
	out, errOut, code := h.run("", "describe", "job", jobID)
	if code != 0 {
		t.Fatal(errOut)
	}
	want(t, out, "Job:              "+jobID, "Title:            Summary", "Status:           paused", "State:            waiting: paused by control plane",
		"Held by:          operator", "Plans in phases:  no", "Expert:           scholar", "Task:\n  Summarise\n  the issues",
		"[completed] Read the issues", "[pending] Write the summary", "Archived to-do lists: 1", "summary: Half done", `deliverables: ["output/summary.md"]`)
	if strings.Contains(out, "Completion report") {
		t.Errorf("a missing report was printed:\n%s", out)
	}
	// The detail names neither the expert nor the project; the list row
	// does. And a finished job's report repeats what it handed over, so that
	// is printed once.
	h.record("GET /api/jobs", 200, map[string]any{"has_more": false, "jobs": []any{map[string]any{"id": jobID, "expert_display_name": "Scholar", "project_name": "website"}}})
	h.record("GET /api/jobs/"+jobID, 200, map[string]any{"id": jobID, "config_name": "scholar", "project_id": "bbbbbbbb-0000-4000-8000-000000000001"})
	h.record("GET "+jobRoute("/completion-report"), 200, map[string]any{"report": map[string]any{"summary": "All done"}})
	out, _, _ = h.run("", "describe", "job", jobID)
	want(t, out, "Expert:   Scholar", "Project:  website (bbbbbbbb-0000-4000-8000-000000000001)", "Completion report:\n  summary: All done")
	if strings.Contains(out, "Handed over") {
		t.Errorf("the hand-over was printed beside the report:\n%s", out)
	}
	if out, _, _ := h.run("", "describe", "job/"+jobID, "-o", "name"); out != "job/"+jobID+"\n" {
		t.Errorf("-o name: %q", out)
	}
	if _, _, code := h.run("", "describe", "expert", "x"); code != exit.Usage {
		t.Errorf("describe of another kind: %d", code)
	}
}

func step(number int, kind string, done bool, extra map[string]any) map[string]any {
	entry := map[string]any{"step_number": number, "step_type": kind, "timestamp": "2026-11-17T09:00:0" + string(rune('0'+number)) + "Z", "started_at": "x"}
	if done {
		entry["completed_at"] = "y"
	}
	for key, value := range extra {
		entry[key] = value
	}
	return entry
}

func TestLogs(t *testing.T) {
	h := newHarness(t)
	h.login()
	entries := []any{
		step(1, "llm", true, map[string]any{"llm": map[string]any{"model": "m", "response_content_preview": "I will read the issues\nthen write", "tool_calls": []any{map[string]any{"name": "read_file"}}}}),
		step(2, "memory_store", true, nil),
		step(3, "tool", true, map[string]any{"tool": map[string]any{"name": "read_file", "success": false, "error": "No such file", "result_preview": "x"}}),
		step(4, "error", true, map[string]any{"error": map[string]any{"type": "Timeout", "message": "took too long"}}),
	}
	audit := h.record("GET "+jobRoute("/audit"), 200, map[string]any{"entries": entries, "total": 4, "hasMore": false})
	out, errOut, code := h.run("", "logs", jobID)
	if code != 0 {
		t.Fatal(errOut)
	}
	if out != "   1  09:00:01  model  I will read the issues → read_file\n"+
		"   3  09:00:03  tool   read_file FAILED: No such file\n"+
		"   4  09:00:04  error  Timeout: took too long\n" {
		t.Errorf("log:\n%s", out)
	}
	if query := (*audit)[0]["_query"].(string); !strings.Contains(query, "filter=all") || !strings.Contains(query, "lean=true") || !strings.Contains(query, "order=asc") || !strings.Contains(query, "limit=200") {
		t.Errorf("query: %s", query)
	}
	if out, _, _ := h.run("", "logs", jobID, "--all-steps"); !strings.Contains(out, "memory_store") {
		t.Errorf("--all-steps:\n%s", out)
	}
	// --tail asks for the newest entries and prints them oldest first.
	h.record("GET "+jobRoute("/audit"), 200, map[string]any{"entries": []any{entries[3], entries[2]}, "hasMore": true})
	out, _, _ = h.run("", "logs", jobID, "--tail", "2", "--filter", "errors")
	if !strings.HasPrefix(out, "   3  ") || !strings.Contains(out, "\n   4  ") {
		t.Errorf("--tail:\n%s", out)
	}
	// A failure inside a 200 is not an empty log.
	h.record("GET "+jobRoute("/audit"), 200, map[string]any{"entries": []any{}, "total": 0, "error": "audit store unavailable"})
	if _, errOut, code := h.run("", "logs", jobID); code != exit.Failed || !strings.Contains(errOut, "audit store is not available") {
		t.Errorf("an error envelope: %d %s", code, errOut)
	}
	for _, refused := range [][]string{{"logs", jobID, "--filter", "debug"}, {"logs", jobID, "--tail", "500"}, {"logs", jobID, "--tail", "5", "-f"}, {"logs", jobID, "-o", "name"}} {
		if _, _, code := h.run("", refused...); code != exit.Usage {
			t.Errorf("%v: %d", refused, code)
		}
	}
}

func TestLogsFollowWaitsForAStepToFinish(t *testing.T) {
	h := newHarness(t)
	h.login()
	// Round 1: one finished step and one that is still running. Round 2:
	// the running step has finished and the job has ended.
	round := 0
	h.handle("GET "+jobRoute("/audit"), func(w http.ResponseWriter, r *http.Request) {
		running := step(2, "tool", round >= 2, map[string]any{"tool": map[string]any{"name": "run_command", "success": true, "result_preview": "done"}})
		all := []any{step(1, "llm", true, map[string]any{"llm": map[string]any{"response_content_preview": "Start"}}), running}
		offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
		offset = min(offset, len(all))
		round++
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"entries": all[offset:], "hasMore": false})
	})
	progressOf(h, "processing", "completed", "completed")
	out, errOut, code := h.run("", "logs", jobID, "-f")
	// The exit code is 0 whatever the job's result.
	if code != 0 {
		t.Fatalf("%d %s", code, errOut)
	}
	if strings.Count(out, "\n") != 2 || !strings.Contains(out, "model  Start") || !strings.Contains(out, "tool   run_command ok: done") {
		t.Errorf("each step once, the tool only when finished:\n%s", out)
	}
	want(t, errOut, "job/"+jobID+" is completed")
}

func TestLogsFollowAsksAgainWhileTheServerIsAway(t *testing.T) {
	h := newHarness(t)
	h.login()
	all := []any{
		step(1, "llm", true, map[string]any{"llm": map[string]any{"response_content_preview": "Start"}}),
		step(2, "tool", true, map[string]any{"tool": map[string]any{"name": "run_command", "success": true, "result_preview": "done"}}),
	}
	reads := 0
	h.handle("GET "+jobRoute("/audit"), func(w http.ResponseWriter, r *http.Request) {
		reads++
		if reads == 2 {
			// The log read fails too, after a progress answer that came.
			awayAnswer(w, "502")
			return
		}
		// The second step exists only after the outage.
		have := all[:1]
		if reads > 2 {
			have = all
		}
		offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
		offset = min(offset, len(have))
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"entries": have[offset:], "hasMore": false})
	})
	progressOf(h, "processing", "away:404", "processing", "completed")
	out, errOut, code := h.run("", "logs", jobID, "-f")
	if code != 0 {
		t.Fatalf("%d %s", code, errOut)
	}
	// Each step once: the cursor kept its place across the outage.
	if strings.Count(out, "\n") != 2 || strings.Count(out, "model  Start") != 1 || strings.Count(out, "tool   run_command ok: done") != 1 {
		t.Errorf("stdout:\n%s", out)
	}
	want(t, errOut, "The server stopped answering", "The server answers again.", "job/"+jobID+" is completed")
}

func TestFilesAndCp(t *testing.T) {
	h := newHarness(t)
	h.login()
	var paths []string
	h.handle("GET "+jobRoute("/repo/contents"), func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Query().Get("path"))
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Query().Get("path") {
		case "":
			json.NewEncoder(w).Encode([]any{map[string]any{"name": "output", "path": "output", "type": "dir", "size": 0}, map[string]any{"name": "plan.md", "path": "plan.md", "type": "file", "size": 12}})
		case "output":
			json.NewEncoder(w).Encode([]any{map[string]any{"name": "big.bin", "path": "output/big.bin", "type": "file", "size": 20000000}, map[string]any{"name": "empty.txt", "path": "output/empty.txt", "type": "file", "size": 0}})
		}
	})
	out, _, code := h.run("", "files", jobID, "--recursive")
	if code != 0 || len(paths) != 2 {
		t.Fatalf("%d %v", code, paths)
	}
	want(t, out, "TYPE  SIZE      PATH", "dir   -         output", "file  12        plan.md", "file  20000000  output/big.bin")
	if out, _, _ := h.run("", "files", "job/"+jobID, "/output/", "-o", "name"); out != "output/big.bin\noutput/empty.txt\n" {
		t.Errorf("a subdirectory: %q", out)
	}

	h.handle("GET "+jobRoute("/repo/file"), func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Query().Get("path") {
		case "plan.md":
			json.NewEncoder(w).Encode(map[string]any{"path": "plan.md", "content": "# The plan\n", "encoding": "utf-8", "size": 11})
		case "output/image.png":
			json.NewEncoder(w).Encode(map[string]any{"content": "iVBORw0K", "encoding": "base64"})
		default:
			json.NewEncoder(w).Encode(map[string]any{"content": "", "encoding": "utf-8", "size": 0})
		}
	})
	if out, _, code := h.run("", "cp", jobID+":plan.md", "-"); code != 0 || out != "# The plan\n" {
		t.Errorf("to stdout: %d %q", code, out)
	}
	directory := t.TempDir()
	if _, errOut, code := h.run("", "cp", "job/"+jobID+":/output/image.png", directory); code != 0 {
		t.Fatalf("into a directory: %d %s", code, errOut)
	}
	if data, _ := os.ReadFile(filepath.Join(directory, "image.png")); string(data) != "\x89PNG\r\n" {
		t.Errorf("base64 was not decoded: %q", data)
	}
	// An empty answer for a file the listing says is not empty is a failure,
	// not an empty file.
	target := filepath.Join(directory, "big.bin")
	if _, errOut, code := h.run("", "cp", jobID+":output/big.bin", target); code != exit.Failed || !strings.Contains(errOut, "20000000 bytes") {
		t.Errorf("a file above the limit: %d %s", code, errOut)
	}
	if _, err := os.Stat(target); err == nil {
		t.Error("an empty file was written for a failed copy")
	}
	if _, _, code := h.run("", "cp", jobID+":output/empty.txt", filepath.Join(directory, "empty.txt")); code != 0 {
		t.Errorf("a file that really is empty: %d", code)
	}
	for _, refused := range [][]string{{"cp", jobID, "x"}, {"cp", jobID + ":", "x"}, {"cp", ":plan.md", "x"}, {"cp", jobID + ":plan.md"}} {
		if _, _, code := h.run("", refused...); code != exit.Usage {
			t.Errorf("%v: %d", refused, code)
		}
	}
}
