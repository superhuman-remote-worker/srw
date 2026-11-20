package cmd

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/spf13/cobra"

	"github.com/superhuman-remote-worker/srw/cli/internal/api"
	"github.com/superhuman-remote-worker/srw/cli/internal/exit"
	"github.com/superhuman-remote-worker/srw/cli/internal/output"
	"github.com/superhuman-remote-worker/srw/cli/internal/routes"
)

// jobPath fills a job route's template.
func jobPath(route routes.Route, id string) string {
	return strings.Replace(route.Path, "{job_id}", id, 1)
}

// jobID turns "job/<id>", "jobs/<id>" or a bare id into a job id. A full id
// is used as it is. A prefix of at least eight characters is looked up; it
// must name exactly one job.
func (a *App) jobID(ctx context.Context, client *api.Client, typed string) (string, error) {
	if before, after, found := strings.Cut(typed, "/"); found {
		if kindOf(before) != "job" {
			return "", exit.Usagef("%q is not a job; write job/<id> or the id alone", typed)
		}
		typed = after
	}
	wanted := strings.ToLower(strings.TrimSpace(typed))
	if isUUID(wanted) {
		return wanted, nil
	}
	if len(wanted) < 8 || strings.Trim(wanted, "0123456789abcdef-") != "" {
		return "", exit.Usagef("%q is not a job id; give the id or at least its first 8 characters", typed)
	}
	// The search also matches titles and task texts, so the rows are
	// filtered here to those whose id starts with what was typed.
	var found []string
	for offset := 0; offset < 2000; offset += 100 {
		query := url.Values{
			"search": {wanted}, "include_archived_projects": {"true"},
			"limit": {"100"}, "offset": {strconv.Itoa(offset)}, "include_total": {"false"},
		}
		var page struct {
			Jobs []struct {
				ID string `json:"id"`
			} `json:"jobs"`
			HasMore bool `json:"has_more"`
		}
		request := api.Request{Method: routes.JobList.Method, Path: routes.JobList.Path, Query: query, Known: true}
		if err := client.JSON(ctx, request, &page); err != nil {
			return "", err
		}
		for _, job := range page.Jobs {
			if strings.HasPrefix(strings.ToLower(job.ID), wanted) && !contains(found, job.ID) {
				found = append(found, job.ID)
			}
		}
		if !page.HasMore {
			break
		}
	}
	switch len(found) {
	case 0:
		return "", exit.New(exit.NotFound, "there is no job whose id starts with %q", wanted)
	case 1:
		return found[0], nil
	}
	return "", exit.Usagef("%q is the start of %d job ids: %s", wanted, len(found), strings.Join(found, ", ")).
		WithHint("give more characters of the id")
}

var autonomyLevels = []string{"full", "review", "partial", "guided", "dependent"}

type runFlags struct {
	file         string
	expert       string
	project      string
	model        string
	autonomy     string
	phases       bool
	lane         string
	priority     int
	connectors   []string
	noConnectors bool
	wait         bool
	timeout      time.Duration
	interval     time.Duration
}

func (a *App) runCommand() *cobra.Command {
	var flags runFlags
	command := &cobra.Command{
		Use:   "run [TASK]",
		Short: "Start a job",
		Long: "Start a job with a task. The task is the argument, a file (-f), or standard\n" +
			"input.\n\n" +
			"With --wait, srw waits for the job and exits by its outcome, as `srw wait`\n" +
			"does: 0 completed, 8 failed or cancelled, 10 stopped for a person, 9 timed\n" +
			"out. By default a job stops for review when its work is done (exit 10);\n" +
			"--autonomy full lets it complete by itself.\n\n" +
			"--phases makes the job plan its work in phases, for long tasks; without the\n" +
			"flag the project and the expert decide.\n\n" +
			"Starting a job cannot be repeated safely: when the answer is lost (exit 6),\n" +
			"check `srw get jobs` before starting it again. A Job manifest applied with\n" +
			"--idempotency-key can be repeated.",
		Example: "  srw run \"Summarise the open issues\" --expert scholar\n" +
			"  srw run -f task.md -p website --autonomy full --wait --timeout 2h\n" +
			"  git diff | srw run --expert reviewer --wait",
		Args: func(command *cobra.Command, args []string) error {
			if len(args) > 1 {
				return exit.Usagef("%s takes one task; quote it", command.CommandPath())
			}
			return nil
		},
		RunE: func(command *cobra.Command, args []string) error {
			return a.run(command, args, flags)
		},
	}
	f := command.Flags()
	f.StringVarP(&flags.file, "file", "f", "", "Read the task from a file; - reads standard input")
	f.StringVar(&flags.expert, "expert", "", "Expert that runs the job: a built-in name or an expert's id")
	f.StringVarP(&flags.project, "project", "p", "", "Project to run the job in, by name or id")
	f.StringVar(&flags.model, "model", "", "Model for this job")
	f.StringVar(&flags.autonomy, "autonomy", "", "When the job stops for a person: "+strings.Join(autonomyLevels, ", "))
	f.BoolVar(&flags.phases, "phases", false, "Plan the job in phases (for long work); --phases=false works on the task directly")
	f.StringVar(&flags.lane, "lane", "", "Execution lane: pinned or stateless")
	f.IntVar(&flags.priority, "priority", 5, "Priority, 0 to 10")
	f.StringArrayVar(&flags.connectors, "connector", nil, "Connector to attach, by id (repeatable); replaces the defaults")
	f.BoolVar(&flags.noConnectors, "no-connectors", false, "Attach no connector")
	f.BoolVar(&flags.wait, "wait", false, "Wait for the job and exit by its outcome")
	f.DurationVar(&flags.timeout, "timeout", 0, "With --wait: give up after this long (0: never), for example 90m")
	f.DurationVar(&flags.interval, "interval", 0, "With --wait: ask at this fixed interval instead of backing off")
	return command
}

func (a *App) run(command *cobra.Command, args []string, flags runFlags) error {
	ctx := command.Context()
	format, err := a.format()
	if err != nil {
		return err
	}
	changed := command.Flags().Changed
	if flags.autonomy != "" && !contains(autonomyLevels, flags.autonomy) {
		// The server would replace an unknown level without a word.
		return exit.Usagef("--autonomy is one of %s, not %q", strings.Join(autonomyLevels, ", "), flags.autonomy)
	}
	if flags.lane != "" && flags.lane != "pinned" && flags.lane != "stateless" {
		return exit.Usagef("--lane is pinned or stateless, not %q", flags.lane)
	}
	if changed("priority") && (flags.priority < 0 || flags.priority > 10) {
		return exit.Usagef("--priority is 0 to 10")
	}
	if flags.noConnectors && len(flags.connectors) > 0 {
		return exit.Usagef("give --connector or --no-connectors, not both")
	}
	if !flags.wait && (changed("timeout") || changed("interval")) {
		return exit.Usagef("--timeout and --interval belong to --wait")
	}
	task, err := a.readTask(args, flags.file)
	if err != nil {
		return err
	}
	to, client, err := a.connect(true)
	if err != nil {
		return err
	}
	body := map[string]any{"description": task}
	if flags.expert != "" {
		body["expert"] = a.expertReference(ctx, client, flags.expert)
	}
	project := flags.project
	if project == "" {
		project = to.Project
	}
	if project != "" {
		id, err := a.projectID(ctx, client, project)
		if err != nil {
			return err
		}
		body["project_id"] = id
	}
	if changed("priority") {
		body["priority"] = flags.priority
	}
	if flags.lane != "" {
		body["execution_lane"] = flags.lane
	}
	override := map[string]any{}
	if flags.model != "" {
		override["llm"] = map[string]any{"model": flags.model}
	}
	if flags.autonomy != "" {
		override["autonomy"] = flags.autonomy
	}
	if len(override) > 0 {
		body["config_override"] = override
	}
	// The typed field, never config_override: the server refuses two values
	// that differ. Not sent without the flag, so the project and the expert
	// decide.
	if changed("phases") {
		body["phases"] = flags.phases
	}
	switch {
	case flags.noConnectors:
		body["datasource_ids"] = []string{}
	case len(flags.connectors) > 0:
		body["datasource_ids"] = flags.connectors
	default:
		// What the Cockpit's form preselects.
		body["use_datasource_defaults"] = true
	}
	request := api.Request{Method: routes.JobCreate.Method, Path: routes.JobCreate.Path, Body: body, Known: true}
	response, err := client.Send(ctx, request)
	if err != nil {
		var coded *exit.Error
		if errors.As(err, &coded) && coded.Code == exit.Unknown {
			coded.Hint = "the job may have started; look at `srw get jobs` before starting it again"
		}
		return err
	}
	var created struct {
		ID            string `json:"id"`
		ExecutionLane string `json:"execution_lane"`
		ProjectID     string `json:"project_id"`
	}
	if err := json.Unmarshal(response.Body, &created); err != nil || created.ID == "" {
		return exit.New(exit.Unknown, "the server answered, but not with a job; look at `srw get jobs`")
	}
	// The id is printed first and always, before any waiting.
	switch format {
	case output.JSON, output.YAML:
		if err := output.Document(a.Out, format, response.Body); err != nil {
			return err
		}
	case output.Name:
		fmt.Fprintln(a.Out, "job/"+created.ID)
	default:
		fmt.Fprintf(a.Out, "job/%s created\n", created.ID)
	}
	if flags.lane != "" && created.ExecutionLane != "" && created.ExecutionLane != flags.lane {
		fmt.Fprintf(a.Err, "note: the job runs on the %s lane, not on %s as asked\n", created.ExecutionLane, flags.lane)
	}
	if !flags.wait {
		return nil
	}
	return a.wait(ctx, client, created.ID, waitFlags{timeout: flags.timeout, interval: flags.interval, answered: true}, format != output.Default)
}

// expertReference turns what was typed after --expert into what the server
// takes: a built-in expert's name, or the id of a stored one. A stored
// expert is found by the name it was applied under or by its display name.
// When the list cannot be read (a token without jobs:read) or holds no
// match, the typed value is sent as it is and the server decides.
func (a *App) expertReference(ctx context.Context, client *api.Client, typed string) string {
	if isUUID(typed) {
		return typed
	}
	rows, err := a.listExperts(ctx, client)
	if err != nil {
		return typed
	}
	found := match(rows, typed, "id", portableName, "name", "display_name")
	if len(found) != 1 || found[0].text("id") == "" {
		return typed
	}
	return found[0].text("id")
}

// readTask takes the task from the argument, a file, or standard input.
func (a *App) readTask(args []string, file string) (string, error) {
	var data []byte
	var err error
	switch {
	case len(args) == 1 && file != "":
		return "", exit.Usagef("give the task as the argument or with -f, not both")
	case len(args) == 1:
		data = []byte(args[0])
	case file == "-" || (file == "" && !a.StdinIsTerminal):
		if a.flags.tokenStdin {
			return "", exit.Usagef("the task and --token-stdin both read standard input; give the task as an argument or a file")
		}
		data, err = io.ReadAll(io.LimitReader(a.In, maxManifestBytes+1))
	case file != "":
		data, err = readLimited(file)
	default:
		return "", exit.Usagef("give the task: as the argument, with -f FILE, or on standard input")
	}
	if err != nil {
		return "", exit.Usagef("cannot read the task: %v", unwrapPath(err))
	}
	if len(data) > maxManifestBytes {
		return "", exit.Usagef("the task is larger than 1 MiB")
	}
	if !utf8.Valid(data) {
		return "", exit.Usagef("the task is not valid UTF-8")
	}
	task := strings.TrimSpace(string(data))
	if task == "" {
		return "", exit.Usagef("the task is empty")
	}
	return task, nil
}

// progress is what GET …/progress answers. Only what srw reads.
type progress struct {
	Status  string   `json:"status"`
	Outcome string   `json:"completion_outcome_kind"`
	State   string   `json:"state"`
	Reasons []string `json:"reasons"`
	// HeldBy is nil on a server that does not report holds, and for a job
	// that is not held.
	HeldBy *string `json:"held_by"`
}

func (a *App) progress(ctx context.Context, client *api.Client, id string, noTimeout bool) (*progress, error) {
	var seen progress
	request := api.Request{Method: routes.JobProgress.Method, Path: jobPath(routes.JobProgress, id), Known: true}
	if err := client.JSON(ctx, request, &seen); err != nil {
		return nil, err
	}
	return &seen, nil
}

func finalStatus(status string) bool {
	return status == "completed" || status == "failed" || status == "cancelled"
}

// needsPerson says whether a job stopped and waits for someone. A paused
// job counts only when it is held: without a hold the dispatcher picks it
// up again by itself.
func (p *progress) needsPerson() bool {
	switch p.Status {
	case "pending_review", "waiting_for_reply":
		return true
	case "paused":
		return p.HeldBy != nil && *p.HeldBy != ""
	}
	return false
}

type waitFlags struct {
	statuses []string
	timeout  time.Duration
	interval time.Duration
	// answered: the server has already answered this command (run --wait).
	answered bool
}

// awayGrace is how long wait and logs -f keep asking a server that stopped
// answering in the middle, for example while it is upgraded.
const awayGrace = 10 * time.Minute

// outage tracks a server that stopped answering as itself.
type outage struct {
	app   *App
	since time.Time // zero while the server answers
}

// rideOut says whether to ask again after err: only when the server is
// away (exit.Error.Away), and for at most awayGrace in a row.
func (o *outage) rideOut(err error) bool {
	var failure *exit.Error
	if !errors.As(err, &failure) || !failure.Away {
		return false
	}
	now := o.app.Now()
	if o.since.IsZero() {
		o.since = now
		fmt.Fprintf(o.app.Err, "The server stopped answering (%s). Asking again for up to %s.\n", failure.Message, awayGrace)
		return true
	}
	return now.Sub(o.since) < awayGrace
}

// back notes an answer after an outage.
func (o *outage) back() {
	if !o.since.IsZero() {
		fmt.Fprintln(o.app.Err, "The server answers again.")
		o.since = time.Time{}
	}
}

// giveUp is the error to end with. After an outage that outlasted the
// grace it is exit 7, whatever the last failed request looked like.
func (o *outage) giveUp(err error, id string) error {
	if o.since.IsZero() {
		return err
	}
	return exit.New(exit.Unreachable, "the server has not answered for %s: %s", awayGrace, err).
		WithHint("the job is not ended by this; watch it again with: srw wait job/%s", id)
}

func (a *App) waitCommand() *cobra.Command {
	var flags waitFlags
	var forValue string
	command := &cobra.Command{
		Use:   "wait JOB",
		Short: "Wait for a job and exit by its outcome",
		Long: "Wait until a job ends or stops for a person, and say which with the exit\n" +
			"code:\n\n" +
			"   0  completed, or it reached a status named with --for\n" +
			"   8  failed or cancelled\n" +
			"  10  stopped and needs a person: in review, waiting for a reply, or paused\n" +
			"      by someone\n" +
			"   9  --timeout passed\n" +
			"   7  the server stopped answering for more than ten minutes\n" +
			" 130  interrupted; the job keeps running\n\n" +
			"A job paused by the system counts as running: it is picked up again by\n" +
			"itself. A status srw does not know counts as running too.\n\n" +
			"A server that stops answering in the middle, for example while it is\n" +
			"upgraded, is asked again for up to ten minutes.",
		Example: "  srw wait job/0b5c2f6e --timeout 2h\n  srw wait 0b5c2f6e --for status=pending_review",
		Args:    exactArgs(1, "one job: job/<id> or the id"),
		RunE: func(command *cobra.Command, args []string) error {
			if forValue != "" {
				value, found := strings.CutPrefix(forValue, "status=")
				if !found || value == "" {
					return exit.Usagef("--for takes status=<status>[,<status>]")
				}
				flags.statuses = strings.Split(value, ",")
			}
			format, err := a.format()
			if err != nil {
				return err
			}
			_, client, err := a.connect(true)
			if err != nil {
				return err
			}
			id, err := a.jobID(command.Context(), client, args[0])
			if err != nil {
				return err
			}
			return a.wait(command.Context(), client, id, flags, format != output.Default)
		},
	}
	f := command.Flags()
	f.StringVar(&forValue, "for", "", "Stop at these statuses with exit 0: status=<status>[,<status>]")
	f.DurationVar(&flags.timeout, "timeout", 0, "Give up after this long (0: never), for example 90m")
	f.DurationVar(&flags.interval, "interval", 0, "Ask at this fixed interval instead of backing off from 2s to 15s")
	return command
}

// wait polls a job's progress. quiet leaves the final line to the caller's
// machine output. Once the server has answered, a server that goes away is
// asked again (see outage): an upgrade of the server must not end a wait.
func (a *App) wait(ctx context.Context, client *api.Client, id string, flags waitFlags, quiet bool) error {
	started := a.Now()
	pause := 2 * time.Second
	last := ""
	answered := flags.answered
	away := outage{app: a}
	for {
		seen, err := a.progress(ctx, client, id, true)
		switch {
		case err != nil && ctx.Err() != nil:
			fmt.Fprintf(a.Err, "The job keeps running. Watch it again with: srw wait job/%s\n", id)
			return err
		case err != nil && !(answered && away.rideOut(err)):
			return away.giveUp(err, id)
		case err == nil:
			answered = true
			away.back()
			if ended, result := a.waitVerdict(id, seen, flags, quiet); ended {
				return result
			}
			// Progress goes to stderr. The line that ends the wait goes to
			// stdout, so a status is printed in one place only.
			if seen.Status != last {
				fmt.Fprintf(a.Err, "job/%s %s\n", id, seen.Status)
			}
			last = seen.Status
		}
		if flags.timeout > 0 && a.Now().Sub(started) >= flags.timeout {
			state := "it is " + last + " and keeps running"
			if !away.since.IsZero() {
				state = "the server stopped answering; the job was " + last
			}
			return exit.New(exit.WaitTimeout, "the job did not end within %s; %s", flags.timeout, state).
				WithHint("watch it again with: srw wait job/%s", id)
		}
		step := pause
		if flags.interval > 0 {
			step = flags.interval
		} else if pause < 15*time.Second {
			pause = min(pause*3/2, 15*time.Second)
		}
		if err := a.sleep(ctx, step); err != nil {
			fmt.Fprintf(a.Err, "The job keeps running. Watch it again with: srw wait job/%s\n", id)
			return err
		}
	}
}

// waitVerdict says whether a progress answer ends the wait, and with what.
func (a *App) waitVerdict(id string, seen *progress, flags waitFlags, quiet bool) (bool, error) {
	done := func(code int, note string) (bool, error) {
		if !quiet {
			line := fmt.Sprintf("job/%s %s", id, seen.Status)
			if seen.Outcome != "" && seen.Outcome != seen.Status {
				line += " (" + seen.Outcome + ")"
			}
			fmt.Fprintln(a.Out, line)
		}
		if code == exit.OK {
			return true, nil
		}
		return true, exit.New(code, "%s", note)
	}
	switch {
	case contains(flags.statuses, seen.Status):
		return done(exit.OK, "")
	case len(flags.statuses) > 0 && finalStatus(seen.Status):
		return done(exit.WaitFailed, fmt.Sprintf("the job ended %s and cannot reach %s any more", seen.Status, strings.Join(flags.statuses, " or ")))
	case seen.Status == "completed":
		return done(exit.OK, "")
	case finalStatus(seen.Status):
		return done(exit.WaitFailed, "the job ended "+seen.Status)
	case len(flags.statuses) == 0 && seen.needsPerson():
		return done(exit.WaitNeedsPerson, waitingFor(id, seen))
	}
	return false, nil
}

func waitingFor(id string, seen *progress) string {
	switch seen.Status {
	case "pending_review":
		return fmt.Sprintf("the job stopped for review; approve it with `srw approve job/%s`, or send it back with `srw resume job/%s --feedback …`", id, id)
	case "waiting_for_reply":
		return "the job waits for a reply; answer it in the Cockpit"
	}
	return fmt.Sprintf("the job is paused and waits for someone; continue it with `srw resume job/%s`", id)
}

// sleep waits, or returns early when the command is interrupted.
func (a *App) sleep(ctx context.Context, pause time.Duration) error {
	if a.Sleep != nil {
		return a.Sleep(ctx, pause)
	}
	timer := time.NewTimer(pause)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// lifecycleCommand builds cancel, pause, resume and approve.
func (a *App) lifecycleCommand(verb, short, long string, route routes.Route, textFlag, bodyKey, textHelp string) *cobra.Command {
	var text string
	command := &cobra.Command{
		Use:   verb + " JOB",
		Short: short,
		Long:  long,
		Args:  exactArgs(1, "one job: job/<id> or the id"),
		RunE: func(command *cobra.Command, args []string) error {
			format, err := a.format()
			if err != nil {
				return err
			}
			_, client, err := a.connect(true)
			if err != nil {
				return err
			}
			id, err := a.jobID(command.Context(), client, args[0])
			if err != nil {
				return err
			}
			request := api.Request{Method: route.Method, Path: jobPath(route, id), Known: true}
			if bodyKey != "" && text != "" {
				request.Body = map[string]string{bodyKey: text}
			} else if route.Method == http.MethodPost {
				request.Body = map[string]string{}
			}
			if verb == "cancel" {
				request.Timeout = api.TeardownTimeout
			}
			response, err := client.Send(command.Context(), request)
			if verb == "approve" || verb == "resume" {
				response, err = a.reviewAgain(command.Context(), client, request, id, response, err)
			}
			if err != nil {
				if verb != "cancel" || !a.cancelledAnyway(command.Context(), client, id, err) {
					return stillReleasing(err, verb, id)
				}
				response = &api.Response{Status: http.StatusOK, Body: []byte(`{"status":"cancelled"}`)}
			}
			switch format {
			case output.JSON, output.YAML:
				return output.Document(a.Out, format, response.Body)
			case output.Name:
				fmt.Fprintln(a.Out, "job/"+id)
				return nil
			}
			var answer struct {
				Status  string `json:"status"`
				Message string `json:"message"`
			}
			_ = json.Unmarshal(response.Body, &answer)
			// The server's own status word, not one of srw's.
			line := fmt.Sprintf("job/%s %s", id, dash(answer.Status))
			if answer.Message != "" {
				line += ": " + answer.Message
			}
			fmt.Fprintln(a.Out, line)
			return nil
		},
	}
	if textFlag != "" {
		command.Flags().StringVar(&text, textFlag, "", textHelp)
	}
	return command
}

// cancelledAnyway looks at the job after a cancel that the server answered
// with an error of its own, or did not answer. The server marks the job
// cancelled first and releases its workspace after; when the release is not
// finished in time it answers 503 for a cancel that did happen. The verb
// asked for a cancelled job, so that is what the exit code reports.
func (a *App) cancelledAnyway(ctx context.Context, client *api.Client, id string, failed error) bool {
	var failure *exit.Error
	if !errors.As(failed, &failure) || (failure.Status < 500 && failure.Code != exit.Unknown) {
		return false
	}
	seen, err := a.progress(ctx, client, id, false)
	if err != nil || seen.Status != "cancelled" {
		return false
	}
	fmt.Fprintf(a.Err, "The server answered with an error (%s), but the job is cancelled. Its workspace may still be released in the background.\n", failure.Message)
	return true
}

// reviewGrace is how long approve and resume keep asking while the server
// refuses to act on a job that still waits for review.
const reviewGrace = 20 * time.Second

// reviewAgain sends an approval, or a resume, again while the server answers
// it with a conflict and the job still waits for review. A job shows
// pending_review a moment before the server has finished stopping it
// (measured on k3d and dev, knowledge-base/knowledge/tests/approve_at_review_stop_409_check.md:
// up to 2.5 s for approve on k3d's pinned lane, 11.5 s on dev's, about 1.3 s
// on the stateless lane, 0.6 s for resume). A request in that time is refused
// with 409 "completion finalizing", "job changed while control was being
// committed" or "pinned approval agent changed". The refusal changed
// nothing, so the request can be repeated. A person never meets it; `srw
// wait` followed by `srw approve` or `srw resume --feedback` does.
func (a *App) reviewAgain(ctx context.Context, client *api.Client, request api.Request, id string, response *api.Response, err error) (*api.Response, error) {
	started, said := a.Now(), false
	for err != nil {
		var failure *exit.Error
		if !errors.As(err, &failure) || failure.Status != http.StatusConflict || a.Now().Sub(started) >= reviewGrace {
			return response, err
		}
		// A parked completion report waits for an administrator, not for time.
		if failure.ErrCode == "completion_parked" {
			return response, err
		}
		// Only for a job that still waits: any other status means the
		// conflict is a real one (approved by someone else, cancelled).
		seen, readErr := a.progress(ctx, client, id, false)
		if readErr != nil || seen.Status != "pending_review" {
			return response, err
		}
		if !said {
			said = true
			fmt.Fprintf(a.Err, "The server has not finished stopping the job for review (%s). Trying again for up to %s.\n", failure.Message, reviewGrace)
		}
		if sleepErr := a.sleep(ctx, time.Second); sleepErr != nil {
			return response, sleepErr
		}
		response, err = client.Send(ctx, request)
	}
	return response, err
}

// stillReleasing adds what to do to the server's 503 for a job whose
// workspace it has not finished releasing.
func stillReleasing(err error, verb, id string) error {
	var failure *exit.Error
	if errors.As(err, &failure) && failure.Status == http.StatusServiceUnavailable && failure.Hint == "" {
		failure.Hint = fmt.Sprintf("the server may still be releasing the job's workspace; look at `srw get job/%s`, then run `srw %s` again", id, verb)
	}
	return err
}

func (a *App) lifecycleCommands() []*cobra.Command {
	return []*cobra.Command{
		a.lifecycleCommand("cancel", "Cancel a job",
			"Cancel a job. It ends as cancelled.\n\n"+
				"The server releases the job's workspace before it answers, which can take a\n"+
				"while. If it answers with an error although the job did end cancelled, srw\n"+
				"says so and exits 0.",
			routes.JobCancel, "", "", ""),
		a.lifecycleCommand("pause", "Pause a running job",
			"Pause a job that is running. It stays paused until someone resumes it.", routes.JobPause, "", "", ""),
		a.lifecycleCommand("resume", "Continue a job, optionally with feedback",
			"Continue a paused or failed job. On a job that stopped for review, resume\n"+
				"sends it back to work; say what to change with --feedback. On a job that is\n"+
				"running, resume interrupts it and queues it again.\n\n"+
				"A job shows that it stopped for review a moment before the server has\n"+
				"finished stopping it. A resume in that moment is refused as a conflict; srw\n"+
				"sends it again for up to 20 seconds while the job still waits for review.",
			routes.JobResume, "feedback", "feedback", "What the job should do differently"),
		a.lifecycleCommand("approve", "Approve a job that stopped for review",
			"Approve a job that stopped for a person. A job that stopped at the end of its\n"+
				"work completes. A job that stopped between two phases continues with the\n"+
				"next phase.\n\n"+
				"A job shows that it stopped for review a moment before the server has\n"+
				"finished stopping it. An approval in that moment is refused as a conflict;\n"+
				"srw sends it again for up to 20 seconds while the job still waits for review.",
			routes.JobApprove, "notes", "notes", "A note for the record (used when the job completes)"),
	}
}

// deleteJob removes a job. A job that is still going must be cancelled
// first.
func (a *App) deleteJob(ctx context.Context, client *api.Client, typed string, yes bool) error {
	id, err := a.jobID(ctx, client, typed)
	if err != nil {
		return err
	}
	seen, err := a.progress(ctx, client, id, false)
	if err != nil {
		return err
	}
	if !finalStatus(seen.Status) {
		return exit.New(exit.Conflict, "job/%s is %s; a job that has not ended cannot be deleted", id, seen.Status).
			WithHint("cancel it first: srw cancel job/%s", id)
	}
	if !yes {
		if err := a.confirm(fmt.Sprintf("Delete job/%s (%s) with its record?", id, seen.Status)); err != nil {
			return err
		}
	}
	// Deleting releases the workspace too; see api.TeardownTimeout. A job
	// that has just ended is often still being released: the server then
	// answers 503 and has deleted nothing, so the request is sent again.
	request := api.Request{Method: routes.JobDelete.Method, Path: jobPath(routes.JobDelete, id), Known: true, Timeout: api.TeardownTimeout}
	started, said := a.Now(), false
	for {
		response, err := client.Send(ctx, request)
		if err == nil {
			return a.printJSONDocument(response.Body, func() []string { return []string{"job/" + id} })
		}
		var failure *exit.Error
		releasing := errors.As(err, &failure) && failure.Code == exit.Failed &&
			(failure.Status == http.StatusServiceUnavailable || stillPruning(failure))
		if !releasing || a.Now().Sub(started) >= api.TeardownTimeout {
			return stillReleasing(err, "delete job/"+id, id)
		}
		if !said {
			said = true
			fmt.Fprintf(a.Err, "The server is still releasing the job's workspace (%s). Trying again for up to %s.\n", failure.Message, api.TeardownTimeout)
		}
		if err := a.sleep(ctx, 5*time.Second); err != nil {
			return err
		}
	}
}

// stillPruning recognises the API's 500 for a stateless job whose workspace
// recovery is not settled yet: "strict checkpoint prune blocked by workspace
// recovery authority" (or "by legacy workspace recovery"). It comes for about
// 15 seconds after such a job ends (measured on k3d), deletes nothing, and
// passes by itself. The server should answer 503 for it, as it does for the
// pinned lane; until it does, delete job treats it the same way.
func stillPruning(failure *exit.Error) bool {
	return failure.Status == http.StatusInternalServerError &&
		strings.HasPrefix(failure.Message, "strict checkpoint prune blocked by")
}

type jobListFlags struct {
	statuses []string
	project  string
	origins  []string
	search   string
	limit    int
	all      bool
	subjobs  bool
}

// getJobs lists jobs, or shows one.
func (a *App) getJobs(ctx context.Context, client *api.Client, to string, format output.Format, name string, list jobListFlags, flags getFlags) error {
	columns := []column{{"ID", "id"}, {"STATUS", "status"}, {"EXPERT", "expert_display_name"}, {"PROJECT", "project_name"}, {"CREATED", "created_at"}, {"TASK", "task"}}
	if name != "" {
		id, err := a.jobID(ctx, client, name)
		if err != nil {
			return err
		}
		response, err := client.Send(ctx, api.Request{Method: routes.JobGet.Method, Path: jobPath(routes.JobGet, id), Known: true})
		if err != nil {
			return err
		}
		rows := decodeRows([]json.RawMessage{response.Body})
		if format == output.Default {
			// The detail names neither the expert nor the project; the
			// list row does.
			if listed := a.jobListRow(ctx, client, id); listed != nil {
				rows = []row{*listed}
			}
		}
		a.presentJobs(rows)
		return a.printRows(format, "job", rows, columns, "id", true, flags)
	}
	if list.limit < 1 || list.limit > 500 {
		return exit.Usagef("--limit is 1 to 500")
	}
	query := url.Values{"limit": {strconv.Itoa(list.limit)}, "include_total": {"false"}}
	for _, status := range list.statuses {
		query.Add("status", status)
	}
	for _, origin := range list.origins {
		query.Add("origin", origin)
	}
	if list.search != "" {
		query.Set("search", list.search)
	}
	project := list.project
	if project == "" {
		project = to
	}
	if project != "" {
		id, err := a.projectID(ctx, client, project)
		if err != nil {
			return err
		}
		query.Set("project_id", id)
	}
	var rows []row
	more := false
	for offset := 0; ; offset += list.limit {
		query.Set("offset", strconv.Itoa(offset))
		var page struct {
			Jobs    []json.RawMessage `json:"jobs"`
			HasMore bool              `json:"has_more"`
		}
		request := api.Request{Method: routes.JobList.Method, Path: routes.JobList.Path, Query: query, Known: true}
		if err := client.JSON(ctx, request, &page); err != nil {
			return err
		}
		rows = append(rows, decodeRows(page.Jobs)...)
		more = page.HasMore
		if !list.all || !page.HasMore || offset+list.limit > 50000 {
			break
		}
	}
	if !list.subjobs {
		// A page is the root jobs plus their matching children.
		roots := rows[:0]
		for _, job := range rows {
			if root, known := job.fields["is_display_root"].(bool); !known || root {
				roots = append(roots, job)
			}
		}
		rows = roots
	}
	a.presentJobs(rows)
	if err := a.printRows(format, "job", rows, columns, "id", false, flags); err != nil {
		return err
	}
	if more && format == output.Default {
		fmt.Fprintf(a.Err, "There are more jobs; --all lists them, or narrow the list with --status, --project or --search.\n")
	}
	return nil
}

// jobListRow finds a job's row in the list, which carries the names the
// detail lacks. Nil when it cannot be found; the caller then shows what the
// detail has.
func (a *App) jobListRow(ctx context.Context, client *api.Client, id string) *row {
	query := url.Values{"search": {id}, "include_archived_projects": {"true"}, "limit": {"50"}, "include_total": {"false"}}
	var page struct {
		Jobs []json.RawMessage `json:"jobs"`
	}
	request := api.Request{Method: routes.JobList.Method, Path: routes.JobList.Path, Query: query, Known: true}
	if client.JSON(ctx, request, &page) != nil {
		return nil
	}
	for _, listed := range decodeRows(page.Jobs) {
		if strings.EqualFold(listed.text("id"), id) {
			return &listed
		}
	}
	return nil
}

// presentJobs adds the table's own fields to job rows: one line for the
// task, and a time a person can read. The rows' JSON is not touched.
func (a *App) presentJobs(rows []row) {
	for _, job := range rows {
		task := job.text("title")
		if task == "" {
			task, _, _ = strings.Cut(strings.TrimSpace(job.text("description")), "\n")
		}
		if a.StdoutIsTerminal && len([]rune(task)) > 60 {
			task = string([]rune(task)[:59]) + "…"
		}
		job.fields["task"] = task
		if created, err := time.Parse(time.RFC3339Nano, job.text("created_at")); err == nil {
			if a.StdoutIsTerminal {
				job.fields["created_at"] = ago(a.Now().Sub(created))
			} else {
				job.fields["created_at"] = created.UTC().Format(time.RFC3339)
			}
		}
		if job.text("expert_display_name") == "" {
			job.fields["expert_display_name"] = job.text("config_name")
		}
	}
}

func ago(elapsed time.Duration) string {
	switch {
	case elapsed < time.Minute:
		return "just now"
	case elapsed < time.Hour:
		return fmt.Sprintf("%dm ago", int(elapsed.Minutes()))
	case elapsed < 48*time.Hour:
		return fmt.Sprintf("%dh ago", int(elapsed.Hours()))
	}
	return fmt.Sprintf("%dd ago", int(elapsed.Hours()/24))
}

func (a *App) describeCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "describe job JOB",
		Short: "Show what a job is doing and why it stopped",
		Long: "One screen about a job: its status, why it is where it is, its to-dos, what\n" +
			"it handed over when it stopped, and its completion report.",
		Example: "  srw describe job 0b5c2f6e\n  srw describe job/0b5c2f6e",
		Args: func(command *cobra.Command, args []string) error {
			if len(args) < 1 || len(args) > 2 {
				return exit.Usagef("%s takes job and an id", command.CommandPath())
			}
			return nil
		},
		RunE: func(command *cobra.Command, args []string) error {
			typed := args[len(args)-1]
			if len(args) == 2 && kindOf(args[0]) != "job" || len(args) == 1 && !strings.Contains(typed, "/") {
				return exit.Usagef("describe is available for jobs: srw describe job <id>")
			}
			return a.describeJob(command.Context(), typed)
		},
	}
}

func (a *App) describeJob(ctx context.Context, typed string) error {
	format, err := a.format()
	if err != nil {
		return err
	}
	_, client, err := a.connect(true)
	if err != nil {
		return err
	}
	id, err := a.jobID(ctx, client, typed)
	if err != nil {
		return err
	}
	// part reads one facet. A 404 on an optional one means "none yet".
	part := func(route routes.Route, optional bool) (map[string]any, []byte, error) {
		request := api.Request{Method: route.Method, Path: jobPath(route, id), Known: true}
		response, err := client.Do(ctx, request)
		if err != nil {
			return nil, nil, err
		}
		if optional && response.Status == http.StatusNotFound {
			return nil, nil, nil
		}
		if err := client.StatusError(request, response); err != nil {
			return nil, nil, err
		}
		fields := map[string]any{}
		_ = json.Unmarshal(response.Body, &fields)
		return fields, response.Body, nil
	}
	job, jobRaw, err := part(routes.JobGet, false)
	if err != nil {
		return err
	}
	if format == output.Name {
		fmt.Fprintln(a.Out, "job/"+id)
		return nil
	}
	if format == output.JSON || format == output.YAML {
		// The job as the server describes it; the other facets have their
		// own routes (`srw api`).
		return output.Document(a.Out, format, jobRaw)
	}
	seen, _, err := part(routes.JobProgress, false)
	if err != nil {
		return err
	}
	todos, _, err := part(routes.JobTodos, true)
	if err != nil {
		return err
	}
	frozen, _, err := part(routes.JobFrozen, true)
	if err != nil {
		return err
	}
	report, _, err := part(routes.JobReport, true)
	if err != nil {
		return err
	}
	text := func(fields map[string]any, key string) string {
		if value, ok := fields[key].(string); ok {
			return value
		}
		return ""
	}
	table := a.table()
	fmt.Fprintf(table, "Job:\t%s\n", id)
	if title := text(job, "title"); title != "" {
		fmt.Fprintf(table, "Title:\t%s\n", title)
	}
	status := text(seen, "status")
	if outcome := text(seen, "completion_outcome_kind"); outcome != "" && outcome != status {
		status += " (" + outcome + ")"
	}
	fmt.Fprintf(table, "Status:\t%s\n", status)
	if state := text(seen, "state"); state != "" {
		line := state
		if reasons, ok := seen["reasons"].([]any); ok && len(reasons) > 0 {
			parts := make([]string, 0, len(reasons))
			for _, reason := range reasons {
				parts = append(parts, fmt.Sprint(reason))
			}
			line += ": " + strings.Join(parts, "; ")
		}
		fmt.Fprintf(table, "State:\t%s\n", line)
	}
	if held := text(seen, "held_by"); held != "" {
		fmt.Fprintf(table, "Held by:\t%s (continues only with `srw resume job/%s`)\n", held, id)
	}
	// Left out when the server does not say (an older job, another harness).
	if phases, known := job["phases"].(bool); known {
		fmt.Fprintf(table, "Plans in phases:\t%s\n", map[bool]string{true: "yes", false: "no"}[phases])
	}
	expert, project := text(job, "config_name"), text(job, "project_id")
	if listed := a.jobListRow(ctx, client, id); listed != nil {
		if name := listed.text("expert_display_name"); name != "" {
			expert = name
		}
		if name := listed.text("project_name"); name != "" {
			project = name + " (" + project + ")"
		}
	}
	for _, field := range [][2]string{{"Expert", expert}, {"Lane", text(job, "execution_lane")}, {"Project", project}, {"Created", text(job, "created_at")}, {"Error", text(job, "error_message")}} {
		if field[1] != "" {
			fmt.Fprintf(table, "%s:\t%s\n", field[0], field[1])
		}
	}
	if err := table.Flush(); err != nil {
		return err
	}
	if task := strings.TrimSpace(text(job, "description")); task != "" {
		fmt.Fprintf(a.Out, "\nTask:\n%s\n", indent(task))
	}
	if items := todoItems(todos["current"]); len(items) > 0 {
		fmt.Fprintf(a.Out, "\nTo-dos:\n")
		for _, item := range items {
			fmt.Fprintf(a.Out, "  [%s] %s\n", item[0], item[1])
		}
	}
	if archives, ok := todos["archives"].([]any); ok && len(archives) > 0 {
		fmt.Fprintf(a.Out, "\nArchived to-do lists: %d\n", len(archives))
	}
	// A finished job's report repeats what it handed over; print it once.
	if frozen != nil && (report == nil || report["report"] == nil) {
		fmt.Fprintf(a.Out, "\nHanded over when it stopped:\n")
		printFields(a.Out, frozen, "summary", "deliverables", "confidence", "notes", "type", "reason")
	}
	if report != nil {
		if body := report["report"]; body != nil {
			fmt.Fprintf(a.Out, "\nCompletion report:\n")
			if written, ok := body.(string); ok {
				fmt.Fprintln(a.Out, indent(strings.TrimSpace(written)))
			} else if fields, ok := body.(map[string]any); ok {
				printFields(a.Out, fields)
			}
		}
	}
	return nil
}

// todoItems reads a to-do list whose exact shape srw does not depend on: a
// list of items, or an object holding one, each item with a status and a
// text.
func todoItems(value any) [][2]string {
	if holder, ok := value.(map[string]any); ok {
		value = holder["todos"]
	}
	list, ok := value.([]any)
	if !ok {
		return nil
	}
	var items [][2]string
	for _, entry := range list {
		fields, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		label := ""
		for _, key := range []string{"content", "text", "title", "description"} {
			if written, ok := fields[key].(string); ok && written != "" {
				label = written
				break
			}
		}
		status, _ := fields["status"].(string)
		if label != "" {
			items = append(items, [2]string{dash(status), label})
		}
	}
	return items
}

// printFields prints the named fields of an object that are set, or all of
// them when none is named.
func printFields(w io.Writer, fields map[string]any, keys ...string) {
	if len(keys) == 0 {
		for key := range fields {
			keys = append(keys, key)
		}
		sortStrings(keys)
	}
	for _, key := range keys {
		value := fields[key]
		switch typed := value.(type) {
		case nil:
			continue
		case string:
			if typed == "" {
				continue
			}
			fmt.Fprintf(w, "  %s: %s\n", key, strings.ReplaceAll(strings.TrimSpace(typed), "\n", "\n    "))
		default:
			encoded, _ := json.Marshal(typed)
			fmt.Fprintf(w, "  %s: %s\n", key, encoded)
		}
	}
}

func indent(text string) string {
	return "  " + strings.ReplaceAll(text, "\n", "\n  ")
}

type logFlags struct {
	tail     int
	filter   string
	allSteps bool
	follow   bool
}

func (a *App) logsCommand() *cobra.Command {
	var flags logFlags
	command := &cobra.Command{
		Use:   "logs JOB",
		Short: "Show what the agent did",
		Long: "Print a job's audit trail: the model's steps and the tools it called. This\n" +
			"is the record the Cockpit shows.\n\n" +
			"-f follows a running job and ends when the job ends or stops for a person.\n" +
			"It exits 0 whatever the job's result; use `srw wait` for the outcome. A\n" +
			"server that stops answering in the middle is asked again for up to ten\n" +
			"minutes.",
		Example: "  srw logs job/0b5c2f6e --tail 20\n  srw logs 0b5c2f6e -f --filter tools",
		Args:    exactArgs(1, "one job: job/<id> or the id"),
		RunE: func(command *cobra.Command, args []string) error {
			switch flags.filter {
			case "", "all", "messages", "tools", "errors":
			default:
				return exit.Usagef("--filter is messages, tools or errors, not %q", flags.filter)
			}
			if flags.tail < 0 || flags.tail > 200 {
				return exit.Usagef("--tail is 1 to 200")
			}
			if flags.tail > 0 && flags.follow {
				return exit.Usagef("give --tail or -f, not both")
			}
			format, err := a.format()
			if err != nil {
				return err
			}
			_, client, err := a.connect(true)
			if err != nil {
				return err
			}
			id, err := a.jobID(command.Context(), client, args[0])
			if err != nil {
				return err
			}
			return a.logs(command.Context(), client, id, flags, format)
		},
	}
	f := command.Flags()
	f.IntVar(&flags.tail, "tail", 0, "Show only the last N entries (at most 200)")
	f.StringVar(&flags.filter, "filter", "", "Show only: messages, tools or errors")
	f.BoolVar(&flags.allSteps, "all-steps", false, "Also show the memory steps")
	f.BoolVarP(&flags.follow, "follow", "f", false, "Keep printing while the job runs")
	return command
}

const auditPage = 200

type auditPageAnswer struct {
	Entries []json.RawMessage `json:"entries"`
	Total   int               `json:"total"`
	HasMore bool              `json:"hasMore"`
	Error   any               `json:"error"`
}

func (a *App) auditPage(ctx context.Context, client *api.Client, id string, flags logFlags, offset, limit int, order string) (*auditPageAnswer, error) {
	filter := flags.filter
	if filter == "" {
		filter = "all"
	}
	query := url.Values{
		"offset": {strconv.Itoa(offset)}, "limit": {strconv.Itoa(limit)},
		"order": {order}, "filter": {filter}, "lean": {"true"},
	}
	var page auditPageAnswer
	request := api.Request{Method: routes.JobAudit.Method, Path: jobPath(routes.JobAudit, id), Query: query, Known: true}
	if err := client.JSON(ctx, request, &page); err != nil {
		return nil, err
	}
	// An error inside a 200 is a failure of the audit store, not an empty
	// trail.
	if page.Error != nil && page.Error != "" {
		return nil, exit.New(exit.Failed, "the audit store is not available on this server (%v)", page.Error)
	}
	return &page, nil
}

func (a *App) logs(ctx context.Context, client *api.Client, id string, flags logFlags, format output.Format) error {
	if format == output.Name {
		return exit.Usagef("`logs` has no name output")
	}
	printer := &logPrinter{app: a, flags: flags, format: format}
	if flags.tail > 0 {
		page, err := a.auditPage(ctx, client, id, flags, 0, flags.tail, "desc")
		if err != nil {
			return err
		}
		for index := len(page.Entries) - 1; index >= 0; index-- {
			printer.print(page.Entries[index])
		}
		return printer.finish()
	}
	// The cursor is an offset into the ascending list. An entry is rewritten
	// in place when it finishes, so the cursor stops at the first entry that
	// has not finished and reads it again on the next round.
	cursor := 0
	read := func() (int, error) {
		printed := 0
		for {
			page, err := a.auditPage(ctx, client, id, flags, cursor, auditPage, "asc")
			if err != nil {
				return printed, err
			}
			for _, entry := range page.Entries {
				if flags.follow && unfinished(entry) {
					return printed, nil
				}
				printer.print(entry)
				cursor++
				printed++
			}
			if !page.HasMore || len(page.Entries) == 0 {
				return printed, nil
			}
		}
	}
	if _, err := read(); err != nil {
		return err
	}
	if !flags.follow {
		return printer.finish()
	}
	pause := 2 * time.Second
	// The first read was answered. A server that goes away after it is
	// asked again, as in wait; the cursor keeps its place.
	away := outage{app: a}
	for {
		seen, err := a.progress(ctx, client, id, true)
		printed := 0
		if err == nil {
			printed, err = read()
		}
		if err != nil {
			if ctx.Err() != nil || !away.rideOut(err) {
				return away.giveUp(err, id)
			}
			if err := a.sleep(ctx, pause); err != nil {
				return err
			}
			continue
		}
		away.back()
		if (finalStatus(seen.Status) || seen.needsPerson()) && printed == 0 {
			// One more read brought nothing new: print what is still held
			// back, finished or not, and end.
			flags.follow = false
			if _, err := read(); err != nil {
				return err
			}
			fmt.Fprintf(a.Err, "job/%s is %s\n", id, seen.Status)
			return printer.finish()
		}
		if printed > 0 {
			pause = 2 * time.Second
		} else if pause < 10*time.Second {
			pause += time.Second
		}
		if err := a.sleep(ctx, pause); err != nil {
			return err
		}
	}
}

// unfinished says whether a model or tool step is still running. It is
// rewritten when it finishes.
func unfinished(entry json.RawMessage) bool {
	var step struct {
		Type      string  `json:"step_type"`
		Completed *string `json:"completed_at"`
		Started   *string `json:"started_at"`
	}
	if json.Unmarshal(entry, &step) != nil {
		return false
	}
	return (step.Type == "llm" || step.Type == "tool") && step.Started != nil && step.Completed == nil
}

type logPrinter struct {
	app     *App
	flags   logFlags
	format  output.Format
	entries []json.RawMessage
}

func (p *logPrinter) print(entry json.RawMessage) {
	var step struct {
		Number    json.Number `json:"step_number"`
		Type      string      `json:"step_type"`
		Node      string      `json:"node_name"`
		Timestamp string      `json:"timestamp"`
		Tool      *struct {
			Name    string `json:"name"`
			Preview string `json:"result_preview"`
			Success *bool  `json:"success"`
			Error   string `json:"error"`
		} `json:"tool"`
		LLM *struct {
			Model   string `json:"model"`
			Preview string `json:"response_content_preview"`
			Calls   []struct {
				Name string `json:"name"`
			} `json:"tool_calls"`
		} `json:"llm"`
		Error *struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(entry, &step) != nil {
		return
	}
	if !p.flags.allSteps && strings.HasPrefix(step.Type, "memory_") {
		return
	}
	if p.format == output.JSON || p.format == output.YAML {
		p.entries = append(p.entries, entry)
		return
	}
	clock := step.Timestamp
	if stamp, err := time.Parse(time.RFC3339Nano, step.Timestamp); err == nil {
		clock = stamp.UTC().Format("15:04:05")
	}
	line := fmt.Sprintf("%4s  %s  ", step.Number.String(), clock)
	switch {
	case step.Error != nil && step.Error.Message != "":
		line += fmt.Sprintf("error  %s: %s", step.Error.Type, firstLine(step.Error.Message))
	case step.Tool != nil && step.Tool.Name != "":
		state := "ok"
		detail := firstLine(step.Tool.Preview)
		if step.Tool.Success != nil && !*step.Tool.Success {
			state = "FAILED"
			if step.Tool.Error != "" {
				detail = firstLine(step.Tool.Error)
			}
		}
		line += fmt.Sprintf("tool   %s %s", step.Tool.Name, state)
		if detail != "" {
			line += ": " + detail
		}
	case step.LLM != nil:
		line += "model  " + firstLine(step.LLM.Preview)
		if len(step.LLM.Calls) > 0 {
			names := make([]string, 0, len(step.LLM.Calls))
			for _, call := range step.LLM.Calls {
				names = append(names, call.Name)
			}
			line += " → " + strings.Join(names, ", ")
		}
	default:
		line += strings.TrimSpace(step.Type + "  " + step.Node)
	}
	fmt.Fprintln(p.app.Out, line)
}

func (p *logPrinter) finish() error {
	if p.format != output.JSON && p.format != output.YAML {
		return nil
	}
	if p.entries == nil {
		p.entries = []json.RawMessage{}
	}
	raw, err := json.Marshal(map[string]any{"items": p.entries})
	if err != nil {
		return err
	}
	return output.Document(p.app.Out, p.format, raw)
}

// firstLine is the first line of a text, shortened for one log line.
func firstLine(text string) string {
	line, _, _ := strings.Cut(strings.TrimSpace(text), "\n")
	if runes := []rune(line); len(runes) > 160 {
		return string(runes[:159]) + "…"
	}
	return line
}

type repoEntry struct {
	Name string `json:"name"`
	Path string `json:"path"`
	Type string `json:"type"`
	Size int64  `json:"size"`
}

func (a *App) repoList(ctx context.Context, client *api.Client, id, directory string) ([]repoEntry, error) {
	query := url.Values{}
	if directory != "" {
		query.Set("path", directory)
	}
	var entries []repoEntry
	request := api.Request{Method: routes.JobRepoContents.Method, Path: jobPath(routes.JobRepoContents, id), Query: query, Known: true}
	if err := client.JSON(ctx, request, &entries); err != nil {
		return nil, err
	}
	return entries, nil
}

func (a *App) filesCommand() *cobra.Command {
	var recursive bool
	command := &cobra.Command{
		Use:   "files JOB [PATH]",
		Short: "List the files a job pushed",
		Long: "List the files in a job's repository. This is what the job pushed last, not\n" +
			"its live workspace: a running job's newest files appear after its next push.",
		Example: "  srw files job/0b5c2f6e\n  srw files 0b5c2f6e output --recursive",
		Args: func(command *cobra.Command, args []string) error {
			if len(args) < 1 || len(args) > 2 {
				return exit.Usagef("%s takes a job and at most one path", command.CommandPath())
			}
			return nil
		},
		RunE: func(command *cobra.Command, args []string) error {
			format, err := a.format()
			if err != nil {
				return err
			}
			ctx := command.Context()
			_, client, err := a.connect(true)
			if err != nil {
				return err
			}
			id, err := a.jobID(ctx, client, args[0])
			if err != nil {
				return err
			}
			start := ""
			if len(args) == 2 {
				start = strings.Trim(args[1], "/")
			}
			var all []repoEntry
			pending := []string{start}
			for len(pending) > 0 {
				directory := pending[0]
				pending = pending[1:]
				entries, err := a.repoList(ctx, client, id, directory)
				if err != nil {
					return err
				}
				for _, entry := range entries {
					all = append(all, entry)
					if recursive && entry.Type == "dir" {
						pending = append(pending, entry.Path)
					}
				}
				if len(all) > 20000 {
					return exit.New(exit.Failed, "more than 20000 entries; list a subdirectory")
				}
			}
			switch format {
			case output.Name:
				for _, entry := range all {
					fmt.Fprintln(a.Out, entry.Path)
				}
				return nil
			case output.JSON, output.YAML:
				raw, err := json.Marshal(map[string]any{"items": all})
				if err != nil {
					return err
				}
				return output.Document(a.Out, format, raw)
			}
			if len(all) == 0 {
				fmt.Fprintln(a.Err, "No files. A job's files appear after its first push.")
				return nil
			}
			table := a.table()
			fmt.Fprintln(table, "TYPE\tSIZE\tPATH")
			for _, entry := range all {
				size := "-"
				if entry.Type == "file" {
					size = strconv.FormatInt(entry.Size, 10)
				}
				fmt.Fprintf(table, "%s\t%s\t%s\n", entry.Type, size, entry.Path)
			}
			return table.Flush()
		},
	}
	command.Flags().BoolVarP(&recursive, "recursive", "r", false, "List subdirectories too")
	return command
}

func (a *App) cpCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "cp JOB:PATH (DEST | -)",
		Short: "Copy a file a job pushed to this machine",
		Long: "Copy one file from a job's repository to a local file, or to standard output\n" +
			"with -. Like `srw files`, it reads what the job pushed last.\n\n" +
			"Files above 10 MiB are not served by this route yet.",
		Example: "  srw cp job/0b5c2f6e:output/report.md report.md\n  srw cp 0b5c2f6e:output/report.md - | less",
		Args:    exactArgs(2, "a source as job/<id>:<path> and a destination (a file, a directory or -)"),
		RunE: func(command *cobra.Command, args []string) error {
			ctx := command.Context()
			job, remote, found := strings.Cut(args[0], ":")
			remote = strings.Trim(remote, "/")
			if !found || job == "" || remote == "" {
				return exit.Usagef("the source is job/<id>:<path>, for example job/0b5c2f6e:output/report.md")
			}
			_, client, err := a.connect(true)
			if err != nil {
				return err
			}
			id, err := a.jobID(ctx, client, job)
			if err != nil {
				return err
			}
			var file struct {
				Content  string `json:"content"`
				Encoding string `json:"encoding"`
			}
			request := api.Request{
				Method: routes.JobRepoFile.Method, Path: jobPath(routes.JobRepoFile, id),
				Query: url.Values{"path": {remote}}, Known: true,
			}
			if err := client.JSON(ctx, request, &file); err != nil {
				return err
			}
			data := []byte(file.Content)
			if file.Encoding == "base64" {
				data, err = base64.StdEncoding.DecodeString(file.Content)
				if err != nil {
					return exit.New(exit.Failed, "the server sent %s in an encoding srw cannot read", remote)
				}
			}
			if len(data) == 0 {
				// A file above the server's size limit can come back empty
				// with a success status. The listing knows the real size.
				parent := path.Dir(remote)
				if parent == "." {
					parent = ""
				}
				entries, err := a.repoList(ctx, client, id, parent)
				if err != nil {
					return err
				}
				for _, entry := range entries {
					if entry.Path == remote && entry.Size > 0 {
						return exit.New(exit.Failed, "%s has %d bytes, but the server sent none; files above 10 MiB are not served by this route yet", remote, entry.Size)
					}
				}
			}
			if args[1] == "-" {
				_, err := a.Out.Write(data)
				return err
			}
			destination := args[1]
			if info, err := os.Stat(destination); err == nil && info.IsDir() {
				destination = filepath.Join(destination, path.Base(remote))
			}
			if err := os.WriteFile(destination, data, 0o644); err != nil {
				return exit.New(exit.Failed, "cannot write %s: %v", destination, unwrapPath(err))
			}
			fmt.Fprintf(a.Err, "%s (%d bytes)\n", destination, len(data))
			return nil
		},
	}
}

// getSessions lists the sessions a user can see. Sessions are read-only
// here; working in one happens in the Cockpit.
func (a *App) getSessions(ctx context.Context, client *api.Client, format output.Format, flags getFlags) error {
	var answer struct {
		Threads []json.RawMessage `json:"threads"`
	}
	request := api.Request{Method: routes.SessionList.Method, Path: routes.SessionList.Path, Known: true}
	if err := client.JSON(ctx, request, &answer); err != nil {
		return err
	}
	rows := decodeRows(answer.Threads)
	for _, session := range rows {
		if session.text("title") == "" {
			session.fields["title"] = session.text("name")
		}
	}
	columns := []column{{"ID", "id"}, {"STATUS", "status"}, {"TITLE", "title"}}
	return a.printRows(format, "session", rows, columns, "id", false, flags)
}
