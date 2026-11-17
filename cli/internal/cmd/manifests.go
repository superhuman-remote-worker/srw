package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/superhuman-remote-worker/srw/cli/internal/api"
	"github.com/superhuman-remote-worker/srw/cli/internal/exit"
	"github.com/superhuman-remote-worker/srw/cli/internal/output"
	"github.com/superhuman-remote-worker/srw/cli/internal/routes"
)

var (
	uuidPattern     = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
	revisionPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
)

func isUUID(value string) bool { return uuidPattern.MatchString(value) }

// jsonOutput marks a command that has always printed JSON and keeps doing so.
var jsonOutput = map[string]string{annotationOutput: "json"}

// scopeFlags choose the scope of a stored lookup or of an apply.
type scopeFlags struct {
	kind, name string
	account    bool
	catalog    bool
	project    string
}

func (s *scopeFlags) add(command *cobra.Command) {
	flags := command.Flags()
	flags.BoolVar(&s.account, "account", false, "Your own account (the default)")
	flags.StringVarP(&s.project, "project", "p", "", "A project, by name or id")
	flags.BoolVar(&s.catalog, "catalog", false, "The shared catalog")
	flags.StringVar(&s.kind, "scope-kind", "", "Scope kind: Account, Project or Catalog (with --scope-name)")
	flags.StringVar(&s.name, "scope-name", "", "Scope name: me, a project id, or shared (with --scope-kind)")
}

// given says whether any scope flag was used.
func (s *scopeFlags) given() bool {
	return s.kind != "" || s.name != "" || s.account || s.catalog || s.project != ""
}

// resolve returns the scope the flags name, or nil when none was given. A
// project name is looked up on the server.
func (s *scopeFlags) resolve(ctx context.Context, a *App, client *api.Client) (map[string]string, error) {
	chosen := 0
	for _, used := range []bool{s.kind != "" || s.name != "", s.account, s.catalog, s.project != ""} {
		if used {
			chosen++
		}
	}
	switch {
	case chosen == 0:
		return nil, nil
	case chosen > 1:
		return nil, exit.Usagef("choose one scope: --account, --project, --catalog, or --scope-kind with --scope-name")
	case s.account:
		return map[string]string{"kind": "Account", "name": "me"}, nil
	case s.catalog:
		return map[string]string{"kind": "Catalog", "name": "shared"}, nil
	case s.project != "":
		project, err := a.resolveProject(ctx, client, s.project)
		if err != nil {
			return nil, err
		}
		return map[string]string{"kind": "Project", "name": project.ID}, nil
	}
	if s.kind == "" || s.name == "" {
		return nil, exit.Usagef("--scope-kind and --scope-name must be given together")
	}
	switch s.kind {
	case "Account", "Project", "Catalog":
	default:
		return nil, exit.Usagef("--scope-kind is Account, Project or Catalog, not %q", s.kind)
	}
	return map[string]string{"kind": s.kind, "name": s.name}, nil
}

// query is the scope as the list routes take it; the default is the account.
func scopeQuery(scope map[string]string) url.Values {
	if scope == nil {
		scope = map[string]string{"kind": "Account", "name": "me"}
	}
	return url.Values{"scope_kind": {scope["kind"]}, "scope_name": {scope["name"]}}
}

// manifestCall sends manifest files to one of the manifest routes.
func (a *App) manifestCall(ctx context.Context, client *api.Client, route routes.Route, body map[string]any, files []manifestFile) (*api.Response, error) {
	request := api.Request{Method: route.Method, Path: route.Path, Body: body, Known: true}
	response, err := client.Do(ctx, request)
	if err != nil {
		return nil, err
	}
	if len(files) > 1 && severalFilesRefused(response.Status, response.Body) {
		return nil, exit.New(exit.Failed, "this server takes one manifest file per call").
			WithHint("it is older than this srw; run the command once per file")
	}
	if err := client.StatusError(request, response); err != nil {
		// With one file the server names no source; the file is known here.
		var coded *exit.Error
		if errors.As(err, &coded) && coded.ErrCode != "" && coded.File == "" && len(files) == 1 {
			coded.File = files[0].Name
		}
		return nil, err
	}
	return response, nil
}

// printJSONDocument prints a server object for a command whose default
// output is JSON. names is called for -o name.
func (a *App) printJSONDocument(raw []byte, names func() []string) error {
	format, err := a.format()
	if err != nil {
		return err
	}
	if format == output.Name {
		if names == nil {
			return exit.Usagef("this command has no name output")
		}
		for _, name := range names() {
			fmt.Fprintln(a.Out, name)
		}
		return nil
	}
	return output.Document(a.Out, format, raw)
}

// documentNames reads kind and name of the documents in a validate or
// preview answer.
func documentNames(raw []byte) []string {
	var answer struct {
		Documents []struct {
			Kind     string `json:"kind"`
			Name     string `json:"name"`
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
		} `json:"documents"`
	}
	if json.Unmarshal(raw, &answer) != nil {
		return nil
	}
	names := make([]string, 0, len(answer.Documents))
	for _, document := range answer.Documents {
		name := document.Name
		if name == "" {
			name = document.Metadata.Name
		}
		names = append(names, strings.ToLower(document.Kind)+"/"+name)
	}
	return names
}

func (a *App) validateCommand() *cobra.Command {
	var files []string
	var format string
	command := &cobra.Command{
		Use:   "validate -f FILE",
		Short: "Check manifests without storing or starting anything",
		Long: "Check the syntax and the structure of manifests. Nothing is stored, nothing is\n" +
			"started, and no access is checked.\n\n" +
			"-f can be repeated. A directory means its .yaml, .yml and .json files. Several\n" +
			"files are checked as one bundle, and an error names the file it is in.",
		Example:     "  srw validate -f team.yaml\n  srw validate -f experts/ -f project.json\n  cat team.yaml | srw validate -f -",
		Args:        noArgs,
		Annotations: jsonOutput,
		RunE: func(command *cobra.Command, _ []string) error {
			manifests, err := a.readManifests(files, format)
			if err != nil {
				return err
			}
			_, client, err := a.connect(true)
			if err != nil {
				return err
			}
			response, err := a.manifestCall(command.Context(), client, routes.ManifestValidate, manifestBody(manifests), manifests)
			if err != nil {
				return err
			}
			return a.printJSONDocument(response.Body, func() []string { return documentNames(response.Body) })
		},
	}
	addFileFlags(command, &files, &format)
	return command
}

func addFileFlags(command *cobra.Command, files *[]string, format *string) {
	command.Flags().StringArrayVarP(files, "file", "f", nil, "Manifest file or directory; repeatable; - reads standard input")
	command.Flags().StringVar(format, "format", "", "Format of the files: json or yaml (default: by extension, else yaml)")
}

func (a *App) previewCommand() *cobra.Command {
	var files []string
	var format, resolution string
	var scope scopeFlags
	command := &cobra.Command{
		Use:   "preview -f FILE",
		Short: "Show what an apply would resolve to, without changing anything",
		Long: "Resolve manifests against what is stored and show the result. Nothing is\n" +
			"stored and nothing is started. The answer carries a planRevision; an apply\n" +
			"given that revision is refused when anything changed in between.\n\n" +
			"--resolution bundle resolves the files against each other only.",
		Example:     "  srw preview -f team.yaml > plan.json\n  srw apply -f team.yaml --plan plan.json",
		Args:        noArgs,
		Annotations: jsonOutput,
		RunE: func(command *cobra.Command, _ []string) error {
			if resolution != "stored" && resolution != "bundle" {
				return exit.Usagef("--resolution is stored or bundle, not %q", resolution)
			}
			manifests, err := a.readManifests(files, format)
			if err != nil {
				return err
			}
			_, client, err := a.connect(true)
			if err != nil {
				return err
			}
			chosen, err := scope.resolve(command.Context(), a, client)
			if err != nil {
				return err
			}
			response, err := a.preview(command.Context(), client, manifests, chosen, resolution)
			if err != nil {
				return err
			}
			return a.printJSONDocument(response.Body, func() []string { return documentNames(response.Body) })
		},
	}
	addFileFlags(command, &files, &format)
	scope.add(command)
	command.Flags().StringVar(&resolution, "resolution", "stored", "stored: against what is stored; bundle: the files against each other only")
	return command
}

func (a *App) preview(ctx context.Context, client *api.Client, manifests []manifestFile, scope map[string]string, resolution string) (*api.Response, error) {
	body := manifestBody(manifests)
	body["resolution"] = resolution
	if scope != nil {
		body["default_scope"] = scope
	}
	return a.manifestCall(ctx, client, routes.ManifestPreview, body, manifests)
}

type applyFlags struct {
	plan             string
	planRevision     string
	expectedVersions string
	expectedVersion  []string
	idempotencyKey   string
	dryRun           string
}

func (a *App) applyCommand() *cobra.Command {
	var files []string
	var format string
	var scope scopeFlags
	var flags applyFlags
	command := &cobra.Command{
		Use:   "apply -f FILE",
		Short: "Store manifests, and start the jobs they describe",
		Long: "Store the resources in the manifests. A Job document also starts the job.\n\n" +
			"srw first asks the server for a preview and then applies with what the\n" +
			"preview saw, so an apply is refused (exit 5) when a resource changed in\n" +
			"between; run it again then. To apply exactly what you reviewed, save a preview\n" +
			"and pass it with --plan.\n\n" +
			"The token needs resources:write. A Job document also needs jobs:write, and a\n" +
			"Project document projects:write.",
		Example: "  srw apply -f team.yaml\n" +
			"  srw apply -f team.yaml --dry-run=server\n" +
			"  srw apply -f job.yaml --idempotency-key \"$CI_PIPELINE_ID\"",
		Args:        noArgs,
		Annotations: jsonOutput,
		RunE: func(command *cobra.Command, _ []string) error {
			return a.apply(command.Context(), files, format, &scope, flags)
		},
	}
	addFileFlags(command, &files, &format)
	scope.add(command)
	f := command.Flags()
	f.StringVar(&flags.plan, "plan", "", "JSON output of `srw preview` for these same files and scope")
	f.StringVar(&flags.planRevision, "plan-revision", "", "The planRevision of a preview you reviewed")
	f.StringVar(&flags.expectedVersions, "expected-versions", "", "JSON file: resource identity to resourceVersion")
	f.StringArrayVar(&flags.expectedVersion, "expected-version", nil, "IDENTITY=VERSION for one resource (repeatable)")
	f.StringVar(&flags.idempotencyKey, "idempotency-key", "", "Send the identical request with the same key to recover a lost answer")
	f.StringVar(&flags.dryRun, "dry-run", "", "server: stop after the preview and print it")
	f.Lookup("dry-run").NoOptDefVal = "server"
	return command
}

func (a *App) apply(ctx context.Context, files []string, format string, scope *scopeFlags, flags applyFlags) error {
	if flags.dryRun != "" && flags.dryRun != "server" {
		return exit.Usagef("--dry-run is server, not %q", flags.dryRun)
	}
	if flags.plan != "" && flags.planRevision != "" {
		return exit.Usagef("give --plan or --plan-revision, not both")
	}
	if flags.idempotencyKey != "" && len(flags.idempotencyKey) > 128 {
		return exit.Usagef("an idempotency key has at most 128 characters")
	}
	versions, err := expectedVersions(flags)
	if err != nil {
		return err
	}
	revision, err := planRevision(flags)
	if err != nil {
		return err
	}
	manifests, err := a.readManifests(files, format)
	if err != nil {
		return err
	}
	_, client, err := a.connect(true)
	if err != nil {
		return err
	}
	chosen, err := scope.resolve(ctx, a, client)
	if err != nil {
		return err
	}
	// Someone who reviewed a plan, or names versions, decides what the apply
	// is checked against. Otherwise the preview decides.
	reviewed := flags.plan != "" || flags.planRevision != "" || flags.expectedVersions != "" || len(flags.expectedVersion) > 0
	previewed := false
	if !reviewed || flags.dryRun != "" {
		response, err := a.preview(ctx, client, manifests, chosen, "stored")
		if err != nil {
			return err
		}
		if flags.dryRun != "" {
			return a.printJSONDocument(response.Body, func() []string { return documentNames(response.Body) })
		}
		var seen struct {
			PlanRevision string `json:"planRevision"`
			Documents    []struct {
				Kind     string `json:"kind"`
				Metadata struct {
					Name  string `json:"name"`
					Scope struct {
						Kind string `json:"kind"`
						Name string `json:"name"`
					} `json:"scope"`
				} `json:"metadata"`
			} `json:"documents"`
			Dependencies map[string]struct {
				ResourceVersion int64 `json:"resourceVersion"`
			} `json:"dependencies"`
		}
		if err := json.Unmarshal(response.Body, &seen); err != nil {
			return exit.New(exit.Failed, "the preview did not answer as expected; nothing was applied")
		}
		revision = seen.PlanRevision
		// The versions of the documents themselves, as the preview saw
		// them. Without them an apply of an edited file is refused for
		// want of a version.
		for _, document := range seen.Documents {
			key := document.Kind + "/" + document.Metadata.Scope.Kind + "/" + document.Metadata.Scope.Name + "/" + document.Metadata.Name
			if stored, ok := seen.Dependencies[key]; ok {
				versions[key] = stored.ResourceVersion
			}
		}
		previewed = true
	}
	body := manifestBody(manifests)
	body["expected_versions"] = versions
	if chosen != nil {
		body["default_scope"] = chosen
	}
	if revision != "" {
		body["plan_revision"] = revision
	}
	if flags.idempotencyKey != "" {
		body["idempotency_key"] = flags.idempotencyKey
	}
	response, err := a.manifestCall(ctx, client, routes.ManifestApply, body, manifests)
	if err != nil {
		var coded *exit.Error
		if previewed && errors.As(err, &coded) && coded.Code == exit.Conflict && coded.Hint == "" {
			coded.Hint = "something changed since the preview; nothing was applied. Run the command again"
		}
		return err
	}
	var applied struct {
		Resources []struct {
			Resource struct {
				Kind     string `json:"kind"`
				Metadata struct {
					Name string `json:"name"`
				} `json:"metadata"`
			} `json:"resource"`
			UID             string `json:"uid"`
			ResourceVersion int64  `json:"resourceVersion"`
			Changed         bool   `json:"changed"`
		} `json:"resources"`
		Executions map[string]string `json:"executions"`
	}
	_ = json.Unmarshal(response.Body, &applied)
	names := func() []string {
		var list []string
		for _, item := range applied.Resources {
			list = append(list, strings.ToLower(item.Resource.Kind)+"/"+item.UID)
		}
		for _, job := range applied.Executions {
			list = append(list, "job/"+job)
		}
		return list
	}
	if err := a.printJSONDocument(response.Body, names); err != nil {
		return err
	}
	// The answer above is for scripts. These lines are for the person.
	for _, item := range applied.Resources {
		state := "unchanged"
		if item.Changed {
			state = "changed"
		}
		fmt.Fprintf(a.Err, "%s/%s %s (version %d)\n", item.Resource.Kind, item.Resource.Metadata.Name, state, item.ResourceVersion)
	}
	for _, job := range applied.Executions {
		fmt.Fprintf(a.Err, "job/%s created\n", job)
	}
	return nil
}

// expectedVersions reads --expected-versions and --expected-version.
func expectedVersions(flags applyFlags) (map[string]int64, error) {
	versions := map[string]int64{}
	if flags.expectedVersions != "" {
		object, err := readJSONObject(flags.expectedVersions)
		if err != nil {
			return nil, err
		}
		for key, value := range object {
			number, ok := value.(json.Number)
			version, convertErr := strconv.ParseInt(number.String(), 10, 64)
			if !ok || convertErr != nil {
				return nil, exit.Usagef("%s: the version of %q is not a whole number", flags.expectedVersions, key)
			}
			versions[key] = version
		}
	}
	for _, item := range flags.expectedVersion {
		split := strings.LastIndex(item, "=")
		if split <= 0 {
			return nil, exit.Usagef("--expected-version takes IDENTITY=VERSION, got %q", item)
		}
		key := item[:split]
		version, err := strconv.ParseInt(item[split+1:], 10, 64)
		if err != nil {
			return nil, exit.Usagef("--expected-version takes IDENTITY=VERSION, got %q", item)
		}
		if _, twice := versions[key]; twice {
			return nil, exit.Usagef("the expected version of %q is given twice", key)
		}
		versions[key] = version
	}
	if len(versions) > 100 {
		return nil, exit.Usagef("at most 100 expected versions")
	}
	for key, version := range versions {
		if version < 1 {
			return nil, exit.Usagef("the expected version of %q must be 1 or more", key)
		}
	}
	return versions, nil
}

// planRevision reads --plan-revision, or the planRevision of --plan.
func planRevision(flags applyFlags) (string, error) {
	revision := flags.planRevision
	if flags.plan != "" {
		reviewed, err := readJSONObject(flags.plan)
		if err != nil {
			return "", err
		}
		if reviewed["operation"] != "preview" || reviewed["resolution"] != "stored" {
			return "", exit.Usagef("--plan takes the JSON output of `srw preview` (a stored preview)")
		}
		revision, _ = reviewed["planRevision"].(string)
		if revision == "" {
			return "", exit.Usagef("%s holds no planRevision", flags.plan)
		}
	}
	if revision != "" && !revisionPattern.MatchString(revision) {
		return "", exit.Usagef("a plan revision looks like sha256:<64 hex digits>")
	}
	return revision, nil
}

// readJSONObject reads a JSON object from a file and refuses duplicate keys:
// a plan or a version list with a key twice says two things.
func readJSONObject(path string) (map[string]any, error) {
	data, err := readLimited(path)
	if err != nil {
		return nil, exit.Usagef("cannot read %s: %v", path, unwrapPath(err))
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.UseNumber()
	opening, err := decoder.Token()
	if delimiter, ok := opening.(json.Delim); err != nil || !ok || delimiter != '{' {
		return nil, exit.Usagef("%s must hold a JSON object", path)
	}
	object := map[string]any{}
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, exit.Usagef("%s is not valid JSON", path)
		}
		key := token.(string)
		if _, twice := object[key]; twice {
			return nil, exit.Usagef("%s holds the key %q twice", path, key)
		}
		var value any
		if err := decoder.Decode(&value); err != nil {
			return nil, exit.Usagef("%s is not valid JSON", path)
		}
		object[key] = value
	}
	return object, nil
}

// storedResource is a resource view of the manifest store.
type storedResource struct {
	Resource        json.RawMessage `json:"resource"`
	UID             string          `json:"uid"`
	ResourceVersion int64           `json:"resourceVersion"`
	// Redacted is nil on a server that does not report it.
	Redacted *bool `json:"redacted"`
}

func resourcePath(uid string) string {
	return strings.Replace(routes.ResourceGet.Path, "{resource_id}", strings.ToLower(uid), 1)
}

func (a *App) fetchResource(ctx context.Context, client *api.Client, uid string) (*storedResource, []byte, error) {
	response, err := client.Send(ctx, api.Request{Method: http.MethodGet, Path: resourcePath(uid), Known: true})
	if err != nil {
		return nil, nil, err
	}
	var stored storedResource
	if err := json.Unmarshal(response.Body, &stored); err != nil {
		return nil, nil, exit.New(exit.Failed, "the server's answer for %s is not a resource", uid)
	}
	return &stored, response.Body, nil
}

func (a *App) exportCommand() *cobra.Command {
	var allowRedacted bool
	var scope scopeFlags
	command := &cobra.Command{
		Use:   "export (UID | KIND NAME)",
		Short: "Print a stored resource as a manifest that can be applied again",
		Long: "Print the manifest a resource was stored from, as YAML, or as JSON with\n" +
			"-o json. This is the one output that is meant to be applied again.\n\n" +
			"A reader who is not the owner gets the document with secret values removed.\n" +
			"Applying such a file would remove those values, so export refuses it unless\n" +
			"--allow-redacted is given. An owner's export can contain credentials: keep\n" +
			"secrets in secret references before a file goes into a repository.",
		Example: "  srw export expert reviewer > reviewer.yaml\n  srw export 0b5c2f6e-…",
		Args: func(command *cobra.Command, args []string) error {
			if len(args) < 1 || len(args) > 2 {
				return exit.Usagef("%s takes a UID, or a kind and a name", command.CommandPath())
			}
			return nil
		},
		RunE: func(command *cobra.Command, args []string) error {
			return a.export(command.Context(), args, &scope, allowRedacted)
		},
	}
	scope.add(command)
	command.Flags().BoolVar(&allowRedacted, "allow-redacted", false, "Export a document that has secret values removed")
	return command
}

func (a *App) export(ctx context.Context, args []string, scope *scopeFlags, allowRedacted bool) error {
	format, err := a.format()
	if err != nil {
		return err
	}
	if format == output.Name {
		return exit.Usagef("`export` prints a manifest; it has no name output")
	}
	_, client, err := a.connect(true)
	if err != nil {
		return err
	}
	uid, err := a.resourceUID(ctx, client, args, scope)
	if err != nil {
		return err
	}
	stored, _, err := a.fetchResource(ctx, client, uid)
	if err != nil {
		return err
	}
	if stored.Redacted != nil && *stored.Redacted {
		if !allowRedacted {
			return exit.New(exit.Failed, "this view of the resource has secret values removed; applying the file back would delete them").
				WithHint("ask the owner to export it, or pass --allow-redacted to export the incomplete document")
		}
		fmt.Fprintln(a.Err, "warning: secret values are removed from this export; applying it back would delete them")
	} else if keys := secretKeys(stored.Resource); len(keys) > 0 {
		fmt.Fprintf(a.Err, "warning: this export contains credentials (%s); do not commit it. Keep secrets in secret references instead.\n", strings.Join(keys, ", "))
	}
	outputFormat := "yaml"
	if format == output.JSON {
		outputFormat = "json"
	}
	body := map[string]any{"source": string(stored.Resource), "format": "json", "output_format": outputFormat}
	var exported struct {
		Source string `json:"source"`
	}
	request := api.Request{Method: routes.ManifestExport.Method, Path: routes.ManifestExport.Path, Body: body, Known: true}
	if err := client.JSON(ctx, request, &exported); err != nil {
		return err
	}
	fmt.Fprint(a.Out, exported.Source)
	if !strings.HasSuffix(exported.Source, "\n") {
		fmt.Fprintln(a.Out)
	}
	return nil
}

// secretKeys lists the keys in a document whose value the server treats as
// a credential (security/config_redaction.py: _is_secret_key).
func secretKeys(document json.RawMessage) []string {
	var value any
	if json.Unmarshal(document, &value) != nil {
		return nil
	}
	found := map[string]bool{}
	var order []string
	var walk func(any)
	walk = func(node any) {
		switch typed := node.(type) {
		case map[string]any:
			for key, child := range typed {
				lower := strings.ToLower(key)
				switch lower {
				case "api_key", "password", "secret", "token", "private_key", "rclone_spec":
				default:
					if !strings.HasSuffix(lower, "_api_key") {
						walk(child)
						continue
					}
				}
				// A null or empty value is a place for a secret, not one.
				if text, isText := child.(string); (isText && text == "") || child == nil {
					continue
				}
				if !found[key] {
					found[key] = true
					order = append(order, key)
				}
			}
		case []any:
			for _, child := range typed {
				walk(child)
			}
		}
	}
	walk(value)
	sortStrings(order)
	return order
}

func (a *App) deleteCommand() *cobra.Command {
	var expectedVersion int64
	var yes bool
	var scope scopeFlags
	command := &cobra.Command{
		Use:   "delete (UID | KIND NAME)",
		Short: "Delete a stored resource, a project or a job",
		Long: "Delete a resource. On a terminal srw asks first; elsewhere it needs --yes, or\n" +
			"--expected-version for a stored resource addressed by its UID.\n\n" +
			"Deleting a project deletes the whole project: its repositories, its cloud\n" +
			"folder and its group. The token needs resources:write, and projects:write for\n" +
			"a project.",
		Example: "  srw delete expert reviewer\n  srw delete 0b5c2f6e-… --expected-version 4",
		Args: func(command *cobra.Command, args []string) error {
			if len(args) < 1 || len(args) > 2 {
				return exit.Usagef("%s takes a UID, or a kind and a name", command.CommandPath())
			}
			return nil
		},
		// The UID form has always printed JSON.
		Annotations: jsonOutput,
		RunE: func(command *cobra.Command, args []string) error {
			return a.delete(command.Context(), args, &scope, expectedVersion, command.Flags().Changed("expected-version"), yes)
		},
	}
	scope.add(command)
	command.Flags().Int64Var(&expectedVersion, "expected-version", 0, "Delete only if the resource is still at this version")
	command.Flags().BoolVarP(&yes, "yes", "y", false, "Do not ask")
	return command
}

func (a *App) delete(ctx context.Context, args []string, scope *scopeFlags, expectedVersion int64, versionGiven, yes bool) error {
	if versionGiven && expectedVersion < 1 {
		return exit.Usagef("--expected-version must be 1 or more")
	}
	_, client, err := a.connect(true)
	if err != nil {
		return err
	}
	if kindName, name, splitErr := splitTarget(args); splitErr == nil && kindName == "job" && !(len(args) == 1 && isUUID(args[0])) {
		if versionGiven || scope.given() {
			return exit.Usagef("a job has no version and no scope; srw delete job <id> [--yes]")
		}
		return a.deleteJob(ctx, client, name, yes)
	}
	byName := len(args) == 2 || !isUUID(args[0])
	// A name can exist in two scopes. Without a person to confirm, a
	// delete by name must say which scope it means.
	if byName && !a.interactive() && !scope.given() && kindOf(args[0]) == "workspacetemplate" {
		return exit.Usagef("a delete by name without a prompt needs a scope (--account, --project or --catalog), or the UID")
	}
	uid, err := a.resourceUID(ctx, client, args, scope)
	if err != nil {
		return err
	}
	if !versionGiven || byName {
		stored, _, err := a.fetchResource(ctx, client, uid)
		if err != nil {
			return err
		}
		var document struct {
			Kind     string `json:"kind"`
			Metadata struct {
				Name  string `json:"name"`
				Scope struct {
					Kind string `json:"kind"`
					Name string `json:"name"`
				} `json:"scope"`
			} `json:"metadata"`
		}
		_ = json.Unmarshal(stored.Resource, &document)
		if !versionGiven {
			expectedVersion = stored.ResourceVersion
		}
		if !yes && !(versionGiven && !byName) {
			what := fmt.Sprintf("%s %q in %s %s (UID %s, version %d)", document.Kind, document.Metadata.Name,
				document.Metadata.Scope.Kind, document.Metadata.Scope.Name, uid, stored.ResourceVersion)
			if document.Kind == "Project" {
				what += ". This deletes the whole project: its repositories, its cloud folder and its group"
			}
			if err := a.confirm("Delete " + what + "?"); err != nil {
				return err
			}
		}
	}
	request := api.Request{
		Method: routes.ResourceDelete.Method, Path: resourcePath(uid),
		Query: url.Values{"expected_version": {strconv.FormatInt(expectedVersion, 10)}}, Known: true,
	}
	response, err := client.Send(ctx, request)
	if err != nil {
		return err
	}
	if err := a.printJSONDocument(response.Body, func() []string { return []string{"resource/" + uid} }); err != nil {
		return err
	}
	if response.Status == http.StatusAccepted {
		fmt.Fprintln(a.Err, "The deletion was accepted and continues in the background.")
	}
	return nil
}

// interactive says whether a person can be asked: both ends are a terminal.
func (a *App) interactive() bool {
	return a.StdinIsTerminal && a.StdoutIsTerminal
}

// confirm asks a yes-or-no question, or refuses when nobody can answer.
func (a *App) confirm(question string) error {
	if !a.interactive() {
		return exit.Usagef("this needs a confirmation; pass --yes to delete without asking")
	}
	fmt.Fprintf(a.Err, "%s [y/N] ", question)
	var answer string
	fmt.Fscanln(a.In, &answer)
	switch strings.ToLower(strings.TrimSpace(answer)) {
	case "y", "yes":
		return nil
	}
	return exit.New(exit.Failed, "not deleted")
}

func (a *App) cacheCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "cache",
		Short: "Inspect or evict prepared workspace artifacts",
		Long: "List or delete the prepared workspace artifacts of a scope. The server needs\n" +
			"Kubernetes manifest hosting and a VM provisioner for these.",
	}
	var listScope, deleteScope scopeFlags
	list := &cobra.Command{
		Use:         "list",
		Short:       "List the prepared workspace artifacts of a scope",
		Args:        noArgs,
		Annotations: jsonOutput,
		RunE: func(command *cobra.Command, _ []string) error {
			_, client, err := a.connect(true)
			if err != nil {
				return err
			}
			chosen, err := listScope.resolve(command.Context(), a, client)
			if err != nil {
				return err
			}
			response, err := client.Send(command.Context(), api.Request{
				Method: routes.CacheList.Method, Path: routes.CacheList.Path, Query: scopeQuery(chosen), Known: true,
			})
			if err != nil {
				return err
			}
			return a.printJSONDocument(response.Body, nil)
		},
	}
	listScope.add(list)
	remove := &cobra.Command{
		Use:         "delete ARTIFACT_ID",
		Short:       "Delete one prepared workspace artifact",
		Args:        exactArgs(1, "one artifact id"),
		Annotations: jsonOutput,
		RunE: func(command *cobra.Command, args []string) error {
			if !isUUID(args[0]) {
				return exit.Usagef("%q is not an artifact id", args[0])
			}
			_, client, err := a.connect(true)
			if err != nil {
				return err
			}
			chosen, err := deleteScope.resolve(command.Context(), a, client)
			if err != nil {
				return err
			}
			path := strings.Replace(routes.CacheDelete.Path, "{artifact_id}", strings.ToLower(args[0]), 1)
			response, err := client.Send(command.Context(), api.Request{
				Method: routes.CacheDelete.Method, Path: path, Query: scopeQuery(chosen), Known: true,
			})
			if err != nil {
				return err
			}
			return a.printJSONDocument(response.Body, nil)
		},
	}
	deleteScope.add(remove)
	command.AddCommand(list, remove)
	return command
}
