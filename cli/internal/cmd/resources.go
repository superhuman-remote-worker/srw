package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"github.com/superhuman-remote-worker/srw/cli/internal/api"
	"github.com/superhuman-remote-worker/srw/cli/internal/exit"
	"github.com/superhuman-remote-worker/srw/cli/internal/output"
	"github.com/superhuman-remote-worker/srw/cli/internal/routes"
)

// kind is one kind of object a command can name.
type kind struct {
	// name is the canonical spelling on the command line.
	name    string
	aliases []string
	// manifest is the server's spelling for a manifest kind.
	manifest string
}

var kinds = []kind{
	{"job", []string{"jobs"}, "Job"},
	{"expert", []string{"experts"}, "Expert"},
	{"project", []string{"projects"}, "Project"},
	{"workspacetemplate", []string{"workspacetemplates", "wt"}, "WorkspaceTemplate"},
	{"connector", []string{"connectors"}, "Connector"},
	// Sessions can be listed. Working in one happens in the Cockpit.
	{"session", []string{"sessions"}, ""},
}

// kindOf returns the canonical kind for what was typed, singular or plural,
// in any case; empty when it is not a kind.
func kindOf(typed string) string {
	lower := strings.ToLower(typed)
	for _, known := range kinds {
		if lower == known.name {
			return known.name
		}
		for _, alias := range known.aliases {
			if lower == alias {
				return known.name
			}
		}
	}
	return ""
}

func manifestKind(name string) string {
	for _, known := range kinds {
		if known.name == name {
			return known.manifest
		}
	}
	return ""
}

func kindNames() string {
	names := make([]string, 0, len(kinds))
	for _, known := range kinds {
		names = append(names, known.name)
	}
	return strings.Join(names, ", ")
}

// splitTarget reads "KIND NAME" or "KIND/NAME" from the arguments.
func splitTarget(args []string) (kindName, name string, err error) {
	typed := args[0]
	if len(args) == 2 {
		name = args[1]
	} else if before, after, found := strings.Cut(typed, "/"); found {
		typed, name = before, after
	}
	kindName = kindOf(typed)
	if kindName == "" {
		return "", "", exit.Usagef("%q is not a kind; the kinds are: %s", typed, kindNames())
	}
	return kindName, name, nil
}

// row is one object of a list, kept as the server sent it.
type row struct {
	raw    json.RawMessage
	fields map[string]any
}

func (r row) text(key string) string {
	switch value := r.fields[key].(type) {
	case string:
		return value
	case nil:
		return ""
	default:
		return fmt.Sprint(value)
	}
}

func decodeRows(items []json.RawMessage) []row {
	rows := make([]row, 0, len(items))
	for _, item := range items {
		fields := map[string]any{}
		_ = json.Unmarshal(item, &fields)
		rows = append(rows, row{raw: item, fields: fields})
	}
	return rows
}

// listRows reads a route that answers a bare JSON array.
func (a *App) listRows(ctx context.Context, client *api.Client, route routes.Route) ([]row, error) {
	response, err := client.Send(ctx, api.Request{Method: route.Method, Path: route.Path, Known: true})
	if err != nil {
		return nil, err
	}
	var items []json.RawMessage
	if err := json.Unmarshal(response.Body, &items); err != nil {
		return nil, exit.New(exit.Failed, "%s did not answer a list", route.Path)
	}
	return decodeRows(items), nil
}

// listConnectors walks the connector catalog, which is paged by a cursor.
func (a *App) listConnectors(ctx context.Context, client *api.Client, search string) ([]row, error) {
	var rows []row
	cursor := ""
	for page := 0; page < 50; page++ {
		query := url.Values{"limit": {"100"}}
		if search != "" {
			query.Set("q", search)
		}
		if cursor != "" {
			query.Set("cursor", cursor)
		}
		var answer struct {
			Items      []json.RawMessage `json:"items"`
			NextCursor string            `json:"next_cursor"`
		}
		request := api.Request{Method: routes.ConnectorCatalog.Method, Path: routes.ConnectorCatalog.Path, Query: query, Known: true}
		if err := client.JSON(ctx, request, &answer); err != nil {
			return nil, err
		}
		rows = append(rows, decodeRows(answer.Items)...)
		if answer.NextCursor == "" {
			return rows, nil
		}
		cursor = answer.NextCursor
	}
	return nil, exit.New(exit.Failed, "the connector list did not end after 5000 rows; narrow it with a name")
}

// storedList reads the manifest store's list of one scope.
type storedList struct {
	rows []row
	// truncated is nil on a server that does not report it.
	truncated *bool
	body      []byte
}

func (a *App) storedList(ctx context.Context, client *api.Client, scope map[string]string, manifestKind, name string) (*storedList, error) {
	query := scopeQuery(scope)
	if manifestKind != "" {
		query.Set("kind", manifestKind)
	}
	if name != "" {
		query.Set("name", name)
	}
	response, err := client.Send(ctx, api.Request{Method: routes.ResourceList.Method, Path: routes.ResourceList.Path, Query: query, Known: true})
	if err != nil {
		return nil, err
	}
	var answer struct {
		Resources []json.RawMessage `json:"resources"`
		Truncated *bool             `json:"truncated"`
	}
	if err := json.Unmarshal(response.Body, &answer); err != nil {
		return nil, exit.New(exit.Failed, "%s did not answer a resource list", routes.ResourceList.Path)
	}
	return &storedList{rows: decodeRows(answer.Resources), truncated: answer.Truncated, body: response.Body}, nil
}

// storedName reads metadata.name of a stored resource view.
func storedName(view row) (kindName, name, scope string) {
	resource, _ := view.fields["resource"].(map[string]any)
	metadata, _ := resource["metadata"].(map[string]any)
	scopeValue, _ := metadata["scope"].(map[string]any)
	kindName, _ = resource["kind"].(string)
	name, _ = metadata["name"].(string)
	scope = fmt.Sprintf("%v/%v", scopeValue["kind"], scopeValue["name"])
	return kindName, name, scope
}

// match finds the rows a name or an id stands for: the id first, then each
// name field in the order given. The filter always runs here as well: a
// server that ignores a name parameter returns the whole list, and trusting
// it would address the first row.
func match(rows []row, wanted string, idKey string, nameKeys ...string) []row {
	var found []row
	for _, candidate := range rows {
		if strings.EqualFold(candidate.text(idKey), wanted) {
			found = append(found, candidate)
		}
	}
	for _, key := range nameKeys {
		if len(found) > 0 {
			break
		}
		for _, candidate := range rows {
			if candidate.text(key) == wanted {
				found = append(found, candidate)
			}
		}
	}
	return found
}

// portableName is the field that holds the name an expert was applied
// under. The server lists an expert stored from a manifest as
// "<name>-<uid>", so that two owners can use the same name; the name a
// person wrote in the manifest is that without the UID.
const portableName = "_name"

func (a *App) listExperts(ctx context.Context, client *api.Client) ([]row, error) {
	rows, err := a.listRows(ctx, client, routes.ExpertList)
	if err != nil {
		return nil, err
	}
	for _, expert := range rows {
		name := expert.text("name")
		if uid := strings.ReplaceAll(expert.text("manifest_uid"), "-", ""); uid != "" {
			name = strings.TrimSuffix(name, "-"+uid)
		}
		expert.fields[portableName] = name
	}
	return rows, nil
}

// one returns the single match, or says why there is none or more than one.
func one(kindName, wanted string, found []row, idKey, nameKey string) (row, error) {
	switch len(found) {
	case 0:
		return row{}, exit.New(exit.NotFound, "there is no %s %q", kindName, wanted).
			WithHint("`srw get %s` lists the ones you can see", kindName)
	case 1:
		return found[0], nil
	}
	candidates := make([]string, 0, len(found))
	for _, candidate := range found {
		candidates = append(candidates, fmt.Sprintf("%s (%s)", candidate.text(nameKey), candidate.text(idKey)))
	}
	sort.Strings(candidates)
	return row{}, exit.Usagef("%q names %d %ss: %s", wanted, len(found), kindName, strings.Join(candidates, ", ")).
		WithHint("use the id instead of the name")
}

type projectRef struct {
	ID          string
	ManifestUID string
}

// projectID is the id of a project named by id or by name. An id is used
// as it is, without a request; a token that may not list projects can then
// still name one.
func (a *App) projectID(ctx context.Context, client *api.Client, wanted string) (string, error) {
	if isUUID(wanted) {
		return strings.ToLower(wanted), nil
	}
	project, err := a.resolveProject(ctx, client, wanted)
	if err != nil {
		return "", err
	}
	return project.ID, nil
}

// resolveProject finds a project by name or id.
func (a *App) resolveProject(ctx context.Context, client *api.Client, wanted string) (*projectRef, error) {
	rows, err := a.listRows(ctx, client, routes.ProjectList)
	if err != nil {
		return nil, err
	}
	found, err := one("project", wanted, match(rows, wanted, "id", "name"), "id", "name")
	if err != nil {
		return nil, err
	}
	return &projectRef{ID: found.text("id"), ManifestUID: found.text("manifest_uid")}, nil
}

// resourceUID turns what was typed into the UID of a stored resource: a UID
// as it is, or a kind and a name looked up where a user sees that kind.
func (a *App) resourceUID(ctx context.Context, client *api.Client, args []string, scope *scopeFlags) (string, error) {
	if len(args) == 1 && isUUID(args[0]) {
		if scope.given() {
			return "", exit.Usagef("a UID names one resource; a scope cannot be given with it")
		}
		return strings.ToLower(args[0]), nil
	}
	kindName, name, err := splitTarget(args)
	if err != nil {
		return "", err
	}
	if name == "" {
		return "", exit.Usagef("give the name or the id of the %s", kindName)
	}
	if kindName == "job" || kindName == "session" {
		return "", exit.Usagef("a %s is not a stored resource; a stored Job definition is addressed by its UID", kindName)
	}
	if kindName != "workspacetemplate" && scope.given() {
		return "", exit.Usagef("a scope is for stored lookups (workspacetemplate); a %s is looked up in the list you can see", kindName)
	}
	notStored := func(found row) error {
		return exit.New(exit.Failed, "the %s %q is not a stored resource (it predates manifests)", kindName, found.text("name"))
	}
	switch kindName {
	case "expert":
		rows, err := a.listExperts(ctx, client)
		if err != nil {
			return "", err
		}
		found, err := one(kindName, name, match(rows, name, "id", portableName, "name", "display_name"), "id", portableName)
		if err != nil {
			return "", err
		}
		if found.text("manifest_uid") == "" {
			return "", notStored(found)
		}
		return found.text("manifest_uid"), nil
	case "project":
		project, err := a.resolveProject(ctx, client, name)
		if err != nil {
			return "", err
		}
		if project.ManifestUID == "" {
			return "", exit.New(exit.Failed, "the project %q is not a stored resource", name)
		}
		return project.ManifestUID, nil
	case "connector":
		search := name
		if isUUID(name) {
			search = ""
		}
		rows, err := a.listConnectors(ctx, client, search)
		if err != nil {
			return "", err
		}
		found, err := one(kindName, name, match(rows, name, "id", "name"), "id", "name")
		if err != nil {
			return "", err
		}
		// A connector's row id is its resource UID when a resource exists.
		return found.text("id"), nil
	}
	// A workspace template lives in the manifest store only.
	if isUUID(name) {
		return strings.ToLower(name), nil
	}
	chosen, err := scope.resolve(ctx, a, client)
	if err != nil {
		return "", err
	}
	list, err := a.storedList(ctx, client, chosen, manifestKind(kindName), name)
	if err != nil {
		return "", err
	}
	if list.truncated == nil {
		// Without the name filter the list is cut at 500 rows before the
		// visibility filter, so a missing name proves nothing.
		return "", exit.New(exit.Failed, "this server cannot look a stored resource up by name").
			WithHint("it is older than this srw; use the UID (`srw get --kind WorkspaceTemplate` lists them)")
	}
	var found []row
	for _, view := range list.rows {
		if _, stored, _ := storedName(view); stored == name {
			found = append(found, view)
		}
	}
	switch len(found) {
	case 0:
		return "", exit.New(exit.NotFound, "there is no %s %q in this scope", kindName, name).
			WithHint("choose another scope with --project or --catalog")
	case 1:
		return found[0].text("uid"), nil
	}
	return "", exit.Usagef("%q names %d %ss in this scope", name, len(found), kindName)
}

type getFlags struct {
	kind      string
	stored    bool
	noHeaders bool
	noTrunc   bool
	jobs      jobListFlags
}

func (a *App) getCommand() *cobra.Command {
	var flags getFlags
	var scope scopeFlags
	command := &cobra.Command{
		Use:   "get [KIND [NAME] | UID]",
		Short: "List objects, or show one",
		Long: "List what you can see of a kind, or show one object.\n\n" +
			"Kinds: " + kindNames() + " (singular or plural; wt is short for\n" +
			"workspacetemplate). Experts, projects and connectors are listed as the Cockpit\n" +
			"lists them. --stored shows the manifest store's view of one scope instead.\n\n" +
			"Two older forms print JSON: `srw get UID` reads one stored resource, and\n" +
			"`srw get [--kind KIND]` without a kind argument lists the stored resources of\n" +
			"a scope.",
		Example: "  srw get jobs --status failed\n  srw get experts\n  srw get project website -o yaml\n  srw get wt --project website\n  srw get 0b5c2f6e-…",
		Args: func(command *cobra.Command, args []string) error {
			if len(args) > 2 {
				return exit.Usagef("%s takes a kind and at most one name, or a UID", command.CommandPath())
			}
			return nil
		},
		RunE: func(command *cobra.Command, args []string) error {
			return a.get(command.Context(), args, flags, &scope)
		},
	}
	scope.add(command)
	f := command.Flags()
	f.StringVar(&flags.kind, "kind", "", "With no kind argument: only stored resources of this manifest kind")
	f.BoolVar(&flags.stored, "stored", false, "Show the manifest store's view of the chosen scope")
	f.BoolVar(&flags.noHeaders, "no-headers", false, "Print the table without its header line")
	f.BoolVar(&flags.noTrunc, "no-trunc", false, "Print full ids")
	f.StringArrayVar(&flags.jobs.statuses, "status", nil, "Jobs: only this status (repeatable)")
	f.StringArrayVar(&flags.jobs.origins, "origin", nil, "Jobs: only this origin, for example user or automation (repeatable)")
	f.StringVar(&flags.jobs.search, "search", "", "Jobs: only those whose task, name or id matches")
	f.IntVar(&flags.jobs.limit, "limit", 50, "Jobs: how many to read per request, 1 to 500")
	f.BoolVar(&flags.jobs.all, "all", false, "Jobs: read every page, not only the first")
	f.BoolVar(&flags.jobs.subjobs, "subjobs", false, "Jobs: also list the jobs that other jobs started")
	return command
}

func (a *App) get(ctx context.Context, args []string, flags getFlags, scope *scopeFlags) error {
	format, err := a.format()
	if err != nil {
		return err
	}
	to, client, err := a.connect(true)
	if err != nil {
		return err
	}
	// The two older forms: a UID, and no argument at all. They print JSON.
	if len(args) == 1 && isUUID(args[0]) {
		if scope.given() || flags.kind != "" {
			return exit.Usagef("a UID names one resource; a scope or a kind cannot be given with it")
		}
		a.jsonByDefault = true
		_, body, err := a.fetchResource(ctx, client, args[0])
		if err != nil {
			return err
		}
		return a.printJSONDocument(body, func() []string { return []string{"resource/" + strings.ToLower(args[0])} })
	}
	if len(args) == 0 {
		a.jsonByDefault = true
		if flags.kind != "" && manifestKindExact(flags.kind) == "" {
			return exit.Usagef("--kind is one of Expert, WorkspaceTemplate, Connector, Project, Job; not %q", flags.kind)
		}
		chosen, err := scope.resolve(ctx, a, client)
		if err != nil {
			return err
		}
		list, err := a.storedList(ctx, client, chosen, flags.kind, "")
		if err != nil {
			return err
		}
		a.warnTruncated(list)
		return a.printJSONDocument(list.body, func() []string { return storedNames(list.rows) })
	}
	if flags.kind != "" {
		return exit.Usagef("--kind is for the list without a kind argument; here the kind is the argument")
	}
	kindName, name, err := splitTarget(args)
	if err != nil {
		return err
	}
	switch kindName {
	case "job":
		if flags.stored {
			return exit.Usagef("jobs are listed as they run; the stored Job definitions are `srw get --kind Job`")
		}
		if scope.account || scope.catalog || scope.kind != "" || scope.name != "" {
			return exit.Usagef("jobs are narrowed with --project, --status, --origin and --search")
		}
		flags.jobs.project = scope.project
		return a.getJobs(ctx, client, to.Project, format, name, flags.jobs, flags)
	case "session":
		if name != "" || flags.stored || scope.given() {
			return exit.Usagef("sessions can only be listed: srw get sessions")
		}
		return a.getSessions(ctx, client, format, flags)
	}
	if flags.stored || kindName == "workspacetemplate" {
		return a.getStored(ctx, client, format, kindName, name, flags, scope)
	}
	if scope.given() {
		return exit.Usagef("a scope is for the manifest store (--stored); `srw get %s` lists what you can see", kindName)
	}
	var rows []row
	var columns []column
	switch kindName {
	case "expert":
		rows, err = a.listExperts(ctx, client)
		columns = []column{{"NAME", portableName}, {"TYPE", "expert_type"}, {"SOURCE", "source"}, {"ID", "id"}}
	case "project":
		rows, err = a.listRows(ctx, client, routes.ProjectList)
		columns = []column{{"NAME", "name"}, {"STATUS", "status"}, {"ID", "id"}}
	case "connector":
		search := name
		if isUUID(name) {
			search = ""
		}
		rows, err = a.listConnectors(ctx, client, search)
		columns = []column{{"NAME", "name"}, {"TYPE", "type"}, {"ID", "id"}}
	}
	if err != nil {
		return err
	}
	single := name != ""
	if single {
		nameKeys := []string{"name"}
		if kindName == "expert" {
			nameKeys = []string{portableName, "name", "display_name"}
		}
		found, err := one(kindName, name, match(rows, name, "id", nameKeys...), "id", nameKeys[0])
		if err != nil {
			return err
		}
		rows = []row{found}
	}
	return a.printRows(format, kindName, rows, columns, "id", single, flags)
}

// getStored shows the manifest store's view of one scope.
func (a *App) getStored(ctx context.Context, client *api.Client, format output.Format, kindName, name string, flags getFlags, scope *scopeFlags) error {
	chosen, err := scope.resolve(ctx, a, client)
	if err != nil {
		return err
	}
	list, err := a.storedList(ctx, client, chosen, manifestKind(kindName), name)
	if err != nil {
		return err
	}
	rows := make([]row, 0, len(list.rows))
	for _, view := range list.rows {
		storedKind, storedNameValue, storedScope := storedName(view)
		if name != "" && storedNameValue != name {
			continue
		}
		// The table reads flat fields; the JSON output keeps the view.
		view.fields["kind"], view.fields["name"], view.fields["scope"] = storedKind, storedNameValue, storedScope
		rows = append(rows, view)
	}
	if name != "" {
		if list.truncated == nil && len(rows) == 0 {
			return exit.New(exit.Failed, "this server cannot look a stored resource up by name").
				WithHint("it is older than this srw; `srw get --kind %s` lists the stored ones as JSON", manifestKind(kindName))
		}
		if len(rows) == 0 {
			return exit.New(exit.NotFound, "there is no %s %q in this scope", kindName, name).
				WithHint("choose another scope with --project or --catalog")
		}
	}
	a.warnTruncated(list)
	if format == output.Default && name == "" {
		listed := "your account"
		if chosen != nil {
			listed = strings.ToLower(chosen["kind"]) + " " + chosen["name"]
		}
		fmt.Fprintf(a.Err, "Stored %ss of %s:\n", kindName, listed)
	}
	columns := []column{{"NAME", "name"}, {"SCOPE", "scope"}, {"VERSION", "resourceVersion"}, {"UID", "uid"}}
	return a.printRows(format, kindName, rows, columns, "uid", name != "", flags)
}

func (a *App) warnTruncated(list *storedList) {
	if list.truncated != nil && *list.truncated {
		fmt.Fprintln(a.Err, "warning: this scope holds more resources than one call returns; the list is cut. Narrow it with a kind or a name.")
	}
}

func storedNames(rows []row) []string {
	names := make([]string, 0, len(rows))
	for _, view := range rows {
		storedKind, _, _ := storedName(view)
		names = append(names, strings.ToLower(storedKind)+"/"+view.text("uid"))
	}
	return names
}

func manifestKindExact(typed string) string {
	for _, known := range []string{"Expert", "WorkspaceTemplate", "Connector", "Project", "Job"} {
		if typed == known {
			return known
		}
	}
	return ""
}

// column is one table column: a header and the row field it shows.
type column struct {
	header string
	field  string
}

// printRows prints rows as a table, or in a machine format. One object is
// printed as the server's object; a list as {"items": [...]} with the
// server's rows unchanged inside.
func (a *App) printRows(format output.Format, kindName string, rows []row, columns []column, idKey string, single bool, flags getFlags) error {
	switch format {
	case output.Name:
		for _, item := range rows {
			fmt.Fprintln(a.Out, kindName+"/"+item.text(idKey))
		}
		return nil
	case output.JSON, output.YAML:
		var raw []byte
		if single {
			raw = rows[0].raw
		} else {
			items := make([]json.RawMessage, 0, len(rows))
			for _, item := range rows {
				items = append(items, item.raw)
			}
			encoded, err := json.Marshal(map[string]any{"items": items})
			if err != nil {
				return err
			}
			raw = encoded
		}
		return output.Document(a.Out, format, raw)
	}
	if len(rows) == 0 {
		fmt.Fprintf(a.Err, "No %s found.\n", kindName)
		return nil
	}
	table := a.table()
	if !flags.noHeaders {
		headers := make([]string, 0, len(columns))
		for _, entry := range columns {
			headers = append(headers, entry.header)
		}
		fmt.Fprintln(table, strings.Join(headers, "\t"))
	}
	for _, item := range rows {
		cells := make([]string, 0, len(columns))
		for _, entry := range columns {
			value := item.text(entry.field)
			// An id is shortened for a person only: on a terminal, and
			// never when the output goes anywhere else.
			if entry.field == idKey && a.StdoutIsTerminal && !flags.noTrunc && isUUID(value) {
				value = value[:8]
			}
			cells = append(cells, dash(value))
		}
		fmt.Fprintln(table, strings.Join(cells, "\t"))
	}
	return table.Flush()
}

func sortStrings(values []string) { sort.Strings(values) }
