package cmd

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/superhuman-remote-worker/srw/cli/internal/api"
	"github.com/superhuman-remote-worker/srw/cli/internal/config"
	"github.com/superhuman-remote-worker/srw/cli/internal/exit"
	"github.com/superhuman-remote-worker/srw/cli/internal/output"
	"github.com/superhuman-remote-worker/srw/cli/internal/routes"
	"github.com/superhuman-remote-worker/srw/cli/internal/target"
)

const tokenMenu = "Settings → API Keys"

// identity is what GET /api/auth/me answers. Only the fields srw shows are
// decoded; unknown fields are ignored.
type identity struct {
	User struct {
		ID          string `json:"id"`
		DisplayName string `json:"display_name"`
		Email       string `json:"email"`
		IsAdmin     bool   `json:"is_admin"`
		IsApproved  bool   `json:"is_approved"`
	} `json:"user"`
	// Credential is null on a server that does not report it yet.
	Credential *struct {
		Method    string     `json:"method"`
		TokenName string     `json:"token_name"`
		Scopes    []string   `json:"scopes"`
		Scope     string     `json:"scope"`
		ExpiresAt *time.Time `json:"expires_at"`
	} `json:"credential"`
}

func (who identity) label() string {
	switch {
	case who.User.DisplayName != "" && who.User.Email != "":
		return fmt.Sprintf("%s <%s>", who.User.DisplayName, who.User.Email)
	case who.User.DisplayName != "":
		return who.User.DisplayName
	case who.User.Email != "":
		return who.User.Email
	}
	return who.User.ID
}

func (a *App) loginCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "login",
		Short: "Save a server and a token on this machine",
		Long: "Save a server and a personal access token as a context, and make it the\n" +
			"current one. Create the token in the Cockpit under " + tokenMenu + ".\n\n" +
			"The token is asked for without echo. In a script, pass it on standard input\n" +
			"with --token-stdin. The context is named after the server's host unless\n" +
			"--context names it.",
		Example: "  srw login --server https://api.example.com\n" +
			"  pass show srw/dev | srw login --server https://api.dev.example.com --context dev --token-stdin",
		Args: noArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return a.login(command.Context())
		},
	}
}

func (a *App) login(ctx context.Context) error {
	// A server in the environment switches every command to environment
	// mode, which never reads a saved login. Logging in would then do
	// nothing a person could see.
	for _, name := range []string{"SRW_API_URL", "COCKPIT_API_URL"} {
		if a.Getenv(name) != "" {
			return exit.Usagef("%s is set, so commands use the environment and would not use a saved login", name).
				WithHint("unset %s (and SRW_TOKEN), then run `srw login` again", name)
		}
	}
	paths, err := a.paths()
	if err != nil {
		return err
	}
	file, err := config.Load(paths)
	if err != nil {
		return exit.Usagef("%v", err)
	}
	reader := bufio.NewReader(a.In)

	server := a.flags.server
	if server == "" {
		if !a.StdinIsTerminal || a.flags.tokenStdin {
			return exit.Usagef("login needs --server when it cannot ask for it")
		}
		fmt.Fprint(a.Err, "Server (the API address, for example https://api.example.com): ")
		line, err := reader.ReadString('\n')
		if err != nil && line == "" {
			return exit.Usagef("no server given")
		}
		server = strings.TrimSpace(line)
	}
	base, err := target.ParseServer(server, a.flags.insecureHTTP)
	if err != nil {
		return err
	}

	name := a.flags.context
	if name == "" {
		name = contextNameFor(base)
	}
	if !config.ValidContextName(name) {
		return exit.Usagef("%q cannot be a context name; use letters, digits, dots, dashes and underscores", name).
			WithHint("choose one with --context")
	}
	if existing := file.Contexts[name]; existing != nil && len(existing.TokenCommand) > 0 {
		return exit.Usagef("the context %q gets its token from a token-command, which would win over a saved token", name).
			WithHint("remove token-command from %s, or log in under another --context", paths.Config)
	}

	token, err := a.askToken(reader)
	if err != nil {
		return err
	}
	if strings.HasPrefix(token, "srw_") {
		fmt.Fprintln(a.Err, "warning: this is an MCP token (srw_…). srw is built for personal access tokens (ak_…), which carry scopes; some commands may behave differently.")
	}

	to := &target.Target{
		Mode: target.Context, Server: base, Context: name, Token: token,
		CAFile: a.flags.caFile, InsecureHTTP: a.flags.insecureHTTP,
	}
	client, err := a.client(to)
	if err != nil {
		return err
	}
	var who identity
	if err := client.JSON(ctx, api.Request{Method: http.MethodGet, Path: routes.AuthMe.Path, Known: true}, &who); err != nil {
		return err
	}

	if file.Contexts == nil {
		file.Contexts = map[string]*config.Context{}
	}
	entry := file.Contexts[name]
	if entry == nil {
		entry = &config.Context{}
		file.Contexts[name] = entry
	}
	entry.Server = base.String()
	if a.flags.caFile != "" {
		entry.CAFile = a.flags.caFile
	}
	if a.flags.insecureHTTP {
		entry.InsecureHTTP = true
	}
	file.CurrentContext = name
	credentials, _, err := config.LoadCredentials(paths)
	if err != nil {
		return exit.Usagef("%v", err)
	}
	if credentials.Tokens == nil {
		credentials.Tokens = map[string]string{}
	}
	credentials.Tokens[name] = token
	// Credentials first: a config that names a context without a token says
	// "run srw login", while a token without a context is never read.
	if err := credentials.Save(paths); err != nil {
		return exit.New(exit.Failed, "cannot write %s: %v", paths.Credentials, err)
	}
	if err := file.Save(paths); err != nil {
		return exit.New(exit.Failed, "cannot write %s: %v", paths.Config, err)
	}

	fmt.Fprintf(a.Out, "Logged in to %s as %s. Context %q is now the current one.\n", base, who.label(), name)
	if !who.User.IsApproved {
		fmt.Fprintln(a.Out, "This account is not approved yet; an admin must approve it before most commands work.")
	}
	if version, err := a.serverVersion(ctx, client); err == nil && version != nil {
		if version.CockpitURL != "" {
			fmt.Fprintf(a.Out, "Tokens are managed at %s/settings/api-keys\n", strings.TrimRight(version.CockpitURL, "/"))
		}
		a.warnAboutSkew(version)
	}
	return nil
}

// askToken reads the token: from standard input with --token-stdin, else
// from a hidden prompt. It never comes from a flag.
func (a *App) askToken(reader *bufio.Reader) (string, error) {
	if a.flags.tokenStdin {
		line, err := reader.ReadString('\n')
		token := strings.TrimSpace(line)
		if token == "" {
			if err != nil {
				return "", exit.Usagef("--token-stdin read no token from standard input")
			}
			return "", exit.Usagef("--token-stdin read an empty line")
		}
		return token, nil
	}
	if !a.StdinIsTerminal {
		return "", exit.Usagef("login cannot ask for the token here; pass it on standard input with --token-stdin")
	}
	fmt.Fprintf(a.Err, "Create a token in the Cockpit under %s, then paste it here.\n", tokenMenu)
	token, err := a.ReadSecret("Token: ")
	if err != nil {
		// Under Git Bash's mintty without a pseudo console the hidden
		// prompt fails with "The handle is invalid".
		return "", exit.Usagef("cannot read the token without echo: %v", err).
			WithHint("pass it on standard input with --token-stdin (on Git Bash, or run srw through winpty)")
	}
	if token == "" {
		return "", exit.Usagef("no token given")
	}
	return token, nil
}

// contextNameFor names a context after its server: the host, with the port
// when the address carries one.
func contextNameFor(base *url.URL) string {
	name := base.Hostname()
	if port := base.Port(); port != "" {
		name += "-" + port
	}
	return strings.ReplaceAll(name, ":", "-")
}

func (a *App) logoutCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "logout",
		Short: "Remove a saved token from this machine",
		Long: "Remove the token of the current context, or of --context, from this machine.\n" +
			"The token stays valid on the server: a token cannot revoke tokens. Revoke it\n" +
			"in the Cockpit under " + tokenMenu + ".",
		Args: noArgs,
		RunE: func(*cobra.Command, []string) error { return a.logout() },
	}
}

func (a *App) logout() error {
	paths, err := a.paths()
	if err != nil {
		return err
	}
	file, err := config.Load(paths)
	if err != nil {
		return exit.Usagef("%v", err)
	}
	name := a.flags.context
	if name == "" {
		name = a.Getenv("SRW_CONTEXT")
	}
	if name == "" {
		name = file.CurrentContext
	}
	if name == "" {
		return exit.Usagef("no context to log out of").WithHint("name one with --context")
	}
	credentials, _, err := config.LoadCredentials(paths)
	if err != nil {
		return exit.Usagef("%v", err)
	}
	if _, ok := credentials.Tokens[name]; !ok {
		fmt.Fprintf(a.Out, "The context %q has no saved token.\n", name)
		return nil
	}
	delete(credentials.Tokens, name)
	if err := credentials.Save(paths); err != nil {
		return exit.New(exit.Failed, "cannot write %s: %v", paths.Credentials, err)
	}
	fmt.Fprintf(a.Out, "Removed the token of context %q from %s.\n", name, paths.Credentials)
	fmt.Fprintf(a.Out, "The token is still valid on the server. Revoke it in the Cockpit under %s.\n", tokenMenu)
	return nil
}

func (a *App) whoamiCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "whoami",
		Short: "Show the user, the server and the token in use",
		Args:  noArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return a.whoami(command.Context())
		},
	}
}

const expiryWarning = 14 * 24 * time.Hour

func (a *App) whoami(ctx context.Context) error {
	format, err := a.format()
	if err != nil {
		return err
	}
	to, client, err := a.connect(true)
	if err != nil {
		return err
	}
	response, err := client.Send(ctx, api.Request{Method: http.MethodGet, Path: routes.AuthMe.Path, Known: true})
	if err != nil {
		return err
	}
	var who identity
	if err := json.Unmarshal(response.Body, &who); err != nil {
		return exit.New(exit.Failed, "%s did not answer JSON; is %s the API address?", routes.AuthMe.Path, to.Server)
	}
	switch format {
	case output.JSON, output.YAML:
		return output.Document(a.Out, format, response.Body)
	case output.Name:
		fmt.Fprintln(a.Out, "user/"+who.User.ID)
		return nil
	}

	table := a.table()
	fmt.Fprintf(table, "User:\t%s\n", who.label())
	fmt.Fprintf(table, "Server:\t%s\n", to.Server)
	if to.Mode == target.Context {
		fmt.Fprintf(table, "Context:\t%s\n", to.Context)
	} else {
		fmt.Fprintf(table, "Context:\tnone (server and token come from the environment)\n")
	}
	fmt.Fprintf(table, "Token from:\t%s\n", to.TokenSource)
	tokenHasNoAdmin := false
	if credential := who.Credential; credential != nil {
		if credential.TokenName != "" {
			fmt.Fprintf(table, "Token name:\t%s\n", credential.TokenName)
		}
		switch credential.Method {
		case "pat":
			fmt.Fprintf(table, "Scopes:\t%s\n", strings.Join(credential.Scopes, ", "))
			tokenHasNoAdmin = !contains(credential.Scopes, "admin")
		case "mcp":
			fmt.Fprintf(table, "Scope:\t%s (an MCP token)\n", credential.Scope)
		}
		if credential.Method == "pat" || credential.Method == "mcp" {
			if credential.ExpiresAt == nil {
				fmt.Fprintf(table, "Expires:\tnever\n")
			} else {
				fmt.Fprintf(table, "Expires:\t%s (%s)\n", credential.ExpiresAt.UTC().Format("2006-01-02"), remaining(credential.ExpiresAt.Sub(a.Now())))
			}
		}
	}
	switch {
	case who.User.IsAdmin:
		fmt.Fprintf(table, "Admin:\tyes\n")
	case tokenHasNoAdmin:
		// Without the admin scope the server drops the owner's admin flag
		// for the request, so lists show the owner's own rows only.
		fmt.Fprintf(table, "Admin:\tno (this token has no admin scope)\n")
	default:
		fmt.Fprintf(table, "Admin:\tno\n")
	}
	if !who.User.IsApproved {
		fmt.Fprintf(table, "Approved:\tno (an admin must approve this account)\n")
	}
	if err := table.Flush(); err != nil {
		return err
	}
	if credential := who.Credential; credential != nil && credential.ExpiresAt != nil {
		if left := credential.ExpiresAt.Sub(a.Now()); left < expiryWarning {
			fmt.Fprintf(a.Err, "warning: this token expires in %s; create a new one under %s and run `srw login` again\n",
				strings.TrimPrefix(remaining(left), "in "), tokenMenu)
		}
	}
	return nil
}

// remaining says how long a token still lasts, in whole days.
func remaining(left time.Duration) string {
	days := int(left.Hours() / 24)
	switch {
	case left <= 0:
		return "expired"
	case days == 0:
		return "in less than a day"
	case days == 1:
		return "in 1 day"
	}
	return fmt.Sprintf("in %d days", days)
}

func contains(list []string, wanted string) bool {
	for _, item := range list {
		if item == wanted {
			return true
		}
	}
	return false
}

// serverInfo is what GET /api/version answers.
type serverInfo struct {
	ReleaseVersion      string   `json:"release_version"`
	SourceRevision      string   `json:"source_revision"`
	ManifestAPIVersions []string `json:"manifest_api_versions"`
	CockpitURL          string   `json:"cockpit_url"`
}

// serverVersion asks the server what it is. A server that does not have the
// route yet answers 404; that is (nil, nil), not an error.
func (a *App) serverVersion(ctx context.Context, client *api.Client) (*serverInfo, error) {
	var info serverInfo
	err := client.JSON(ctx, api.Request{Method: http.MethodGet, Path: routes.Version.Path, Known: true}, &info)
	var coded *exit.Error
	if errors.As(err, &coded) && coded.Status == http.StatusNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &info, nil
}
