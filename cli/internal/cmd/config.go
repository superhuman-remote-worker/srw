package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/superhuman-remote-worker/srw/cli/internal/config"
	"github.com/superhuman-remote-worker/srw/cli/internal/exit"
	"github.com/superhuman-remote-worker/srw/cli/internal/output"
	"github.com/superhuman-remote-worker/srw/cli/internal/remote"
)

func (a *App) configCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "config",
		Short: "Manage the saved logins (contexts)",
		Long: "A context is one server with its options. `srw login` creates one; these\n" +
			"commands list, switch, change and remove them. Tokens are kept in a separate\n" +
			"file and are never shown.\n\n" +
			"The config file can name a command that prints the token (token-command), so\n" +
			"it is read only from the config directory, never from the working directory.",
	}
	var project string
	setContext := &cobra.Command{
		Use:   "set-context NAME",
		Short: "Create a context, or change its server and options",
		Long: "Create a context, or change one. Uses the global flags --server, --ca-file\n" +
			"and --insecure-http, and --project for the default project. It stores no\n" +
			"token; `srw login --context NAME` does.",
		Args: exactArgs(1, "one context name"),
		RunE: func(command *cobra.Command, args []string) error {
			return a.setContext(args[0], project, command.Flags().Changed("project"))
		},
	}
	setContext.Flags().StringVar(&project, "project", "", "Default project of the context (a name or an id)")
	command.AddCommand(
		&cobra.Command{
			Use:   "get-contexts",
			Short: "List the contexts",
			Args:  noArgs,
			RunE:  func(*cobra.Command, []string) error { return a.getContexts() },
		},
		&cobra.Command{
			Use:   "use-context NAME",
			Short: "Make a context the current one",
			Args:  exactArgs(1, "one context name"),
			RunE:  func(_ *cobra.Command, args []string) error { return a.useContext(args[0]) },
		},
		setContext,
		&cobra.Command{
			Use:   "delete-context NAME",
			Short: "Remove a context and its saved token",
			Args:  exactArgs(1, "one context name"),
			RunE:  func(_ *cobra.Command, args []string) error { return a.deleteContext(args[0]) },
		},
		&cobra.Command{
			Use:   "view",
			Short: "Print the config file's content, without tokens",
			Args:  noArgs,
			RunE:  func(*cobra.Command, []string) error { return a.viewConfig() },
		},
	)
	return command
}

func (a *App) loadConfig() (config.Paths, *config.File, error) {
	paths, err := a.paths()
	if err != nil {
		return paths, nil, err
	}
	file, err := config.Load(paths)
	if err != nil {
		return paths, nil, exit.Usagef("%v", err)
	}
	return paths, file, nil
}

func (a *App) getContexts() error {
	format, err := a.format()
	if err != nil {
		return err
	}
	_, file, err := a.loadConfig()
	if err != nil {
		return err
	}
	names := file.Names()
	switch format {
	case output.Name:
		for _, name := range names {
			fmt.Fprintln(a.Out, "context/"+name)
		}
		return nil
	case output.JSON, output.YAML:
		type item struct {
			Name    string `json:"name"`
			Current bool   `json:"current"`
			Server  string `json:"server"`
			Project string `json:"project,omitempty"`
		}
		items := make([]item, 0, len(names))
		for _, name := range names {
			entry := file.Contexts[name]
			items = append(items, item{name, name == file.CurrentContext, entry.Server, entry.Project})
		}
		raw, err := json.Marshal(map[string]any{"items": items})
		if err != nil {
			return err
		}
		return output.Document(a.Out, format, raw)
	}
	if len(names) == 0 {
		fmt.Fprintln(a.Out, "No context yet. Run `srw login`.")
		return nil
	}
	table := a.table()
	fmt.Fprintln(table, "CURRENT\tNAME\tSERVER\tPROJECT")
	for _, name := range names {
		entry := file.Contexts[name]
		current := ""
		if name == file.CurrentContext {
			current = "*"
		}
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\n", current, name, entry.Server, dash(entry.Project))
	}
	return table.Flush()
}

func (a *App) useContext(name string) error {
	paths, file, err := a.loadConfig()
	if err != nil {
		return err
	}
	if _, ok := file.Contexts[name]; !ok {
		return exit.New(exit.NotFound, "there is no context %q", name).
			WithHint("`srw config get-contexts` lists the contexts")
	}
	file.CurrentContext = name
	if err := file.Save(paths); err != nil {
		return exit.New(exit.Failed, "cannot write %s: %v", paths.Config, err)
	}
	fmt.Fprintf(a.Out, "Context %q is now the current one.\n", name)
	return nil
}

func (a *App) setContext(name, project string, projectGiven bool) error {
	if a.flags.context != "" {
		return exit.Usagef("name the context as the argument, not with --context")
	}
	if !config.ValidContextName(name) {
		return exit.Usagef("%q cannot be a context name; use letters, digits, dots, dashes and underscores", name)
	}
	paths, file, err := a.loadConfig()
	if err != nil {
		return err
	}
	entry := file.Contexts[name]
	created := entry == nil
	if created {
		if a.flags.server == "" {
			return exit.Usagef("a new context needs --server")
		}
		entry = &config.Context{}
	}
	insecure := a.flags.insecureHTTP || entry.InsecureHTTP
	if a.flags.server != "" {
		base, err := remote.ParseServer(a.flags.server, insecure)
		if err != nil {
			return err
		}
		entry.Server = base.String()
	}
	if a.flags.caFile != "" {
		entry.CAFile = a.flags.caFile
	}
	if a.flags.insecureHTTP {
		entry.InsecureHTTP = true
	}
	if projectGiven {
		entry.Project = project
	}
	if file.Contexts == nil {
		file.Contexts = map[string]*config.Context{}
	}
	file.Contexts[name] = entry
	if err := file.Save(paths); err != nil {
		return exit.New(exit.Failed, "cannot write %s: %v", paths.Config, err)
	}
	if created {
		fmt.Fprintf(a.Out, "Context %q created. It has no token yet: run `srw login --context %s --server %s`.\n", name, name, entry.Server)
	} else {
		fmt.Fprintf(a.Out, "Context %q changed.\n", name)
	}
	return nil
}

func (a *App) deleteContext(name string) error {
	paths, file, err := a.loadConfig()
	if err != nil {
		return err
	}
	if _, ok := file.Contexts[name]; !ok {
		return exit.New(exit.NotFound, "there is no context %q", name)
	}
	credentials, _, err := config.LoadCredentials(paths)
	if err != nil {
		return exit.Usagef("%v", err)
	}
	// The token first: a context without a token is harmless, a token
	// without a context would stay behind unseen.
	if _, ok := credentials.Tokens[name]; ok {
		delete(credentials.Tokens, name)
		if err := credentials.Save(paths); err != nil {
			return exit.New(exit.Failed, "cannot write %s: %v", paths.Credentials, err)
		}
	}
	delete(file.Contexts, name)
	wasCurrent := file.CurrentContext == name
	if wasCurrent {
		file.CurrentContext = ""
	}
	if err := file.Save(paths); err != nil {
		return exit.New(exit.Failed, "cannot write %s: %v", paths.Config, err)
	}
	fmt.Fprintf(a.Out, "Context %q and its saved token are removed from this machine. The token is still valid on the server.\n", name)
	if wasCurrent {
		fmt.Fprintln(a.Out, "There is no current context now; choose one with `srw config use-context NAME`.")
	}
	return nil
}

func (a *App) viewConfig() error {
	format, err := a.format()
	if err != nil {
		return err
	}
	if format == output.Name {
		return exit.Usagef("`config view` has no name output; use `config get-contexts -o name`")
	}
	paths, file, err := a.loadConfig()
	if err != nil {
		return err
	}
	// The view is built from the config file alone. Tokens live in another
	// file and never pass through here.
	type contextView struct {
		Server       string   `json:"server"`
		CAFile       string   `json:"ca-file,omitempty"`
		Project      string   `json:"project,omitempty"`
		TokenCommand []string `json:"token-command,omitempty"`
		InsecureHTTP bool     `json:"insecure-http,omitempty"`
	}
	view := struct {
		File           string                 `json:"file"`
		CurrentContext string                 `json:"current-context"`
		Contexts       map[string]contextView `json:"contexts"`
	}{File: paths.Config, CurrentContext: file.CurrentContext, Contexts: map[string]contextView{}}
	for name, entry := range file.Contexts {
		view.Contexts[name] = contextView{entry.Server, entry.CAFile, entry.Project, entry.TokenCommand, entry.InsecureHTTP}
	}
	raw, err := json.Marshal(view)
	if err != nil {
		return err
	}
	if format == output.JSON {
		return output.WriteJSON(a.Out, raw)
	}
	return output.WriteYAML(a.Out, raw)
}

// dash stands in for a missing optional value in a table.
func dash(value string) string {
	if value == "" {
		return "-"
	}
	return value
}
