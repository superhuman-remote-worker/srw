// Package cmd holds the commands of srw.
package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/superhuman-remote-worker/srw/cli/internal/api"
	"github.com/superhuman-remote-worker/srw/cli/internal/buildinfo"
	"github.com/superhuman-remote-worker/srw/cli/internal/config"
	"github.com/superhuman-remote-worker/srw/cli/internal/exit"
	"github.com/superhuman-remote-worker/srw/cli/internal/output"
	"github.com/superhuman-remote-worker/srw/cli/internal/target"
)

// App is one run of srw: its streams, its surroundings and its global flags.
// Tests build their own; nothing below reads the process directly.
type App struct {
	In  io.Reader
	Out io.Writer
	Err io.Writer

	Getenv func(string) string
	Home   string
	Now    func() time.Time

	// StdinIsTerminal says whether a person can be asked something.
	StdinIsTerminal bool
	// ReadSecret reads a line without echoing it.
	ReadSecret func(prompt string) (string, error)

	Version string
	Commit  string

	// Transport and Pauses replace the HTTP transport and the pauses
	// between the tries of a GET (tests).
	Transport http.RoundTripper
	Pauses    []time.Duration

	flags struct {
		server       string
		context      string
		tokenStdin   bool
		caFile       string
		insecureHTTP bool
		output       string
	}
	// started is set once a command's own code runs. An error before that
	// is a usage error: an unknown command, a bad flag, a wrong argument.
	started bool
	// jsonByDefault is set for the commands that have always printed JSON.
	jsonByDefault bool
}

// Main runs srw with the process's own streams and returns the exit code.
func Main() int {
	home, _ := os.UserHomeDir()
	app := &App{
		In: os.Stdin, Out: os.Stdout, Err: os.Stderr,
		Getenv: os.Getenv, Home: home, Now: time.Now,
		StdinIsTerminal: term.IsTerminal(int(os.Stdin.Fd())),
		ReadSecret:      readSecret,
		Version:         buildinfo.Version, Commit: buildinfo.Commit,
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	// After the first Ctrl-C the handler is removed, so a second one ends
	// the process even when a command does not return.
	go func() {
		<-ctx.Done()
		stop()
	}()
	return app.Run(ctx, os.Args[1:])
}

// Run executes one command line and returns the exit code. It is the one
// place that prints errors and maps them to exit codes.
func (a *App) Run(ctx context.Context, args []string) int {
	root := a.Root()
	root.SetArgs(args)
	root.SetIn(a.In)
	root.SetOut(a.Out)
	root.SetErr(a.Err)
	err := root.ExecuteContext(ctx)
	if err == nil {
		return exit.OK
	}
	if ctx.Err() != nil || errors.Is(err, context.Canceled) {
		fmt.Fprintln(a.Err, "interrupted")
		return exit.Interrupted
	}
	var coded *exit.Error
	if !a.started && !errors.As(err, &coded) {
		err = exit.Usagef("%v", err).WithHint("run `srw --help` for the commands and their flags")
	}
	output.Error(a.Err, a.errorsAsJSON(), err)
	return exit.CodeOf(err)
}

// Root builds the command tree.
func (a *App) Root() *cobra.Command {
	root := &cobra.Command{
		Use:   "srw",
		Short: "The command-line client of Superhuman Remote Worker",
		Long: "srw keeps configuration as files and runs work on a Superhuman Remote Worker\n" +
			"installation, from a terminal or a pipeline.\n\n" +
			"On your own machine, run `srw login` once. In a pipeline, set SRW_API_URL and\n" +
			"SRW_TOKEN instead; a saved login is not used then.",
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRunE: func(command *cobra.Command, _ []string) error {
			a.started = true
			a.jsonByDefault = command.Annotations[annotationOutput] == "json"
			_, err := a.format()
			return err
		},
	}
	flags := root.PersistentFlags()
	// No default may depend on the environment: the generated reference
	// would differ between machines.
	flags.StringVar(&a.flags.server, "server", "", "API address. Uses the environment, not a saved login (also: SRW_API_URL)")
	flags.StringVar(&a.flags.context, "context", "", "Saved login to use instead of the current one (also: SRW_CONTEXT)")
	flags.BoolVar(&a.flags.tokenStdin, "token-stdin", false, "Read the token from the first line of standard input")
	flags.StringVar(&a.flags.caFile, "ca-file", "", "PEM file of certificate authorities to trust instead of the system's")
	flags.BoolVar(&a.flags.insecureHTTP, "insecure-http", false, "Allow http:// for a server that is not on this machine")
	flags.StringVarP(&a.flags.output, "output", "o", "", "Output format: json, yaml or name (also: SRW_OUTPUT)")
	root.SetFlagErrorFunc(func(_ *cobra.Command, err error) error {
		return exit.Usagef("%v", err).WithHint("run the command with --help for its flags")
	})
	root.AddCommand(
		a.loginCommand(),
		a.logoutCommand(),
		a.whoamiCommand(),
		a.configCommand(),
		a.versionCommand(),
		a.apiCommand(),
	)
	return root
}

// annotationOutput marks a command whose default output is JSON.
const annotationOutput = "srw/default-output"

// format is the output format asked for: -o, else SRW_OUTPUT.
func (a *App) format() (output.Format, error) {
	value := a.flags.output
	if value == "" {
		value = a.Getenv("SRW_OUTPUT")
	}
	return output.Parse(value)
}

func (a *App) errorsAsJSON() bool {
	format, err := a.format()
	if err != nil {
		return false
	}
	return format == output.JSON || (format == output.Default && a.jsonByDefault)
}

func (a *App) paths() (config.Paths, error) {
	paths, err := config.Locate(a.Getenv, a.Home)
	if err != nil {
		return paths, exit.Usagef("%v", err)
	}
	return paths, nil
}

func (a *App) host() target.Host {
	return target.Host{
		Getenv: a.Getenv, Home: a.Home, Stdin: a.In, Stderr: a.Err,
		StdinIsTerminal: a.StdinIsTerminal,
	}
}

func (a *App) options() target.Options {
	return target.Options{
		Server: a.flags.server, Context: a.flags.context, TokenStdin: a.flags.tokenStdin,
		CAFile: a.flags.caFile, InsecureHTTP: a.flags.insecureHTTP,
	}
}

// connect resolves the server and the token and builds the client.
func (a *App) connect(wantToken bool) (*target.Target, *api.Client, error) {
	paths, err := a.paths()
	if err != nil {
		return nil, nil, err
	}
	to, err := target.Resolve(a.options(), a.host(), paths, wantToken)
	if err != nil {
		return nil, nil, err
	}
	client, err := a.client(to)
	return to, client, err
}

func (a *App) client(to *target.Target) (*api.Client, error) {
	return api.New(to, a.Version, api.Options{Transport: a.Transport, Pauses: a.Pauses})
}

// table returns a writer that aligns tab-separated columns.
func (a *App) table() *tabwriter.Writer {
	return tabwriter.NewWriter(a.Out, 0, 8, 2, ' ', 0)
}

// noArgs and exactArgs are cobra's validators with the usage exit code.
func noArgs(command *cobra.Command, args []string) error {
	if len(args) > 0 {
		return exit.Usagef("%s takes no argument, got %q", command.CommandPath(), strings.Join(args, " "))
	}
	return nil
}

func exactArgs(count int, what string) cobra.PositionalArgs {
	return func(command *cobra.Command, args []string) error {
		if len(args) != count {
			return exit.Usagef("%s takes %s", command.CommandPath(), what)
		}
		return nil
	}
}

// readSecret reads a line from the terminal without echoing it. The terminal
// state is saved first and restored when the prompt is interrupted, or the
// shell would be left without echo.
func readSecret(prompt string) (string, error) {
	descriptor := int(os.Stdin.Fd())
	state, err := term.GetState(descriptor)
	if err != nil {
		return "", err
	}
	fmt.Fprint(os.Stderr, prompt)
	interrupts := make(chan os.Signal, 1)
	signal.Notify(interrupts, os.Interrupt)
	done := make(chan struct{})
	defer func() {
		signal.Stop(interrupts)
		close(done)
	}()
	go func() {
		select {
		case <-interrupts:
			_ = term.Restore(descriptor, state)
			fmt.Fprintln(os.Stderr)
			os.Exit(exit.Interrupted)
		case <-done:
		}
	}()
	secret, err := term.ReadPassword(descriptor)
	fmt.Fprintln(os.Stderr)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(secret)), nil
}
