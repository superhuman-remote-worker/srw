package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/superhuman-remote-worker/srw/cli/internal/exit"
	"github.com/superhuman-remote-worker/srw/cli/internal/output"
)

func (a *App) versionCommand() *cobra.Command {
	var clientOnly bool
	command := &cobra.Command{
		Use:   "version",
		Short: "Show the version of srw and of the server",
		Long: "Show the version of this srw and of the server it is set up for. srw works\n" +
			"with the server of its own minor version; a difference is reported here and\n" +
			"by `srw login`, not by every command.",
		Args: noArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return a.version(command.Context(), clientOnly)
		},
	}
	command.Flags().BoolVar(&clientOnly, "client", false, "Show the version of srw only; do not ask the server")
	return command
}

func (a *App) version(ctx context.Context, clientOnly bool) error {
	format, err := a.format()
	if err != nil {
		return err
	}
	if format == output.Name {
		return exit.Usagef("`version` has no name output")
	}
	var info *serverInfo
	// server says why there is no server version, in a person's words.
	server := ""
	var failure error
	if !clientOnly {
		info, server, failure = a.askServer(ctx)
	}
	if format == output.JSON || format == output.YAML {
		document := map[string]any{"client": map[string]string{"version": a.Version, "commit": a.Commit}}
		if !clientOnly {
			if info != nil {
				document["server"] = info
			} else {
				document["server"] = nil
			}
		}
		raw, err := json.Marshal(document)
		if err != nil {
			return err
		}
		if err := output.Document(a.Out, format, raw); err != nil {
			return err
		}
		return failure
	}
	table := a.table()
	fmt.Fprintf(table, "Client:\t%s (commit %s)\n", a.Version, a.Commit)
	if !clientOnly {
		if info != nil {
			fmt.Fprintf(table, "Server:\t%s (commit %s), manifest API %s\n",
				info.ReleaseVersion, info.SourceRevision, strings.Join(info.ManifestAPIVersions, ", "))
		} else {
			fmt.Fprintf(table, "Server:\t%s\n", server)
		}
	}
	if err := table.Flush(); err != nil {
		return err
	}
	if info != nil {
		a.warnAboutSkew(info)
	}
	return failure
}

// askServer asks the configured server for its version. The client's version
// is always printed, so "no server" and "an older server" are answers, not
// failures; a server that was asked and could not answer is a failure.
func (a *App) askServer(ctx context.Context) (*serverInfo, string, error) {
	_, client, err := a.connect(true)
	if err != nil {
		var coded *exit.Error
		if errors.As(err, &coded) && (coded.Code == exit.Usage || coded.Code == exit.Auth) && a.flags.server == "" && a.flags.context == "" {
			return nil, "unknown (" + coded.Message + ")", nil
		}
		return nil, "unknown", err
	}
	info, err := a.serverVersion(ctx, client)
	if err != nil {
		return nil, "unknown", err
	}
	if info == nil {
		return nil, "unknown (older server)", nil
	}
	return info, "", nil
}

// warnAboutSkew prints one line when the server is of another minor version.
// A server that does not say, or a build without a release version, is
// unknown and gets no warning.
func (a *App) warnAboutSkew(info *serverInfo) {
	clientMajor, clientMinor, ok := majorMinor(a.Version)
	if !ok {
		return
	}
	serverMajor, serverMinor, ok := majorMinor(info.ReleaseVersion)
	if !ok || (clientMajor == serverMajor && clientMinor == serverMinor) {
		return
	}
	fmt.Fprintf(a.Err, "warning: this srw is %d.%d and the server is %d.%d; some commands may not work. Install the srw of the server's version.\n",
		clientMajor, clientMinor, serverMajor, serverMinor)
}

func majorMinor(version string) (major, minor int, ok bool) {
	parts := strings.SplitN(strings.TrimPrefix(version, "v"), ".", 3)
	if len(parts) != 3 {
		return 0, 0, false
	}
	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, false
	}
	minor, err = strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, false
	}
	return major, minor, true
}
