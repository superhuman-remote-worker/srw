package cmd

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/spf13/cobra"

	"github.com/superhuman-remote-worker/srw/cli/internal/api"
	"github.com/superhuman-remote-worker/srw/cli/internal/exit"
)

func (a *App) apiCommand() *cobra.Command {
	var method, input string
	var headers []string
	var include bool
	command := &cobra.Command{
		Use:   "api PATH",
		Short: "Send an authenticated request to any API path",
		Long: "Send a request to a path of the API and print the answer as it comes. This is\n" +
			"the way to reach a route that has no command of its own.\n\n" +
			"The method is GET unless -X names another; it is never guessed from the\n" +
			"input. The path starts with a slash and may carry a query string. Addresses\n" +
			"of other servers are refused: the token goes only to the configured server.",
		Example: "  srw api /api/models\n" +
			"  srw api '/api/jobs?status=failed&limit=5'\n" +
			"  srw api -X PUT /api/jobs/0b5c…/cancel\n" +
			"  srw api -X POST /api/jobs --input job.json",
		Args:        exactArgs(1, "one path, for example /api/jobs"),
		Annotations: map[string]string{annotationOutput: "json"},
		RunE: func(command *cobra.Command, args []string) error {
			return a.api(command.Context(), strings.ToUpper(method), args[0], input, headers, include)
		},
	}
	flags := command.Flags()
	flags.StringVarP(&method, "method", "X", http.MethodGet, "HTTP method")
	flags.StringVar(&input, "input", "", "File to send as the request body; - reads standard input")
	flags.StringArrayVarP(&headers, "header", "H", nil, "Request header as name:value (repeatable)")
	flags.BoolVarP(&include, "include", "i", false, "Print the status line and the response headers first")
	return command
}

func (a *App) api(ctx context.Context, method, path, input string, headers []string, include bool) error {
	if input == "-" && a.flags.tokenStdin {
		return exit.Usagef("--input - and --token-stdin both read standard input; give the body as a file")
	}
	parsed, err := url.Parse(path)
	if err != nil || parsed.Scheme != "" || parsed.Host != "" || !strings.HasPrefix(parsed.Path, "/") {
		return exit.Usagef("%q is not a path on the server; give a path such as /api/jobs", path).
			WithHint("the token is sent only to the configured server")
	}
	request := api.Request{Method: method, Path: parsed.Path, Query: parsed.Query(), Header: http.Header{}}
	for _, header := range headers {
		name, value, ok := strings.Cut(header, ":")
		if !ok || strings.TrimSpace(name) == "" {
			return exit.Usagef("%q is not a header; write name:value", header)
		}
		if strings.EqualFold(strings.TrimSpace(name), "Authorization") {
			return exit.Usagef("the Authorization header is set by srw from the configured token")
		}
		request.Header.Add(strings.TrimSpace(name), strings.TrimSpace(value))
	}
	if input != "" {
		var body []byte
		if input == "-" {
			body, err = io.ReadAll(a.In)
		} else {
			body, err = os.ReadFile(input)
		}
		if err != nil {
			return exit.Usagef("cannot read the request body: %v", err)
		}
		// The body is sent as text. Invalid UTF-8 would be replaced on the
		// way without a word.
		if !utf8.Valid(body) {
			return exit.Usagef("the request body is not valid UTF-8")
		}
		request.RawBody = body
		request.ContentType = request.Header.Get("Content-Type")
		if request.ContentType == "" {
			request.ContentType = "application/json"
		}
	}
	_, client, err := a.connect(true)
	if err != nil {
		return err
	}
	response, err := client.Do(ctx, request)
	if err != nil {
		return err
	}
	if include {
		fmt.Fprintf(a.Out, "HTTP %d %s\n", response.Status, http.StatusText(response.Status))
		names := make([]string, 0, len(response.Header))
		for name := range response.Header {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			for _, value := range response.Header[name] {
				fmt.Fprintf(a.Out, "%s: %s\n", name, client.Redact(value))
			}
		}
		fmt.Fprintln(a.Out)
	}
	// The body is printed for every status: an error body is the answer the
	// caller asked for. The exit code follows the status.
	if _, err := a.Out.Write(response.Body); err != nil {
		return err
	}
	if len(response.Body) > 0 && response.Body[len(response.Body)-1] != '\n' {
		fmt.Fprintln(a.Out)
	}
	return client.StatusError(request, response)
}
