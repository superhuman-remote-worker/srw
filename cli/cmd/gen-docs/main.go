// Command gen-docs writes the command reference of srw, docs/cli-reference.md.
//
//	go run ./cmd/gen-docs > ../docs/cli-reference.md
//
// It is its own program so that nothing it needs is linked into srw. The page
// is committed, and a test fails when it differs from what this prints, so a
// changed flag cannot ship without its documentation.
package main

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/superhuman-remote-worker/srw/cli/internal/cmd"
)

func main() {
	if err := write(os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

const header = `---
title: srw CLI reference
description: Every command, flag, example and environment variable of srw, the command-line client of Superhuman Remote Worker.
---

# srw CLI reference

This page lists every command of ` + "`srw`" + ` with its flags and examples, and the
environment variables it reads. The commands are generated from the program's
own help text, so they match the version of the documentation you are
reading. For installing, logging in and a first walkthrough, see
[srw CLI](cli.md).

`

// environment is what srw reads from the environment. A test fails when the
// program reads a variable that is not listed here.
var environment = []struct{ name, meaning string }{
	{"SRW_API_URL", "API address. Setting it uses the environment instead of a saved login, as --server does"},
	{"COCKPIT_API_URL", "Older name of SRW_API_URL, read when that is not set"},
	{"SRW_TOKEN", "Token for a server given by --server or SRW_API_URL. It is never sent to the server of a saved login. Without it, the first line of ~/.config/srw/token is used"},
	{"SRW_CONTEXT", "Saved login to use instead of the current one, as --context does"},
	{"SRW_OUTPUT", "Output format, as -o does: json, yaml or name"},
	{"SRW_CONFIG", "Absolute path of the config file, in place of ~/.config/srw/config.yaml. credentials.yaml is read beside it"},
	{"XDG_CONFIG_HOME", "When it is an absolute path, the config directory is $XDG_CONFIG_HOME/srw"},
	{"HTTPS_PROXY", "Proxy for requests to the API. NO_PROXY lists the hosts that bypass it"},
}

func write(w io.Writer) error {
	root := cmd.Reference()
	var page strings.Builder
	page.WriteString(header)
	page.WriteString("## Global flags\n\nThese work with every command.\n\n")
	flags(&page, root.PersistentFlags())
	page.WriteString("## Environment variables\n\n| Variable | Meaning |\n|---|---|\n")
	for _, variable := range environment {
		fmt.Fprintf(&page, "| `%s` | %s |\n", variable.name, literal(variable.meaning))
	}
	page.WriteString("\n")
	for _, command := range root.Commands() {
		section(&page, command)
	}
	_, err := io.WriteString(w, page.String())
	return err
}

func section(page *strings.Builder, command *cobra.Command) {
	if command.Hidden || command.Name() == "help" {
		return
	}
	fmt.Fprintf(page, "## %s\n\n%s.\n\n", command.CommandPath(), literal(strings.TrimSuffix(command.Short, ".")))
	if command.Runnable() {
		fmt.Fprintf(page, "```text\n%s\n```\n\n", command.UseLine())
	}
	// The long text is the help as the program prints it: it holds aligned
	// lists and placeholders in angle brackets, which Markdown would reflow
	// or read as tags.
	if long := strings.TrimSpace(command.Long); long != "" && long != command.Short {
		fmt.Fprintf(page, "```text\n%s\n```\n\n", long)
	}
	if command.LocalNonPersistentFlags().HasAvailableFlags() {
		flags(page, command.LocalNonPersistentFlags())
	}
	if example := strings.TrimRight(command.Example, "\n"); example != "" {
		fmt.Fprintf(page, "Examples:\n\n```text\n%s\n```\n\n", example)
	}
	for _, child := range command.Commands() {
		section(page, child)
	}
}

// flags prints a flag set as a table.
func flags(page *strings.Builder, set *pflag.FlagSet) {
	page.WriteString("| Flag | Meaning |\n|---|---|\n")
	set.VisitAll(func(flag *pflag.Flag) {
		if flag.Hidden || flag.Name == "help" {
			return
		}
		name := "--" + flag.Name
		if flag.Shorthand != "" {
			name = "-" + flag.Shorthand + ", " + name
		}
		value, usage := pflag.UnquoteUsage(flag)
		if value != "" {
			name += " " + value
		}
		if flag.DefValue != "" && flag.DefValue != "false" && flag.DefValue != "0" && flag.DefValue != "[]" && flag.DefValue != "0s" {
			usage += " (default " + flag.DefValue + ")"
		}
		fmt.Fprintf(page, "| `%s` | %s |\n", name, literal(usage))
	})
	page.WriteString("\n")
}

// literal makes a help text safe as Markdown prose in a table cell: a pipe
// would end the cell, and an angle bracket would start a tag.
func literal(text string) string {
	return strings.NewReplacer("|", "\\|", "<", "\\<", ">", "\\>").Replace(text)
}
