---
title: srw CLI reference
description: Every command, flag, example and environment variable of srw, the command-line client of Superhuman Remote Worker.
---

# srw CLI reference

This page lists every command of `srw` with its flags and examples, and the
environment variables it reads. The commands are generated from the program's
own help text, so they match the version of the documentation you are
reading. For installing, logging in and a first walkthrough, see
[srw CLI](cli.md).

## Global flags

These work with every command.

| Flag | Meaning |
|---|---|
| `--ca-file string` | PEM file of certificate authorities to trust instead of the system's |
| `--context string` | Saved login to use instead of the current one (also: SRW_CONTEXT) |
| `--insecure-http` | Allow http:// for a server that is not on this machine |
| `-o, --output string` | Output format: json, yaml or name (also: SRW_OUTPUT) |
| `--server string` | API address. Uses the environment, not a saved login (also: SRW_API_URL) |
| `--token-stdin` | Read the token from the first line of standard input |

## Environment variables

| Variable | Meaning |
|---|---|
| `SRW_API_URL` | API address. Setting it uses the environment instead of a saved login, as --server does |
| `COCKPIT_API_URL` | Older name of SRW_API_URL, read when that is not set |
| `SRW_TOKEN` | Token for a server given by --server or SRW_API_URL. It is never sent to the server of a saved login. Without it, the first line of ~/.config/srw/token is used |
| `SRW_CONTEXT` | Saved login to use instead of the current one, as --context does |
| `SRW_OUTPUT` | Output format, as -o does: json, yaml or name |
| `SRW_CONFIG` | Absolute path of the config file, in place of ~/.config/srw/config.yaml. credentials.yaml is read beside it |
| `XDG_CONFIG_HOME` | When it is an absolute path, the config directory is $XDG_CONFIG_HOME/srw |
| `HTTPS_PROXY` | Proxy for requests to the API. NO_PROXY lists the hosts that bypass it |

## srw api

Send an authenticated request to any API path.

```text
srw api PATH [flags]
```

```text
Send a request to a path of the API and print the answer as it comes. This is
the way to reach a route that has no command of its own.

The method is GET unless -X names another; it is never guessed from the
input. The path starts with a slash and may carry a query string. Addresses
of other servers are refused: the token goes only to the configured server.
```

| Flag | Meaning |
|---|---|
| `-H, --header stringArray` | Request header as name:value (repeatable) |
| `-i, --include` | Print the status line and the response headers first |
| `--input string` | File to send as the request body; - reads standard input |
| `-X, --method string` | HTTP method (default GET) |

Examples:

```text
  srw api /api/models
  srw api '/api/jobs?status=failed&limit=5'
  srw api -X PUT /api/jobs/0b5c…/cancel
  srw api -X POST /api/jobs --input job.json
```

## srw apply

Store manifests, and start the jobs they describe.

```text
srw apply -f FILE [flags]
```

```text
Store the resources in the manifests. A Job document also starts the job.

srw first asks the server for a preview and then applies with what the
preview saw, so an apply is refused (exit 5) when a resource changed in
between; run it again then. To apply exactly what you reviewed, save a preview
and pass it with --plan.

The token needs resources:write. A Job document also needs jobs:write, and a
Project document projects:write.
```

| Flag | Meaning |
|---|---|
| `--account` | Your own account (the default) |
| `--catalog` | The shared catalog |
| `--dry-run string` | server: stop after the preview and print it |
| `--expected-version stringArray` | IDENTITY=VERSION for one resource (repeatable) |
| `--expected-versions string` | JSON file: resource identity to resourceVersion |
| `-f, --file stringArray` | Manifest file or directory; repeatable; - reads standard input |
| `--format string` | Format of the files: json or yaml (default: by extension, else yaml) |
| `--idempotency-key string` | Send the identical request with the same key to recover a lost answer |
| `--plan srw preview` | JSON output of srw preview for these same files and scope |
| `--plan-revision string` | The planRevision of a preview you reviewed |
| `-p, --project string` | A project, by name or id |
| `--scope-kind string` | Scope kind: Account, Project or Catalog (with --scope-name) |
| `--scope-name string` | Scope name: me, a project id, or shared (with --scope-kind) |

Examples:

```text
  srw apply -f team.yaml
  srw apply -f team.yaml --dry-run=server
  srw apply -f job.yaml --idempotency-key "$CI_PIPELINE_ID"
```

## srw approve

Approve a job that stopped for review.

```text
srw approve JOB [flags]
```

```text
Approve a job that stopped for a person. A job that stopped at the end of its
work completes. A job that stopped between two phases continues with the
next phase.

A job shows that it stopped for review a moment before the server has
finished stopping it. An approval in that moment is refused as a conflict;
srw sends it again for up to 20 seconds while the job still waits for review.
```

| Flag | Meaning |
|---|---|
| `--notes string` | A note for the record (used when the job completes) |

## srw cache

Inspect or evict prepared workspace artifacts.

```text
List or delete the prepared workspace artifacts of a scope. The server needs
Kubernetes manifest hosting and a VM provisioner for these.
```

## srw cache delete

Delete one prepared workspace artifact.

```text
srw cache delete ARTIFACT_ID [flags]
```

| Flag | Meaning |
|---|---|
| `--account` | Your own account (the default) |
| `--catalog` | The shared catalog |
| `-p, --project string` | A project, by name or id |
| `--scope-kind string` | Scope kind: Account, Project or Catalog (with --scope-name) |
| `--scope-name string` | Scope name: me, a project id, or shared (with --scope-kind) |

## srw cache list

List the prepared workspace artifacts of a scope.

```text
srw cache list [flags]
```

| Flag | Meaning |
|---|---|
| `--account` | Your own account (the default) |
| `--catalog` | The shared catalog |
| `-p, --project string` | A project, by name or id |
| `--scope-kind string` | Scope kind: Account, Project or Catalog (with --scope-name) |
| `--scope-name string` | Scope name: me, a project id, or shared (with --scope-kind) |

## srw cancel

Cancel a job.

```text
srw cancel JOB
```

```text
Cancel a job. It ends as cancelled.

The server releases the job's workspace before it answers, which can take a
while. If it answers with an error although the job did end cancelled, srw
says so and exits 0.
```

## srw completion

Generate the autocompletion script for the specified shell.

```text
Generate the autocompletion script for srw for the specified shell.
See each sub-command's help for details on how to use the generated script.
```

## srw completion bash

Generate the autocompletion script for bash.

```text
srw completion bash
```

```text
Generate the autocompletion script for the bash shell.

This script depends on the 'bash-completion' package.
If it is not installed already, you can install it via your OS's package manager.

To load completions in your current shell session:

	source <(srw completion bash)

To load completions for every new session, execute once:

#### Linux:

	srw completion bash > /etc/bash_completion.d/srw

#### macOS:

	srw completion bash > $(brew --prefix)/etc/bash_completion.d/srw

You will need to start a new shell for this setup to take effect.
```

| Flag | Meaning |
|---|---|
| `--no-descriptions` | disable completion descriptions |

## srw completion fish

Generate the autocompletion script for fish.

```text
srw completion fish [flags]
```

```text
Generate the autocompletion script for the fish shell.

To load completions in your current shell session:

	srw completion fish | source

To load completions for every new session, execute once:

	srw completion fish > ~/.config/fish/completions/srw.fish

You will need to start a new shell for this setup to take effect.
```

| Flag | Meaning |
|---|---|
| `--no-descriptions` | disable completion descriptions |

## srw completion powershell

Generate the autocompletion script for powershell.

```text
srw completion powershell [flags]
```

```text
Generate the autocompletion script for powershell.

To load completions in your current shell session:

	srw completion powershell | Out-String | Invoke-Expression

To load completions for every new session, add the output of the above command
to your powershell profile.
```

| Flag | Meaning |
|---|---|
| `--no-descriptions` | disable completion descriptions |

## srw completion zsh

Generate the autocompletion script for zsh.

```text
srw completion zsh [flags]
```

```text
Generate the autocompletion script for the zsh shell.

If shell completion is not already enabled in your environment you will need
to enable it.  You can execute the following once:

	echo "autoload -U compinit; compinit" >> ~/.zshrc

To load completions in your current shell session:

	source <(srw completion zsh)

To load completions for every new session, execute once:

#### Linux:

	srw completion zsh > "${fpath[1]}/_srw"

#### macOS:

	srw completion zsh > $(brew --prefix)/share/zsh/site-functions/_srw

You will need to start a new shell for this setup to take effect.
```

| Flag | Meaning |
|---|---|
| `--no-descriptions` | disable completion descriptions |

## srw config

Manage the saved logins (contexts).

```text
A context is one server with its options. `srw login` creates one; these
commands list, switch, change and remove them. Tokens are kept in a separate
file and are never shown.

The config file can name a command that prints the token (token-command), so
it is read only from the config directory, never from the working directory.
```

## srw config delete-context

Remove a context and its saved token.

```text
srw config delete-context NAME
```

## srw config get-contexts

List the contexts.

```text
srw config get-contexts
```

## srw config set-context

Create a context, or change its server and options.

```text
srw config set-context NAME [flags]
```

```text
Create a context, or change one. Uses the global flags --server, --ca-file
and --insecure-http, and --project for the default project. It stores no
token; `srw login --context NAME` does.
```

| Flag | Meaning |
|---|---|
| `--project string` | Default project of the context (a name or an id) |

## srw config use-context

Make a context the current one.

```text
srw config use-context NAME
```

## srw config view

Print the config file's content, without tokens.

```text
srw config view
```

## srw cp

Copy a file a job pushed to this machine.

```text
srw cp JOB:PATH (DEST | -)
```

```text
Copy one file from a job's repository to a local file, or to standard output
with -. Like `srw files`, it reads what the job pushed last.

Files above 10 MiB are not served by this route yet.
```

Examples:

```text
  srw cp job/0b5c2f6e:output/report.md report.md
  srw cp 0b5c2f6e:output/report.md - | less
```

## srw delete

Delete a stored resource, a project or a job.

```text
srw delete (UID | KIND NAME) [flags]
```

```text
Delete a resource. On a terminal srw asks first; elsewhere it needs --yes, or
--expected-version for a stored resource addressed by its UID.

Deleting a project deletes the whole project: its repositories, its cloud
folder and its group. The token needs resources:write, and projects:write for
a project.

Deleting a job also releases its workspace, which can take a minute or two
after the job has ended. srw waits for it: it asks again while the server
answers that it is still releasing the workspace. When the answer is lost on
the way, srw looks at the job and sends the delete again while the job is
still there.
```

| Flag | Meaning |
|---|---|
| `--account` | Your own account (the default) |
| `--catalog` | The shared catalog |
| `--expected-version int` | Delete only if the resource is still at this version |
| `-p, --project string` | A project, by name or id |
| `--scope-kind string` | Scope kind: Account, Project or Catalog (with --scope-name) |
| `--scope-name string` | Scope name: me, a project id, or shared (with --scope-kind) |
| `-y, --yes` | Do not ask |

Examples:

```text
  srw delete expert reviewer
  srw delete 0b5c2f6e-… --expected-version 4
```

## srw describe

Show what a job is doing and why it stopped.

```text
srw describe job JOB
```

```text
One screen about a job: its status, why it is where it is, its to-dos, what
it handed over when it stopped, and its completion report.
```

Examples:

```text
  srw describe job 0b5c2f6e
  srw describe job/0b5c2f6e
```

## srw export

Print a stored resource as a manifest that can be applied again.

```text
srw export (UID | KIND NAME) [flags]
```

```text
Print the manifest a resource was stored from, as YAML, or as JSON with
-o json. This is the one output that is meant to be applied again.

A reader who is not the owner gets the document with secret values removed.
Applying such a file would remove those values, so export refuses it unless
--allow-redacted is given. An owner's export can contain credentials: keep
secrets in secret references before a file goes into a repository.
```

| Flag | Meaning |
|---|---|
| `--account` | Your own account (the default) |
| `--allow-redacted` | Export a document that has secret values removed |
| `--catalog` | The shared catalog |
| `-p, --project string` | A project, by name or id |
| `--scope-kind string` | Scope kind: Account, Project or Catalog (with --scope-name) |
| `--scope-name string` | Scope name: me, a project id, or shared (with --scope-kind) |

Examples:

```text
  srw export expert reviewer > reviewer.yaml
  srw export 0b5c2f6e-…
```

## srw files

List the files a job pushed.

```text
srw files JOB [PATH] [flags]
```

```text
List the files in a job's repository. This is what the job pushed last, not
its live workspace: a running job's newest files appear after its next push.
```

| Flag | Meaning |
|---|---|
| `-r, --recursive` | List subdirectories too |

Examples:

```text
  srw files job/0b5c2f6e
  srw files 0b5c2f6e output --recursive
```

## srw get

List objects, or show one.

```text
srw get [KIND [NAME] | UID] [flags]
```

```text
List what you can see of a kind, or show one object.

Kinds: job, expert, project, workspacetemplate, connector, session (singular or plural; wt is short for
workspacetemplate). Experts, projects and connectors are listed as the Cockpit
lists them. --stored shows the manifest store's view of one scope instead.

Two older forms print JSON: `srw get UID` reads one stored resource, and
`srw get [--kind KIND]` without a kind argument lists the stored resources of
a scope.
```

| Flag | Meaning |
|---|---|
| `--account` | Your own account (the default) |
| `--all` | Jobs: read every page, not only the first |
| `--catalog` | The shared catalog |
| `--kind string` | With no kind argument: only stored resources of this manifest kind |
| `--limit int` | Jobs: how many to read per request, 1 to 500 (default 50) |
| `--no-headers` | Print the table without its header line |
| `--no-trunc` | Print full ids |
| `--origin stringArray` | Jobs: only this origin, for example user or automation (repeatable) |
| `-p, --project string` | A project, by name or id |
| `--scope-kind string` | Scope kind: Account, Project or Catalog (with --scope-name) |
| `--scope-name string` | Scope name: me, a project id, or shared (with --scope-kind) |
| `--search string` | Jobs: only those whose task, name or id matches |
| `--status stringArray` | Jobs: only this status (repeatable) |
| `--stored` | Show the manifest store's view of the chosen scope |
| `--subjobs` | Jobs: also list the jobs that other jobs started |

Examples:

```text
  srw get jobs --status failed
  srw get experts
  srw get project website -o yaml
  srw get wt --project website
  srw get 0b5c2f6e-…
```

## srw login

Save a server and a token on this machine.

```text
srw login
```

```text
Save a server and a personal access token as a context, and make it the
current one. Create the token in the Cockpit under Settings → API Keys.

The token is asked for without echo. In a script, pass it on standard input
with --token-stdin. The context is named after the server's host unless
--context names it.
```

Examples:

```text
  srw login --server https://api.example.com
  pass show srw/dev | srw login --server https://api.dev.example.com --context dev --token-stdin
```

## srw logout

Remove a saved token from this machine.

```text
srw logout
```

```text
Remove the token of the current context, or of --context, from this machine.
The token stays valid on the server: a token cannot revoke tokens. Revoke it
in the Cockpit under Settings → API Keys.
```

## srw logs

Show what the agent did.

```text
srw logs JOB [flags]
```

```text
Print a job's audit trail: the model's steps and the tools it called. This
is the record the Cockpit shows.

-f follows a running job and ends when the job ends or stops for a person.
It exits 0 whatever the job's result; use `srw wait` for the outcome. A
server that stops answering in the middle is asked again for up to ten
minutes.
```

| Flag | Meaning |
|---|---|
| `--all-steps` | Also show the memory steps |
| `--filter string` | Show only: messages, tools or errors |
| `-f, --follow` | Keep printing while the job runs |
| `--tail int` | Show only the last N entries (at most 200) |

Examples:

```text
  srw logs job/0b5c2f6e --tail 20
  srw logs 0b5c2f6e -f --filter tools
```

## srw pause

Pause a running job.

```text
srw pause JOB
```

```text
Pause a job that is running. It stays paused until someone resumes it.
```

## srw preview

Show what an apply would resolve to, without changing anything.

```text
srw preview -f FILE [flags]
```

```text
Resolve manifests against what is stored and show the result. Nothing is
stored and nothing is started. The answer carries a planRevision; an apply
given that revision is refused when anything changed in between.

--resolution bundle resolves the files against each other only.
```

| Flag | Meaning |
|---|---|
| `--account` | Your own account (the default) |
| `--catalog` | The shared catalog |
| `-f, --file stringArray` | Manifest file or directory; repeatable; - reads standard input |
| `--format string` | Format of the files: json or yaml (default: by extension, else yaml) |
| `-p, --project string` | A project, by name or id |
| `--resolution string` | stored: against what is stored; bundle: the files against each other only (default stored) |
| `--scope-kind string` | Scope kind: Account, Project or Catalog (with --scope-name) |
| `--scope-name string` | Scope name: me, a project id, or shared (with --scope-kind) |

Examples:

```text
  srw preview -f team.yaml > plan.json
  srw apply -f team.yaml --plan plan.json
```

## srw resume

Continue a job, optionally with feedback.

```text
srw resume JOB [flags]
```

```text
Continue a paused or failed job. On a job that stopped for review, resume
sends it back to work; say what to change with --feedback. On a job that is
running, resume interrupts it and queues it again.

A job shows that it stopped for review a moment before the server has
finished stopping it. A resume in that moment is refused as a conflict; srw
sends it again for up to 20 seconds while the job still waits for review.
```

| Flag | Meaning |
|---|---|
| `--feedback string` | What the job should do differently |

## srw run

Start a job.

```text
srw run [TASK] [flags]
```

```text
Start a job with a task. The task is the argument, a file (-f), or standard
input.

With --wait, srw waits for the job and exits by its outcome, as `srw wait`
does: 0 completed, 8 failed or cancelled, 10 stopped for a person, 9 timed
out. By default a job stops for review when its work is done (exit 10);
--autonomy full lets it complete by itself.

--phases makes the job plan its work in phases, for long tasks; without the
flag the project and the expert decide.

Starting a job cannot be repeated safely: when the answer is lost (exit 6),
check `srw get jobs` before starting it again. A Job manifest applied with
--idempotency-key can be repeated.
```

| Flag | Meaning |
|---|---|
| `--autonomy string` | When the job stops for a person: full, review, partial, guided, dependent |
| `--connector stringArray` | Connector to attach, by id (repeatable); replaces the defaults |
| `--expert string` | Expert that runs the job: a built-in name or an expert's id |
| `-f, --file string` | Read the task from a file; - reads standard input |
| `--interval duration` | With --wait: ask at this fixed interval instead of backing off |
| `--lane string` | Execution lane: pinned or stateless |
| `--model string` | Model for this job |
| `--no-connectors` | Attach no connector |
| `--phases` | Plan the job in phases (for long work); --phases=false works on the task directly |
| `--priority int` | Priority, 0 to 10 (default 5) |
| `-p, --project string` | Project to run the job in, by name or id |
| `--timeout duration` | With --wait: give up after this long (0: never), for example 90m |
| `--wait` | Wait for the job and exit by its outcome |

Examples:

```text
  srw run "Summarise the open issues" --expert scholar
  srw run -f task.md -p website --autonomy full --wait --timeout 2h
  git diff | srw run --expert reviewer --wait
```

## srw validate

Check manifests without storing or starting anything.

```text
srw validate -f FILE [flags]
```

```text
Check the syntax and the structure of manifests. Nothing is stored, nothing is
started, and no access is checked.

-f can be repeated. A directory means its .yaml, .yml and .json files. Several
files are checked as one bundle, and an error names the file it is in.
```

| Flag | Meaning |
|---|---|
| `-f, --file stringArray` | Manifest file or directory; repeatable; - reads standard input |
| `--format string` | Format of the files: json or yaml (default: by extension, else yaml) |

Examples:

```text
  srw validate -f team.yaml
  srw validate -f experts/ -f project.json
  cat team.yaml | srw validate -f -
```

## srw version

Show the version of srw and of the server.

```text
srw version [flags]
```

```text
Show the version of this srw and of the server it is set up for. srw works
with the server of its own minor version; a difference is reported here and
by `srw login`, not by every command.
```

| Flag | Meaning |
|---|---|
| `--client` | Show the version of srw only; do not ask the server |

## srw wait

Wait for a job and exit by its outcome.

```text
srw wait JOB [flags]
```

```text
Wait until a job ends or stops for a person, and say which with the exit
code:

   0  completed, or it reached a status named with --for
   8  failed or cancelled
  10  stopped and needs a person: in review, waiting for a reply, or paused
      by someone
   9  --timeout passed
   7  the server stopped answering for more than ten minutes
 130  interrupted; the job keeps running

A job paused by the system counts as running: it is picked up again by
itself. A status srw does not know counts as running too.

A server that stops answering in the middle, for example while it is
upgraded, is asked again for up to ten minutes.
```

| Flag | Meaning |
|---|---|
| `--for string` | Stop at these statuses with exit 0: status=\<status\>[,\<status\>] |
| `--interval duration` | Ask at this fixed interval instead of backing off from 2s to 15s |
| `--timeout duration` | Give up after this long (0: never), for example 90m |

Examples:

```text
  srw wait job/0b5c2f6e --timeout 2h
  srw wait 0b5c2f6e --for status=pending_review
```

## srw whoami

Show the user, the server and the token in use.

```text
srw whoami
```

