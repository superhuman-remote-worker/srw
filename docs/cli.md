---
title: srw CLI
description: Install srw, the command-line client of Superhuman Remote Worker, log in, keep configuration as files and run jobs from a terminal or a pipeline.
---

# srw CLI

`srw` is one program that talks to an SRW installation from a terminal or a
pipeline. It does two things: it keeps configuration as files (experts,
connectors, workspace templates, projects), and it runs work (start a job,
wait for it, read what the agent did, fetch the result).

It uses the same API as the Cockpit and can do what its token allows. Every
command, flag and environment variable is listed in the
[srw CLI reference](cli-reference.md).

## Install

On Linux and macOS:

```bash
curl -fsSL https://docs.superhuman-remote-worker.com/install.sh | sh
```

The script downloads the archive for your machine from the GitHub release,
checks it against the release's `checksums.txt`, and puts `srw` into
`~/.local/bin`. It does not use `sudo` and does not edit your shell profile.
Pass `--version 0.3.0` for a specific release, or `--dir /some/bin` for
another directory:

```bash
curl -fsSL https://docs.superhuman-remote-worker.com/install.sh | sh -s -- --version 0.3.0 --dir "$HOME/bin"
```

To install by hand, download `srw_<os>_<arch>.tar.gz` (`linux` or `darwin`,
`amd64` or `arm64`) and `checksums.txt` from the
[release](https://github.com/superhuman-remote-worker/srw/releases/latest),
check the archive with `sha256sum -c checksums.txt --ignore-missing`, and
unpack `srw` into a directory on your `PATH`. To check who built the
archive, name the one you downloaded:

```bash
gh attestation verify srw_linux_amd64.tar.gz --repo superhuman-remote-worker/srw
```

On Windows, download `srw_windows_amd64.zip`, unpack `srw.exe` into a
directory on your `PATH`, and run it from PowerShell. The program is not
signed, so Windows may ask before the first start.

In a pipeline you can use the image instead:
`ghcr.io/superhuman-remote-worker/srw-cli`, tagged `vX.Y.Z` and `latest`. Its
entrypoint is `srw`, so `docker run --rm -e SRW_API_URL -e SRW_TOKEN
ghcr.io/superhuman-remote-worker/srw-cli get jobs` runs one command. It has a
shell, so it also works as a job image. GitHub Actions starts a `container:`
with a command of its own; GitLab CI needs the entrypoint cleared:

```yaml
deploy-experts:
  image:
    name: ghcr.io/superhuman-remote-worker/srw-cli:v0.3.0
    entrypoint: [""]
  script:
    - srw apply -f experts/
```

Use the `srw` of your installation's version. `srw version` shows both and
warns when they differ.

## Log in

Create a personal access token in the Cockpit under **Settings → API Keys**.
For everyday use tick `jobs:read`, `jobs:write` and `resources:write`, and
`projects:write` if you want to create or delete projects from files. Then:

```bash
srw login --server https://api.example.com
```

`srw` asks for the token without showing it, checks it, and saves the server
and the token as a *context*. The server is the address of the API, the same
one the [API guide](api.md) uses.

```bash
srw whoami
```

shows the user, the server, and the token's name, scopes and expiry.
`srw logout` removes the token from your machine. It stays valid on the
server until you revoke it in the Cockpit.

## A first walkthrough

Store an expert from a file, start a job with it, wait, and fetch the result.

```yaml
# reviewer.yaml
apiVersion: srw/v1alpha1
kind: Expert
metadata:
  name: reviewer
spec:
  runtime:
    adapter: srw/v1
    config:
      config_name: worker_base
      prompts:
        persona: Review the change you are given and list concrete problems.
```

```bash
srw validate -f reviewer.yaml        # check the file, change nothing
srw apply -f reviewer.yaml           # store it
srw get experts                      # it is in the list

srw run "Review the README and write output/review.md" --expert reviewer --wait
```

`run` prints `job/<id>` and, with `--wait`, waits for the job. By default a
job stops for your review when its work is done. `srw` then exits with code
10 and says how to continue:

```bash
srw logs job/<id>                    # what the agent did
srw files job/<id> --recursive       # what it produced
srw cp job/<id>:output/review.md .   # fetch a file
srw approve job/<id>                 # accept the result
```

A job is named by its id or by the first eight characters of it. Use
`--autonomy full` when no person should be asked; the job then completes by
itself and `--wait` exits with 0.

## Configuration as files

| Command | Does |
|---|---|
| `srw validate -f FILE` | Checks syntax and structure. Stores nothing. |
| `srw preview -f FILE` | Shows what an apply would resolve to. Stores nothing. |
| `srw apply -f FILE` | Stores the resources. A Job document also starts the job. |
| `srw get KIND [NAME]` | Lists experts, projects, connectors, workspace templates, jobs or sessions, or shows one. |
| `srw export KIND NAME` | Prints a stored resource as a manifest you can apply again. |
| `srw delete KIND NAME` | Deletes a stored resource, a project or a job. |

`-f` can be repeated, and a directory means its `.yaml`, `.yml` and `.json`
files. Several files are checked and applied as one bundle, and an error
names the file it is in.

`apply` asks the server for a preview first and applies exactly what the
preview saw. If someone changed a resource in between, nothing is applied
and `srw` exits with code 5; run it again. To apply exactly what you
reviewed, save the preview and pass it back:

```bash
srw preview -f team.yaml > plan.json
srw apply -f team.yaml --plan plan.json
```

By default a file is stored in your own account. `--project NAME` stores it
in a project, and `--catalog` in the shared catalog (admins only).

The manifest format is described in the
[manifest examples](https://github.com/superhuman-remote-worker/srw/tree/main/examples/manifests).

## Running work

| Command | Does |
|---|---|
| `srw run "TASK"` | Starts a job. The task can also come from `-f FILE` or standard input. |
| `srw wait JOB` | Waits and reports the outcome as an exit code. |
| `srw logs JOB` | Prints what the agent did. `-f` follows a running job. |
| `srw describe job JOB` | Status, why it stopped, to-dos and the completion report. |
| `srw files JOB` and `srw cp JOB:PATH DEST` | Lists and copies the files the job pushed. |
| `srw cancel`, `pause`, `resume`, `approve` | Control a job. `resume --feedback "…"` sends a job in review back to work. |

`files` and `cp` read what the job pushed last, not its live workspace. A
running job's newest files appear after its next push.

## Everything else

`srw api PATH` sends a request with your token to any path of the API and
prints the answer. Use it for a route that has no command of its own:

```bash
srw api /api/models
srw api '/api/jobs?status=failed&limit=5'
srw api -X POST /api/jobs --input job.json
```

The method is `GET` unless `-X` names another. The token goes only to the
configured server; an address of another server is refused. The routes are
described in the [API guide](api.md).

`srw cache list` and `srw cache delete` show and remove the prepared
workspace artifacts of an account, a project or the shared catalog. They
need an installation that runs VM workspaces.

## Output and exit codes

Tables are for people. For a script, add `-o json`, `-o yaml` or `-o name`
(`kind/id`, which every command accepts as input), or set `SRW_OUTPUT`. The
format never depends on whether the output goes to a terminal.
`validate`, `preview`, `apply` and `api` print JSON by default.

| Code | Meaning |
|---|---|
| 0 | Success |
| 1 | The operation failed (anything not listed below) |
| 2 | Wrong usage |
| 3 | Not logged in, or the token may not do this |
| 4 | Not found: the API says the object does not exist |
| 5 | Conflict: something changed in between |
| 6 | Outcome unknown: a change was sent and the answer was lost, or a proxy in front of the API answered it after passing it on (a 502 or a timeout) |
| 7 | The API could not be reached, or its certificate was not accepted. Also a proxy's answer in the API's place, such as Cloudflare's 52x, unless the request was a change that may have reached the API (then 6) |
| 8 | `wait`: the job failed or was cancelled |
| 9 | `wait`: timed out |
| 10 | `wait`: the job stopped and needs a person |
| 130 | Interrupted |

Errors go to standard error: one line for a person, or
`{"error": {...}}` when the command's output is JSON.

## Token scopes per command

| Commands | Scopes the token needs |
|---|---|
| `whoami`, `version` | any |
| `get`, `describe`, `logs`, `wait`, `files`, `cp`, `validate`, `preview`, `export`, `cache list` | `jobs:read` |
| `get sessions` | `chat:read` |
| `run`, `cancel`, `pause`, `resume`, `approve`, `delete job` | `jobs:write`; also `jobs:read` for names, id prefixes and `delete job` |
| `apply`, `delete` of a stored resource | `resources:write` |
| `apply` with a Job document | also `jobs:write` |
| `apply` or `delete` of a Project | also `projects:write` |
| Changes to the shared catalog, `cache delete` | `admin` |
| `api` | what the route needs; see the [scope table](api.md#scopes) |

A token without the `admin` scope acts without admin rights even when its
owner is an admin. `srw whoami` says so.

## In a pipeline

Set two variables instead of logging in. A saved login is not used then.

```bash
export SRW_API_URL=https://api.example.com
export SRW_TOKEN=ak_...        # from your secret store

srw apply -f experts/
srw run -f task.md --expert reviewer --autonomy full --wait --timeout 2h
```

- `--autonomy full` lets the job complete without a person. Without it the
  job stops for review and `--wait` exits with 10. If a review is what you
  want, `srw wait JOB --for status=pending_review` exits with 0 at that
  point.
- `wait` and `logs -f` ride out a server that stops answering in the middle,
  for example during an upgrade: they ask again for up to ten minutes before
  they exit with code 7.
- `srw wait JOB --for status=pending_review && srw approve JOB` works as
  written, and so does `srw resume JOB --feedback "…"`: for a moment after a
  job stops for review the server still refuses both, and `approve` and
  `resume` send the request again for up to 20 seconds.
- Starting a job with `run` cannot be repeated safely. If the answer is lost
  (code 6), look at `srw get jobs` before starting it again. A start that can
  be repeated is a Job manifest applied with
  `srw apply -f job.yaml --idempotency-key "$CI_PIPELINE_ID"`.
- Give the pipeline its own token with only the scopes it needs.

## Several servers

Each `srw login` saves a context, named after the server unless you pass
`--context NAME`.

```bash
srw config get-contexts
srw config use-context staging
srw --context production get jobs
```

`SRW_CONTEXT` selects a context for one shell. A server given with
`--server` or `SRW_API_URL` always means "use the environment": the token
then comes from `SRW_TOKEN`, and the two ways are never mixed. In
particular, an exported `SRW_TOKEN` is never sent to the server of a saved
context.

The files are in `~/.config/srw/` on every system (or `$XDG_CONFIG_HOME/srw`,
or beside the file named by `SRW_CONFIG`): `config.yaml` holds no secret,
`credentials.yaml` holds the tokens and is readable by you only. A context
can fetch its token from a command instead, for example a password manager:

```yaml
contexts:
  production:
    server: https://api.example.com
    token-command: ["pass", "show", "srw/production"]
```

## Proxies and private certificates

`srw` uses the proxy from `HTTPS_PROXY` and `NO_PROXY`. For a server whose
certificate comes from your own certificate authority, pass `--ca-file` with
the authority's PEM file at `srw login`; it is saved with the context. Plain
`http://` is accepted only for a server on your own machine, unless you pass
`--insecure-http`.

## Security notes

- The config file can name a command that `srw` runs (`token-command`). It
  is read only from the config directory, never from the working directory.
  Do not accept a `config.yaml` from someone you would not run a script from.
- An export can contain credentials, because it is the stored document.
  `srw export` warns when it does. Keep secrets in secret references before
  a file goes into a repository.
- If you are not the owner of a resource, you see it with secret values
  removed. `srw export` refuses such a view, because applying it back would
  delete those values.
- `srw` never prints the token.

## Troubleshooting

| Message | What to do |
|---|---|
| `no server configured` | Run `srw login`, or set `SRW_API_URL` and `SRW_TOKEN`. |
| `the token is unknown, revoked or expired` | Create a new token under **Settings → API Keys**. |
| `Insufficient token scope: requires …` | Create a token that also has the named scope. |
| `this server does not have …` | The server is older than your `srw`. Install the `srw` of the server's version. |
| `the server's TLS certificate was not accepted` | Pass `--ca-file` with your certificate authority's file. |
| `the API at … is not answering: … from something in front of it` (or `from Cloudflare in front of it`) | A proxy answered instead of the API. The server may be restarting; if it stays, the address is not the API's. |
| `… got 502 … from something in front of it; the request may have reached the API before that` | A change got a proxy's answer instead of the API's (exit 6). Look at the object (`srw get`, `srw describe job`) before you send it again. `srw cancel` looks by itself and exits 0 when the job is cancelled; `srw delete job` looks and sends the delete again while the job is still there. |
| `… redirects to …` | The server address is not the API's. Check `https`, the host and the path. |
| `completion finalizing`, `job changed while control was being committed` or `pinned approval agent changed` on `srw approve` or `srw resume` | The job has only just stopped for review and the server has not finished stopping it. Both commands send the request again by themselves for up to 20 seconds. If it stays, look at the job with `srw describe job` and run the command again. |
| `This job's last completion report could not be processed and is parked …` | The server could not process the job's last report and stopped trying. Resume, approve and the other job controls answer this until an administrator unparks the report (`POST /api/admin/completion-commands/{id}/unpark`, the id is in the message) or force-resolves it. `srw approve` does not ask again for this one. |
| `Workspace authority retirement is incomplete`, or `strict checkpoint prune blocked by workspace recovery authority` | The server is still releasing the job's workspace. `srw delete job` asks again by itself for up to three minutes, and `srw cancel` exits 0 when the job is cancelled. If it stays, look at the job with `srw get` and run the command again. |

Shell completion: `srw completion bash`, `zsh`, `fish` or `powershell` print
the script and say how to load it. On macOS, bash completion needs
bash-completion v2 and a newer Bash than the system's.

## Licence

`srw` is source-available under the same licence as the platform,
FSL-1.1-ALv2. Using it in your own pipelines and keeping a copy on an
internal mirror is internal use, which the licence permits.
