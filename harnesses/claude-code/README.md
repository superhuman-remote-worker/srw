# Claude Code harness image

`ghcr.io/<repo>-harness-claude-code` runs the Claude Code agent loop (the CLI
bundled in the `claude-agent-sdk` wheel) as a generic SRW Job, and operates the
Job's workspace over SSH. It has no SRW hooks beyond the delivered files, so an
operator approves it by digest in `manifestHosting.approvedImages` like any
image. A complete Job is in
[`examples/manifests/claude-code-harness-job.yaml`](../../examples/manifests/claude-code-harness-job.yaml).

The image is Python 3.12 (slim Debian, glibc) running as `10001:10001` with
`HOME=/tmp`. It has no Node and no git. `/home/agent-host/workspace` exists
inside the image because the CLI's local working directory mirrors the
workspace root path the model sees.

## What it reads

SRW delivers three files; the harness reads them from the paths in
`SRW_CONFIG_FILE`, `SRW_TASK_FILE` and `SRW_BINDINGS_FILE`
(`/run/srw/config.json`, `/run/srw/task.json`, `/run/srw/bindings.json`), plus
the workspace SSH material under `/run/srw-workspace/` (`id_ed25519`,
`known_hosts`, `ssh`). The workspace is reached as `agent-host` on port 30022,
root `/home/agent-host/workspace`. A missing or invalid file ends the run with
exit code 2 and a message naming the file or setting; supplied values are
never echoed.

The process environment is scrubbed to `PATH`. The harness is proxy-less:
proxy and CA variables (`HTTP(S)_PROXY`, `SSL_CERT_FILE`, ...) are not passed on
and are not used.

## config.json (version 1)

Unknown keys are refused. A new field means a new image, and so a new digest to
approve.

| Field | Default | Bounds / values |
|---|---|---|
| `version` | required | `1` |
| `model` | required | non-empty string |
| `systemPrompt` | `{type: preset}` | `{type: preset}`, `{type: preset, append: "<text>"}` or `{type: custom, text: "<text>"}` |
| `tools` | `[Bash, Read, Write, Edit]` | non-empty, distinct names from those four |
| `limits.maxTurns` | 200 | integer 1 to 10000 |
| `limits.maxBudgetUsd` | 20 | number above 0, at most 1000 |
| `limits.timeoutSeconds` | 3600 | integer 60 to 604800; wall-clock bound on the session |
| `effort` | unset | `low`, `medium`, `high`, `xhigh`, `max` |
| `thinking` | unset | `{type: adaptive}`, `{type: disabled}` or `{type: enabled, budget_tokens: N}` |
| `autoCompact` | `true` | boolean |
| `workspaceEnv` | `[]` | upper-case environment variable names to forward into the workspace |

Fixed settings, not configurable and recorded in `result.json`:
`permissionMode: default`, `settingSources: []`, `toolSearch: false`,
`autoMemory: false`, `claudeMd: false`, `nonessentialTraffic: false`,
`fallbackModel: null`, `outputLimitChars: 30000`, MCP server name `ws`.

### No code execution from settings

Nothing in `config.json` reaches code execution. Every key is typed, bounded
and checked against an allow-list. The CLI's own built-in tools are off; no
settings sources, hooks, plugins, skills, CLAUDE.md files or extra MCP servers
are loaded; the only tools are the four harness tools below, served by an
in-process MCP server named `ws`. The CLI runs with a frozen environment.

## task.json

```json
{"text": "...", "data": {"inputs": [{"url": "...", "path": "..."}],
                          "upload": {"url": "...", "method": "PUT", "headers": {}}}}
```

- `text` is the prompt.
- `data.inputs[]`: each `url` (http or https) is streamed into the workspace at
  the relative `path` (no leading `/`, no `..`) before the session starts. A
  failed download ends the run with exit code 2.
- `data.upload`: where the bundle goes (`method` `PUT` or `POST`, default `PUT`;
  optional string `headers`). Without it nothing is archived or uploaded:
  `result.json` and the transcript stay in the pod and end with it (the exit
  code still reports the outcome). Prefer a presigned URL over a secret header.

## Credentials

The model credential is `CLAUDE_CODE_OAUTH_TOKEN` (create one with
`claude setup-token`) or `ANTHROPIC_API_KEY`, set through a `runtime.env`
secretRef:

```yaml
env:
  CLAUDE_CODE_OAUTH_TOKEN:
    secretRef: {name: claude-code, key: token, scope: {kind: Account, name: personal}}
```

`workspaceEnv` cannot forward the model credential: naming
`CLAUDE_CODE_OAUTH_TOKEN` or `ANTHROPIC_API_KEY` there is a configuration error.
The model never sees it; the commands it runs in the workspace cannot read it.

### workspaceEnv and env connectors

`workspaceEnv` lists variables the harness copies from its own environment into
the workspace (written to an environment file the shell tool sources). Deliver
them with an `srw.env/v1` connector, whose `config.env` maps names to values or
to `{credential: <name>}` selections resolved from the connector's
`credentials` (`{secretRef: {name, key, scope}}`). A listed name that is not
set is a configuration error (exit 2).

Give each run its own database credentials (a per-run user and password, as in
the example): the model can read these values inside the workspace and they end
up in transcripts and workspace archives.

## Tools

The model sees four tools, named and shaped like Claude Code's own, all running
in the workspace over SSH:

- `Bash(command, timeout)`: runs under `bash`. The working directory persists
  between calls; shell variables do not. `timeout` is milliseconds, default
  120000, maximum 600000.
- `Read(file_path, offset, limit)`: line-windowed (2000 lines by default, long
  lines cut at 2000 characters). Non-regular files and binary files are refused.
- `Write(file_path, content)`: writes a whole file. An existing target that is
  not a regular file (a FIFO, a device, a directory) is refused.
- `Edit(file_path, old_string, new_string, replace_all)`: exact replacement;
  files over 10 MiB are refused.

Output over 30000 characters is truncated. The full output is saved under
`<workspace root>/.srw/tool-output/N.txt` and the truncated result names that
absolute path so the model can `Read` a window of it.

The model controls its workspace, including the login shell's start-up files
(`~/.bashrc` runs before every command sent over SSH). The harness tolerates
them: every workspace operation frames its output with a per-call random marker
and ignores what a start-up file prints before it (more than 1 MiB is an
error), keeps at most 64 KiB of stderr, and is bounded in time: 120 seconds for
each file operation, the `Bash` timeout for commands, 30 minutes for each input
download and 10 minutes for the workspace archive.

## Bundle

On every path once the target is known the harness uploads `bundle.tar`, an
uncompressed tar (the workspace archive inside is gzipped already), holding:

- `transcript.jsonl`: the session transcript.
- `result.json`: `version`, `exitCode`, `exitReason`, `startedAt`, `finishedAt`,
  `durationSeconds`, `config` (effective settings), `configHash`, `prompt`,
  `inputs` (with checksums), `archive`, `versions` (`harness`, `sdk`, `cli`),
  `result` (`subtype`, `isError`, `finalMessage`, `numTurns`, `totalCostUsd`,
  `usage`, `modelUsage`, `sessionId`, `stopReason`, `durationMs`,
  `durationApiMs`, `apiErrorStatus`), `timedOut`, `interrupted`, `error`.
- `workspace.tar.gz`: the workspace tree, excluding `.srw/tool-output`,
  `.srw-env` and `.srw-run`. Only with an upload target.

The harness pod has a fixed 2Gi of ephemeral storage, and at its peak it holds
the workspace archive, a copy of it inside the bundle and the transcripts. So
the archive is bounded, and `result.json` records under `archive` what it left
out:

- Staged inputs are not archived (`excludedInputs`): their size and SHA-256 are
  in `result.json` under `inputs`. Changes the model made to an input file are
  therefore not kept; write results to new files.
- Regular files over 256 MiB are skipped (`skipped`).
- The other regular files fill a budget of 768 MiB (uncompressed), smallest
  first, ties by path. Files that do not fit are omitted: `omitted` lists them
  (at most 1000 paths), `omittedCount` and `omittedBytes` count them, and
  `truncated` is `true`.
- Directories, symlinks and other entries are archived as tar stores them; an
  unreadable or vanished file is left out without failing the archive.
- The archive must finish within 10 minutes and stay under 832 MiB (the budget
  plus 64 MiB for tar and gzip overhead); otherwise it fails (exit code 1), its
  error is recorded under `archive.error`, and the bundle is uploaded with the
  transcript and `result.json` only.

The workspace archive file is deleted as soon as it is in the bundle. Uploads
are tried three times with backoff.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | The session finished with a `success` result and every later step succeeded. |
| 1 | Harness error: workspace SSH failure, workspace archive failure or an unexpected exception. |
| 2 | Configuration error: invalid or missing config, task or bindings file; no model credential; a `workspaceEnv` name not set; input staging failed. |
| 3 | Run error: the session produced no result, an error result, or a non-`success` subtype (for example `error_max_turns`, `error_max_budget_usd`). |
| 4 | Interrupted: the `limits.timeoutSeconds` deadline passed or SRW sent SIGTERM (cancel). Takes precedence over 3. |
| 5 | The bundle upload failed. |

Precedence: a failure before the session gives 1 or 2; after the session 4
beats 3 beats 0; a success (0) is replaced by a later archive failure (1), a
stop request while archiving or uploading (4) or an upload failure (5).
`result.json` records the code and a reason.

On SIGTERM the pod has about 30 seconds. The harness interrupts the session,
skips the workspace archive and uploads `transcript.jsonl` and `result.json`
within 10 seconds.

## Workspace requirements

The workspace image needs `bash`, coreutils (`timeout`), `tar` (GNU tar; the
archive uses its options), `gzip` and `find` (GNU findutils: the archive uses
`-printf`). Check that the workspace image you select provides them.

## Timeouts

`limits.timeoutSeconds` bounds the Claude Code session. A Job's own
`timeoutSeconds` includes queue time, workspace start and the upload, so set it
generously (the example uses three days around a one-hour session).
