# Claude Code harness image

`ghcr.io/superhuman-remote-worker/srw-harness-claude-code` is a harness image:
a drop-in replacement for SRW's agent image that runs the Claude Code agent
loop (the CLI bundled in the `claude-agent-sdk` wheel) on a Job and works in
the Job's workspace over SSH. It is the worked example for
[Build a harness image](../../docs/harness-images.md), which describes the
contract this image implements.

An Expert selects it with `adapter: srw/v1` and the image's digest. A Job then
runs as an ordinary SRW agent pod of this image, with a pinned lane and one
cold pod for the Job. An operator approves the image by digest in
`manifestHosting.approvedImages`, and the submitting user needs the
`custom_harness_images` grant (administrators bypass it). The examples are
[`claude-code-harness-expert.yaml`](../../examples/manifests/claude-code-harness-expert.yaml)
and [`claude-code-harness-job.yaml`](../../examples/manifests/claude-code-harness-job.yaml).

The image is Python 3.12 (slim Debian, glibc) running as `999:999` (the identity SRW's agent pod gives every image, with a
read-only root; the image has an account for it) with `HOME=/tmp`. It has no Node and no git. `/home/agent-host/workspace` exists
inside the image because the CLI's local working directory mirrors the
workspace root path the model sees.

## What it does

1. Serves the v1 harness routes on `AGENT_PORT` (`8001`): `/health`, `/ready`,
   `/status`, `/job/start` and `/job/cancel`.
2. Registers with the orchestrator (`image` from `AGENT_IMAGE`, `pod_uid` from
   `POD_UID`, `build_sha` from `BUILD_SHA`, `agent_mode: "worker"`) and
   heartbeats at the interval it is given.
3. Takes one Job on `POST /job/start`, after checking the `recipient`, and
   answers 202.
4. Runs Claude Code against the Job's workspace, then reports on
   `POST /api/jobs/{id}/complete`, deregisters and exits.

Its `/ready` declares `pinned_recipient_binding: true`, `jobs: true` and
`model_protocols: ["anthropic-messages"]`. It declares no `pause`, so the
operator's pause is refused for these Jobs and the Job is cancelled instead. It
declares no `steer` or `sessions`, so operator guidance for a running Job is
ignored and sessions never run on it.

A custom pod gets no warm pool and is never drained or scaled down by SRW, so
the image ends itself: after its report, after a cancel, on a drain intent while
idle, or after 600 seconds without a Job.

## The model

The Job's model arrives in the start request. This image speaks only the
Anthropic Messages API, so the model must be a catalog model whose **API shape**
is `anthropic-messages` (Admin → Models). Set that shape on an endpoint-backed
model to point the harness at an Anthropic-compatible server or the
subscription proxy. A Job with a model of another shape is refused before the
pod starts with a plain error; a model that arrives with no credential and no
base URL is reported as an error.

The Expert's instructions are appended to Claude Code's own system prompt. The
model's reasoning level becomes the CLI's `effort`.

Fixed settings, not configurable from a Job or an Expert: `permissionMode:
default`, no settings sources, hooks, plugins, skills, CLAUDE.md files or extra
MCP servers; a limit of 200 turns, a budget of 20 USD and a wall-clock bound of
3600 seconds. The harness image holds itself to the rule that nothing a Job
supplies reaches code execution. Python starts in isolated mode (`python3 -I`),
so environment variables such as `PYTHONPATH` cannot make the interpreter load
code before the harness runs, and the process environment is cleared of everything but `PATH` before
the CLI starts, so the pod's other variables (the internal key, the database
credentials) never reach the CLI or the model.

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

The model controls its workspace, including the login shell's start-up files.
The harness tolerates them: every workspace operation frames its output with a
per-call random marker and ignores what a start-up file prints before it.

## Workspace connection

The harness connects with the workspace's SSH key and user from the start
request. It checks the workspace's host key against the
`workspace_ssh_host_key_fingerprint` the request carries (`SHA256:<base64>`)
during the SSH handshake, before authenticating. A request without a
fingerprint (or with a malformed one) is refused and the Job ends with an
error, as SRW's own agent refuses a sandbox workspace without one. Connecting is retried
for up to 90 seconds: a fresh workspace's sshd and the network policy admitting
the harness both lag behind the Pod's readiness. An SSH-level error such as a
rejected host key fails at once.

The workspace image needs `bash`, coreutils (`timeout`), `tar` and `find`.

## The result

On success the report carries `freeze_data` with `status: "job_completed"`,
`freeze_type: "job_complete"`, the model's final message as `summary`,
`deliverables` (the paths the `Write` and `Edit` tools touched, relative to the workspace
root, sorted) and `confidence`. The Review page shows these. `goal_achieved` is
`true` only when the Job's autonomy is `full`, as for SRW's own agent; otherwise
the finished Job waits in Review.

The session transcript is copied to `.srw/claude-code/transcript.jsonl` in the
workspace, on a best-effort basis, so the result can be reviewed with the
workspace.

A failure is reported as an error with a plain message: `timed out: the 3600 s
wall-clock limit was reached`, `run error: error_max_turns` and so on. Values
that must stay secret (the model key and the delivery proof) are replaced by
`***` in anything the harness prints or reports.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | The Job was reported; or cancelled (the orchestrator has already recorded the cancel, so no report follows); or, while idle, a drain intent or the idle timeout. |
| 1 | The completion report was refused or failed, or the runner raised an unexpected error. |
| 2 | `ORCHESTRATOR_URL` or `MCP_INTERNAL_KEY` is not set, or `AGENT_PORT` is not a port number. |
| 4 | SIGTERM or SIGINT. A running Job is stopped and no report follows. |

Progress lines go to stdout as `[srw-harness] <what>`.

## Network

The pod must reach the orchestrator, the workspace over SSH and the model
endpoint the Job names. If the model endpoint is outside the cluster, make sure
the installation's network policy lets this pod reach it.
