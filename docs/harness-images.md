---
title: Build a harness image
description: Replace SRW's agent with your own image that takes Jobs over a small HTTP contract, and run it from an Expert.
---

# Build a harness image

The *harness* is the program that runs a Job: it takes the task, talks to a
model and works in the Job's workspace. SRW ships its own, the SRW agent image,
and uses it for every Expert by default. A harness image is a drop-in
replacement for it. It starts as the same kind of pod, registers with the
orchestrator the way the SRW agent does, takes one Job, reports the result and
exits. Everything around it (queueing, workspace, model routing, the Review
page, cancel, cost and audit) stays SRW's.

This guide covers what the orchestrator expects from such an image: the HTTP
routes it serves, the calls it makes, the environment it starts in, the request
it receives and the report it sends back. The
[Claude Code harness](../harnesses/claude-code/README.md) is the worked
example: a complete image of this kind in about 3000 lines of Python.

## When to build one

Use SRW's own agent unless you need a different agent loop: another vendor's
coding agent, a specialised pipeline, a tool you already run. A harness image
is not a way to run arbitrary containers; use a
[generic Job](../examples/manifests/README.md) for that.

What a custom harness image gets, and does not:

- **One cold pod for each Job.** Its pod is single-use and runs on the pinned
  lane. A custom image gets no warm pool, is never drained for SRW build drift
  and is excluded from idle scale-down. It must therefore end by itself: after
  its report, after a cancel, or after being idle too long (the Claude Code
  image waits 600 seconds for a Job).
- **Jobs only.** Sessions never run on a custom image (the request is refused
  with 422, "jobs only").
- **The same pod spec as SRW's agent.** The ConfigMap and the Secret SRW's
  agent gets are also injected into it, so an approved image is trusted with
  them. Nothing in a Job or Expert can add a `command`, `args`, `env` or
  probes to it: the image starts on its own entrypoint.

## Approve the image

An operator approves images by digest in `manifestHosting.approvedImages`, a
list of `{name, image, description}` entries:

```yaml
manifestHosting:
  approvedImages:
    - name: claude-code
      image: ghcr.io/superhuman-remote-worker/srw-harness-claude-code@sha256:<digest>
      description: Claude Code harness
```

Each `image` must be digest-pinned (`repo[:tag]@sha256:<64 hex>`) and an
Expert's `image` must equal it exactly. The list binds administrators too: the
default `[]` approves nothing. The
[manifest guide](../examples/manifests/README.md#approved-harness-images)
describes the setting, and the
[Helm values](https://docs.superhuman-remote-worker.com/reference/helm-values/)
list it.

Running an approved image also needs the `custom_harness_images` capability
grant (Admin → Grants). It is denied by default and resolves user, then
project, then global. Administrators bypass it.

## Name the image on an Expert

An Expert runs a harness image when its runtime uses the `srw/v1` adapter and
names the image:

```yaml
apiVersion: srw/v1alpha1
kind: Expert
metadata:
  name: claude-code-harness
  scope: {kind: Account, name: personal}
spec:
  runtime:
    adapter: srw/v1
    image: ghcr.io/superhuman-remote-worker/srw-harness-claude-code@sha256:<digest>
    config:
      config_name: worker_base
```

A Job selects that Expert like any other and, as with every `srw/v1` Job,
uses `completion: {mode: Reported}` and one attempt. The full pair is in
[`claude-code-harness-expert.yaml`](../examples/manifests/claude-code-harness-expert.yaml)
and [`claude-code-harness-job.yaml`](../examples/manifests/claude-code-harness-job.yaml).

## The pod your image starts in

The pod is SRW's agent pod with your image in place of its own. Your program
finds this in its environment:

| Variable | Meaning |
| --- | --- |
| `ORCHESTRATOR_URL` | Where to register and report (from the ConfigMap) |
| `MCP_INTERNAL_KEY` | The key sent as the `X-Internal-Key` header on every call to the orchestrator (from the Secret) |
| `AGENT_PORT` | The port to serve on; `8001` |
| `AGENT_CONFIG` | The config name of the Expert's `config_name` |
| `AGENT_EXPERT_ID` | The Expert's id, when the Job has one |
| `AGENT_IMAGE` | The image reference the pod runs; send it back when you register |
| `POD_UID` | The pod's UID; send it back when you register and when you report |
| `BUILD_SHA` | The build revision to report at registration, when the ConfigMap sets one |

The pod's container runs as **uid 999 and gid 999**, which overrides the
image's `USER`. It sets `runAsNonRoot`, a read-only root filesystem and drops
all capabilities. The writable mounts are `/tmp`, `/run`, `/home/srw` and
`/workspace`; everything else in the image is read-only. Give the image an
account for uid 999 if its tools look the user up by uid (asyncssh calls
`getpass.getuser()` and Node's `os.userInfo()` fails on an unknown uid), and
set `USER` and `LOGNAME` to its name. Test the image under
`--user 999:999 --read-only`.

The ConfigMap and the Secret are injected whole (`envFrom`), so the pod also
holds the rest of SRW's agent environment. Read only what you need. The
workspace's SSH private key is a file in the pod; the start request names its
path.

## The v1 harness API

### What the orchestrator calls on your pod

Serve these on `AGENT_PORT`. The orchestrator addresses the pod by IP.

| Route | Required | Purpose |
| --- | --- | --- |
| `GET /health` | yes | Liveness: `status`, `agent_id`, `uptime_seconds` |
| `GET /ready` | yes | Readiness and capabilities (below) |
| `GET /status` | yes | Current job id and mode |
| `POST /job/start` | yes | Hand the pod a Job |
| `POST /job/cancel` | yes | Stop the running Job |
| `POST /job/pause`, `POST /job/resume` | optional | Operator pause; declare `pause: true` |
| `GET /job/current` | optional | The running Job's id |
| `GET /system/info`, `POST /system/shell-state` | optional | Diagnostics SRW's agent serves |

SRW's agent serves more routes (`/metrics`, the `/session/*` routes). A custom
image does not need them.

`GET /ready` answers `ready`, `message`, `connections` and a `capabilities`
object:

| Capability | Meaning |
| --- | --- |
| `pinned_recipient_binding` | **Must be `true`.** Without it the orchestrator never delivers a Job and the Job silently requeues. |
| `jobs` | `true` when the image runs Jobs. A `/ready` without the key is treated as an earlier SRW pod, which runs Jobs; `false` refuses the Job. |
| `model_protocols` | The API shapes the image can call: `openai-responses`, `openai-chat`, `anthropic-messages`. Omitted means any model. A Job whose model has another shape is refused before it starts, with a plain error. |
| `pause` | `true` when `/job/pause` works. Without it (and with `jobs` declared) the operator's pause gets 409, "This job's harness does not support pause; cancel it instead." |
| `steer` | Declared for documentation. Operator guidance for a running Job is stored in the database; a harness that does not read it ignores guidance. |
| `sessions` | Declared for documentation. Sessions never run on a custom image. |

### What your pod calls on the orchestrator

Every call carries `X-Internal-Key: $MCP_INTERNAL_KEY`.

1. **Register.** `POST /api/agents/register` with `config_name` (from
   `AGENT_CONFIG`), `pod_ip`, `pod_port`, `hostname`, `pid`,
   `image` (from `AGENT_IMAGE`), `pod_uid` (from `POD_UID`), `build_sha`
   and `agent_mode: "worker"`. The answer carries `agent_id`,
   `heartbeat_interval_seconds` and `dispatch_process_generation`. Register
   after your routes are being served.
2. **Heartbeat.** `POST /api/agents/{agent_id}/heartbeat` with `status`
   (`ready`, `working` with `current_job_id`, `draining`) at the returned
   interval. A `ready` heartbeat is what makes the orchestrator dispatch. The
   answer's `intents` object can hold `should_drain`; honour it while idle by
   deregistering (`DELETE /api/agents/{agent_id}`) and exiting. A 404 means
   the orchestrator forgot you: register again.
3. **Complete.** `POST /api/jobs/{job_id}/complete` (below).

### `POST /job/start`

The orchestrator sends the Job and a `recipient` naming the pod it believes it
is talking to. Do these in order:

1. **Verify the recipient.** `recipient.expected_agent_id`,
   `expected_pod_uid`, `expected_process_generation` and `expected_job_id`
   must match your `agent_id`, `POD_UID`, `dispatch_process_generation` and
   the request's `job_id`. Answer 409 on a mismatch. The same check applies to
   `/job/cancel`.
2. **Answer 202 quickly** with `job_id` and `status: "accepted"`, and run the
   Job in the background. If the request carries `pinned_delivery_id`,
   `pinned_projection_digest` and `pinned_delivery_proof`, echo the first two
   in the answer. The three come together or not at all: a partial set is a
   409.
3. **Do not refuse a Job you cannot run with a non-2xx answer**, because that
   leaves it claimed and requeued. Accept it, then complete it with an error
   whose message says why.

What you read from the request:

| Field | Content |
| --- | --- |
| `job_id`, `description`, `instructions` | The Job and its task text |
| `resolved_config.agent.llm` | `model`, `provider`, `client_protocol`, `base_url`, `api_key` for the model to call |
| `resolved_config.agent.workspace.remote` | `host`, `port`, `username`, `key_path`, `workspace_path` of the workspace to work in over SSH |
| `workspace_ssh_host_key_fingerprint` | The workspace's host key as `SHA256:<base64>` |
| `resolved_config.agent.autonomy` | The Job's autonomy level; `full` or another (`partial` when absent) |
| `recipient`, `pinned_*` | The check above and the fields to echo |

`config_override` is `None` whenever `resolved_config` is sent. Check the
workspace host key against `workspace_ssh_host_key_fingerprint` during the SSH
handshake, before authenticating, so the key never goes to a host you did not
verify. SRW's own agent refuses a sandbox workspace that arrives without a
fingerprint, and so should you.

### `POST /job/cancel`

The orchestrator has already recorded the cancellation. Stop the work and
answer. Do not call `complete`; the cancellation is the outcome.

### Completion

`POST /api/jobs/{job_id}/complete` with:

- `agent_id`, and `should_stop`, `goal_achieved`, `error` (`null` on success).
  `goal_achieved` is `true` only when the Job's autonomy (`resolved_config.agent.autonomy`,
  default `partial`) is `full`, as for SRW's own agent. Otherwise send `false`: the
  finished Job then waits in Review;
- the pinned fields you echoed, `pinned_delivery_proof` as received, plus
  `pinned_process_generation` (the `dispatch_process_generation` from
  registration) and `pinned_pod_uid`;
- `freeze_data`, which the Review page reads: `status: "job_completed"`,
  `freeze_type: "job_complete"`, `summary`, `deliverables` (a list of
  paths relative to the workspace root, such as `hello.txt`) and `confidence` (a number; the Claude Code image sends `1.0`) for a reviewable
  result.

Report a failure as `error: {"message": ..., "type": ..., "recoverable": ...}`.
The message is shown to the user, so keep it plain and keep secrets out of it.
After the report, deregister and exit.

## Models and API shapes

The model for the Job arrives in `resolved_config.agent.llm` with the API
shape it is called with in `client_protocol`. An endpoint-backed model in the
catalog has an **API shape** select (Admin → Models), so one Anthropic-only
harness can use an Anthropic-compatible server or the subscription proxy: set
`anthropic-messages` on that endpoint's model. List the shapes your image
speaks in `model_protocols` so SRW refuses a mismatch before the pod starts.
SRW's own agent sends no sampling parameters (`temperature`, `top_p`, `top_k`)
to a non-Claude model on this shape.

## Direct database access

SRW's own agent reads and writes the application database directly. A harness
image may do the same, because it runs in the control plane with the same
Secret. These are the tables SRW's agent uses (found with
`grep -ln "asyncpg\|neo4j" src/agent -r`):

`jobs`, `agents`, `agent_audit`, `run_queue`, `completion_effects`,
`job_sources`, `sources`, `source_tags`, `source_annotations`,
`source_embeddings`, `citations`, `threads`, `thread_messages`,
`thread_events`, `thread_input_deliveries`.

This is optional, and these tables are not part of the harness contract:
migrations change them in any release, and an image that depends on one breaks
when it changes. Prefer the HTTP calls above.

## Checklist

1. `/ready` declares `pinned_recipient_binding: true` and `jobs: true`, and
   `pause` and `model_protocols` where they apply.
2. The image registers, sends heartbeats, and exits by itself after a report,
   a cancel, a drain intent or an idle timeout.
3. `/job/start` checks the recipient first, answers 202, and reports a Job it
   cannot run as an error.
4. The SSH host key is checked against the fingerprint in the handshake.
5. The report carries the pinned fields and a complete `freeze_data`.
6. The image's digest is in `manifestHosting.approvedImages` and the user has
   the `custom_harness_images` grant.
