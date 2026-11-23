---
name: delegate-a-job
description: Use before creating a worker job that needs a specific model, a VM or lite workspace, different lifecycle passes, or the planned procedure in phases for long work — the config_override knobs and phases, and what each one costs.
display_name: Delegate a Job
icon: rocket_launch
color: "#89b4fa"
tags:
  - jobs
  - delegation
  - configuration
---

# Delegate a job

`create_job` starts a worker with the defaults its expert and project
supply. That is the right choice most of the time — reach for this skill only
when the work genuinely needs something else: a stronger or cheaper model, root
access, a repo checkout, a different set of lifecycle passes, or one small task
done in one loop.

Overrides go in `config_override`, a JSON object merged **last** — it wins over
both the project default and the expert. Set only the keys you mean to change;
everything you omit keeps resolving normally.

## Before you write an override

Call `get_session_context`. It reports the chat model IDs this deployment
actually routes and your effective grants. Both matter:

- Model IDs are per-deployment. A guessed ID has no transport and the job fails
  at dispatch instead of running.
- An override above your grants is refused when you create the job, naming the
  capability you lack.

## The knobs

**Model** — `{"llm": {"model": "<id from get_session_context>"}}`

Pin the worker's model. Use it to buy reasoning depth for a hard job, or to
spend less on a mechanical one.

**Workspace backend** — `{"workspace": {"backend": "sandbox" | "vm" | "virtual" | "none"}}`

- `sandbox` — the default. A shell-capable container; clones repositories.
- `vm` — a full VM with root. Needed for installing system packages, running
  containers, or kernel-level work. Requires the `vm_workspace` grant and boots
  in minutes rather than seconds, so ask for it only when root is the point.
- `virtual` / `none` — lite tiers with **no workspace pod and no shell**. Cheap
  and fast for pure reasoning or writing. They cannot clone repositories, so a
  job with a repository connector is refused outright.

**Lifecycle passes** — `{"verification": {"enabled": false}}`,
`{"scholar": {"enabled": false}}`, `{"curator": {"enabled": true}}`

`verification` spawns a critic that reviews and can resume the job; `scholar`
runs a research pass before the work starts; `curator` extracts knowledge into
the project KB as the job runs. Turn verification and scholar off when you are
already orchestrating the review yourself, and curation on when the job should
leave the project's knowledge better than it found it.

**Autonomy** — `{"autonomy": "full"}`

Lets the job run to completion without pausing for review. Capped by your
`autonomy_ceiling` grant.

**Phases** — `phases=True` or `phases=False` on `create_job`

Leave it out to use the expert's own setting. Pass `True` for long or
open-ended work on an expert that does not plan in phases by itself: the
worker then plans, works through todos in phases and reviews its result. Pass
`False` to have an expert that plans in phases do a small task directly. A
job without phases stops only at its end, so `partial`, `guided` and
`dependent` autonomy act like `review`.

**Memory** — `{"memory": {"required": true}}`

Makes the job pause for re-dispatch rather than run blind if its
embedding-backed stores are unavailable. Worth setting for any job whose value
depends on recalling prior work.

**Tools** — `{"tools": {"<category>": [...]}}`

Adds or replaces a tool category. `shell`, `delegation`, and the connector
categories each need their own grant.

## What gets refused

Job creation fails immediately, naming the reason, when the override exceeds
your grants:

| Override | Grant required |
| --- | --- |
| `workspace.backend: "vm"` | `vm_workspace` |
| `tools.shell` | `shell_tools` |
| `delegation.enabled` / `tools.delegation` | `delegation` |
| connector tool categories | `datasource_tools` |
| `tools.browser_direct` | `browser` |
| `llm.model` outside your permitted set | `model_selection` |
| `autonomy` above your ceiling | `autonomy_ceiling` |
| `interactive.permission_mode` above your ceiling | `permission_mode` |

Credential and transport keys — `api_key`, `base_url`, `env_keys`, anything
ending `_api_key` — are rejected before the request is sent. Routing is resolved
server-side from the model ID; pass the ID and nothing else.

## Picking an expert instead

`expert_id` (from `list_experts`) is often the better lever: a DB-backed expert
carries a model, a backend, and its own prompts together, already tuned. Use
`config_override` to adjust one dimension of an otherwise-right expert, not to
rebuild one by hand. `expert_id` cannot be combined with a `config_name` other
than `worker_base`.
