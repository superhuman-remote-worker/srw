---
title: Automate SRW with the API
description: Call the SRW API from n8n, Zapier, scripts or curl with a personal access token, to create jobs and manage automations from outside the Cockpit.
---

# Automate SRW with the API

The Cockpit runs on the same HTTP API you can call yourself. Use it when a
schedule on the **Automations** page is not enough: start a job when an email
arrives, chain jobs together, or connect SRW to n8n, Zapier or your own
scripts.

From a terminal or a pipeline, the [srw CLI](cli.md) uses this same API, with
commands for the common tasks and `srw api` for everything else.

## Before you start

You need:

- the **API address** of your installation. With separate hostnames this is
  `https://api.<your-domain>`. A single-origin installation uses its one SRW
  address. Every endpoint path starts with `/api`.
- a **personal access token** (see below).

The full reference for every endpoint is part of your installation. Open
`<API address>/api/docs` for the interactive reference, `/api/redoc` for a
read-only version, or download the OpenAPI document from `/api/openapi.json`.
The **API documentation** link on the Automations page opens the same
reference. The reference has no **Authorize** button yet, so use it to look up
request and response shapes, and send the calls from your own tool.

## Create a personal access token

In the Cockpit, open **Settings → API Keys**.

1. Name the token after the tool that will use it, for example
   `n8n production`.
2. Choose its scopes. Give the token only the scopes the tool needs.
3. Choose an expiry: 30 days, 90 days, 1 year (the default) or never.
4. Copy the token. It starts with `ak_` and is shown only once. If you lose
   it, create a new one.

Send the token in the `Authorization` header of every request:

```text
Authorization: Bearer ak_<your-token>
```

A token acts as the user who created it and can reach only what that user
can reach. If the user is suspended, their tokens stop working.

A tool can check its own token with `GET /api/auth/me`. Beside `user`, the
answer carries `credential`: `method` (`pat` for a token), the token's
`token_name`, its `scopes` and `expires_at`. `GET /api/version` returns the
installation's `release_version`, `source_revision`, the
`manifest_api_versions` it accepts and, when set, the Cockpit's address as
`cockpit_url`. Both routes work with any token.

**Rotate** creates a replacement token with the same name, scopes and expiry.
The old token keeps working for 24 hours so you can switch your tool over.
**Revoke** stops a token immediately.

### Scopes

| Scope | Allows |
|---|---|
| `jobs:read` | Listing and reading jobs and automations; reading projects, experts, skills, connectors, models and manifests; validating, previewing and exporting manifests. |
| `jobs:write` | Creating, renaming, pausing, resuming, cancelling, approving and deleting jobs; creating and changing automations. |
| `chat:read` | Reading sessions and their message history. |
| `chat:write` | Starting sessions and sending messages. |
| `knowledge:read` | Searching knowledge, reading sources and citations. |
| `knowledge:write` | Creating, changing and deleting knowledge notes and citations. |
| `resources:write` | Applying manifests and deleting stored resources: the configuration of your own account and of projects you can edit. This changes what later jobs run. A manifest that contains a Job also needs `jobs:write`; one that contains a Project also needs `projects:write`. |
| `projects:write` | Creating, changing and deleting projects through manifests, including what a project's officer may spend. Used together with `resources:write`. |
| `admin` | Everything above, plus the direct routes that create, change and delete projects, experts, skills, connectors and models, changes to the shared catalog, and administration. Only an admin can create a token with it. |

`write` does not include `read`: a tool that creates jobs and then checks on
them needs `jobs:read` and `jobs:write`. A request whose token lacks the
scope gets `403` with `Insufficient token scope: requires …` naming the
missing scope.

A token without `admin` can use projects, experts, skills and connectors in
the jobs it creates. It can change them through manifests only. With
`resources:write`, `POST /api/manifests/apply` and
`DELETE /api/resources/{id}` change what you could change in the Cockpit: your
own account, and projects where you are an editor or the owner. The shared
catalog stays with admins. Creating, changing or deleting a project, expert,
skill, connector or model through its own route (for example
`PATCH /api/projects/{id}`) still needs `admin`.

Tokens cannot create or change other tokens, SSH keys or MCP tokens, and they
cannot open WebSocket connections.

## Jobs

### Create a job

```bash
curl -sS -X POST "https://api.example.com/api/jobs" \
  -H "Authorization: Bearer $SRW_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
        "description": "Summarize the pull requests merged this week.",
        "expert": "scholar",
        "config_override": {"autonomy": "review"}
      }'
```

The response is the new job. Its `id` is what the other job endpoints take.

The fields most integrations use:

| Field | Meaning |
|---|---|
| `description` | Required. What the agent should do. It is also how the job is listed until someone names it. |
| `expert` | The expert that runs the job: a bundled expert name such as `scholar` or `developer`, or the ID of an expert from the Cockpit. `GET /api/experts` lists what you can use. Leave it out to use the installation's default worker expert. |
| `project_id` | Run the job in a project. You need at least the editor role on it. |
| `priority` | 0 to 10, default 5. |
| `phases` | `true` or `false`: whether the job plans in phases. Without phases the job does its task directly and ends with `job_complete`; with phases it plans, works through todos in phases and reviews its result, for long work. Leave it out to use what the project default or the expert says, which is `false` unless one of them sets `phases`. A job that starts inside a live session (`live_session`) always plans in phases, so `false` is refused there. `GET /api/jobs` and `GET /api/jobs/{id}` return `phases` for each job (`null` for a job from before this setting and for one on a custom harness). |
| `config_override` | Per-job settings. `{"autonomy": "review"}` stops the job for your review when it finishes; `"full"` completes it without stopping. |
| `instructions` | Extra instructions for the agent. |

The reference lists the remaining fields.

The job starts with status `created` and waits until an agent is free. It
then moves to `processing` and ends as `completed`, `failed`, `cancelled`, or
`pending_review` when it waits for your review.

### Follow and control a job

| Request | Does |
|---|---|
| `GET /api/jobs` | Lists the jobs you can see. Filter with `status`, `origin` or `project_id` (each can repeat); `search` matches the task text, the name and the start of the ID. |
| `GET /api/jobs/{id}` | Returns one job, including its `status`. |
| `GET /api/jobs/{id}/progress` | Returns the job's progress and `status`. `held_by` is `null` unless the job waits for an explicit resume (for example after a pause); a `paused` job without it is picked up again by itself. |
| `PATCH /api/jobs/{id}` | Renames the job. Body: `{"title": "…"}`, at most 200 characters; `{"title": null}` removes the name. The task text (`description`) does not change. |
| `PUT /api/jobs/{id}/pause` | Pauses the job. It stays paused until you resume it. |
| `POST /api/jobs/{id}/resume` | Resumes a paused or failed job. Optional body: `{"feedback": "…"}`. |
| `POST /api/jobs/{id}/approve` | Approves a job in `pending_review`. Optional body: `{"notes": "…"}`. |
| `PUT /api/jobs/{id}/cancel` | Cancels the job. |
| `DELETE /api/jobs/{id}` | Deletes the job. Only its creator, the project owner or an admin can delete it. |

SRW does not send webhooks yet. To act when a job finishes, poll
`GET /api/jobs/{id}` until its status is one of the end states above.

## Automations

Everything on the Automations page is also available through the API, which
is useful for setting automations up from a script or keeping a copy of them.

| Request | Does |
|---|---|
| `POST /api/automations` | Creates an automation. Returns `201` and the automation. |
| `GET /api/automations` | Lists your automations. With `?project_id=…`, lists every automation on that project. |
| `GET /api/automations/{id}` | Returns one automation. |
| `PATCH /api/automations/{id}` | Changes the fields you send. |
| `DELETE /api/automations/{id}` | Deletes the automation. Jobs it already started are kept. Returns `204`. |
| `POST /api/automations/{id}/run-now` | Starts a job now. The schedule is not changed. |
| `POST /api/automations/{id}/pause` | Stops the schedule. |
| `POST /api/automations/{id}/resume` | Starts the schedule again from now. |
| `GET /api/automations/{id}/runs` | Lists the jobs the automation started, newest first. |

A scheduled automation:

```bash
curl -sS -X POST "https://api.example.com/api/automations" \
  -H "Authorization: Bearer $SRW_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
        "name": "Weekly PR summary",
        "cron_expr": "0 8 * * MON",
        "timezone": "Europe/Berlin",
        "expert": "scholar",
        "prompt": "Summarize the pull requests merged last week.",
        "autonomy": "review"
      }'
```

| Field | Meaning |
|---|---|
| `name` | Required. Up to 200 characters. |
| `cron_expr` | Required for a schedule. A standard cron expression, for example `0 8 * * MON`. An invalid expression returns `400`. |
| `timezone` | IANA time zone for the schedule. Default `UTC`. |
| `expert` | Required. A bundled expert name such as `scholar`. |
| `expert_id` | To use an expert you created in the Cockpit, set `expert` to `worker_base` and `expert_id` to its ID. Other combinations are refused with `422`. |
| `prompt` | Required. What each job should do. |
| `autonomy` | `full`, `review` (default), `partial`, `guided` or `dependent`. |
| `config_override` | Settings for each job the automation starts. `{"phases": true}` makes each job plan in phases, for long work; without the key each job follows its expert. A value that is neither true nor false is refused with `422`, and so is `false` on an automation that answers live sessions (`trigger_type: live`). |
| `project_id` | Runs the jobs in a project. You need at least the editor role on it. |
| `enabled` | `false` creates the automation paused. |
| `max_fires_per_day` | Upper limit on jobs per day. Default 100. |

An automation with an expert from the Cockpit checks each time it fires that
you can still use that expert. If you cannot, the run fails instead of
falling back to another expert. To switch a `PATCH` back to a bundled expert,
send `"expert_id": null` with the new `expert`.

## Examples

### n8n: start a job for each new email

1. Add an **Email Trigger (IMAP)** node for the mailbox.
2. Add an **HTTP Request** node:
   - Method: `POST`
   - URL: `https://api.example.com/api/jobs`
   - Authentication: **Generic Credential Type → Header Auth**, with name
     `Authorization` and value `Bearer ak_<your-token>`
   - Send Body: on, Body Content Type: **JSON**, Specify Body: **Using
     JSON**, with this expression (`JSON.stringify` keeps quotes and line
     breaks in the email from breaking the JSON):

     ```text
     {
       "description": {{ JSON.stringify("Draft a reply to this email.\n\nSubject: " + $json.subject + "\n\n" + $json.text) }},
       "expert": "scholar",
       "config_override": {"autonomy": "review"}
     }
     ```

The token needs the `jobs:write` scope. Each email creates a job that waits
for your review in the Cockpit.

### Python

```python
import os
import time

import httpx

API = "https://api.example.com/api"
HEADERS = {"Authorization": f"Bearer {os.environ['SRW_TOKEN']}"}
END_STATES = {"completed", "failed", "cancelled", "pending_review"}

job = httpx.post(
    f"{API}/jobs",
    headers=HEADERS,
    json={"description": "List the open issues labelled 'bug'.", "expert": "scholar"},
    timeout=30,
).raise_for_status().json()

while True:
    status = httpx.get(f"{API}/jobs/{job['id']}", headers=HEADERS, timeout=30).json()["status"]
    if status in END_STATES:
        break
    time.sleep(30)

print(job["id"], status)
```

This token needs `jobs:read` and `jobs:write`.

## Errors

| Status | Usual cause |
|---|---|
| `401` | The token is missing, malformed, revoked or expired. |
| `403` | The token lacks the scope, or your user cannot access the job, project or automation. |
| `404` | The resource does not exist or is not visible to you. |
| `409` | The request conflicts with the current state, for example run-now on an automation whose project is archived. |
| `422` | The body does not match the schema. The response names the field. |

If an endpoint behaves differently from this page or the reference, please
[open an issue](https://github.com/superhuman-remote-worker/srw/issues).
