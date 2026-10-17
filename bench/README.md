# bench/ — the paired-run measurement suite

The instrument behind the guardrail-loosening loop: a fixed set of job
templates with machine-checkable outcomes, a submitter that pins inputs and
fires replicates against an SRW installation, and a reporter that computes the
phase/ceremony metrics from `knowledge-base/knowledge/issues/phase_model_overhead_amnesia_loop.md`
§10. Protocol and rationale: `knowledge-base/knowledge/features/worker_runtime_strategy.md` §9.

The core rules the tooling encodes:

- **Pinned inputs.** Every job pins the model (`config_override.llm.model`)
  and runs `autonomy: full` so nothing parks in `pending_review`. Task text
  is fixed in `tasks.yaml` — never edit a task in place; add a new id.
- **Outcomes over vibes.** Tasks declare `required_deliverables`, which the
  orchestrator's deliverable gate enforces at `/complete` — so
  `status=completed` *is* the outcome check.
- **One variable per comparison.** An A/B run changes exactly one thing
  between arms (a config key, a prompt file). The first use is no A/B at
  all: baseline replicates to quantify within-config variance.
- **Exclusion rules.** Jobs that die on infrastructure (provisioning,
  provider outage) are excluded, not counted as failures of the config.
  Jobs with >15% of requests after their last phase archive are flagged
  `tail_anomaly` (two-episode jobs — see the `8302c195` case in issue doc
  §10) and reported separately.

## Quickstart

```bash
# The orchestrator's API base URL. Unset, the scripts use a local
# orchestrator on http://localhost:8085, for example:
#   kubectl -n srw port-forward svc/srw-orchestrator 8085:8085
export SRW_API_URL=https://api.example.com
export SRW_TOKEN=<mcp token>                    # X-MCP-Token value

# Baseline: every task × 3 replicates, ≤2 jobs in flight, pinned model
python bench/submit.py --run-id baseline-01 --replicates 3

# Watch / resume after interruption (already-submitted pairs are skipped)
python bench/submit.py --run-id baseline-01 --replicates 3 --resume

# Report (works on partial runs; terminal jobs only in group stats)
python bench/report.py --run-id baseline-01

# Or hand the same resolved task set to the server-side Job Bench. The returned
# UUID is the durable run id used by the report command; no laptop process stays
# alive after submission.
python bench/submit.py --server --run-id baseline-01 --replicates 3
python bench/report.py --server --run-id <returned-run-uuid>
```

Runs land in `bench/runs/<run-id>/` (gitignored): `manifest.json` written
incrementally by the submitter, `report.json` by the reporter.

In `--server` mode the frozen spec and submission ledger instead live in the
orchestrator's `bench_runs` row. The CLI reads and resolves `tasks.yaml` before
posting it; the server never reads a mutable task registry.

For the U2 phase-skills gate, run the audit helper against the audit database
with the member job ids from the server report:

```bash
psql "$AUDIT_DB_URL" -v job_ids='uuid-1,uuid-2' \
  -f bench/queries/phase_illegal_calls.sql
```

Its first result separates correctly rejected phase-mismatched attempts from
unsafe or unclassified calls. The latter must be zero. Its second result checks
the persistent `[phase: ...]` blocks: skills-mode jobs must have no requests
without a block, and new phases normally increase the count one at a time;
historical legacy-mode jobs intentionally have zero blocks. A compaction may
drop older, superseded blocks, which is reported separately rather than treated
as loss of the current block. The query's single-phase tool list has a test that
compares it directly with `TOOL_REGISTRY`, so registry changes cannot silently
stale the acceptance check.

## Task families

| prefix | family | config | what it exercises |
|---|---|---|---|
| S* | small | worker_base | the ceremony floor (plan → tiny execute → review) |
| M* | medium | worker_base | multi-part deliverables, single job |
| D* | dev | developer | multi-phase spec/red/green flow, self-contained code katas |
| R* | research | scholar | exploration sweeps + subjob spawning (noisy by design) |
| A* | automation-shaped | worker_base | the recurring-digest shape, inputs inlined |
| D3, D4 | dev, frontend | engineer | single-phase implement + verify with a check the task supplies (D3 inline module with three bugs and a duplicated helper; D4 a static page against an inline checker) |
| D5 | dev-repo | engineer | clone a pinned public repository, extend it in its own conventions, run its suite (network; excluded as `infra` if the clone fails) |
| O1 | ops | engineer | install a CLI into the sandbox, run it, report verbatim (network for `pip`) |

All tasks are self-contained (inputs embedded in the description) so the
task itself cannot drift between runs. Real-repo fix tasks are deliberately
absent from v1 — they change as the repo changes, which breaks pinning.

## Known limitations (v1)

- **Project memory couples replicates.** Jobs in the same project share
  Memory-Light recall, so replicate 3 can recall replicate 1's memories.
  v1 accepts this (it matches production behaviour); pass `--project-id`
  to scope a run to a dedicated project, and interleaved submission order
  spreads any order effect across tasks. For real A/Bs, use a distinct
  `project_id` on each arm to prevent cross-arm contamination.
- **Scholar tasks are high-variance** (subjob fan-out). One task in the
  set on purpose; don't let R* dominate group medians.
- **Cost attribution is main-loop only.** The reporter counts
  `call_type=main` requests; auxiliary calls (memory extraction, curation)
  are not in the ceremony numbers, matching the §10 methodology.
- **Raw and uncached input are different metrics.** Uncached input is reported
  input minus reported cache-read tokens. Report both: a larger stable prefix
  can increase total input while reducing uncached input. This is not an
  invoice calculation or a claim about how a subscription counts tokens.
- **Pin auxiliary inference when isolating a provider.** Setting an arm's
  `model` pins its main loop only. Also set `config_override.auxiliary.model`
  when background tasks must use that provider; otherwise deployment defaults
  still apply. Check actual model usage across all archived call types.

## Operating notes (from `baseline-02`, the first live server-side run)

- **Auth:** the orchestrator wants the token in the `Authorization: Bearer`
  slot (`X-MCP-Token` alone 401s on every route). `bench/_api.py` sends both.
- **Run row shape:** `state` on `/api/bench/runs/{id}` is the submission
  ledger itself — a JSON **list**, not `{jobs: [...]}`.
- **Reading requests:** `/api/jobs/{id}/llm-requests` returns
  `{entries, total, ...}` — parse `entries` (and prefer `total` over
  `len(entries)`); `requests`/`items` do not exist.
- **Ledger staleness:** an entry's `final_status` freezes at the sweeper's
  first terminal observation. Cancelling a job *after* that leaves the old
  label (e.g. `waiting_for_reply`) in the ledger — check the job row for
  truth.
- **`waiting_for_reply` counts as terminal** to the sweeper: a parked job
  frees its in-flight slot; the run does not block on it.
- **Multi-replica orchestrators double-submit:** with 2 replicas the tick
  race fired 3×/30 pairs (twins 2–5 ms apart). Until the advisory-lock fix
  lands (`knowledge-base/knowledge/issues/bench_sweeper_multi_replica_race.md`), watch a running
  run for duplicate (task, arm, replicate) pairs and cancel the younger twin
  (`PUT /api/jobs/{id}/cancel`).
- **Reports heal retroactively:** the report endpoint recomputes from audit
  rows at read time, so a run that finished during an observability gap
  yields full metrics afterwards — nothing is lost by not watching.
- **Mid-flight provider outages** classify as task failures, not infra
  (`knowledge-base/knowledge/issues/bench_infra_exclusion_misses_midflight_outages.md`) — scan
  per-job request timelines for ≫retry-ceiling gaps before trusting a
  failed row.

## Operating notes (from `p4-floor-trim-01` + `s4m2-rerun-01`, 2026-08-07)

- **Two-arm runs:** `submit.py --server --arms developer,engineer` builds one
  arm per bundled config name (arm name = config name; the arm's config_name
  overrides each task's), e.g. the developer-vs-engineer campaign
  (`knowledge-base/knowledge/features/engineer_expert.md` §6):
  `python bench/submit.py --server --run-id dev-vs-eng-01 --replicates 3 --model MiniMax-M3 --arms developer,engineer --only D1-wordfreq-kata,D2-inventory-bugfix,D3-ledger-refactor,D4-static-page,D5-clone-and-extend,O1-install-and-report`.
  Add **`--isolate-arms`** whenever the variable under test could be carried by
  project-scoped state: it creates one throwaway `bench-<run-id>-<arm>` project
  per arm and stamps `project_id` on each, so Memory-Light recall and KB notes
  cannot couple the treatments. Without it every arm inherits the run project and
  the second arm to reach a task can recall the first's memories of it.
  `bench/queries/first_command_latency.sql` reports attach → first shell command
  and the audit rows before it, over the member job ids. **Read it only for ops
  tasks:** on dev tasks it does not measure ceremony, because a strategic phase
  that opens with `git log`/`git tag` reaches its "first command" within a minute
  on any expert (dev-vs-eng-01: developer 1.0 min vs engineer 2.8 min, while the
  developer took 3–7× the requests). Time-to-first-*edit* and request count are
  the ceremony signals there.
  For arms that need distinct `config_override`s, build the spec yourself (tasks
  from `tasks.yaml`, `arms: [{name, model, config_override, project_id}, ...]`)
  and POST `/api/bench/runs` directly. The sweeper
  schedules arms adjacent within each replicate's shuffled wave — that
  adjacency is what neutralizes time-varying confounds (pool growth,
  contention), so never split arms across runs or clusters.
- **U2 attribution controls are retired:** WP5 used temporary legacy/union and
  skills/filtered arms to isolate phase prose from schema shape. WP6 removed
  those live configuration switches after the MiniMax verdict; use the recorded
  run IDs in the design note for historical comparisons.
- **Running on k3d:** mint the test id_token per the auth memo (admin-cli
  password grant, use `id_token` not `access_token`); `POST /api/projects`
  requires `user_id` in the body. Give memory-sensitive experiments a
  `project_id` in the spec — without a shared project the pool stays empty
  and memory-related treatments measure nothing.
- **`max_in_flight` can be raised mid-run** via
  `jsonb_set(spec,'{max_in_flight}', ...)` on the run row — the sweeper
  re-reads the spec every tick. It is an operational knob, not a pin; note
  the change in the analysis (contention shifts absolute walls). Observed:
  effective concurrency lands ~1 below the setting (dispatch cooldowns +
  pair staggering), so mif 2→4 bought ~1.5×, not 2×.
- **Cross-cluster wall poisoning:** concurrent bench runs on k3d and dev
  share the same LLM server — each inflates the other's walls (~2× per-req
  latency observed). Within-run A/Bs survive (symmetric); single-arm runs
  meant for wall comparison must not overlap another run. Token/request
  metrics are contention-immune.
- **Stuck-watch calibration:** D-family tasks legitimately exceed 3.5 h
  under contention; triage stuck alerts by iteration cadence in agent logs
  (advancing ≈30–60 s/turn = grinding, not wedged) before intervening. A
  bench job struggling on its task is *data*; only infra wedges get
  rescued/excluded.
