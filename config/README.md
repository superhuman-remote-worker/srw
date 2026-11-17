# Agent Configuration

This directory contains the reference SRW harness's manifests and private assets.
Bundled `experts/*/config.yaml` and `subagents/*/config.yaml` are now
`srw/v1alpha1` **Expert manifests**. Their existing settings live under
`spec.runtime.config.config`; the inheritance, model, prompt and tool behavior
described below belongs to the explicitly selected `srw/v1` harness adapter.
Other harnesses own their private configuration language.

```yaml
apiVersion: srw/v1alpha1
kind: Expert
metadata:
  name: application-helper
  scope: {kind: Account, name: personal}
spec:
  runtime:
    adapter: srw/v1
    config:
      config_name: worker_base
      asset_name: developer
      config:
        llm: {model: your-model}
        tools: {workspace: [read_file, write_file]}
      prompts:
        persona: Help implement and verify the assigned change.
```

With `adapter: srw/v1`, omit `image` to use the installed SRW harness. Bundled and
new editor-created Experts use this binding, so Helm/Tilt image upgrades do not
require editing every Expert or Project. Generic hosting still requires an image.
An explicit SRW `image` is an admission constraint: it must match the installed
image, and a mismatch is rejected on Job, Session and roster admission. Choose
generic hosting to launch an arbitrary image.

Earlier imports stored a concrete installation image. Remove that field through
a versioned resource update to adopt the installed binding. Restarting or rerunning
the importer preserves existing authored definitions. Active Projects keep frozen
Expert content and need a complete Project update to adopt the new binding.
Migrated Projects also retain a server-owned editor recipe; public export/reapply
adopts native authoring and re-resolves references, so review that transition before
using it to update a migrated Project. Existing execution generations remain unchanged.

The reference adapter launches the installation-managed worker image, including
on later stateless attachments. Each new execution snapshot records the concrete
image selected at admission. This is provenance: it does not select an old image
from the pool after a rollout. Generic hosting separately pins observed image
digests for retries and workspace handoffs.

Within this private contract, `config_name` selects the actual configuration
base. Optional `asset_name` selects an installed Expert or subagent directory
for prompt files, model-family matrices and skills only. For example, a bundled
Developer uses `config_name: worker_base` and `asset_name: developer`; its authored
settings remain solely in `config`. Removing a setting from the saved manifest
therefore cannot restore it from Developer's original file.
Experts with these asset selections or ordered layers use manifest export; the
old fragment export cannot represent those inputs and explicitly refuses them.
Server-side SRW roster selection resolves installed names to Catalog revisions
before rendering, so edited or retired named definitions cannot fall back to
their original files. The execution snapshot preserves those selected revisions.
Stored roster targets currently require `config_name` to name `expert_base`,
`worker_base`, `session_base` or `subagent_base` (including their legacy aliases).
Their authored settings are re-rooted onto the subagent role. Other private bases
remain available to top-level Experts; roster selection reports them as
unsupported instead of silently ignoring their inherited settings.

The SRW adapter also accepts `runtime.config.layers`, an ordered array of private
config objects. It applies the Expert leaf and then these layers above the
execution owner's account defaults. Project composition uses this to freeze
shared and per-Expert overrides without flattening their null-as-delete behavior
or copying one account's model defaults into everyone else's Project. A null
remains authored data in the manifest; only this adapter interprets it during
resolution. Later request overrides still apply last. This private field has no
meaning to a generic harness.

Saved expert IDs, grants and default references remain stable. Their canonical
payload lives in the resource store; the old `experts.config` and
`experts.prompts` payloads are emptied during migration. Existing expert editing
and selection APIs project the SRW private settings from that resource.
`scripts/migrate-expert-resources.py` previews pending stored conversions;
`--apply` performs the migration after schema migration, using the installed
harness binding. Optional `--image <trusted-image>` adds an explicit image constraint.
It preserves existing resource edits on reruns. Imported bundled resources are
also preserved on restart; upgrading one uses an explicit resource update.

Deleting an application user retires personal definitions and preserves completed
execution and workspace history. Shared Project definitions keep their existing
membership authority. Unfinished work, retained workspaces and shared defaults
must be resolved first; Project Experts need a remaining Project owner. User
deletion does not provision, transfer or delete external cloud accounts.

Projects now freeze their available Expert definitions and worker/session defaults
in one active revision. The migration retains Project IDs and membership, moves
shared and per-Expert overrides into ordered SRW private layers, and clears the
old Project override columns. Existing editors project those authored overrides
from the active resource; changing a default publishes a complete new revision.
Changing a source Expert alone leaves an active Project's composition unchanged.

Startup defers an unclaimed legacy Project when it has no members and no user
selecting it as their default. Its data stays intact and a warning identifies
the Project; explicitly establishing ownership makes it eligible on a later run.
A Project with members but no owner still requires ownership repair before
migration. Startup never invents ownership or discards a Project to complete
the conversion.

Historical cloud-backed Projects and Sessions may also need their missing
installation authority repaired before upgrading. The admin operation
`POST /api/admin/system-settings/main_cloud/backfill-instance-authority` previews
the mapping and verifies it against the live installation; inspect that preview
before repeating with `?apply=true`. Ambiguous installation history is rejected.
Do not fill those references with a guessed instance or relax the database checks.

The existing Officer kit is represented by
`team.controller: {type: srw/officer-v1, config: ...}`. Its private payload contains
`config` and `communicationPolicy`; thread IDs, leases, holds and observed state
stay in runtime tables. Authorized Officer kit edits update this Project revision
in the same transaction. Automatic commissioning, native fixed-Expert slots and
global Project limits are not implemented by this controller and are rejected
when applied. Existing commissioning and hold/release operations still own those
lifecycle transitions.

Referencing a bundled Catalog Expert as a typed Project default lazily binds one
stable global Expert ID to that same source resource. The picker shows it once;
the Project continues to borrow the source through its authored `ref`.

The standalone file conversion is repeatable with
`scripts/migrate-bundled-expert-manifests.py --check`. Prompt files, model-family
matrices and skill assets remain beside each bundled manifest. See
[manifest examples](../examples/manifests/README.md) for the generic resource
contract and the other building blocks.

## Directory Structure

```
config/
├── expert_base.yaml             # The ONE shared root every expert resolves on (every role)
├── overlays/                    # Role overlays — each `$extends: expert_base`
│   ├── worker.yaml              #   public name `worker_base`  (job / phase-loop experts)
│   ├── session.yaml             #   public name `session_base` (interactive / persistent experts)
│   └── subagent.yaml            #   public name `subagent_base` (roster entries; declares `$ignore_keys`)
├── schema.json                  # JSON Schema for config validation
├── model_config_matrix.yaml     # Per model family: prompt/instruction filenames, inference params, context limits, reasoning, guardrails
├── guardrails/                  # Per-family tool examples and nudges (named by the matrix's `guardrails.file`)
├── README.md                    # This file
├── experts/                     # Bundled roles and application-default seed bundles
│   └── <expert>/
│       ├── config.yaml              # Expert manifest; private overlay under runtime.config.config
│       ├── model_config_matrix.yaml # Expert-level matrix override (optional)
│       └── skills/                  # Expert-local skill overrides (optional)
│           ├── strategic-phase/SKILL.md
│           ├── tactical-phase/SKILL.md
│           └── direct-task/SKILL.md # Its own method for jobs without phases (see Phases)
├── subagents/                   # Subagent library — small experts a roster references by name (see subagents/README.md)
│   └── <name>/
│       ├── config.yaml              # Expert manifest; SRW private leaf extends expert_base
│       └── persona.txt              # Prompt files next to the config, like an expert's
├── skills/                      # Bundled skills, including the two hidden worker phase skills
│   ├── strategic-phase/SKILL.md
│   ├── tactical-phase/SKILL.md
│   └── direct-task/SKILL.md     # The instructions of a job without phases (see Phases)
├── prompts/                     # System, persona, and auxiliary prompt templates
│   ├── systemprompt.txt         # Worker (job) system prompt scaffold
│   ├── systemprompt_interactive.txt # Session system prompt scaffold
│   ├── systemprompt_subagent.txt    # Subagent (roster child) system prompt scaffold
│   ├── persona.txt              # Agent persona/identity prompt
│   ├── summarization_prompt.txt # Context compaction prompt
│   ├── systemprompt_minimax.txt # MiniMax M2.7-optimized system prompt
│   ├── persona_minimax.txt      # MiniMax M2.7-optimized persona
│   ├── summarization_prompt_minimax.txt  # MiniMax M2.7-optimized summarization
│   ├── systemprompt_minimax_m3.txt        # MiniMax M3 system prompt (1M ctx, multimodal)
│   ├── persona_minimax_m3.txt             # MiniMax M3 persona
│   ├── summarization_prompt_minimax_m3.txt # MiniMax M3 summarization
│   ├── systemprompt_glm.txt                # GLM-5.2 worker system prompt
│   ├── systemprompt_interactive_glm.txt    # GLM-5.2 persistent-chat system prompt
│   ├── persona_glm.txt                     # GLM-5.2 persona
│   ├── systemprompt_glm_5_3.txt            # GLM-5.3 / Flash worker prompt
│   ├── systemprompt_interactive_glm_5_3.txt # GLM-5.3 / Flash chat prompt
│   ├── persona_glm_5_3.txt                 # GLM-5.3 / Flash persona
│   ├── systemprompt_muse_spark_1_3.txt     # Muse Spark 1.3 worker prompt
│   ├── systemprompt_interactive_muse_spark_1_3.txt # Muse Spark 1.3 chat prompt
│   ├── persona_muse_spark_1_3.txt          # Muse Spark 1.3 persona
│   ├── systemprompt_kimi_k3.txt            # Kimi K3 worker prompt
│   ├── systemprompt_interactive_kimi_k3.txt # Kimi K3 chat prompt
│   ├── persona_kimi_k3.txt                 # Kimi K3 persona
│   ├── systemprompt_gemini_4_argon.txt     # Gemini 4 Argon / 3.8 Flash worker prompt
│   ├── systemprompt_interactive_gemini_4_argon.txt # Gemini 4 Argon / 3.8 Flash chat prompt
│   └── persona_gemini_4_argon.txt          # Gemini 4 Argon / 3.8 Flash persona
└── templates/                   # Instruction templates (non-prompt files)
    ├── instructions.md                  # Default agent instructions
    ├── instructions_minimax.md          # MiniMax M2.7-optimized instructions
    ├── instructions_minimax_m3.md       # MiniMax M3 instructions (+ multimodal / long-context guidance)
    ├── strategic_todos_initial.yaml     # Initial todos for job start
    ├── strategic_todos_transition.yaml  # Todos for phase transitions
    ├── strategic_todos_resume.yaml      # Todos for job resume with feedback
    ├── live_tactical_todos.yaml         # First (tactical) phase of a live-session job
    ├── live_post_session_todos.yaml     # Strategic phase after a live session ended
    ├── direct_task_todos_initial.yaml   # The one to-do of a job without phases
    ├── direct_task_todos_resume.yaml    # Its to-do after a resume with feedback
    ├── direct_task_instructions.md      # Default instructions of a job without phases
    ├── workspace_template.md            # (deprecated; workspace.md removed — unused)
    └── phase_retrospective_template.md  # Template for phase retrospectives
```

## Roles, the shared root and the overlays

Every expert resolves on the same chain, most specific last:

```
expert_base  <-  overlays/<role>  <-  expert ($extends chain)  <-  model family (matrix)  <-  job / thread / roster override
```

- `expert_base.yaml` carries everything every role shares (llm, tools, limits,
  memory, auxiliary, browser, shell, ...). It is never loaded on its own by a
  runtime.
- `overlays/<role>.yaml` adds the role's own keys and the role's values of
  shared keys. The worker overlay owns the phase loop (`instruction_files`,
  `phase_settings`, `delegation`, `autonomy`, `verification`, `scholar`,
  `curator`, `communication`, the `core` tool group); the session overlay owns
  the canvas grant, the session-only application groups and the session memory
  writers; the subagent overlay is a read-only tool floor with memory and
  background tasks off.
- The overlays' **public names** are `worker_base`, `session_base` and
  `subagent_base`. They are what `$extends`, `--config`, `config_name` and the
  experts API use; `default`/`defaults`, `persistent_default`/`persistent_defaults`
  and the file spelling `overlays/<role>` are accepted aliases. A path such as
  `config/worker_base.yaml` still loads (it lands on the overlay).

**Role re-rooting.** A config is normally loaded on the root its own chain
names. When it is resolved *for* a role — a job resolves for `worker`, a
session for `session`, a roster entry for `subagent` — the loader
(`load_and_merge_config(path, role=...)`) replaces the link that ends the chain
with that role's overlay. So a session expert dispatched as a job gains the
worker keys underneath it, and a worker expert used in a session sits on the
session overlay; the expert's own behavioral values win. The orchestrator binds execution-owned
infrastructure separately after this private merge.

**Workspace ownership.** A Job or Session selects its workspace independently
of its Expert. Infrastructure fields (`workspace.backend` and `workspace.vm`)
in an SRW Expert's private configuration do not select or size that workspace.
Behavioral workspace settings, such as Git versioning, remain private settings.
Selection precedence is explicit execution choice, then the active Project's
workspace default, then account/role defaults (worker: sandbox, session: virtual).
An explicit `workspace: null` means no workspace.

The managed role defaults are defined in
`shared.runtime.core.workspace_selection.execution_workspace_config`.
`expert_base.yaml` contains harness behavior and does not select infrastructure.

Experts can publish `spec.workspacePreference: {backend: sandbox}`. The creation
forms show this recommendation and materialize it as an explicit execution choice
when no Project default or manual selection takes precedence. API callers must
choose to follow recommendations themselves. Listing shell tools never allocates
a machine: the SRW harness filters tools for the actual backend and retains the
existing authorized workspace-upgrade flow.

The existing Job and Session endpoints accept a top-level `workspace` using the
manifest binding shape, for example `{"template":{"ref":{"name":"build-env"}}}`.
References resolve in the selected Project/Account scope; use `ref.scope` when
selecting a template from another scope. Inline templates work too. Existing
`config_override.workspace` requests remain compatible, but cannot be combined
with a second top-level backend selection.
Selected template and Project revisions are captured at admission; source edits
do not change existing execution snapshots. Children keep their existing
workspace inheritance.

VM WorkspaceTemplates support prebuilt `environment.image` references and
`resources: {cpu: 12, memory: 24Gi, storage: 120Gi}`. CPU is a whole core count;
memory/storage use `Mi`, `Gi`, or `Ti`. The controller applies its rootdisk
minimum to storage requests. Pin images by digest for reproducible selection.
Same-cluster VM hosting can enable `environment.prepare`, digest-aware pull
policies and scoped `Reuse`/`Rebuild` caching. Each workspace clones the prepared
disk before its own initialization. See [workspace preparation](../examples/manifests/workspace-preparation.md)
for operator prerequisites, cache behavior and the prepared-template example.
Same-cluster VM templates support ordered, unprivileged `initialize` commands
before agent dispatch. Completed setup survives resume on the same persistent
rootdisk. See the [initialized VM example](../examples/manifests/srw-initialized-development-vm.yaml)
and its [runtime limits](../examples/manifests/README.md#execution-owned-workspace-selection).
Jobs can select `retention: Retain` for a same-cluster VM, then reuse its disk
through `workspace.instanceRef.uid` after the instance becomes `Detached`.
`GET /api/jobs/{id}` exposes `workspace_instance_id`; instance status and explicit
storage deletion use `/api/workspace-instances/{uid}`. Reuse is exclusive and
limited to the same Account or Project. Successful initialization persists across
Jobs, while each attachment gets a new VM and SSH host identity. Sessions keep
their existing suspend/resume behavior; retained-instance selection for Sessions
is rejected. See the [first assignment](../examples/manifests/srw-retained-development-vm.yaml)
and [reuse example](../examples/manifests/srw-retained-job.yaml).
Images must implement the SRW VM guest/SSH contract. See the
[development VM template](../examples/manifests/srw-development-vm.yaml) and
[selection examples](../examples/manifests/README.md#execution-owned-workspace-selection).
The selected VM image/resources are captured with execution policy, independently
of the typed harness settings, and reused for Job dispatch and Session resume.

Bundled Experts now declare advisory preferences. For stored SRW Experts, run
`scripts/migrate-workspace-preferences.py` to preview versioned changes, then
`--apply --plan-revision <returned-revision>` against the intended database.
The migration preserves execution history and active Project generations;
managed Experts require an explicit update of their owning Project. Generic
harness configuration remains opaque. See the
[workspace examples](../examples/manifests/srw-workspace-selection.yaml).

**Ignored keys.** A role overlay may declare `$ignore_keys`, a list of dotted
paths its role never reads. They are pruned from the merged config after every
merge, again after the job/thread override layers, and after a roster
override, so no later layer can re-introduce them. A key that does not apply
to a role is dropped silently — never an error. Today only the subagent
overlay declares any (`workspace.backend/remote/mounts/structure/instructions_template/initial_files/git_versioning`,
`autonomy`, `verification`, `scholar`, `curator`, `phase_settings`, `tools.core`,
`delegation`, `tools.delegation`, `communication`, `officer`, `headless`); the worker and session
overlays declare none, so a re-rooted expert keeps everything it authored.

Never read a base file directly — an overlay alone is only the role's residue.
Use `load_role_base(role)` (the merged `expert_base` + overlay) from
`src/shared/runtime/core/loader.py`.

## Creating a Custom Agent Config

For orchestrator-managed Experts, use the manifest structure at the top of this
guide. The file examples below configure a directly launched SRW harness and its
private assets.

### Option 1: Single File Config

Create a worker YAML file that extends the worker role base:

```yaml
# yaml-language-server: $schema=schema.json
$extends: worker_base

agent_id: my_agent
display_name: My Custom Agent
description: Does custom things

tools:
  research:
    - web_search
    - search_papers
  citation:
    - cite_web
```

Save as `config/my_agent.yaml` and run:

```bash
python -m agent --config my_agent
```

Persistent/session experts use `$extends: session_base` instead; a shared
"small expert" meant for rosters uses `$extends: subagent_base`. The legacy
names `default`, `defaults`, `persistent_default`, and `persistent_defaults`
remain accepted as compatibility aliases, but new configs should use the
explicit public root names. Whichever root an expert names, it can be used in
every role (see "Roles, the shared root and the overlays" above).

### Option 2: Directory Config (with prompt overrides)

For configs that need custom prompts or instructions, create a directory:

```
config/
└── my_agent/
    ├── config.yaml              # Expert overlay (extends a role base)
    ├── model_config_matrix.yaml # Expert-level matrix overrides (optional)
    ├── instructions.md          # Custom instructions (optional)
    └── skills/
        ├── strategic-phase/SKILL.md # Custom planning/review guidance (optional)
        └── tactical-phase/SKILL.md  # Custom execution guidance (optional)
```

### Model Config Matrix

Everything that depends on the model family lives in one file,
`config/model_config_matrix.yaml`. Its top-level keys are model families, and
`default` is the fall-through. The loader derives the family from the model ID
(`family_of()` in `src/shared/runtime/core/model_registry.py`); an
`openrouter/`, `groq/`, `codex/` or `openai/` prefix is ignored. A model ID it
does not recognize uses `default`.

Each family block can have these sections:

| Section | Holds |
|---------|-------|
| `prompts` | Prompt type → file in `config/prompts/`: `systemprompt`, `systemprompt_interactive`, `persona`, `summarization`, `memory_extraction`, `curation`, `knowledge_assembler`, `citation_verification` |
| `instructions` | Template type → file in `config/templates/`: `instructions`, `strategic_todos_initial`, `strategic_todos_transition`, `strategic_todos_resume`, `live_tactical_todos`, `live_post_session_todos`, `workspace_template` |
| `settings` | Inference parameters and model limits, e.g. `temperature`, `top_p`, `top_k`, `multimodal`, `parallel_tool_calls`, `model_max_context_tokens`, `max_output_tokens`, `image_tokens`, `structured_output_method`, `shell_mode`, `session_max_concurrent` |
| `reasoning` | How the family's reasoning is controlled: `method`, `default`, `options` |
| `guardrails` | `file:` a file in `config/guardrails/` with tool-call examples and nudges |

An excerpt:

```yaml
default:
  reasoning: { method: effort_enum, delivery: native, default: high, options: [low, medium, high, xhigh] }
  prompts:
    systemprompt: systemprompt.txt
    persona: persona.txt
    summarization: summarization_prompt.txt
  instructions:
    instructions: instructions.md
  guardrails:
    file: default.yaml
  settings:
    temperature: 0.0
    multimodal: false
    parallel_tool_calls: false
    model_max_context_tokens: 128000
    image_tokens: { mode: flat, flat: 1600 }

minimax-m3:           # a family names only what differs from `default`
  prompts:
    systemprompt: systemprompt_minimax_m3.txt
    persona: persona_minimax_m3.txt
  instructions:
    instructions: instructions_minimax_m3.md
  guardrails:
    file: minimax_m3.yaml
  settings:
    temperature: 1.0
    top_p: 0.95
    multimodal: true
    model_max_context_tokens: 1000000
```

An expert directory may hold its own `model_config_matrix.yaml` with only the
entries that differ from the base file (see `config/experts/developer/`).

How each section resolves:

- **`prompts` and `instructions`** pick a filename from the first match of:
  1. expert matrix → model family
  2. expert matrix → `default`
  3. base matrix → model family
  4. base matrix → `default`

  The loader then looks for the file in the expert directory before the
  framework directory (`config/prompts/` or `config/templates/`). In each
  directory it tries the family's filename first, then the plain one
  (for example `persona_minimax_m3.txt`, then `persona.txt`).
- **`settings`**: the expert matrix is deep-merged over the base matrix, then
  the family block over `default`. Most keys are written to `llm`, but an `llm`
  key that the expert or a layer above it (project, user or job/thread
  override) sets itself wins over the family value. A few keys go elsewhere:
  `image_tokens` and `pdf_render_dpi` to `limits`, `shell_mode` to
  `shell.mode` (only when no config sets it), and `session_max_concurrent` to
  the `delegation` block.
- **`guardrails`**: the `default` file, with the family's file merged on top.
  An expert matrix can name its own file per family; it is read from the
  expert's `guardrails/` directory and merged over the base file.
- **`reasoning`** comes from the base matrix only: the family's block, else
  `default`'s.

Context limits are derived from the model's window; see
[Context Management](#context-management).

When a deployment enables database overrides (`agent.configDbOverridesEnabled`
in the Helm values, `CONFIG_DB_OVERRIDES_ENABLED` for the agent), entries an
administrator stores through `/api/admin/config/overrides` win over these files
for prompts, instructions, settings and guardrails: the family's entry first,
then a global one.

### Resolved Config JSONB

For orchestrated work, the orchestrator records resolved SRW settings, prompt and
instruction content, and admitted policy in the canonical execution snapshot
before delivery. Jobs freeze at admission; Sessions retain versioned configuration
generations. Delivery reads that snapshot, rechecks current authority, and supplies
transient workspace and connector bindings. It does not resolve edited Expert
files again. The authoritative records are `srw_execution_specs` and their revision
history. Older `resolved_config` columns remain explicit compatibility and
historical inputs; the migration never reconstructs missing past settings from
today's defaults.

## Configuration Reference

### Required Fields

| Field | Type | Description |
|-------|------|-------------|
| `agent_id` | string | Unique identifier (lowercase, underscores allowed) |
| `display_name` | string | Human-readable name |

### Autonomy Level

Controls when the agent pauses for human review at phase boundaries and after job completion.

```yaml
autonomy: review  # full | review | partial | guided | dependent
```

The worker overlay sets `review`, so a job that does not choose a level runs
under `review`. Autonomy applies to jobs only; sessions and subagents ignore it.

| Level | After 1st Strategic | After Nth Strategic | After Tactical | After job_complete |
|-------|:---:|:---:|:---:|:---:|
| `full` | - | - | - | auto-complete |
| `review` | - | - | - | freeze |
| `partial` | freeze | - | - | freeze |
| `guided` | freeze | freeze | - | freeze |
| `dependent` | freeze | freeze | freeze | freeze |

- **`full`** — Fully autonomous. Never freezes. On `job_complete`, the agent writes `job_completion.json` directly and the orchestrator marks the job `completed` (or starts the critic when `verification.enabled`).
- **`review`** (default) — Runs freely through all phases but freezes after `job_complete` for human review before marking as completed.
- **`partial`** — Freezes after the first strategic phase boundary for early feedback, then runs freely. Freezes again at `job_complete`.
- **`guided`** — Freezes at every strategic phase boundary for review. Tactical phases run freely. Freezes at `job_complete`.
- **`dependent`** — Freezes at every phase boundary (strategic and tactical). Maximum human oversight.

When frozen at a phase boundary (`freeze_type: "phase_boundary"`), approving the job sets its status back to `processing` and the agent continues. When frozen at job completion (`freeze_type: "job_complete"`), approving writes `job_completion.json` and sets status to `completed`.

### Phases

A job either plans in phases or it does not. **Without phases**, the default,
the job gets a kickoff prompt, works with its tools and ends with
`job_complete`: no plan, no todos to stage, no review phase. **With phases**
it follows the planned procedure: plan, work through todos, review. That
procedure is for long or open-ended work that outlives several context
compactions, where the plan, the todos and the notes in the workspace carry
the job across them.

```yaml
phases: true  # true | false; absent means false
```

The key sits at the top level of an expert's config, next to `autonomy`, and
applies to jobs only. An expert that is meant for long work sets it. A job
can override its expert: the `phases` field of `POST /api/jobs` and of the
`create_job` tools, `spec.phases` in a Job manifest, or `phases` in an
automation's `config_override`. A project default sits between the two. The
value is decided when the job is created and frozen with the rest of its
configuration. Two kinds of job always plan in phases, whatever the expert
says: a verification critic and a job that starts inside a live session.

Without phases the framework replaces, for any expert:

- the to-do templates (`strategic_todos_*.yaml`, including an expert's own)
  with one placeholder to-do (`templates/direct_task_todos_initial.yaml`, and
  `templates/direct_task_todos_resume.yaml` after feedback). The job has no
  tool to tick it; `job_complete` closes it;
- the four procedure skills (`strategic-phase`, `tactical-phase`,
  `todo-guide`, `verify-before-done`) with the `direct-task` skill;
- the five phase tools (`next_phase_todos`, `todo_complete`, `todo_list`,
  `request_replan`, `mark_complete`); `job_complete` ends the job;
- the framework's default instructions (`templates/instructions.md`) with the
  short `templates/direct_task_instructions.md`.

It keeps the expert's persona, its own `instructions.md`, its tools,
connectors and model, and the job's autonomy level. A job without phases
stops once, at its end, so `partial`, `guided` and `dependent` act like
`review`.

An expert can bring its own method for jobs without phases. Put these files in
its folder:

- `direct_task_todos_initial.yaml` and `direct_task_todos_resume.yaml`, with
  exactly one to-do each. A file with any other number of to-dos is ignored
  and the framework's file is used.
- `skills/direct-task/SKILL.md`, in place of the framework's `direct-task`
  skill.

An expert whose persona, instructions or own phase skills describe a phased
method sets `phases: true`. Without it, a job on that expert would be asked for
steps it has no tools for, and the method in its phase skills would not reach
it. The bundled developer, scholar, curator, designer, bughunter, product-qa,
critic and receptionist do this.

### LLM Configuration

```yaml
llm:
  model: openai/gpt-oss-120b  # The one model every phase and session turn runs on
  temperature: 0.0            # Optional: omit to use the model family's value
  reasoning_level: high       # Levels depend on the model family
  base_url: null              # Custom API endpoint
  timeout: 600                # Seconds
  max_retries: 3              # Provider-SDK retries (the worker overlay sets 0)
  summarization:              # Optional: another model for context compaction
    model: your-summary-model
```

An expert has one `llm.model`. The legacy per-phase tiers `llm.strategic`,
`llm.tactical` and `llm.subagent` still load, with a deprecation warning:
`strategic` (else `tactical`) becomes `llm.model` when the layer sets no model
of its own, and `subagent` moves to `subagents.llm`.

Inference parameters you leave out come from the model family (see
[Model Config Matrix](#model-config-matrix)). A `reasoning_level` the family
does not offer is clamped to the nearest level it does, preferring lower ones.

### Workspace Configuration

```yaml
workspace:
  structure:
    - archive/
    - output/
    - reference/
  max_read_words: 25000
```

### Tool Categories

`tools` has one key per tool category. Its value is a **policy** for that
category, not a list of members: the tool catalog (`src/shared/tool_catalog/`,
re-exported as `TOOL_REGISTRY` by `src/agent/tools/registry.py`) decides which
tools a category holds.

| Value | Meaning |
|-------|---------|
| `true` | Every tool in the category that config may grant (see "Grants" below). Follows the catalog, so a tool added later is included. |
| `false` | No tools. |
| `{only: [a, b]}` | Exactly these tools. |
| `{except: [a]}` | `true` minus these tools. Follows the catalog. |
| `[a, b]` | Legacy spelling of `{only: [a, b]}`. |
| `[]` | Legacy spelling of `false`. |

```yaml
tools:
  workspace: true                             # every file tool
  research: {except: [crawl_website, map_website]}
  citation: {only: [cite_web, list_sources]}
  shell: [run_command, shell_read]            # shell must list its tools
  browser_direct: false
```

- A layer's value replaces the inherited one; lists are not merged. A child
  `{except: [...]}` starts again from the whole category, even when its parent
  used `only`.
- `shell` and `delegation` must list their tools: only a list,
  `{only: [...]}` or `false` is accepted. `true` and `except` would pick up a
  tool added to the category later without review.
- `shell.mode` selects the shell flavour: `stateless` (the fallback) binds
  `run_command`, `persistent` binds `shell_execute`; each name is mapped to the
  other to match the mode. The model family can set the default mode
  (`settings.shell_mode` in the matrix); an explicit `shell.mode` wins.
- `git` is dropped when `shell` is not empty: the shell can run `git` against
  any repository in the workspace (`ToolsConfig.__post_init__` in
  `src/shared/runtime/core/loader.py`).
- A workspace without a shell drops `shell`, `browser_direct` and `git`; one
  without file tools drops `workspace`, `skills` and `canvas`.
- `orchestrator: true` also grants the `job_control` and `job_inspection`
  tools that `true` would grant there; it predates the split into three groups.
- Every name must exist in the catalog. `tests/test_config_tool_names_are_registered.py`
  checks the bundled configs.

| Category | Purpose | Tools |
|----------|---------|-------|
| `workspace` | Workspace file operations | `read_file`, `write_file`, `edit_file`, `list_files`, `delete_file`, `search_files`, `file_exists`, `move_file`, `rename_file`, `copy_file`, `get_document_info`, `create_directory`, `delete_directory` |
| `skills` | Load a skill (see [Skills](#skills)) | `use_skill` |
| `core` | Worker todos and job completion (the phase loop) | `next_phase_todos`, `todo_complete`, `todo_list`, `request_replan`, `mark_complete`, `job_complete`, `request_workspace_upgrade`, `sleep`, `notify_user` |
| `session_task` | A session's lightweight todo list | `task_add`, `task_complete`, `task_list` |
| `research` | Web search, page extraction, papers | `web_search`, `extract_webpage`, `crawl_website`, `map_website`, `search_papers`, `download_paper`, `get_paper_info`, `research_topic` |
| `browser_direct` | Step-by-step control of a browser on the workspace | `browser_navigate`, `browser_snapshot`, `browser_click`, `browser_type`, `browser_select`, `browser_scroll`, `browser_screenshot`, `browser_back`, `browser_close` |
| `citation` | Citations and the source library | `cite_document`, `cite_web`, `list_sources`, `get_citation`, `list_citations`, `edit_citation`, `withdraw_citation`, `annotate_source`, `get_annotations`, `tag_source`, `search_library`, `generate_bibliography` |
| `knowledge` | Project knowledge base: notes and search | `kb_write`, `kb_update`, `kb_delete`, `kb_read`, `kb_list`, `kb_search`, `kb_grep`, `kb_related`, `kb_contradictions`, `kb_provenance`, `kb_unanswered`, `kb_export`, `kb_lint`, `kb_index` |
| `memory` | Search project memory (bound only while `memory.enabled`, `memory.manager.enabled` and its pipeline extension are on) | `memory_search` |
| `git` | Read git history of the job's repository and attached repositories | `git_log`, `git_show`, `git_diff`, `git_status`, `git_tags` |
| `shell` | Run commands on the workspace | `run_command`, `cancel_command`, `shell_execute`, `shell_read`, `srw_cloud_status` |
| `evaluation` | Critic verdicts: approve a job or return it with feedback | `approve_job_verdict`, `return_job_with_feedback` |
| `communication` | Message people by email | `send_message` |
| `delegation` | Built-in subagents (see [Delegation](#delegation)) | `delegate_agent`, `wait_agent`, `message_agent`, `stop_agent`, `list_agents` |
| `canvas` | Dynamic Canvas presentation (sessions) | `get_canvas`, `set_canvas`, `clear_canvas` |
| `orchestrator` | Projects, repositories and session context | `get_session_context`, `get_current_project`, `list_project_jobs`, `list_project_repositories`, `get_default_project_repository`, `checkout_project_repository` |
| `job_control` | Create, steer and review jobs | `accept_job_diff`, `acknowledge_job_message`, `approve_job`, `assign_job`, `cancel_job`, `create_job`, `delete_job`, `escalate_job_message`, `pause_job`, `promote_job`, `reject_job_diff`, `reply_to_job_message`, `resume_job_with_feedback`, `send_message_to_job`, `steer_job` |
| `job_inspection` | Read jobs, their files, logs and audit trail | `get_audit_bulk`, `get_audit_timerange`, `get_audit_trail`, `get_chat_bulk`, `get_chat_history`, `get_current_todos`, `get_frozen_job`, `get_job`, `get_job_completion_report`, `get_job_diff`, `get_job_file`, `get_job_log`, `get_job_progress`, `get_job_summary`, `get_llm_request`, `get_message_thread`, `get_shell_state`, `get_stuck_jobs`, `get_todo_archive`, `get_todos`, `get_workspace_overview`, `list_job_commits`, `list_job_evidence`, `list_job_files`, `list_jobs`, `list_llm_requests`, `list_message_threads`, `list_todo_archives`, `read_job_evidence`, `search_audit` |
| `agent_catalog` | Read experts and skills | `list_experts`, `get_expert`, `list_skills`, `search_skills`, `get_skill` |
| `catalog_authoring` | Create or update experts, skills and automations from bundles (needs the `catalog_authoring` capability grant) | `get_expert_bundle`, `set_expert_bundle`, `get_skill_bundle`, `set_skill_bundle`, `get_automation_bundle`, `set_automation_bundle` |
| `workflows` | Inspect automations and project loops; draft disabled automations | `list_automations`, `get_automation`, `list_automation_runs`, `propose_automation`, `get_project_loop`, `list_project_loop_jobs`, `explain_project_loop` |
| `loop` | Loop-campaign planning; set per job by a planner loop, not in bundled configs | `loop_plan` |
| `product_help` | Product guide and capability readers for sessions | `read_product_guide`, `get_product_capabilities` |
| `graph` | Neo4j datasource | `cypher_query`, `cypher_execute`, `get_database_schema` |
| `sql` | PostgreSQL datasource | `sql_query`, `sql_schema`, `sql_execute` |
| `mongodb` | MongoDB datasource | `mongo_query`, `mongo_aggregate`, `mongo_schema`, `mongo_insert`, `mongo_update` |
| `webdav` | WebDAV / cloud-storage datasource | `webdav_list`, `webdav_read`, `webdav_info`, `webdav_write`, `webdav_delete` |
| `repo` | Write to an attached repository datasource | `repo_checkout`, `repo_commit`, `repo_push`, `repo_pull`, `repo_open_pr`, `repo_pr_status` |
| `email` | IMAP/SMTP mailbox datasource | `email_list_folders`, `email_list`, `email_search`, `email_read`, `email_move`, `email_flag`, `email_draft`, `email_send` |
| `mcp` | Tools of attached MCP datasources, discovered at runtime. `true` takes all of them; `except` is not accepted. | — |

**Phases.** In a worker's phase loop some tools work in one phase only, and a
call in the other phase is refused. Sessions and subagents have no phases.

- Strategic only: `next_phase_todos`, `job_complete`, `approve_job_verdict`,
  `return_job_with_feedback`.
- Tactical only: `request_replan`; every `research`, `browser_direct`,
  `graph`, `sql` and `mongodb` tool; `webdav_write`, `webdav_delete`,
  `email_move`, `email_flag`, `email_draft`, `email_send`.

`phase_settings.strategic_tools` lets an expert call named tactical-only tools
in its strategic phase as well.

**Grants.** Two kinds of tools are not reached by `true` or `except`:

- *Explicit grant*: config grants these only by naming them. That is every
  `delegation` tool (which also needs `delegation.enabled`); in `job_control`
  every tool except `approve_job`, `cancel_job`, `create_job`, `pause_job` and
  `resume_job_with_feedback`; and in `job_inspection` every tool except
  `get_job`, `get_job_completion_report`, `get_job_file`, `list_job_evidence`,
  `list_job_files`, `list_jobs` and `read_job_evidence`.
- *Granted by the runtime*: code binds these, not config, so leave the key
  out. `graph`, `sql`, `mongodb`, `webdav`, `repo` and `email` are bound when
  a datasource of that type is attached (see
  [Multi-Stage Config Pipeline](#multi-stage-config-pipeline-database-tools)).
  `session_task` and `product_help` are bound in sessions. The single tools
  `request_workspace_upgrade`, `sleep`, `notify_user`,
  `checkout_project_repository` and `srw_cloud_status` each have their own
  runtime switch.

See `expert_base.yaml` (the shared groups) and `overlays/worker.yaml` /
`overlays/session.yaml` (the role-owned groups) for the conservative inherited
tool surfaces. Privileged and orchestration-oriented groups such as shell,
delegation, automations, and loops are opt-in at the expert layer.

### Research & Browser Configuration

```yaml
research:
  proxy:
    enabled: false       # Enable proxy for paywalled content
    type: socks5         # "http", "socks5", or "none"
    host: localhost       # Proxy host (e.g., SSH tunnel)
    port: 1080            # Proxy port

browser:
  snapshot:
    include_screenshot: auto  # "auto" (if model is multimodal) | true | false
    max_dom_chars: 40000      # Truncate DOM text beyond this
  security:
    allowed_domains: []       # Empty = allow all domains
    blocked_domains: []
    blocked_schemes: ["file", "javascript", "data"]
```

The browser itself runs on the workspace (`browser-exec` daemon) — the agent
pod never executes Chromium. See the public
[workspace architecture](../docs/architecture.md#workspace-tiers).

Proxy can also be set via environment variables: `RESEARCH_PROXY_TYPE`, `RESEARCH_PROXY_HOST`, `RESEARCH_PROXY_PORT`, `RESEARCH_PROXY_USER`, `RESEARCH_PROXY_PASS`.

### Database Connections

```yaml
connections:
  postgres: true
```

External datasources (Neo4j, MongoDB, and additional PostgreSQL instances) are
managed through the datasource connector system and resolved at dispatch.

### Multi-Stage Config Pipeline (Database Tools)

Datasource tool categories (`graph`, `sql`, `mongodb`, `webdav`, `repo`, `email`) are **not** controlled by the agent config YAML. Instead, they go through a multi-stage pipeline:

```
1. Agent config (config/*.yaml)        → User defines base tools (workspace, research, etc.)
2. Orchestrator datasource override    → System injects/strips database tools based on attached datasources
3. Final resolved config               → What the agent actually receives
```

- If a datasource is attached to the job, the orchestrator **injects** the corresponding tool category (even if the config doesn't list it).
- If no datasource of a type is attached, the orchestrator **strips** the category (even if the config lists it).
- The `read_only` flag on the datasource controls whether write tools are included.

This means the agent config controls non-database tools, while the orchestrator controls database tools based on what's actually connected. See `build_datasource_tool_override()` in `src/orchestrator/services/agent_datasource_payload.py`.

### Skills

`use_skill` is the only tool in the `skills` group, which every expert inherits
from `expert_base.yaml`. The skill list in the system prompt follows the tool:

- With `use_skill` bound, the agent sees the skill list (bundled skills plus the
  user's, global and project skills) and the skill files are copied to
  `skills/<name>/` in the workspace. A model-family prompt template without an
  `{available_skills}` placeholder gets the list appended after its body.
- With `skills: []` (or `false`), the agent sees no list and gets no skill files.
- An `instruction_files` entry with `skill:` and a `before_tool:` trigger makes
  the agent read that skill before the tool runs, so it brings `use_skill` back
  even when `skills: []`. `phase_start:` bindings do not: their text arrives as
  a message. Every worker that keeps the worker overlay's `todo-guide` and
  `verify-before-done` gates therefore has `use_skill`.
- Workspace tiers without file tools drop the group, like `workspace`.

A `workspace` list that still names `use_skill` (written before the group
existed) is converted when the config is loaded.

### Context Management

```yaml
limits:
  message_count_threshold: 300
  # Derived from the model's context window, never set here:
  # model_max_context_tokens, context_threshold_tokens,
  # message_count_min_tokens
  # (summarization budgets are not config leaves — they are computed at call
  # time from the auxiliary model's window, see src/agent/core/summarizer.py)

context_management:
  compact_on_archive: true   # Set by the worker overlay
  keep_recent_messages: 10
```

The context limits follow one number, the model's context window: the model's
`context_window` in **Settings → Models → Catalog** when it has one, otherwise
the family's `settings.model_max_context_tokens` from the
[Model Config Matrix](#model-config-matrix). `limits.model_max_context_tokens`
is that window, and `context_threshold_tokens` and `message_count_min_tokens`
are 80% of it (`_apply_settings_matrix` in `src/shared/runtime/core/loader.py`).
To run a model below its full window, give it a smaller `context_window` in
the catalog.

### Verification

Auto-spawn a critic job after `job_complete` to review deliverables:

```yaml
verification:
  enabled: true          # Spawn critic job after job_complete
  critic_config: critic  # Which expert config to use for the reviewer
  max_rounds: 3          # Max feedback round-trips before auto-accepting
```

### Delegation

`delegation` lets an expert hand work to the children in its
[`subagents`](#subagents) roster. One tool starts a child: `delegate_agent`.
Four more manage children that run in the background: `wait_agent`,
`message_agent`, `stop_agent` and `list_agents`. Each tool is bound only when
the expert sets `delegation.enabled: true` **and** names that tool in
`tools.delegation`. A child never receives these tools, so delegation is one
level deep.

```yaml
delegation:
  enabled: true
  max_concurrent: 4                 # Worker parent: children running at once; more calls queue
  run_in_background_default: false  # Used when a delegate_agent call omits run_in_background

tools:
  delegation: [delegate_agent, wait_agent, message_agent, stop_agent, list_agents]
```

| Key | Default | Meaning |
|-----|---------|---------|
| `enabled` | `false` | Opt-in. Without it no delegation tool is bound. |
| `max_concurrent` | `4` | A worker parent's cap on children running at once. Further calls queue. |
| `run_in_background_default` | `false` | `false`: the parent waits and gets the child's report as the tool result. `true`: the parent gets a receipt, and the child's completion is pushed to it later. |
| `session_max_concurrent` | family value, else `6` | A session parent's cap on children running at once (1–20). The family value is `settings.session_max_concurrent` in the matrix. |
| `session_max_calls_per_turn` | `20` | `delegate_agent` calls one session turn may make. Further calls get an error result. |

Whether a session may run children in parallel at all is a deployment setting
(`orchestrator.sessionSubagentFanoutLanes` in the Helm values), not expert
configuration. Stored layers that still use the old tool names
(`spawn_subagent`, `delegate_work`) or the old keys (`max_depth`,
`default_timeout`, `max_timeout`, `allowed_configs`, `mode`, `light`) keep
loading: the names map to `delegate_agent`, the keys are dropped, and the
loader logs a deprecation warning.

### Subagents

`subagents` is the expert's roster: the children `delegate_agent` can start.

```yaml
subagents:
  default: explorer                      # Used when a delegate_agent call names no subagent_type
  llm: {model: inherit}                  # Roster-wide model; inherit = the parent's model
  roster:
    explorer: {$ref: subagents/explorer} # Library entry from config/subagents/
    reviewer:                            # A full expert as a child, with one key changed
      $ref: critic
      llm: {model: claude-haiku-4-5}
    summarizer:                          # Inline entry: any expert key
      description: Reads the files named in the brief and returns a short summary.
      tools: {workspace: {only: [read_file, list_files, search_files]}}
      write_policy: none
      return: summary
```

- `roster` maps a name to an entry. The name is the `subagent_type` the
  parent passes to `delegate_agent`; `description` is what the parent sees for
  it.
- An entry is inline, or a `$ref` whose sibling keys are merged over the
  referenced expert. `$ref` takes a bundled expert (`critic` or
  `experts/critic`), a library entry (`explorer` or `subagents/explorer`) or a
  DB expert ID, never a path. A bare name tries `config/experts/` first, then
  `config/subagents/`.
- Every entry resolves on the subagent role:
  `expert_base` ← `overlays/subagent` ← the entry. An inline entry without
  `tools` gets that overlay's read-only tool floor; a `$ref` keeps the
  referenced expert's tools, minus `delegation`. Keys a child does not use
  (`autonomy`, `phase_settings`, `tools.core`, `delegation`, ...) are dropped.
- Model: the entry's own `llm` wins over the roster-wide `subagents.llm`,
  which wins over the `$ref` target's. `model: inherit` (the subagent
  overlay's default) means the parent's model and transport. Each entry gets
  the matrix settings of its own model family.
- Depth is 1: a referenced expert's own `subagents` block is dropped.
- A job or thread override can change one entry, for example
  `{subagents: {roster: {explorer: {llm: {model: ...}}}}}`.
- Runtime keys per entry: `isolation` (`shared` | `worktree`), `write_policy`
  (`none` | `scratch_only` | `owned_paths` | `full`; omitted means `full`, so
  set it), `return` (`summary` | `structured` | `evidence` | `diff`), and child
  budgets under `limits` (`max_turns`, `max_tokens`, `return_budget_tokens`,
  `stale_idle_s`, `stale_in_tool_s`).

The roster does nothing until [delegation](#delegation) is on. The resolution
chain, failure policy and how to add a library entry are in
[config/subagents/README.md](subagents/README.md).

### Memory Light

Recall system backed by PostgreSQL and pgvector hybrid search. It stores
and retrieves project-scoped insights across context compactions; see
[state, knowledge, and recovery](../docs/architecture.md#state-knowledge-and-recovery).
`expert_base.yaml` turns it on; set `memory.enabled: false` to turn it off.

```yaml
memory:
  enabled: true
  budget_tokens: 10000
  max_memories_per_injection: 25
  observer_interval: 5
  # No embedding model key: the orchestrator passes the Embedding default from
  # Settings → Models to the agent as EMBEDDING_* variables.
```

## Inheritance

Configs use `$extends: worker_base`, `$extends: session_base` or
`$extends: subagent_base` to inherit a role base (`expert_base` + that role's
overlay), or `$extends: <expert>` to build on another expert's chain. Deep
merge applies at every link:
- Objects (dicts): Recursively merged
- Arrays (lists): Override replaces entirely
- Scalars: Override replaces
- `null` value: Clears the key from result

Example clearing an inherited array:

```yaml
$extends: worker_base

tools:
  research: null  # Clears all research tools
```

`null` clears a key for *that* merge only — a later layer (a job override)
re-adds it. Keys a role must never see are declared with `$ignore_keys` on the
role overlay instead (see above); they are pruned after every layer.

## Z.ai GLM-5.3 models

The `glm-5.3` family covers the text-only flagship; `glm-5.3-flash` also enables
image input. Both use the GLM-5.3 worker/chat/persona prompts, temperature `1.0`,
top-p `0.95`, reasoning `max` (options: `low`, `high`, `max`), a 1M-token context,
and a 131,072-token output budget. Reasoning cannot be disabled. Older models
such as GLM-5.2 retain the `glm` family.

In **Settings → Models → Catalog**, add `z-ai/glm-5.3-flash` using your OpenRouter provider;
`openrouter/z-ai/glm-5.3-flash` is also recognized. The family is detected
automatically. For Z.ai's OpenAI-compatible endpoint, use the bare model ID
`glm-5.3-flash`. Explicit model/expert/user settings override the family defaults.

Sampling and reasoning follow [Z.ai's documented settings](https://docs.z.ai/guides/vlm/glm-5.3-flash).
Context limits can vary by OpenRouter provider; an explicit catalog context/output
limit overrides the family values. Adding a family does not create a catalog row.

## Meta Muse Spark 1.3

The `muse-spark-1.3` family supplies worker/chat/persona prompts, image input,
a 1,048,576-token context window, and a 131,072-token response budget including
reasoning. The response budget follows SRW's existing runtime ceiling;
[OpenRouter advertises a larger provider limit](https://openrouter.ai/api/v1/models/meta/muse-spark-1.3/endpoints).

Reasoning defaults to `medium`, with `minimal`, `low`, `medium`, `high`, `xhigh`,
and `max` available. Reasoning is required, as declared in the
[OpenRouter model catalog](https://openrouter.ai/api/v1/models).
Temperature `1.0` and top-p `1.0` follow the conventional defaults listed in
[OpenRouter's parameter guide](https://openrouter.ai/docs/api/reference/parameters),
not a verified Meta-specific recommendation. Explicit settings override them.

In **Settings → Models → Catalog**, add `meta/muse-spark-1.3` using your OpenRouter provider.
The family is detected automatically, including with an `openrouter/` prefix.
The separately selected `meta/muse-spark-1.3-contributor` also uses this family;
family detection preserves the selected model ID and tier. Adding the family
does not register a catalog row. Audio/video input transport is outside this
family configuration change.

## Moonshot Kimi K3

The `kimi-k3` family supplies worker/chat/persona prompts, image input, a
1,048,576-token context window, and a 131,072-token response budget including
reasoning (Moonshot's own `max_completion_tokens` default). Only K3 is matched:
K2.x rows keep the `default` family, since they take a `thinking` toggle instead
of an effort field and have a 256K window.

Reasoning is always on and cannot be disabled. `reasoning_effort` accepts `low`,
`high`, and `max`; the family defaults to `max`, the
[provider default](https://platform.kimi.ai/docs/guide/use-reasoning-effort).
Moonshot warns that changing the level mid-conversation invalidates prefix-cache
hits. The native API
[fixes the sampling parameters](https://platform.kimi.ai/docs/api/models-overview):
temperature 1.0 (the family's value), top_p 0.95 (left unset), and both penalties at 0.

Structured output uses `json_schema`. Do not switch the family to
`function_calling`: forcing a named tool returns HTTP 400 while thinking is on.
Moonshot also asks for each assistant message's `reasoning_content` to be sent
back on later turns. SRW's OpenAI-compatible transport does not do that, so the
K3 prompts ask the model for a one-line visible intent before each tool call.

In **Settings → Models → Catalog**, add `moonshotai/kimi-k3` with your OpenRouter provider, or
`kimi-k3` on an OpenAI-compatible endpoint at `https://api.moonshot.ai/v1`. The
native API accepts images only as base64 or `ms://` file IDs, not public URLs.
The family is detected automatically, including `openrouter/` and `moonshotai/`
prefixes. Adding the family does not register a catalog row.

## Qwen3.8-27B

The `qwen3.8-27b` family is settings-only (default prompts): image and video
input, a 262,144-token native context, and the model card's thinking-mode
sampling (temperature 1.0, top_p 0.95, top_k 20). Only the 27B is matched.
Qwen3.8 Max has its own family (below), Qwen3.8 Flash stays on `default`, and
`qwen3-8b` is the older Qwen3 8B.

Reasoning effort is `low`, `medium`, or `xhigh` (default, Qwen's recommendation
for multi-turn agent work). The official chat template raises an error on any
other value, so a requested `high` is sent as `medium` and `max` as `xhigh`. The
template also rejects a system message anywhere but first. The family therefore
sets `single_system_message: true`, which folds each request to one leading
system message before it is sent (later system messages become user messages).
Families without the setting are unchanged.

Self-host with vLLM using `--reasoning-parser qwen3 --enable-auto-tool-choice
--tool-call-parser qwen3_xml` ([recipe](https://recipes.vllm.ai/Qwen/Qwen3.8-27B)),
then add the served model in **Settings → Models → Catalog** on an OpenAI-compatible endpoint.
Via OpenRouter, add `qwen/qwen3.8-27b`. The family is detected automatically;
pick it by hand if the served model name does not contain `qwen3.8-27b`.

## Qwen3.8 Max

The `qwen3.8-max` family covers Qwen3.8 Max and Max Prime (the same model on a
higher-throughput SKU). It is settings-only: image and video input, a 1,000,000
token window, a 131,072-token response cap, and temperature 1.0, top_p 0.95,
top_k 20. The open-weight Qwen3.8-2.4T-A95B is not matched: it is text-only,
and its chat template raises an error on images.

Reasoning effort is `low`, `medium`, or `xhigh` (default). The
[API](https://www.alibabacloud.com/help/en/model-studio/deep-thinking) maps
`high` and `max` to `xhigh`, so the family offers only the levels that differ.
Like the 27B, the family sets `single_system_message: true`: Alibaba documents
no rule for later system messages, and the open variant's template rejects them.
Structured output stays on `json_schema`, because thinking mode cannot force a
tool call.

In **Settings → Models → Catalog**, add `qwen3.8-max` on an OpenAI-compatible endpoint at
Alibaba Model Studio, or `qwen/qwen3.8-max-0902` / `qwen/qwen3.8-max-prime` with
your OpenRouter provider. Expect long outputs: Artificial Analysis measured
about twice the median token use.

## xAI Grok 4.7

The `grok-4.7` family is settings-only: image input, a 500,000-token window,
and temperature 0.7 / top_p 0.95. Reasoning is always on; effort is `low`,
`medium`, `high` (default), or `xhigh`
([xAI docs](https://docs.x.ai/developers/model-capabilities/text/reasoning.md)).
Only 4.7 is matched; older 4.x rows differ in effort ladder and window and keep
`default`. The xAI API rejects `stop`, `presence_penalty`, and
`frequency_penalty` on reasoning models, and SRW sends none of them.

xAI bills every token in a request at double rate once the prompt reaches
200,000 tokens. With the full window, compaction starts at 400,000, so long jobs
pay double for most late turns. To stay under that tier, set the model's
`context_window` to 250,000 in **Settings → Models → Catalog**.

Add `grok-4.7` on an OpenAI-compatible endpoint at `https://api.x.ai/v1`,
`x-ai/grok-4.7` with your OpenRouter provider, or connect the Grok Build
subscription.

## Claude Sonnet 5 / 5.5

The `claude-sonnet-5` family covers Sonnet 5 and 5.5. Both accept all five
effort levels (`low` to `max`, default `high`), while 4.x Sonnet keeps the
`claude-sonnet` family and `low`/`medium`/`high`. The other settings match the
Opus 5 families: 1M context, image input at the high-resolution cap, parallel
tool calls, and temperature 1.0.

Sonnet 5.5 returns HTTP 400 for forced tool use, for disabling thinking, and for
any non-default sampling value. SRW sends none of these: structured output uses
`json_schema`, and 1.0 is Anthropic's default temperature. Do not switch the
family to `function_calling`.

## Gemini 4 Argon / Gemini 3.8 Flash

The `gemini-4-argon` family covers Argon, including its allowlist-only
`-cyber-permissive` variant. The `gemini-3.8-flash` family covers 3.8 Flash.
Flash-Lite, TTS, image and Live ids are not matched, and neither are older
Gemini rows. Those keep the generic `gemini` family. Both families use
the same worker/chat/persona prompts. They follow the structure of Google's
Gemini CLI system prompt, which Google sends to every Gemini 3.x model. Both
families also have image input, parallel tool calls, a 1,048,576-token window
and a 65,536-token response budget that includes thinking. Argon's window is
shared by input and output.

Thinking is always on. Effort is `low`, `medium` (default, as at Google), or
`high`. `minimal` and `thinking_budget` return HTTP 400 on Argon. SRW clamps
`minimal` and "off" to `low` and never sends a budget. The native Google
provider sends the level as `thinking_level`. OpenRouter and OpenAI-compatible
endpoints send their own effort field. Both models ignore temperature, top_p and
top_k. The families leave top_p and top_k unset and keep temperature at 1.0,
because the factories always send one.

Long decode (Argon output past 65,536 tokens through continuation tokens) is
not wired. A non-streaming direct HTTP request stops at 32,768 output tokens.
Google asks for thought signatures to be sent back on function-calling turns.
The native client keeps them within a live turn. A resumed history without
them can be rejected, as with older Gemini 3 models.

In **Settings → Models → Catalog**, add `gemini-4-argon` or `gemini-3.8-flash` with a Google
provider, or `google/gemini-4-argon` / `google/gemini-3.8-flash` with your
OpenRouter provider. The family is detected automatically, including
`openrouter/`, `google/` and `models/` prefixes. Adding the family does not
create a catalog row. Sources: the
[Gemini models list](https://ai.google.dev/gemini-api/docs/models), the
[Argon guide](https://ai.google.dev/gemini-api/docs/generate-content/latest-model),
and the [3.8 Flash model page](https://ai.google.dev/gemini-api/docs/models/gemini-3.8-flash).

## DeepSeek V4

The `deepseek` family covers DeepSeek V4 Pro and Flash, on DeepSeek's own API
(`https://api.deepseek.com/v1`, an OpenAI-compatible endpoint) or through
OpenRouter. Thinking is on by default; the reasoning arrives beside the answer
as `reasoning_content` and is kept on the assistant message.

The family sets `replay_reasoning_content: true`: every later request sends each
assistant turn's `reasoning_content` back, unchanged. DeepSeek's
[thinking-mode guide](https://api-docs.deepseek.com/guides/thinking_mode) asks
for this whenever the request carries tools, and documents a 400 without it.
Measured on the direct API on 2026-11-14, a request without it still returned
200, and the model kept its reasoning only until the next user message. With the
setting on, it keeps it across SRW's phase changes and reminders too, and the
request no longer depends on what the provider remembers. An assistant message
SRW wrote itself is sent with an empty `reasoning_content`.

The reasoning sent back counts as input tokens. A job or an expert turns it off
with `llm.replay_reasoning_content: false`. The setting can only be turned off
this way, not on: a family without it never gets the field. OpenRouter documents
`reasoning_content` as an alias of its own `reasoning` field on input. Other
families whose providers ask for the same (Kimi K3, Qwen with
`preserve_thinking`, GLM with `clear_thinking: false`, MiniMax) do not have the
setting yet; each needs one live check first.

The family also sets `send_max_tokens: true`: the output cap goes out as
`max_tokens`. The client library sends it as `max_completion_tokens`, and
DeepSeek's own API does not apply a cap under that name. Measured on 2026-11-16:
a request with `max_completion_tokens: 96` came back with 2,425 completion
tokens and a normal finish; with `max_tokens: 96` every reply was cut at 96
tokens, all of them reasoning. The cap counts reasoning tokens.
Without the setting, the family's `max_output_tokens` is not what limits a
reply; the provider's default is. Other OpenAI-compatible endpoints may need
the same; check one request with a low cap before trusting it.

## Schema Validation

Add the schema comment at the top of your YAML file for IDE autocompletion:

```yaml
# yaml-language-server: $schema=schema.json
```

This works with VS Code + Red Hat YAML extension.

## Running Agents

`python -m agent` always starts the agent's API server (port 8001 by default).
It takes jobs and sessions from the orchestrator; it has no flags to run a job
itself.

```bash
# Use the worker framework base directly (normally a named expert is selected)
python -m agent

# Use custom config: config/<name>/config.yaml, config/experts/<name>/,
# config/subagents/<name>/, then config/<name>.yaml
python -m agent --config my_agent

# Use explicit path
python -m agent --config /path/to/config.yaml

# Keep the process alive after each job or session (needed when you start it by hand)
python -m agent --config my_agent --port 8001 --loop

# Accept only jobs, or only interactive sessions (default: both)
python -m agent --mode worker --port 8001 --loop
python -m agent --mode persistent --port 8002 --loop
```
