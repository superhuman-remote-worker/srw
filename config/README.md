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
├── model_config_matrix.yaml     # Per model family: prompt/instruction filenames, inference params, context limits
├── README.md                    # This file
├── experts/                     # Bundled roles and application-default seed bundles
│   └── <expert>/
│       ├── config.yaml              # Expert manifest; private overlay under runtime.config.config
│       ├── model_config_matrix.yaml # Expert-level matrix override (optional)
│       └── skills/                  # Expert-local skill overrides (optional)
│           ├── strategic-phase/SKILL.md
│           └── tactical-phase/SKILL.md
├── subagents/                   # Subagent library — small experts a roster references by name (see subagents/README.md)
│   └── <name>/
│       ├── config.yaml              # Expert manifest; SRW private leaf extends expert_base
│       └── persona.txt              # Prompt files next to the config, like an expert's
├── skills/                      # Bundled skills, including the two hidden worker phase skills
│   ├── strategic-phase/SKILL.md
│   └── tactical-phase/SKILL.md
├── prompts/                     # System, persona, and auxiliary prompt templates
│   ├── systemprompt.txt         # Main system prompt
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
│   └── persona_muse_spark_1_3.txt          # Muse Spark 1.3 persona
└── templates/                   # Instruction templates (non-prompt files)
    ├── instructions.md                  # Default agent instructions
    ├── instructions_minimax.md          # MiniMax M2.7-optimized instructions
    ├── instructions_minimax_m3.md       # MiniMax M3 instructions (+ multimodal / long-context guidance)
    ├── strategic_todos_initial.yaml     # Initial todos for job start
    ├── strategic_todos_transition.yaml  # Todos for phase transitions
    ├── strategic_todos_resume.yaml      # Todos for job resume with feedback
    ├── workspace_template.md            # (deprecated; workspace.md removed — unused)
    ├── todo_guide.md                    # Todo crafting guide
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
`autonomy`, `verification`, `scholar`, `curator`, `phase_settings`,
`delegation`, `communication`, `officer`, `headless`); the worker and session
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
    ├── config.yaml              # Expert overlay (extends a mode base)
    ├── model_config_matrix.yaml # Expert-level matrix overrides (optional)
    ├── instructions.md          # Custom instructions (optional)
    └── skills/
        ├── strategic-phase/SKILL.md # Custom planning/review guidance (optional)
        └── tactical-phase/SKILL.md  # Custom execution guidance (optional)
```

### Two Matrix Systems

The agent uses two parallel matrix systems with the same 4-level fallback chain:

**Prompt Matrix** (`prompt_matrix.yaml`) — resolves system prompts:
- Entries include `systemprompt`, `persona`, and `summarization`
- File search: expert directory → `config/prompts/`

**Instruction Matrix** (`instruction_matrix.yaml`) — resolves non-prompt templates:
- Entries: `instructions`, `strategic_todos_initial`, `strategic_todos_transition`, `strategic_todos_resume`, `workspace_template`, `todo_guide`
- File search: expert directory → `config/templates/`

Both use the same resolution chain (4 levels):

1. Expert matrix → model-specific key → type
2. Expert matrix → `"default"` key → type
3. Base matrix → model-specific key → type
4. Base matrix → `"default"` key → type

Once the filename is resolved, the loader checks the expert directory first for the file, falling back to the framework directory (`config/prompts/` or `config/templates/`).

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
autonomy: partial  # full | review | partial | guided | dependent
```

| Level | After 1st Strategic | After Nth Strategic | After Tactical | After job_complete |
|-------|:---:|:---:|:---:|:---:|
| `full` | - | - | - | auto-complete |
| `review` | - | - | - | freeze |
| `partial` | freeze | - | - | freeze |
| `guided` | freeze | freeze | - | freeze |
| `dependent` | freeze | freeze | freeze | freeze |

- **`full`** — Fully autonomous. Never freezes. On `job_complete`, writes `job_completion.json` directly and sets DB status to `completed`.
- **`review`** — Runs freely through all phases but freezes after `job_complete` for human review before marking as completed.
- **`partial`** (default) — Freezes after the first strategic phase boundary for early feedback, then runs freely. Freezes again at `job_complete`.
- **`guided`** — Freezes at every strategic phase boundary for review. Tactical phases run freely. Freezes at `job_complete`.
- **`dependent`** — Freezes at every phase boundary (strategic and tactical). Maximum human oversight.

When frozen at a phase boundary (`freeze_type: "phase_boundary"`), approving the job sets its status back to `processing` and the agent continues. When frozen at job completion (`freeze_type: "job_complete"`), approving writes `job_completion.json` and sets status to `completed`.

### LLM Configuration

```yaml
llm:
  model: openai/gpt-oss-120b
  temperature: 0.0
  reasoning_level: high  # low, medium, high
  base_url: null         # Custom API endpoint
  timeout: 600           # Seconds
  max_retries: 3
```

### Workspace Configuration

```yaml
workspace:
  structure:
    - archive/
    - output/
    - tools/
  max_read_words: 25000
```

### Tool Categories

Tools are organized into categories. Each category maps to a module under `src/agent/tools/`:

```yaml
tools:
  # File operations (src/agent/tools/workspace/)
  workspace:
    - read_file
    - write_file
    - edit_file
    - list_files
    - delete_file
    - search_files
    - file_exists
    - move_file
    - rename_file
    - copy_file
    - get_workspace_summary
    - get_document_info
    - create_directory
    - delete_directory

  # Task management + completion (src/agent/tools/core/)
  core:
    - next_phase_todos      # Stage todos for next tactical phase
    - todo_complete          # Mark current todo done
    - todo_list              # List current todos
    - request_replan         # End the phase early and re-plan, keeping all work
    - mark_complete          # Signal phase/task completion
    - job_complete           # Signal final completion (strategic only)

  # Research: web, papers, browser, workflows (src/agent/tools/research/)
  research:
    - web_search             # Tavily web search
    - extract_webpage        # Extract content from a URL
    - crawl_website          # Crawl a website following links
    - map_website            # Map a website's link structure
    - search_papers          # Search arXiv or Semantic Scholar
    - download_paper         # Download PDF (arXiv → Unpaywall → browser fallback)
    - get_paper_info         # Paper metadata via Semantic Scholar
    - research_topic         # Multi-database literature search + download
    # NOTE: browse_website / download_from_website were removed from the
    # registry — the agent drives the browser itself via the browser_direct
    # group below. Names listed here must exist in TOOL_REGISTRY; an unknown
    # name fails the whole batch load (tests/test_config_tool_names_are_registered.py).

  # Citation management (src/agent/tools/citation/)
  citation:
    - cite_document
    - cite_web
    - list_sources
    - get_citation
    - list_citations
    - edit_citation
    - annotate_source
    - get_annotations
    - tag_source
    - search_library
    - generate_bibliography

  # Database tool categories (src/agent/tools/graph/, sql/, mongodb/)
  # These are injected/stripped automatically by the orchestrator based on
  # which datasources are attached to the job. Usually left empty in config.
  graph: []      # Neo4j: execute_cypher_query, get_database_schema
  sql: []        # PostgreSQL: sql_query, sql_schema, sql_execute
  mongodb: []    # MongoDB: mongo_query, mongo_aggregate, mongo_schema, mongo_insert, mongo_update

  # Shell command execution (src/agent/tools/shell/)
  # Mode controlled by shell.mode: "stateless" (default) or "persistent"
  shell:
    - run_command     # Execute commands, get output (stateless mode, default)
    - shell_read      # Read more output from scrollback
    # Alternative (persistent mode): shell_execute + shell_read

  # Evaluation tools for critic agents (src/agent/tools/evaluation/)
  # Enable in critic config for approve/return capabilities.
  evaluation: []

  # Version control (src/agent/tools/git/) — reads the job's own repo by default and
  # an attached repository datasource with repo="<clone-dir>".
  #
  # ONLY BOUND WHEN THE AGENT HAS NO SHELL TOOLS. If `shell` above is
  # non-empty, ToolsConfig.__post_init__ (src/shared/runtime/core/loader.py) drops this whole
  # group: a shell can run git against any repository in the workspace, and
  # granting both gives the agent two ways to ask one question — the weaker of
  # which silently answers about a different repo. Shell-having agents should
  # be told to run `git ...` (and `git -C repos/<name> ...`) instead.
  git:
    - git_log
    - git_show
    - git_diff
    - git_status
    - git_tags
```

Select which tools your agent needs. For example, a research-focused agent:

```yaml
tools:
  research:
    - web_search
    - search_papers
    - download_paper
    - research_topic
  citation:
    - cite_web
    - cite_document
```

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

Database tool categories (`graph`, `sql`, `mongodb`) are **not** controlled by the agent config YAML. Instead, they go through a multi-stage pipeline:

```
1. Agent config (config/*.yaml)        → User defines base tools (workspace, research, etc.)
2. Orchestrator datasource override    → System injects/strips database tools based on attached datasources
3. Final resolved config               → What the agent actually receives
```

- If a datasource is attached to the job, the orchestrator **injects** the corresponding tool category (even if the config doesn't list it).
- If no datasource of a type is attached, the orchestrator **strips** the category (even if the config lists it).
- The `read_only` flag on the datasource controls whether write tools are included.

This means the agent config controls non-database tools, while the orchestrator controls database tools based on what's actually connected. See `_build_datasource_tool_override()` in `src/orchestrator/main.py`.

### Context Management

Context limits are model-dependent and set via `settings_matrix.yaml` (see below). The values below are defaults that get overridden per model family:

```yaml
limits:
  message_count_threshold: 300
  # Model-dependent (set in settings_matrix.yaml, NOT here):
  # context_threshold_tokens, model_max_context_tokens,
  # message_count_min_tokens
  # (summarization budgets are not config leaves — they are computed at call
  # time from the auxiliary model's window, see src/agent/core/summarizer.py)

context_management:
  compact_on_archive: true
  keep_recent_tool_results: 150
  keep_recent_messages: 10
```

### Settings Matrix

`settings_matrix.yaml` is the single source of truth for model-family-specific inference parameters and context limits. Keys match `detect_model_family()` output in `src/shared/runtime/core/loader.py`.

```yaml
# Resolution: default → family-specific (deep_merge)

default:
  model_max_context_tokens: 128000
  limits:
    model_max_context_tokens: 100000
    context_threshold_tokens: 80000

minimax:              # MiniMax M2.7 — 204K context, text-only
  temperature: 1.0
  top_p: 0.95
  limits:
    model_max_context_tokens: 150000
    context_threshold_tokens: 100000

minimax-m3:           # MiniMax M3 — 1M context (MSA), native multimodal; distinct family from minimax
  temperature: 1.0
  top_p: 0.95
  multimodal: true
  model_max_context_tokens: 1000000
  limits:
    model_max_context_tokens: 200000
    context_threshold_tokens: 150000

deepseek:
  model_max_context_tokens: 64000
  limits:
    context_threshold_tokens: 40000
```

Experts can place their own `settings_matrix.yaml` in their directory.

### Verification

Auto-spawn a critic job after `job_complete` to review deliverables:

```yaml
verification:
  enabled: true          # Spawn critic job after job_complete
  critic_config: critic  # Which expert config to use for the reviewer
  max_rounds: 3          # Max feedback round-trips before auto-accepting
```

### Memory Light

Opt-in recall system backed by PostgreSQL and pgvector hybrid search. It stores
and retrieves project-scoped insights across context compactions; see
[state, knowledge, and recovery](../docs/architecture.md#state-knowledge-and-recovery).

```yaml
memory:
  enabled: true
  budget_tokens: 10000
  max_memories_per_injection: 25
  observer_interval: 5
  embedding_model: qwen3-embedding-8b
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

In **Admin → Models**, add `z-ai/glm-5.3-flash` using your OpenRouter provider;
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

In **Admin → Models**, add `meta/muse-spark-1.3` using your OpenRouter provider.
The family is detected automatically, including with an `openrouter/` prefix.
The separately selected `meta/muse-spark-1.3-contributor` also uses this family;
family detection preserves the selected model ID and tier. Adding the family
does not register a catalog row. Audio/video input transport is outside this
family configuration change.

## Schema Validation

Add the schema comment at the top of your YAML file for IDE autocompletion:

```yaml
# yaml-language-server: $schema=schema.json
```

This works with VS Code + Red Hat YAML extension.

## Running Agents

```bash
# Use the worker framework base directly (normally a named expert is selected)
python -m agent

# Use custom config
python -m agent --config my_agent

# Use explicit path
python -m agent --config /path/to/config.yaml

# As API server
python -m agent --config my_agent --port 8001
```
