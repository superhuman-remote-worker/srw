# Subagent library (`config/subagents/`)

Small, shared experts an expert's roster can reference by name. One directory
per entry, same layout and schema as a bundled expert:

Each `config.yaml` is an `srw/v1alpha1` Expert manifest. The roster snippets
below are **SRW harness private configuration**, written inside
`spec.runtime.config.config` with `spec.runtime.adapter: srw/v1`. They describe
delegation within that harness process. A generic harness image cannot be
inserted into an SRW in-process roster; use a separate manifest execution for it.

```
config/subagents/
└── explorer/
    ├── config.yaml     # Expert manifest; private leaf has $extends: expert_base
    └── persona.txt     # prompt files next to the config, resolved like an expert's
```

Loader/resolver: `src/shared/runtime/core/subagent_roster.py`.

## Referencing an entry

An expert (bundled YAML or a DB expert's config fragment) declares its roster:

```yaml
subagents:
  default: explorer                     # what a delegate_agent call falls back to
  llm: {model: claude-haiku-4-5}        # roster-wide default model (optional)
  roster:
    explorer: {$ref: subagents/explorer}      # library entry (`$ref: explorer` also works)
    reviewer: {$ref: critic}                  # a FULL expert used as a child
    implementer:                              # inline small expert, same schema
      description: Implements ONE fully specified, bounded change and returns the diff.
      llm: {model: inherit}
      tools: {workspace: [read_file, write_file, edit_file, search_files, list_files]}
      prompts: {system: implementer.txt}
      isolation: shared
      write_policy: owned_paths
      limits: {max_turns: 150, max_tokens: 600000, return_budget_tokens: 3000}
      return: diff
```

`$ref` grammar — exactly three forms, never a path:

| `$ref`                         | Resolves to                                   |
|--------------------------------|-----------------------------------------------|
| `critic` / `experts/critic`    | `config/experts/critic/config.yaml`           |
| `explorer` / `subagents/explorer` | `config/subagents/explorer/config.yaml`    |
| `3f2a…-uuid`                   | a DB expert row (orchestrator only)           |

A bare name tries `config/experts/<name>` first, then `config/subagents/<name>`.
Sibling keys next to `$ref` deep-merge over the referenced expert (an entry
is "that expert, plus these keys"). A job or thread override deep-merges into
`subagents.roster.<name>` before the entry is materialised, so
`config_override: {subagents: {roster: {explorer: {llm: {model: …}}}}}` pins
one child's model.

## How an entry resolves

Every entry — inline or `$ref` — is materialised at resolve time (orchestrator
dispatch / session attach via `resolve_config`; the agent's disk fallback via
`load_agent_config`) into a full config dict:

```
expert_base <- overlays/subagent <- [the $ref target's own $extends chain]
            <- subagents.llm (roster-wide)  <- the entry's sibling keys (+ override)
            -> `inherit` -> parent's llm.model + transport, entry marked `llm._inherit_llm`
            -> settings matrix for the ENTRY's model family
            -> the subagent overlay's `$ignore_keys` pruned (workspace.backend,
               autonomy, verification, phase_settings, delegation, tools.delegation, …)
```

- The roster-wide `subagents.llm` sits **below** the entry's own `llm` and
  **above** the base and the `$ref` target — a per-entry `llm.model` always
  wins; the roster-wide model applies to every entry that does not pin one.
- `llm.model: inherit` (the subagent overlay's default) means the parent's
  model; the resolver copies the parent's `model` / `provider` / `base_url` /
  `api_key` / `model_max_context_tokens` and marks the entry so a later parent
  model change is re-synced when the config is re-parsed.
- Depth is 1: a referenced expert's own `subagents` block is dropped.
- The resolved entry carries bookkeeping keys: `_ref`, `_ref_kind`
  (`bundled` | `library` | `db`), `_ref_name` (DB rows), `_deployment_dir`
  (repo-relative directory whose prompt files apply — the target's for a
  `$ref`, the parent expert's for an inline entry), `prompts` +
  `_persona_source: db` (a DB row's prompt text, inlined).
- Failure policy: an unknown or malformed disk `$ref`, a `$extends` cycle or
  a chain deeper than three expert links behind the target raise
  `RosterResolutionError` on the disk path (a bundled typo fails loudly) and
  drop the entry at dispatch (recorded in the blob's `agent._roster_warnings`
  — a roster never fails a job). A DB expert id with no prefetched row is
  dropped anywhere the DB is not reachable.

## Entry keys

Everything the expert schema allows (`config/schema.json`), plus, per entry:

| key            | U1                                            | U3 (roster runtime) |
|----------------|-----------------------------------------------|---------------------|
| `description`  | the text the parent sees per `subagent_type`  | same                |
| `llm`          | model / transport / params; `inherit`          | same                |
| `tools`        | the child's tool groups (read-only floor by default) | bound as-is  |
| `prompts`      | carried verbatim (inline: file names against `_deployment_dir`; DB: inlined text) | rendered |
| `isolation`    | carried verbatim (`shared` \| `worktree`)      | enforced            |
| `write_policy` | carried verbatim (`none` \| `scratch_only` \| `owned_paths` \| `full`); **omitting it means `full`** — declare it explicitly | enforced |
| `limits`       | ordinary `limits` keys parse; the child budgets (`max_turns`, `max_tokens`, `return_budget_tokens`, `stale_idle_s`, `stale_in_tool_s`) are carried, not yet parsed | enforced |
| `return`       | carried verbatim (`summary` \| `structured` \| `evidence` \| `diff`) | shapes the result |

The resolved entry is data until a `delegate_agent` call names it: the roster
runtime (`src/agent/subagents/`) turns the entry into a running child. Foreground
calls return the report directly; background calls return a durable receipt
and push the completion automatically. `delegate_agent` and each control
(`wait_agent`, `message_agent`, `stop_agent`, `list_agents`) are bound only
when the parent sets `delegation.enabled` AND explicitly names that tool in
`tools.delegation`. Children strip the entire category and cannot delegate.

## Adding a library entry

1. `mkdir config/subagents/<name>` with a `config.yaml` that `$extends:
   expert_base`, sets `tags: [subagent]`, a `description`, `llm: {model:
   inherit}` and an explicit, minimal `tools` block. Restate the read-only
   groups you rely on: a standalone load (`--config <name>`, the tool-grants
   snapshot) resolves on `expert_base`, not on the subagent overlay, and
   `expert_base` grants writes and a browser.
   **Declare `write_policy` even when it is `none`.** An entry that omits it
   resolves to `full`, which is what the child is told in its
   `<subagent_environment>` block, what the spawn metadata records and what the
   cockpit's Subagents row displays — misleading for an entry whose tools carry
   no workspace mutator. The shipped `explorer` had exactly this gap
   (found on the U3 WP7 k3d run, fixed in `460f905c`).
2. Put prompt files (`persona.txt`, …) next to it — resolved via the entry's
   `_deployment_dir` like a bundled expert's. **A persona is plain role prose,
   never a template**: the six framework placeholders (`{agent_display_name}`,
   `{expert_identity}`, `{available_skills}`, `{subagent_environment}`,
   `{prompt_content}`, `{phase_number}`) belong to the prompt assembler and are
   rejected on the DB write path — a persona is a substitution *value*, so a
   token inside it would never be expanded anyway. Write the display name
   literally. See `ASSEMBLER_OWNED_PROMPT_TOKENS` in
   `src/shared/runtime/core/expert_resolution.py`.
3. Run `UPDATE_TOOL_GRANTS_SNAPSHOT=1 pytest tests/test_config_tool_grants_snapshot.py`
   and review the added `subagents/<name>` entry, then
   `pytest tests/test_expert_roles_golden.py tests/test_subagent_roster.py tests/test_tool_policy.py`.
