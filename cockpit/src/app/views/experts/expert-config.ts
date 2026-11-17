import {deepMergeConfig, omitKeys, pickKeys} from '../agent-settings/config-merge';

/**
 * Top-level config keys owned by the structured editor controls
 * (execution-group, the model select, tools-group, advanced-accordion, the
 * subagents editor). Everything else (e.g. `instruction_files`, leftover
 * `agent_id`/`display_name` from a duplicated bundled expert) is surfaced in
 * the raw-JSON "Advanced (other keys)" flap. Keep in sync with the controls'
 * `getOverrides()` keys.
 */
export const MANAGED_CONFIG_KEYS: ReadonlySet<string> = new Set([
  'tools',
  'delegation', // tools-group
  'llm', // model select + advanced-accordion
  'subagents', // model select (roster-wide llm) + subagents editor
  'autonomy',
  'scholar',
  'verification', // execution-group (worker)
  'phases', // the editor's "Plans in phases" toggle (worker)
  'interactive', // execution-group (session) + advanced-accordion
  'memory',
  'limits',
  'context_management', // advanced-accordion
  'workspace',
  'shell',
  'research',
  'browser',
  'auxiliary', // advanced-accordion
]);

/**
 * Managed keys a control emits WHOLESALE rather than as a diff. A deep-merge
 * over the stored fragment cannot express a removal — a roster entry the
 * author deleted would survive through the managed base — so these keys are
 * replaced by the control's value instead of merged with it.
 */
export const REPLACED_CONFIG_KEYS: readonly string[] = ['subagents'];

export interface ExpertConfigSplit {
  /** The expert's own values for managed keys — kept as the save baseline so
   *  nothing pinned is dropped when a control emits no override. */
  managed: Record<string, unknown>;
  /** Pretty JSON of the unmanaged remainder for the raw flap ('' if empty). */
  rawRemainderText: string;
}

/** Split a stored expert config fragment into the managed part and the
 *  unmanaged remainder shown (and editable) in the raw-JSON flap. */
export function splitExpertConfig(
  fragment: Record<string, unknown>,
): ExpertConfigSplit {
  const managed = pickKeys(fragment, MANAGED_CONFIG_KEYS);
  const remainder = omitKeys(fragment, MANAGED_CONFIG_KEYS);
  const rawRemainderText = Object.keys(remainder).length
    ? JSON.stringify(remainder, null, 2)
    : '';
  return {managed, rawRemainderText};
}

/**
 * Assemble the final config to persist. Order is load-bearing:
 *  1. `managedBase` — the expert's own managed values, so anything a control
 *     leaves untouched (and re-emits as `{}`) survives the round-trip.
 *  2. `groupOverrides` — the structured controls' edits, which win on changed
 *     leaves over the stale base.
 *  3. `rawFlap` — the hand-edited unmanaged remainder, applied last so a power
 *     user can still override anything (it should only carry unmanaged keys).
 * Deep-merge recurses objects and replaces arrays/scalars, so an unowned subkey
 * (e.g. `workspace.structure`) survives via `managedBase` automatically.
 *
 * `REPLACED_CONFIG_KEYS` (the `subagents` roster) are the exception: when the
 * controls emit one of them it replaces the base value wholesale — the
 * subagents editor round-trips the whole block, and a deep-merge would
 * resurrect every entry the author removed.
 */
export function assembleExpertConfig(
  fragment: Record<string, unknown>,
  groupOverrides: Record<string, unknown>,
  rawFlap: Record<string, unknown>,
): Record<string, unknown> {
  const managedBase = pickKeys(fragment, MANAGED_CONFIG_KEYS);
  const replaced = REPLACED_CONFIG_KEYS.filter((k) => k in groupOverrides);
  const mergeBase = replaced.length ? omitKeys(managedBase, replaced) : managedBase;
  return deepMergeConfig(deepMergeConfig(mergeBase, groupOverrides), rawFlap);
}

// ---- Legacy per-phase LLM tiers -------------------------------------------

const LEGACY_LLM_TIERS = ['strategic', 'tactical', 'subagent'] as const;

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/** A tier block that carries anything at all (the server's `_live`). */
function live(block: unknown): block is Record<string, unknown> {
  return isPlainObject(block) && Object.keys(block).length > 0;
}

/** Drop `null` leaves — a serialized blob's explicit `base_url: null` must never
 *  clear a base key (the server's `_clean`). */
function clean(block: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(
    Object.entries(block).filter(([, v]) => v !== null && v !== undefined),
  );
}

/**
 * Client-side mirror of the loader's `normalize_llm_tiers` (src/core/loader.py)
 * for a config the cockpit is about to read or edit.
 *
 * U1 collapsed the per-phase model tiers: an expert has one `llm.model`, and
 * the subagent reader model lives at `subagents.llm`. A fragment authored
 * before that (a stored DB expert, an old `config_override`) still carries
 * `llm.strategic` / `llm.tactical` / `llm.subagent`; the server maps them at
 * every seam it reads through, but the expert detail's merged `config` and the
 * export bundle's raw fragment arrive as stored. This lifts them the same way
 * so a legacy fragment prefills the single-model controls correctly and is
 * saved back in the new shape:
 *
 *  - **Layer-local** (default; a fragment at birth): when the fragment sets no
 *    `llm.model` of its own, the chosen phase block (strategic first; tactical
 *    only without a strategic block; a block carrying a `model` is preferred
 *    over a params-only one) is deep-merged into `llm`. An explicit
 *    `llm.model` wins and the phase blocks are dropped — the July "phase pin
 *    shadowed the selected model" rule.
 *  - **Merged** (`merged: true`; a base ⊕ fragment dict such as the detail's
 *    `config`): `strategic.model` > `tactical.model` > `model`, faithful to
 *    what the old `get_phase_config("strategic")` ran — the base's placeholder
 *    `llm.model` must not shadow the expert's pin here.
 *
 * Under both rules `llm.subagent` moves to `subagents.llm` unless the fragment
 * already authors one, and the legacy keys are deleted. Never mutates the
 * input; a fragment without legacy keys is returned by identity.
 */
export function liftLegacyTiers(
  fragment: Record<string, unknown>,
  opts?: {merged?: boolean},
): Record<string, unknown> {
  const llm = fragment['llm'];
  if (!isPlainObject(llm) || !LEGACY_LLM_TIERS.some((k) => k in llm)) return fragment;

  const out: Record<string, unknown> = {...fragment};
  let nextLlm: Record<string, unknown> = {...llm};
  const blocks: Partial<Record<(typeof LEGACY_LLM_TIERS)[number], unknown>> = {};
  for (const tier of LEGACY_LLM_TIERS) {
    if (tier in nextLlm) {
      blocks[tier] = nextLlm[tier];
      delete nextLlm[tier];
    }
  }

  // --- strategic / tactical -> llm ---
  const phaseBlocks = (['strategic', 'tactical'] as const)
    .filter((name) => live(blocks[name]))
    .map((name) => blocks[name] as Record<string, unknown>);
  const chosen = phaseBlocks.find((b) => !!b['model']) ?? phaseBlocks[0] ?? null;
  if (chosen && (opts?.merged || !nextLlm['model'])) {
    nextLlm = deepMergeConfig(nextLlm, clean(chosen));
  }
  out['llm'] = nextLlm;

  // --- subagent -> subagents.llm ---
  const subagent = blocks['subagent'];
  if (live(subagent)) {
    const subagents: Record<string, unknown> = isPlainObject(out['subagents'])
      ? {...out['subagents']}
      : {};
    if (!live(subagents['llm'])) {
      subagents['llm'] = clean(subagent);
      out['subagents'] = subagents;
    }
  }
  return out;
}
