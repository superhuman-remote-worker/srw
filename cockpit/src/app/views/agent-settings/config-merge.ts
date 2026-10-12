/**
 * Pure config-merge helpers shared by the agent-settings form, its groups and
 * the expert editor. `deepMergeConfig` mirrors the private `deepMerge` in
 * `agent-settings.types.ts` (override replaces arrays + scalars, recurses
 * nested plain objects) but is exported + immutable for use in `computed()`.
 */

/** Deep-merge two plain config objects immutably. Override replaces arrays and
 *  scalars; nested plain objects are merged recursively. `__proto__`,
 *  `constructor` and `prototype` are never config keys and are skipped: an
 *  own `__proto__` key (JSON.parse makes one) would otherwise replace the
 *  merged object's prototype. */
export function deepMergeConfig(
  base: Record<string, unknown>,
  override: Record<string, unknown>,
): Record<string, unknown> {
  const result: Record<string, unknown> = {...base};
  for (const key of Object.keys(override)) {
    if (key === '__proto__' || key === 'constructor' || key === 'prototype') continue;
    const sv = override[key];
    const tv = result[key];
    if (
      sv &&
      typeof sv === 'object' &&
      !Array.isArray(sv) &&
      tv &&
      typeof tv === 'object' &&
      !Array.isArray(tv)
    ) {
      result[key] = deepMergeConfig(
        tv as Record<string, unknown>,
        sv as Record<string, unknown>,
      );
    } else {
      result[key] = sv;
    }
  }
  return result;
}

function toSet(keys: ReadonlySet<string> | readonly string[]): ReadonlySet<string> {
  return keys instanceof Set ? keys : new Set(keys);
}

/** New object containing only the named top-level keys present in `source`. */
export function pickKeys(
  source: Record<string, unknown>,
  keys: ReadonlySet<string> | readonly string[],
): Record<string, unknown> {
  const set = toSet(keys);
  const out: Record<string, unknown> = {};
  for (const k of Object.keys(source)) {
    if (set.has(k)) out[k] = source[k];
  }
  return out;
}

/** New object with the named top-level keys removed from `source`. */
export function omitKeys(
  source: Record<string, unknown>,
  keys: ReadonlySet<string> | readonly string[],
): Record<string, unknown> {
  const set = toSet(keys);
  const out: Record<string, unknown> = {};
  for (const k of Object.keys(source)) {
    if (!set.has(k)) out[k] = source[k];
  }
  return out;
}
