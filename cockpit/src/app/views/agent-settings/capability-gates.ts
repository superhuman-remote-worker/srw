/** Pure capability-gate helpers for editor control-greying. `grants === null`
 * means the caller is an admin (unrestricted) — every gate is open. No Angular
 * imports so it is unit-testable in isolation. Mirrors the backend PDP semantics
 * (src/core/capability_grants.py): restrict-only, deny-by-default for bools. */
import type {GrantCatalog} from '../../core/models/api.model';
import {normalizePermissionMode} from '../../core/models/permission-mode';

type Grants = Record<string, unknown> | null;

/** True if a bool capability is granted (admin ⇒ always; absent ⇒ denied). */
export function hasGrant(grants: Grants, key: string): boolean {
  if (grants === null) return true;
  return grants[key] === true;
}

/** Enum options at or below the granted ceiling (admin ⇒ all). */
export function allowedEnumOptions(
  grants: Grants,
  key: string,
  all: string[],
  catalog: GrantCatalog,
): string[] {
  if (grants === null) return all;
  let order = catalog[key]?.order ?? all;
  let ceiling = grants[key];
  if (key === 'permission_mode') {
    // Grant rows (and an older catalog) may still spell read_only `supervised`.
    order = order.map((o) => normalizePermissionMode(o) ?? o);
    ceiling = normalizePermissionMode(ceiling) ?? ceiling;
  }
  if (typeof ceiling !== 'string' || !order.includes(ceiling)) return all;
  const max = order.indexOf(ceiling);
  return all.filter((o) => !order.includes(o) || order.indexOf(o) <= max);
}

/** True if `model` is permitted by the model_selection grant (null/admin ⇒ all). */
export function isModelAllowed(grants: Grants, model: string): boolean {
  if (grants === null) return true;
  const sel = grants['model_selection'];
  if (sel == null) return true;
  return Array.isArray(sel) ? sel.includes(model) : true;
}

/** i18n key for the lock hint on a gated control ('' ⇒ not gated). */
export function gateReason(grants: Grants, key: string): string {
  return hasGrant(grants, key) ? '' : `grants.locked.${key}`;
}
