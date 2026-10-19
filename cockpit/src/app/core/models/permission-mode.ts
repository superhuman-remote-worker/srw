/**
 * Session permission modes, and the one place their legacy spelling is mapped.
 * Mirrors `src/shared/permission_modes.py`.
 *
 * `read_only` was called `supervised` until 2026-10
 * (knowledge-base/knowledge/features/tool_permission_tiers.md, D2). The server
 * keeps sending the old name for a while — stored thread rows, journal
 * `mode.changed` frames, user preferences, expert configs, grant rows — so every
 * place a mode enters the cockpit goes through `normalizePermissionMode`, and
 * the cockpit itself only ever emits the canonical names.
 *
 * Not to be confused with connector access (`project_datasources.read_only`,
 * manifest `access: ReadOnly`), which also renders as "Read-only".
 */

/** Most to least restrictive. Grant ceilings compare positions in this list. */
export const PERMISSION_MODES = ['read_only', 'auto_accept', 'autonomous'] as const;

export type PermissionMode = (typeof PERMISSION_MODES)[number];

export const DEFAULT_PERMISSION_MODE: PermissionMode = 'read_only';

/** Old spelling → canonical name. */
export const LEGACY_PERMISSION_MODES: Readonly<Record<string, PermissionMode>> = {
  supervised: 'read_only',
};

/** The canonical name for `value` (a canonical name or a legacy spelling of
 *  one), or `null` for anything else — the caller decides what an unknown or
 *  missing value means at its site. */
export function normalizePermissionMode(value: unknown): PermissionMode | null {
  if (typeof value !== 'string') return null;
  if ((PERMISSION_MODES as readonly string[]).includes(value)) return value as PermissionMode;
  return Object.prototype.hasOwnProperty.call(LEGACY_PERMISSION_MODES, value)
    ? LEGACY_PERMISSION_MODES[value]
    : null;
}

/** The short label for a mode (badge, menus). */
export const PERMISSION_MODE_LABEL_KEYS: Readonly<Record<PermissionMode, string>> = {
  read_only: 'chat.settings.modeReadOnly',
  auto_accept: 'chat.settings.modeAutoAccept',
  autonomous: 'chat.settings.modeAutonomous',
};
