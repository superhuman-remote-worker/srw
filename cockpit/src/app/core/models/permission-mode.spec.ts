import { describe, expect, it } from 'vitest';
import de from '../../../assets/i18n/de-DE.json';
import en from '../../../assets/i18n/en.json';
import {
  DEFAULT_PERMISSION_MODE,
  LEGACY_PERMISSION_MODES,
  normalizePermissionMode,
  PERMISSION_MODE_LABEL_KEYS,
  PERMISSION_MODES,
} from './permission-mode';

function lookup(tree: unknown, key: string): unknown {
  return key
    .split('.')
    .reduce<unknown>(
      (node, part) =>
        node && typeof node === 'object' ? (node as Record<string, unknown>)[part] : undefined,
      tree,
    );
}

describe('permission modes', () => {
  it('lists the canonical names most to least restrictive, read_only first', () => {
    expect(PERMISSION_MODES).toEqual(['read_only', 'auto_accept', 'autonomous']);
    expect(DEFAULT_PERMISSION_MODE).toBe('read_only');
  });

  it('passes canonical names through', () => {
    for (const mode of PERMISSION_MODES) expect(normalizePermissionMode(mode)).toBe(mode);
  });

  it('maps the legacy `supervised` spelling to read_only', () => {
    expect(LEGACY_PERMISSION_MODES).toEqual({ supervised: 'read_only' });
    expect(normalizePermissionMode('supervised')).toBe('read_only');
  });

  it('answers null for anything else, so each call site picks its own fallback', () => {
    for (const value of [
      undefined,
      null,
      '',
      'Supervised',
      'read-only',
      'toString',
      'constructor',
      42,
      {},
    ]) {
      expect(normalizePermissionMode(value)).toBeNull();
    }
  });

  it('has a label for every mode in both locales', () => {
    for (const mode of PERMISSION_MODES) {
      const key = PERMISSION_MODE_LABEL_KEYS[mode];
      expect(typeof lookup(en, key), `${key} in en.json`).toBe('string');
      expect(typeof lookup(de, key), `${key} in de-DE.json`).toBe('string');
    }
  });
});
