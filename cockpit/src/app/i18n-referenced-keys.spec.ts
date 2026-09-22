import {readFileSync, readdirSync, statSync} from 'node:fs';
import {fileURLToPath} from 'node:url';
import {dirname, join, relative} from 'node:path';
import {describe, expect, it} from 'vitest';
import de from '../assets/i18n/de-DE.json';
import en from '../assets/i18n/en.json';

// Every transloco key the app names by a string LITERAL must exist in both
// locales. Transloco renders a missing key as its own dotted path, and the
// parity check (scripts/check-i18n-parity.mjs) cannot see it when the key is
// missing from both files alike — which is how the /settings LLM-provider key
// card shipped seven raw keys and a borrowed PAT heading
// (knowledge-base/knowledge/issues/settings_menu_dead_and_unwired_controls.md §9).
//
// Recognised references (templates in .html files and inline `template:`
// strings alike, plus TypeScript):
//   'a.b' | transloco                         pipe on a literal
//   (cond ? 'a.b' : 'c.d') | transloco        pipe on a parenthesised ternary
//   translate('a.b') / selectTranslate('a.b') / t('a.b')   (t = the
//     `*transloco="let t"` directive variable, or a local translate wrapper)
//   translate(cond ? 'a.b' : 'c.d')
//   errors.translate(err, 'a.b')              ErrorMessageService fallback key
//   key: 'a.b' / labelKey: 'a.b' (a `key:` or `…Key:` property), only when
//     `a` is a real top-level namespace so an unrelated storage key cannot
//     trip it
//
// Dynamic keys — `'prefix.' + x`, `prefix.${x}`, a key held in a variable —
// cannot be resolved statically and are skipped, never guessed at. If a
// literal reference must stay unresolved on purpose, add it to
// ALLOWED_MISSING with the reason; nothing belongs there today.
const ALLOWED_MISSING: Record<string, string> = {};

const here = dirname(fileURLToPath(import.meta.url));
const KEY = String.raw`[A-Za-z][\w-]*(?:\.[\w-]+)+`;
const WHOLE_KEY = new RegExp(`^${KEY}$`);

function flatten(tree: object, prefix = '', out = new Set<string>()): Set<string> {
  for (const [k, v] of Object.entries(tree)) {
    const key = prefix ? `${prefix}.${k}` : k;
    if (v && typeof v === 'object' && !Array.isArray(v)) flatten(v, key, out);
    else out.add(key);
  }
  return out;
}

function sourceFiles(dir: string, out: string[] = []): string[] {
  for (const name of readdirSync(dir)) {
    const full = join(dir, name);
    if (statSync(full).isDirectory()) {
      // _parked/ holds reference snapshots that are never compiled.
      if (name !== '_parked') sourceFiles(full, out);
    } else if (/\.(ts|html)$/.test(name) && !/\.spec\.ts$/.test(name)) {
      out.push(full);
    }
  }
  return out;
}

/** Index of the `(` matching the `)` at `close`, or -1. */
function openParen(text: string, close: number): number {
  let depth = 0;
  for (let i = close; i >= 0; i--) {
    if (text[i] === ')') depth++;
    else if (text[i] === '(' && --depth === 0) return i;
  }
  return -1;
}

/** Literal i18n keys referenced by one source text. */
export function referencedKeys(text: string, namespaces: ReadonlySet<string>): string[] {
  const keys: string[] = [];
  for (const pipe of text.matchAll(/\|\s*transloco\b/g)) {
    let i = pipe.index! - 1;
    while (i >= 0 && /\s/.test(text[i])) i--;
    if (text[i] === "'" || text[i] === '"') {
      const literal = text.slice(text.lastIndexOf(text[i], i - 1) + 1, i);
      if (WHOLE_KEY.test(literal)) keys.push(literal);
    } else if (text[i] === ')') {
      // Only the ternary branches of the group: a literal passed as a call
      // argument inside the condition is not a key.
      const group = text.slice(openParen(text, i) + 1, i);
      for (const m of group.matchAll(new RegExp(String.raw`(?:^|[?:])\s*(['"])(${KEY})\1`, 'g'))) {
        keys.push(m[2]);
      }
    }
  }
  const call = String.raw`\b(?:translate|selectTranslate|t)\s*\(\s*`;
  for (const m of text.matchAll(new RegExp(String.raw`${call}(['"])(${KEY})\1`, 'g'))) {
    keys.push(m[2]);
  }
  for (const m of text.matchAll(
    new RegExp(String.raw`${call}[^()'"]*\?\s*(['"])(${KEY})\1\s*:\s*(['"])(${KEY})\3`, 'g'),
  )) {
    keys.push(m[2], m[4]);
  }
  for (const m of text.matchAll(
    new RegExp(String.raw`\b(?:errors|errorMessages?)\.translate\s*\([^,()]+,\s*(['"])(${KEY})\1`, 'g'),
  )) {
    keys.push(m[2]);
  }
  for (const m of text.matchAll(new RegExp(String.raw`\b(?:key|\w*Key)\s*:\s*(['"])(${KEY})\1`, 'g'))) {
    if (namespaces.has(m[2].split('.')[0])) keys.push(m[2]);
  }
  return keys;
}

describe('i18n: statically referenced transloco keys', () => {
  const locales = {en: flatten(en), 'de-DE': flatten(de)};
  const namespaces = new Set([...Object.keys(en), ...Object.keys(de)]);
  const references = new Map<string, string>();
  const appRoot = here;
  for (const file of sourceFiles(appRoot)) {
    const text = readFileSync(file, 'utf8');
    for (const key of referencedKeys(text, namespaces)) {
      if (!references.has(key)) references.set(key, relative(appRoot, file));
    }
  }

  it('finds the app’s references at all (guards the scanner itself)', () => {
    expect(references.size).toBeGreaterThan(1000);
  });

  for (const [locale, keys] of Object.entries(locales)) {
    it(`every referenced key exists in ${locale}.json`, () => {
      const missing = [...references]
        .filter(([key]) => !keys.has(key) && !(key in ALLOWED_MISSING))
        .map(([key, file]) => `${key}  (${file})`)
        .sort();
      expect(missing).toEqual([]);
    });
  }

  it('keeps ALLOWED_MISSING honest: every entry is still referenced and still missing', () => {
    const stale = Object.keys(ALLOWED_MISSING).filter(
      (key) => !references.has(key) || (locales.en.has(key) && locales['de-DE'].has(key)),
    );
    expect(stale).toEqual([]);
  });
});

describe('referencedKeys', () => {
  const ns = new Set(['chat', 'common', 'settings']);

  it('reads pipe literals, ternary branches and parameterised pipes', () => {
    const text = `
      {{ 'chat.a' | transloco }}
      [title]="(busy() ? 'chat.b' : 'chat.c') | transloco"
      {{ 'chat.d' | transloco: {n: 1} }}
      {{ (count === 1 ? 'chat.e' : cond ? 'chat.f' : 'chat.g') | transloco }}`;
    expect(referencedKeys(text, ns)).toEqual(['chat.a', 'chat.b', 'chat.c', 'chat.d', 'chat.e', 'chat.f', 'chat.g']);
  });

  it('reads translate-style calls, the ErrorMessageService fallback and …Key properties', () => {
    const text = `
      this.transloco.translate('chat.a', {x: 1});
      t.translate(n === 1 ? 'chat.b' : 'chat.c');
      this.t('common.d');
      this.errors.translate(err, 'common.e');
      const opt = {labelKey: 'settings.f', key: 'chat.g', storageKey: 'srw.cache.v1'};`;
    expect(referencedKeys(text, ns)).toEqual([
      'chat.a', 'common.d', 'chat.b', 'chat.c', 'common.e', 'settings.f', 'chat.g',
    ]);
  });

  it('skips dynamic keys and non-key literals', () => {
    const text = `
      {{ ('chat.rewind.refusal.' + code) | transloco }}
      {{ key | transloco }}
      this.transloco.translate(\`chat.rewind.refusal.\${key}\`);
      this.transloco.translate(prefix + '.title');
      {{ (chat.mode('conversation.x') ? label : other) | transloco }}
      {{ 'plain' | transloco }}`;
    expect(referencedKeys(text, ns)).toEqual([]);
  });
});
