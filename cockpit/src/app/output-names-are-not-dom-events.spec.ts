import {readFileSync, readdirSync, statSync} from 'node:fs';
import {fileURLToPath} from 'node:url';
import {dirname, join, relative} from 'node:path';
import {describe, expect, it} from 'vitest';

// An output named after a DOM event is also a DOM listener. For
// `(change)="x()"` on a component host, Angular subscribes to the component's
// `change` output AND listens for native `change` events on the host element,
// so every bubbling `change` from a descendant (a projected textarea losing
// focus, a checkbox, a file input) calls x() too. Angular's guidance is "do not
// use native event names for your custom events" (angular/angular#13997);
// angular-eslint's `no-output-native` enforces it. The Cockpit runs no ESLint,
// so this spec is that rule. It exists because the settings family's `change`
// output turned a description field's blur into a tool preview that disabled
// Create between mousedown and click
// (knowledge-base/knowledge/issues/create_job_first_click_swallowed_after_typing_description.md).
//
// Two checks:
//   1. No `output()`, `outputFromObservable()` or `@Output()` in src/ (specs
//      excluded) has a property name or alias in NATIVE_EVENT_NAMES.
//   2. No template binds a renamed output by its old DOM-event name on the
//      components that used to declare it: a leftover `(change)=` on
//      `<app-agent-settings>` still compiles under strictTemplates — as a
//      plain DOM listener that brings the bug back.
//
// If an output must keep a native name (frozen public API), list it in
// ALLOWED with the reason; nothing belongs there today.
const ALLOWED: Record<string, string> = {};

// Copied verbatim from angular-eslint (the list `no-output-native` checks):
// https://github.com/angular-eslint/angular-eslint/blob/main/packages/utils/src/eslint-plugin/get-native-event-names.ts
// at commit af0af836775fdcb80c652454bd65cb853f2e26f5 (2026-11-01), "sourced from
// @mdn/browser-compat-data@8.1.7". Refresh it from there; do not edit by hand.
export const NATIVE_EVENT_NAMES: ReadonlySet<string> = new Set([
  'abort', 'afterprint', 'afterscriptexecute', 'animationcancel', 'animationend',
  'animationiteration', 'animationstart', 'appinstalled', 'auxclick', 'beforeinput',
  'beforeinstallprompt', 'beforematch', 'beforeprint', 'beforescriptexecute', 'beforetoggle',
  'beforeunload', 'beforexrselect', 'beginEvent', 'blur', 'cancel', 'canplay', 'canplaythrough',
  'change', 'click', 'close', 'command', 'compositionend', 'compositionstart',
  'compositionupdate', 'contentvisibilityautostatechange', 'contextlost', 'contextmenu',
  'contextrestored', 'copy', 'cuechange', 'cut', 'dblclick', 'devicemotion', 'deviceorientation',
  'deviceorientationabsolute', 'DOMActivate', 'DOMContentLoaded', 'DOMMouseScroll', 'drag',
  'dragend', 'dragenter', 'dragexit', 'dragleave', 'dragover', 'dragstart', 'drop',
  'durationchange', 'elementgeometryupdate', 'emptied', 'encrypted', 'ended', 'endEvent',
  'enterpictureinpicture', 'error', 'focus', 'focusin', 'focusout', 'formdata', 'freeze',
  'fullscreenchange', 'fullscreenerror', 'gamepadconnected', 'gamepaddisconnected',
  'gesturechange', 'gestureend', 'gesturestart', 'gotpointercapture', 'hashchange', 'input',
  'installresult', 'interest', 'invalid', 'keydown', 'keypress', 'keyup', 'languagechange',
  'leavepictureinpicture', 'load', 'loadeddata', 'loadedmetadata', 'loadstart', 'location',
  'loseinterest', 'lostpointercapture', 'message', 'messageerror', 'mousedown', 'mouseenter',
  'mouseleave', 'mousemove', 'mouseout', 'mouseover', 'mouseup', 'mousewheel',
  'MozMousePixelScroll', 'offline', 'online', 'orientationchange', 'pagehide', 'pagereveal',
  'pageshow', 'pageswap', 'paint', 'paste', 'pause', 'play', 'playing', 'pointercancel',
  'pointerdown', 'pointerenter', 'pointerleave', 'pointerlockchange', 'pointerlockerror',
  'pointermove', 'pointerout', 'pointerover', 'pointerrawupdate', 'pointerup', 'popstate',
  'prerenderingchange', 'progress', 'promptaction', 'promptdismiss', 'ratechange',
  'readystatechange', 'rejectionhandled', 'repeatEvent', 'reset', 'resize', 'resume', 'scroll',
  'scrollend', 'scrollsnapchange', 'scrollsnapchanging', 'search', 'securitypolicyviolation',
  'seeked', 'seeking', 'select', 'selectionchange', 'selectstart', 'slotchange', 'stalled',
  'storage', 'stream', 'submit', 'suspend', 'timeupdate', 'toggle', 'touchcancel', 'touchend',
  'touchmove', 'touchstart', 'track', 'transitioncancel', 'transitionend', 'transitionrun',
  'transitionstart', 'unhandledrejection', 'unload', 'validationstatuschange',
  'visibilitychange', 'volumechange', 'vrdisplayactivate', 'vrdisplayconnect',
  'vrdisplaydeactivate', 'vrdisplaydisconnect', 'vrdisplaypresentchange', 'waiting',
  'waitingforkey', 'webglcontextcreationerror', 'webglcontextlost', 'webglcontextrestored',
  'webkitmouseforcechanged', 'webkitmouseforcedown', 'webkitmouseforceup',
  'webkitmouseforcewillbegin', 'wheel',
]);

/** Components whose output used to carry a DOM-event name, and that name. A
 *  host binding of the old name would silently become a DOM listener. */
export const RENAMED_OUTPUTS: Readonly<Record<string, string>> = {
  'app-agent-settings': 'change',
  'app-execution-group': 'change',
  'app-model-group': 'change',
  'app-tools-group': 'change',
  'app-datasources-group': 'change',
  'app-advanced-accordion': 'change',
  'app-subagents-editor': 'change',
  'app-panel-header': 'close',
  'app-citations-panel': 'close',
};

const here = dirname(fileURLToPath(import.meta.url));
const SRC = join(here, '..');
const IDENT = String.raw`[A-Za-z_$][\w$]*`;

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

function lineOf(text: string, index: number): number {
  return text.slice(0, index).split('\n').length;
}

/** The argument text of the call whose name ends at `i`, past an optional
 *  type argument list (which may itself hold parentheses), or ''. */
function callArguments(text: string, i: number): string {
  while (/\s/.test(text[i] ?? '')) i++;
  if (text[i] === '<') {
    let depth = 0;
    for (; i < text.length; i++) {
      if (text[i] === '<') depth++;
      else if (text[i] === '>' && text[i - 1] !== '=' && --depth === 0) break;
    }
    i++;
    while (/\s/.test(text[i] ?? '')) i++;
  }
  if (text[i] !== '(') return '';
  const start = i + 1;
  let depth = 0;
  for (; i < text.length; i++) {
    if (text[i] === '(') depth++;
    else if (text[i] === ')' && --depth === 0) return text.slice(start, i);
  }
  return '';
}

export interface DeclaredOutput {
  /** The class property. */
  property: string;
  /** The alias templates bind, when one is set. */
  alias: string | null;
  line: number;
}

/** Every output one source text declares, by property name and alias. */
export function declaredOutputs(text: string): DeclaredOutput[] {
  const found: DeclaredOutput[] = [];
  const fn = new RegExp(String.raw`\b(${IDENT})\s*=\s*(?:output|outputFromObservable)\b`, 'g');
  for (const m of text.matchAll(fn)) {
    const args = callArguments(text, m.index! + m[0].length);
    const alias = /\balias\s*:\s*(['"`])([^'"`]+)\1/.exec(args)?.[2] ?? null;
    found.push({property: m[1], alias, line: lineOf(text, m.index!)});
  }
  const decorator = new RegExp(
    String.raw`@Output\(\s*(?:(['"\`])([^'"\`]*)\1)?\s*\)\s*(?:(?:public|protected|private|readonly|override|declare)\s+)*(${IDENT})`,
    'g',
  );
  for (const m of text.matchAll(decorator)) {
    found.push({property: m[3], alias: m[2] || null, line: lineOf(text, m.index!)});
  }
  return found;
}

/** The opening tag starting at `<` index `start`, quotes respected. */
function openingTag(text: string, start: number): string {
  let quote: string | null = null;
  for (let i = start; i < text.length; i++) {
    const c = text[i];
    if (quote) {
      if (c === quote) quote = null;
    } else if (c === '"' || c === "'") {
      quote = c;
    } else if (c === '>') {
      return text.slice(start, i + 1);
    }
  }
  return text.slice(start);
}

export interface StaleBinding {
  selector: string;
  event: string;
  line: number;
}

/** Host bindings of a renamed output's old DOM-event name, in one source text
 *  (an .html template or a .ts file with an inline template). */
export function staleBindings(text: string): StaleBinding[] {
  const found: StaleBinding[] = [];
  const selectors = Object.keys(RENAMED_OUTPUTS).join('|');
  for (const m of text.matchAll(new RegExp(`<(${selectors})(?=[\\s/>])`, 'g'))) {
    const event = RENAMED_OUTPUTS[m[1]];
    const tag = openingTag(text, m.index!);
    if (new RegExp(String.raw`\(\s*${event}\s*\)\s*=`).test(tag)) {
      found.push({selector: m[1], event, line: lineOf(text, m.index!)});
    }
  }
  return found;
}

const FILES = sourceFiles(SRC).map((path) => ({path: relative(SRC, path), text: readFileSync(path, 'utf8')}));

describe('outputs are not named after DOM events', () => {
  it('scans the app source', () => {
    // A broken walk would make the checks below pass vacuously.
    expect(FILES.some((f) => f.path.endsWith('agent-settings.component.ts'))).toBe(true);
    expect(FILES.flatMap((f) => (f.path.endsWith('.ts') ? declaredOutputs(f.text) : [])).length).toBeGreaterThan(50);
  });

  it('no output, and no output alias, is named after a native DOM event', () => {
    const offenders: string[] = [];
    for (const file of FILES.filter((f) => f.path.endsWith('.ts'))) {
      for (const out of declaredOutputs(file.text)) {
        for (const name of [out.property, out.alias]) {
          const key = `${file.path}:${name}`;
          if (name && NATIVE_EVENT_NAMES.has(name) && !(key in ALLOWED)) {
            offenders.push(`${file.path}:${out.line} output "${name}"`);
          }
        }
      }
    }
    expect(offenders).toEqual([]);
  });

  it('no template binds a renamed output by its old DOM-event name', () => {
    const offenders = FILES.flatMap((file) =>
      staleBindings(file.text).map((b) => `${file.path}:${b.line} (${b.event}) on <${b.selector}>`),
    );
    expect(offenders).toEqual([]);
  });
});

describe('the output-name scanner', () => {
  it('finds signal outputs, observable outputs and decorated outputs, with aliases', () => {
    const text = [
      'change = output<void>();',
      'readonly close = output();',
      'protected readonly select = outputFromObservable(this.picked$);',
      'readonly picked = output<(id: string) => void>({alias: "input"});',
      'toggled = outputFromObservable(this.toggle$, {alias: \'toggle\'});',
      'grouped = output<',
      '  Record<string, number>',
      '>();',
      '@Output() click = new EventEmitter<void>();',
      '@Output()',
      '  readonly focus = new EventEmitter<void>();',
      "@Output('submit') saved = new EventEmitter<void>();",
    ].join('\n');

    expect(declaredOutputs(text).map((o) => [o.property, o.alias])).toEqual([
      ['change', null],
      ['close', null],
      ['select', null],
      ['picked', 'input'],
      ['toggled', 'toggle'],
      ['grouped', null],
      ['click', null],
      ['focus', null],
      ['saved', 'submit'],
    ]);
  });

  it('lets non-native names through', () => {
    const text = [
      'settingsChange = output<void>();',
      'changed = output<string>();',
      'closed = output<void>();',
      'readonly valueChange = new EventEmitter<string>();',
    ].join('\n');
    const names = declaredOutputs(text).map((o) => o.property);

    expect(names).toEqual(['settingsChange', 'changed', 'closed']);
    expect(names.filter((n) => NATIVE_EVENT_NAMES.has(n))).toEqual([]);
  });

  it('flags a stale host binding however the tag is laid out', () => {
    const template = `
      <app-agent-settings
        mode="job"
        [disabled]="a > b"
        (change)="onSettingsChange()"
      >
        <textarea (change)="native()"></textarea>
      </app-agent-settings>
      <app-panel-header [title]="t" (close)="onClose()" />
    `;

    expect(staleBindings(template)).toEqual([
      {selector: 'app-agent-settings', event: 'change', line: 2},
      {selector: 'app-panel-header', event: 'close', line: 9},
    ]);
  });

  it('leaves the renamed bindings and native listeners on other elements alone', () => {
    const template = `
      <app-agent-settings (settingsChange)="x()" (keydown.enter)="y($event)"></app-agent-settings>
      <app-agent-settings-extra (change)="z()"></app-agent-settings-extra>
      <select (change)="pick($event)"></select>
      <app-citations-panel (closed)="hide()" />
    `;

    expect(staleBindings(template)).toEqual([]);
  });
});
