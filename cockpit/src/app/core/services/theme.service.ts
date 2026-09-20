import {computed, effect, inject, Injectable, PLATFORM_ID, signal} from '@angular/core';
import {isPlatformBrowser} from '@angular/common';

/** Concrete theme keys — each maps to a body class `theme-<key>`. */
export type ConcreteTheme = 'travertine' | 'senate';

/** What the user picked. `system` follows OS dark/light preference. */
export type ThemePreference = ConcreteTheme | 'system';

/** Effective theme that's actually applied to <body>. */
export type ResolvedTheme = ConcreteTheme;

/**
 * Accent keys — the second appearance axis. Each maps to a body class
 * `accent-<key>` beside the `theme-<key>` class; `_theme-config.scss`
 * ($accents) holds the tokens. Tyrian is the default there too
 * ($default-accent) so the no-JS first paint and a fresh device agree.
 */
export type AccentPreference = 'tyrian' | 'porphyry' | 'graphite';
export const ACCENT_OPTIONS: readonly AccentPreference[] = ['tyrian', 'porphyry', 'graphite'];
export const DEFAULT_ACCENT: AccentPreference = 'tyrian';
export function isAccentPreference(value: unknown): value is AccentPreference {
  return typeof value === 'string' && (ACCENT_OPTIONS as readonly string[]).includes(value);
}

const STORAGE_KEY = 'cockpit:theme';
const ACCENT_STORAGE_KEY = 'cockpit:accent';
const VALID_PREFERENCES: ReadonlySet<ThemePreference> = new Set<ThemePreference>([
  'travertine', 'senate', 'system',
]);

// Legacy keys → Roman replacements. Old localStorage values are silently
// rewritten on first read so users keep a sensible look.
//   dark/light: Catppuccin-era keys.
//   praetorian: retired high-contrast theme; lifted Senate took its place.
const LEGACY_MIGRATIONS: Record<string, ThemePreference> = {
  dark: 'senate',
  light: 'travertine',
  praetorian: 'senate',
};

/**
 * Owns appearance-theme resolution and propagation.
 *
 * Stored as a per-device preference (localStorage) rather than backend-persisted —
 * users often want different themes on different devices (laptop vs phone) and
 * the body class is needed before the API responds for first paint.
 *
 * Resolution: stored preference → 'system' (which follows prefers-color-scheme,
 * mapping to senate/travertine).
 * `setPreference` is the only mutation entry point; the body class swap happens
 * via an effect, so it stays in lockstep with the resolved signal.
 */
@Injectable({providedIn: 'root'})
export class ThemeService {
  private readonly isBrowser = isPlatformBrowser(inject(PLATFORM_ID));

  /** What the user picked. `system` means follow OS preference live. */
  readonly preference = signal<ThemePreference>(this.readStoredPreference());

  /** Accent axis. No `system` value: the OS has no opinion on brand colour. */
  readonly accent = signal<AccentPreference>(this.readStoredAccent());

  /** Live OS preference. Updates when the user flips dark/light at the OS level. */
  private readonly systemPrefersDark = signal<boolean>(this.readSystemPrefersDark());

  /** Effective theme that's actually applied to <body>. */
  readonly resolved = computed<ResolvedTheme>(() => {
    const pref = this.preference();
    if (pref === 'system') return this.systemPrefersDark() ? 'senate' : 'travertine';
    return pref;
  });

  constructor() {
    if (this.isBrowser) {
      // Watch the OS-level preference; only matters when preference === 'system',
      // but keeping the listener live is cheaper than attaching/detaching.
      const mql = window.matchMedia('(prefers-color-scheme: dark)');
      const onChange = (event: MediaQueryListEvent) => this.systemPrefersDark.set(event.matches);
      // Older Safari (<14) lacks addEventListener on MediaQueryList — fall back gracefully.
      if (typeof mql.addEventListener === 'function') {
        mql.addEventListener('change', onChange);
      } else {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        (mql as any).addListener?.(onChange);
      }
    }

    // Apply both appearance axes before reading their CSS token for browser/PWA
    // chrome. One effect keeps the color in sync on theme and accent changes.
    effect(() => {
      this.applyBodyClass(this.resolved());
      this.applyAccentClass(this.accent());
      this.applyBrowserThemeColor();
    });
  }

  /** User picks a theme (or 'system'). Persists locally; body class follows via effect. */
  setPreference(pref: ThemePreference): void {
    this.preference.set(pref);
    if (this.isBrowser) {
      try {
        window.localStorage.setItem(STORAGE_KEY, pref);
      } catch {
        // localStorage might be blocked (private mode, quota); preference still
        // takes effect for the session, just doesn't survive a reload.
      }
    }
  }

  /** User picks an accent. Persists locally; body class follows via effect. */
  setAccent(accent: AccentPreference): void {
    if (!isAccentPreference(accent)) return;
    this.accent.set(accent);
    if (this.isBrowser) {
      try {
        window.localStorage.setItem(ACCENT_STORAGE_KEY, accent);
      } catch {
        // see setPreference — the choice still holds for the session
      }
    }
  }

  private readStoredAccent(): AccentPreference {
    if (!this.isBrowser) return DEFAULT_ACCENT;
    try {
      const raw = window.localStorage.getItem(ACCENT_STORAGE_KEY);
      if (isAccentPreference(raw)) return raw;
    } catch {
      // ignore
    }
    return DEFAULT_ACCENT;
  }

  private readStoredPreference(): ThemePreference {
    if (!this.isBrowser) return 'system';
    try {
      const raw = window.localStorage.getItem(STORAGE_KEY);
      if (!raw) return 'system';
      // Migrate legacy Catppuccin keys in-place so subsequent reads stay quiet.
      if (raw in LEGACY_MIGRATIONS) {
        const migrated = LEGACY_MIGRATIONS[raw];
        try {
          window.localStorage.setItem(STORAGE_KEY, migrated);
        } catch {
          // ignore — migration is best-effort
        }
        return migrated;
      }
      if (VALID_PREFERENCES.has(raw as ThemePreference)) return raw as ThemePreference;
    } catch {
      // ignore
    }
    return 'system';
  }

  private readSystemPrefersDark(): boolean {
    if (!this.isBrowser) return true;
    return window.matchMedia('(prefers-color-scheme: dark)').matches;
  }

  private applyBodyClass(theme: ResolvedTheme): void {
    if (!this.isBrowser) return;
    const body = document.body;
    // Strip every existing theme-* class then add the active one.
    const toRemove: string[] = [];
    body.classList.forEach((c) => {
      if (c.startsWith('theme-')) toRemove.push(c);
    });
    toRemove.forEach((c) => body.classList.remove(c));
    body.classList.add(`theme-${theme}`);
  }

  private applyBrowserThemeColor(): void {
    if (!this.isBrowser) return;
    const color = window.getComputedStyle(document.body).getPropertyValue('--accent-color').trim();
    // Preserve the purple HTML fallback if styles haven't loaded.
    if (!color) return;
    document.querySelectorAll<HTMLMetaElement>('meta[name="theme-color"], meta[name="msapplication-TileColor"]')
      .forEach((meta) => { meta.content = color; });
  }

  private applyAccentClass(accent: AccentPreference): void {
    if (!this.isBrowser) return;
    const body = document.body;
    const toRemove: string[] = [];
    body.classList.forEach((c) => {
      if (c.startsWith('accent-')) toRemove.push(c);
    });
    toRemove.forEach((c) => body.classList.remove(c));
    body.classList.add(`accent-${accent}`);
  }
}
