import {afterEach, beforeEach, describe, expect, it, vi} from 'vitest';
import {TestBed} from '@angular/core/testing';
import {ThemeService} from './theme.service';

interface MockMql {
  matches: boolean;
  listeners: Array<(event: {matches: boolean}) => void>;
  addEventListener: (type: 'change', cb: (event: {matches: boolean}) => void) => void;
  removeEventListener: (type: 'change', cb: (event: {matches: boolean}) => void) => void;
  fire: (matches: boolean) => void;
}

function makeMockMql(initialMatches: boolean): MockMql {
  const mql: MockMql = {
    matches: initialMatches,
    listeners: [],
    addEventListener: (_type, cb) => {
      mql.listeners.push(cb);
    },
    removeEventListener: (_type, cb) => {
      mql.listeners = mql.listeners.filter((l) => l !== cb);
    },
    fire: (matches: boolean) => {
      mql.matches = matches;
      mql.listeners.forEach((cb) => cb({matches}));
    },
  };
  return mql;
}

describe('ThemeService', () => {
  let mql: MockMql;
  const originalMatchMedia = window.matchMedia;

  beforeEach(() => {
    TestBed.resetTestingModule();
    document.body.className = '';
    window.localStorage.clear();
    mql = makeMockMql(true);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (window as any).matchMedia = vi.fn().mockReturnValue(mql);
  });

  afterEach(() => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (window as any).matchMedia = originalMatchMedia;
    document.body.className = '';
  });

  function makeService(): ThemeService {
    TestBed.configureTestingModule({providers: [ThemeService]});
    const service = TestBed.inject(ThemeService);
    TestBed.tick();
    return service;
  }

  describe('initial state', () => {
    it('defaults to system when no stored preference (resolves via OS)', () => {
      const service = makeService();
      expect(service.preference()).toBe('system');
      // Default mock has prefers-color-scheme: dark → senate.
      expect(service.resolved()).toBe('senate');
      expect(document.body.classList.contains('theme-senate')).toBe(true);
      expect(document.body.classList.contains('theme-travertine')).toBe(false);
    });

    it('defaults to system and resolves to travertine on light OS', () => {
      mql = makeMockMql(false);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (window as any).matchMedia = vi.fn().mockReturnValue(mql);
      const service = makeService();
      expect(service.preference()).toBe('system');
      expect(service.resolved()).toBe('travertine');
      expect(document.body.classList.contains('theme-travertine')).toBe(true);
    });

    it('reads stored preference from localStorage', () => {
      window.localStorage.setItem('cockpit:theme', 'travertine');
      const service = makeService();
      expect(service.preference()).toBe('travertine');
      expect(service.resolved()).toBe('travertine');
      expect(document.body.classList.contains('theme-travertine')).toBe(true);
    });

    it('ignores invalid stored values and falls back to system', () => {
      window.localStorage.setItem('cockpit:theme', 'gibberish');
      const service = makeService();
      expect(service.preference()).toBe('system');
    });

    it('reads stored Roman theme preference', () => {
      window.localStorage.setItem('cockpit:theme', 'senate');
      const service = makeService();
      expect(service.preference()).toBe('senate');
      expect(service.resolved()).toBe('senate');
      expect(document.body.classList.contains('theme-senate')).toBe(true);
    });

    it('migrates legacy "dark" preference to senate', () => {
      window.localStorage.setItem('cockpit:theme', 'dark');
      const service = makeService();
      expect(service.preference()).toBe('senate');
      expect(document.body.classList.contains('theme-senate')).toBe(true);
      // Migration should rewrite the stored value so subsequent reads are clean.
      expect(window.localStorage.getItem('cockpit:theme')).toBe('senate');
    });

    it('migrates legacy "light" preference to travertine', () => {
      window.localStorage.setItem('cockpit:theme', 'light');
      const service = makeService();
      expect(service.preference()).toBe('travertine');
      expect(document.body.classList.contains('theme-travertine')).toBe(true);
      expect(window.localStorage.getItem('cockpit:theme')).toBe('travertine');
    });

    it('migrates retired "praetorian" preference to senate', () => {
      window.localStorage.setItem('cockpit:theme', 'praetorian');
      const service = makeService();
      expect(service.preference()).toBe('senate');
      expect(document.body.classList.contains('theme-senate')).toBe(true);
      expect(document.body.classList.contains('theme-praetorian')).toBe(false);
      expect(window.localStorage.getItem('cockpit:theme')).toBe('senate');
    });
  });

  describe('setPreference', () => {
    it('applies senate class on body', () => {
      const service = makeService();
      service.setPreference('senate');
      TestBed.tick();
      expect(document.body.classList.contains('theme-senate')).toBe(true);
      expect(document.body.classList.contains('theme-travertine')).toBe(false);
    });

    it('applies travertine class on body', () => {
      const service = makeService();
      service.setPreference('travertine');
      TestBed.tick();
      expect(document.body.classList.contains('theme-travertine')).toBe(true);
      expect(document.body.classList.contains('theme-senate')).toBe(false);
    });

    it('persists choice to localStorage', () => {
      const service = makeService();
      service.setPreference('travertine');
      expect(window.localStorage.getItem('cockpit:theme')).toBe('travertine');

      service.setPreference('system');
      expect(window.localStorage.getItem('cockpit:theme')).toBe('system');
    });

    it('strips previous theme class when switching', () => {
      const service = makeService();
      service.setPreference('senate');
      TestBed.tick();
      expect(document.body.classList.contains('theme-senate')).toBe(true);

      service.setPreference('travertine');
      TestBed.tick();
      expect(document.body.classList.contains('theme-travertine')).toBe(true);
      expect(document.body.classList.contains('theme-senate')).toBe(false);
    });
  });

  describe('system preference', () => {
    it('resolves to senate when system prefers dark', () => {
      mql = makeMockMql(true);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (window as any).matchMedia = vi.fn().mockReturnValue(mql);
      const service = makeService();
      service.setPreference('system');
      TestBed.tick();
      expect(service.resolved()).toBe('senate');
      expect(document.body.classList.contains('theme-senate')).toBe(true);
    });

    it('resolves to travertine when system prefers light', () => {
      mql = makeMockMql(false);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (window as any).matchMedia = vi.fn().mockReturnValue(mql);
      const service = makeService();
      service.setPreference('system');
      TestBed.tick();
      expect(service.resolved()).toBe('travertine');
      expect(document.body.classList.contains('theme-travertine')).toBe(true);
    });

    it('flips body class when system preference changes (preference=system)', () => {
      mql = makeMockMql(true);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (window as any).matchMedia = vi.fn().mockReturnValue(mql);
      const service = makeService();
      service.setPreference('system');
      TestBed.tick();
      expect(service.resolved()).toBe('senate');

      mql.fire(false);
      TestBed.tick();
      expect(service.resolved()).toBe('travertine');
      expect(document.body.classList.contains('theme-travertine')).toBe(true);
      expect(document.body.classList.contains('theme-senate')).toBe(false);
    });

    it('does not flip when preference is explicit (system OS change ignored)', () => {
      mql = makeMockMql(true);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (window as any).matchMedia = vi.fn().mockReturnValue(mql);
      const service = makeService();
      service.setPreference('travertine');
      TestBed.tick();
      expect(service.resolved()).toBe('travertine');

      mql.fire(true);
      TestBed.tick();
      expect(service.resolved()).toBe('travertine');
    });
  });
  describe('accent axis', () => {
    it('defaults to tyrian and stamps accent-tyrian on body', () => {
      const service = makeService();
      expect(service.accent()).toBe('tyrian');
      expect(document.body.classList.contains('accent-tyrian')).toBe(true);
    });

    it('reads a stored accent from localStorage', () => {
      window.localStorage.setItem('cockpit:accent', 'graphite');
      const service = makeService();
      expect(service.accent()).toBe('graphite');
      expect(document.body.classList.contains('accent-graphite')).toBe(true);
    });

    it('ignores an unknown stored accent and falls back to tyrian', () => {
      window.localStorage.setItem('cockpit:accent', 'teal');
      expect(makeService().accent()).toBe('tyrian');
    });

    it('setAccent swaps the accent class, persists, and leaves the theme class alone', () => {
      const service = makeService();
      service.setPreference('senate');
      service.setAccent('porphyry');
      TestBed.tick();
      expect(document.body.classList.contains('accent-porphyry')).toBe(true);
      expect(document.body.classList.contains('accent-tyrian')).toBe(false);
      expect(document.body.classList.contains('theme-senate')).toBe(true);
      expect(window.localStorage.getItem('cockpit:accent')).toBe('porphyry');
    });

    it('setAccent rejects values outside the accent set', () => {
      const service = makeService();
      service.setAccent('teal' as never);
      TestBed.tick();
      expect(service.accent()).toBe('tyrian');
      expect(document.body.classList.contains('accent-tyrian')).toBe(true);
    });
  });

  describe('PWA theme color', () => {
    let styles: HTMLStyleElement;
    let meta: HTMLMetaElement;

    beforeEach(() => {
      styles = document.createElement('style');
      // Distinct values prove that metadata follows the applied CSS token,
      // including when the app theme overrides the OS preference.
      styles.textContent = `
        .theme-travertine.accent-tyrian { --accent-color: #5f499c; }
        .theme-senate.accent-tyrian { --accent-color: #7f65ca; }
        .theme-senate.accent-porphyry { --accent-color: #cc4647; }
      `;
      meta = document.createElement('meta');
      meta.name = 'theme-color';
      meta.content = '#5f499c';
      document.head.append(styles, meta);
    });

    afterEach(() => {
      styles.remove();
      meta.remove();
    });

    it('restores the stored accent and updates it without a reload', () => {
      window.localStorage.setItem('cockpit:accent', 'porphyry');
      const service = makeService();
      expect(meta.content).toBe('#cc4647');

      service.setAccent('tyrian');
      TestBed.tick();
      expect(meta.content).toBe('#7f65ca');
    });

    it('follows system changes and then an explicit app theme', () => {
      const service = makeService();
      expect(meta.content).toBe('#7f65ca');
      mql.fire(false);
      TestBed.tick();
      expect(meta.content).toBe('#5f499c');

      service.setPreference('senate');
      TestBed.tick();
      expect(meta.content).toBe('#7f65ca');
    });

    it('keeps the static fallback when theme styles are unavailable', () => {
      styles.remove();
      makeService();
      expect(meta.content).toBe('#5f499c');
    });
  });
});
