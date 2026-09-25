import {describe, expect, it} from 'vitest';
import {routes} from './app.routes';
import {authGuard} from './core/guards/auth.guard';
import {adminGuard} from './core/guards/admin.guard';

/**
 * A plain structural test over the exported `routes` array — no TestBed, no
 * router harness. It exists because Task 9 (admin section shell) nests six
 * previously-flat, individually-guarded admin routes under one `/admin`
 * parent and hoists their `canActivate` up to that parent. That refactor is
 * correct Angular, but the diff *removes* `canActivate` from six route
 * objects, and a mistake there (a guard silently dropped instead of hoisted)
 * is an access-control hole that no build error and no visual check would
 * catch. This file is that check.
 */
const ADMIN_CHILD_PATHS = [
  'models',
  'users',
  'config',
  'grants',
  'usage',
  'capacity',
  // Settings sections only admins can use (navigation_fixed_rail.md §5) —
  // moved under /admin so they inherit the parent's adminGuard.
  'subscriptions',
  'cloud',
];

describe('app.routes — admin section shell', () => {
  const admin = routes.find((r) => r.path === 'admin');

  it('has an /admin parent route', () => {
    expect(admin).toBeDefined();
  });

  // Written as a plain boolean rather than `expect(x).toContain(fn)`: a probe
  // during development found that this vitest/chai version's `toContain`
  // vacuously PASSES when the actual value is `undefined` and the expected
  // value is a function (it correctly throws for a string expected value,
  // just not a function one). `.includes(...) ?? false` has no such matcher
  // edge case — it is a plain boolean, so `toBe(true)` cannot pass vacuously.
  it('gates the /admin parent with both authGuard and adminGuard', () => {
    expect(admin?.canActivate?.includes(authGuard) ?? false).toBe(true);
    expect(admin?.canActivate?.includes(adminGuard) ?? false).toBe(true);
  });

  it('has exactly eight page children plus the empty-path redirect', () => {
    expect(admin?.children).toHaveLength(9);
  });

  // The rail owns the admin sub-navigation now; a component here would bring
  // back a second nav column beside it.
  it('renders no shell component of its own', () => {
    expect(admin?.component).toBeUndefined();
  });

  it.each(['subscriptions', 'cloud'])('renders the %s settings section', (path) => {
    expect(admin?.children?.find((r) => r.path === path)?.data?.['section']).toBe(path);
  });

  it("redirects the empty child path ('') to models", () => {
    const empty = admin?.children?.find((r) => r.path === '');
    expect(empty).toBeDefined();
    expect(empty?.redirectTo).toBe('models');
    expect(empty?.pathMatch).toBe('full');
  });

  it.each(ADMIN_CHILD_PATHS)('has a %s child route', (path) => {
    expect(admin?.children?.find((r) => r.path === path)).toBeDefined();
  });

  // This is the assertion that catches a dropped guard: it passes whether
  // adminGuard lives on the parent (today's design) or on the child
  // (an equally valid alternative) — it only fails when NEITHER carries it,
  // which is the actual access-control hole.
  it.each(ADMIN_CHILD_PATHS)('leaves no admin child (%s) unprotected by adminGuard', (path) => {
    const child = admin?.children?.find((r) => r.path === path);
    const protectedByParent = admin?.canActivate?.includes(adminGuard) ?? false;
    const protectedByChild = child?.canActivate?.includes(adminGuard) ?? false;
    expect(protectedByParent || protectedByChild).toBe(true);
  });

  it.each(ADMIN_CHILD_PATHS)('leaves no admin child (%s) unprotected by authGuard', (path) => {
    const child = admin?.children?.find((r) => r.path === path);
    const protectedByParent = admin?.canActivate?.includes(authGuard) ?? false;
    const protectedByChild = child?.canActivate?.includes(authGuard) ?? false;
    expect(protectedByParent || protectedByChild).toBe(true);
  });

  it('still redirects admin/providers to admin/models', () => {
    const r = routes.find((route) => route.path === 'admin/providers');
    expect(r?.redirectTo).toBe('admin/models');
  });

  it('still redirects admin/llm to admin/models', () => {
    const r = routes.find((route) => route.path === 'admin/llm');
    expect(r?.redirectTo).toBe('admin/models');
  });
});

describe('app.routes — settings sections', () => {
  const SECTIONS = ['general', 'defaults', 'provider-keys', 'notifications', 'mcp'];

  it('sends /settings to the General section', () => {
    const r = routes.find((route) => route.path === 'settings');
    expect(r?.redirectTo).toBe('settings/general');
    expect(r?.pathMatch).toBe('full');
  });

  it.each(SECTIONS)('routes settings/%s to its section behind authGuard', (section) => {
    const r = routes.find((route) => route.path === `settings/${section}`);
    expect(r?.data?.['section']).toBe(section);
    expect(r?.canActivate?.includes(authGuard) ?? false).toBe(true);
  });

  it.each(['settings/api-keys', 'settings/ssh-keys'])('keeps the %s page', (path) => {
    expect(routes.find((route) => route.path === path)).toBeDefined();
  });
});
