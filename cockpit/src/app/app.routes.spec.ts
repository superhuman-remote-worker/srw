import {describe, expect, it} from 'vitest';
import {Route} from '@angular/router';
import {routes} from './app.routes';
import {ChatPageComponent} from './views/chat/chat-page.component';
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

  it('loads the connector driver matrix on demand for any signed-in user', async () => {
    const route = routes.find((r) => r.path === 'settings/connector-drivers');
    expect(route?.canActivate).toEqual([authGuard]);
    expect(typeof route?.loadComponent).toBe('function');
    const component = await route!.loadComponent!();
    expect(component).toBe(
      (await import('./views/connector-drivers/connector-drivers-page.component'))
        .ConnectorDriversPageComponent,
    );
  });

  it('loads API key settings on demand behind the existing auth guard', async () => {
    const route = routes.find((r) => r.path === 'settings/api-keys');
    expect(route?.canActivate?.includes(authGuard) ?? false).toBe(true);
    expect(route?.component).toBeUndefined();
    expect(typeof route?.loadComponent).toBe('function');
    const component = await route!.loadComponent!();
    expect(component).toBe(
      (await import('./views/settings/api-keys/api-keys-page.component')).ApiKeysPageComponent,
    );
  });
});

describe('app.routes — workspace templates (Slice A3)', () => {
  it.each([
    ['workspaces', './views/workspaces/workspace-templates-page.component', 'WorkspaceTemplatesPageComponent'],
    ['workspaces/new', './views/workspaces/workspace-template-editor.component', 'WorkspaceTemplateEditorComponent'],
    ['workspaces/:uid', './views/workspaces/workspace-template-editor.component', 'WorkspaceTemplateEditorComponent'],
  ])('loads %s on demand behind the auth guard', async (path, file, name) => {
    const route = routes.find((r) => r.path === path);
    expect(route?.canActivate?.includes(authGuard) ?? false).toBe(true);
    expect(route?.component).toBeUndefined();
    const component = await route!.loadComponent!();
    expect(component).toBe((await import(/* @vite-ignore */ file))[name]);
  });

  it('declares workspaces/new before workspaces/:uid', () => {
    const paths = routes.map((r) => r.path);
    expect(paths.indexOf('workspaces/new')).toBeLessThan(paths.indexOf('workspaces/:uid'));
  });
});

describe('app.routes — lazy pages', () => {
  // The landing page IS ChatPageComponent, so it is in the initial bundle
  // anyway; sessions/:threadId reuses it rather than adding an async hop.
  const EAGER_TOP_LEVEL_PATHS = ['', 'sessions/:threadId'];

  const walk = (list: Route[]): Route[] => list.flatMap((r) => [r, ...walk(r.children ?? [])]);
  const all = walk(routes);

  // Only a top-level route may carry `component`, and only the two Chat routes.
  const eagerOffenders = (tree: Route[]): string[] =>
    walk(tree)
      .filter((r) => r.component)
      .filter((r) => !(tree.includes(r) && EAGER_TOP_LEVEL_PATHS.includes(r.path ?? '\0') && r.component === ChatPageComponent))
      .map((r) => r.path ?? '(pathless)');

  it('gives only the landing page and the session view an eager component', () => {
    const offenders = eagerOffenders(routes);
    expect(offenders, `routes with an eager component: ${offenders.join(', ')}`).toEqual([]);
  });

  it('flags eager components that hide behind a reused path, a pathless route or nesting', () => {
    const synthetic: Route[] = [
      {path: '', component: ChatPageComponent},
      {path: 'x', children: [{path: '', component: ChatPageComponent}]},
      {component: ChatPageComponent, children: []},
      {path: 'sessions/:threadId', component: class Other {}},
    ];
    expect(eagerOffenders(synthetic)).toEqual(['', '(pathless)', 'sessions/:threadId']);
  });

  it.each(EAGER_TOP_LEVEL_PATHS)("renders '%s' with ChatPageComponent", (path) => {
    expect(routes.find((r) => r.path === path)?.component).toBe(ChatPageComponent);
  });

  it('resolves every loadComponent to a component class', async () => {
    const lazy = all.filter((r) => r.loadComponent);
    expect(lazy.length).toBeGreaterThan(0);
    for (const route of lazy) {
      const resolved = await (route.loadComponent as () => Promise<unknown>)();
      expect(typeof resolved, `loadComponent of '${route.path}'`).toBe('function');
    }
  });
});
