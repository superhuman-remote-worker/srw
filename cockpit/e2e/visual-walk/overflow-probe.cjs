// Overflow probe — a review aid, not a test. Signs in to a running cockpit
// (defaults to the k3d stack at https://localhost with the `test` fixture user),
// screenshots every route at a desktop and a phone viewport, opens the rail
// menus, and runs an in-page probe on each page that reports:
//   page-hscroll   the document scrolls sideways
//   past-viewport  an element's right edge lies beyond the viewport
//   spill-x/-y     content wider/taller than its box with overflow visible
//   clip-x         text cut by overflow:hidden with no ellipsis
// Output: <out>/<route>--<viewport>.png, probe.json, console-errors.json.
//
//   node e2e/visual-walk/overflow-probe.cjs
//   PROBE_ROUTES=/jobs,/projects PROBE_MENUS=0 node e2e/visual-walk/overflow-probe.cjs
//   PROBE_THREAD=<id> PROBE_PROJECT=<id>   adds /sessions/<id> and /projects/<id>
//
// Expect some noise: visually-hidden live regions (clip-x on .sr-only /
// .status-announce), the collapsed canvas pane, a 3px glyph spill on the
// Cinzel brand mark, and horizontally scrolling chip strips on phones.
const {chromium} = require('playwright');
const fs = require('node:fs');
const path = require('node:path');

const BASE = process.env.PROBE_BASE || 'https://localhost';
const OUT = process.env.PROBE_OUT || path.join(process.cwd(), 'playwright-report', 'overflow-probe');
const ONLY = (process.env.PROBE_ONLY || '').split(',').filter(Boolean);
const USER = process.env.PROBE_USER || 'test';
const PASSWORD = process.env.PROBE_PASSWORD || 'test';
const THREAD = process.env.PROBE_THREAD || '';
const PROJECT = process.env.PROBE_PROJECT || '';
const MENUS = process.env.PROBE_MENUS !== '0';

const DEFAULT_ROUTES = [
  '/', '/sessions', '/sessions/new', ...(THREAD ? [`/sessions/${THREAD}`] : []),
  '/jobs', '/jobs/new', '/jobs/review', '/inbox',
  '/projects', ...(PROJECT ? [`/projects/${PROJECT}`] : []),
  '/datasources', '/contacts', '/experts', '/experts/new', '/skills', '/skills/new',
  '/automations', '/settings/general', '/settings/defaults', '/settings/provider-keys',
  '/settings/notifications', '/settings/mcp', '/settings/api-keys', '/settings/ssh-keys',
  '/admin/models', '/admin/subscriptions', '/admin/users', '/admin/config', '/admin/grants',
  '/admin/cloud', '/admin/usage', '/admin/capacity',
  '/workbench',
];
const ROUTES = process.env.PROBE_ROUTES ? process.env.PROBE_ROUTES.split(',') : DEFAULT_ROUTES;
const VIEWPORTS = [
  {name: 'desktop', width: 1440, height: 900},
  {name: 'mobile', width: 390, height: 844},
];

const slug = (r) =>
  r === '/' ? 'home' : r.replace(/^\//, '').replace(/[/:]/g, '_').replace(/[0-9a-f-]{36}/, 'id');

const PROBE = () => {
  const vw = document.documentElement.clientWidth;
  const vh = document.documentElement.clientHeight;
  const desc = (el) => {
    const cls = [...el.classList].slice(0, 3).join('.');
    const txt = (el.textContent || '').trim().replace(/\s+/g, ' ').slice(0, 40);
    return `${el.tagName.toLowerCase()}${el.id ? '#' + el.id : ''}${cls ? '.' + cls : ''}${txt ? ' "' + txt + '"' : ''}`;
  };
  const chain = (el) => {
    const parts = [];
    let cur = el;
    for (let i = 0; cur && i < 3; i++) {
      parts.push(cur.tagName.toLowerCase() + [...cur.classList].slice(0, 2).map((c) => '.' + c).join(''));
      cur = cur.parentElement;
    }
    return parts.join(' < ');
  };
  const out = [];
  const docScrollX = document.documentElement.scrollWidth - vw;
  if (docScrollX > 1) out.push({kind: 'page-hscroll', by: docScrollX});
  for (const el of document.querySelectorAll('body *')) {
    if (!(el instanceof HTMLElement)) continue;
    const cs = getComputedStyle(el);
    if (cs.display === 'none' || cs.visibility === 'hidden' || cs.opacity === '0') continue;
    const r = el.getBoundingClientRect();
    if (r.width < 1 || r.height < 1) continue;
    if (r.bottom < 0 || r.top > vh) continue;
    if (r.right > vw + 1 && r.left < vw && cs.position !== 'fixed') {
      out.push({kind: 'past-viewport', el: desc(el), in: chain(el.parentElement), right: Math.round(r.right), vw});
    }
    if (el.clientWidth > 0 && el.scrollWidth > el.clientWidth + 1) {
      const ox = cs.overflowX;
      if (ox === 'visible') {
        out.push({kind: 'spill-x', el: desc(el), in: chain(el.parentElement), scrollW: el.scrollWidth, clientW: el.clientWidth});
      } else if ((ox === 'hidden' || ox === 'clip') && cs.textOverflow !== 'ellipsis' && !el.querySelector('*')) {
        out.push({kind: 'clip-x', el: desc(el), in: chain(el.parentElement), scrollW: el.scrollWidth, clientW: el.clientWidth});
      }
    }
    if (el.clientHeight > 0 && el.scrollHeight > el.clientHeight + 2 && cs.overflowY === 'visible' && cs.display !== 'inline') {
      out.push({kind: 'spill-y', el: desc(el), in: chain(el.parentElement), scrollH: el.scrollHeight, clientH: el.clientHeight});
    }
  }
  return out.slice(0, 60);
};

async function login(page) {
  await page.goto(BASE + '/', {waitUntil: 'domcontentloaded'});
  const username = page.locator('#username');
  // The rail is `attached` but not visible on a phone (collapsed drawer).
  const shell = page.locator('app-root app-sidebar').first();
  await Promise.race([
    username.waitFor({state: 'visible', timeout: 30_000}),
    shell.waitFor({state: 'attached', timeout: 30_000}),
  ]);
  if (await username.isVisible().catch(() => false)) {
    await username.fill(USER);
    await page.locator('#password').fill(PASSWORD);
    await page.locator('#kc-login').click();
  }
  await shell.waitFor({state: 'attached', timeout: 60_000});
}

async function capture(page, name, vp, report) {
  await page.waitForTimeout(1_500);
  await page.screenshot({path: path.join(OUT, `${name}--${vp}.png`)});
  const findings = await page.evaluate(PROBE).catch((e) => [{kind: 'probe-error', msg: String(e)}]);
  report.push({name, vp, findings});
  process.stdout.write(`${name}--${vp}: ${findings.length} findings\n`);
}

(async () => {
  fs.mkdirSync(OUT, {recursive: true});
  const browser = await chromium.launch();
  const report = [];
  const consoleErrors = [];
  for (const vp of VIEWPORTS) {
    if (ONLY.length && !ONLY.includes(vp.name)) continue;
    const ctx = await browser.newContext({
      viewport: {width: vp.width, height: vp.height},
      ignoreHTTPSErrors: true,
      locale: process.env.PROBE_LOCALE || 'en-US',
      serviceWorkers: 'block',
    });
    const page = await ctx.newPage();
    page.on('pageerror', (e) => consoleErrors.push({vp: vp.name, url: page.url(), err: String(e).slice(0, 300)}));
    page.on('console', (m) => {
      if (m.type() === 'error') consoleErrors.push({vp: vp.name, url: page.url(), err: m.text().slice(0, 300)});
    });
    await login(page);
    for (const route of ROUTES) {
      // 'load' + a settle: the app holds sockets and polls, so 'networkidle' never fires.
      await page.goto(BASE + route, {waitUntil: 'load'}).catch((e) => process.stdout.write(`goto ${route} failed: ${e}\n`));
      await capture(page, slug(route), vp.name, report);
    }
    if (MENUS) {
      await page.goto(BASE + '/', {waitUntil: 'load'});
      await page.waitForTimeout(800);
      if (vp.name === 'mobile') {
        const toggle = page.locator('app-sidebar-toggle button').first();
        if (await toggle.count()) {
          await toggle.click();
          await capture(page, 'home-drawer-open', vp.name, report);
          await page.keyboard.press('Escape');
        }
      }
      for (const [name, sel] of [['home-account-menu', 'app-rail-account-menu button']]) {
        const trigger = page.locator(sel).first();
        if (await trigger.count()) {
          await trigger.click();
          await capture(page, name, vp.name, report);
          await page.keyboard.press('Escape');
          await page.waitForTimeout(300);
        }
      }
    }
    await ctx.close();
  }
  await browser.close();
  fs.writeFileSync(path.join(OUT, 'probe.json'), JSON.stringify(report, null, 1));
  fs.writeFileSync(path.join(OUT, 'console-errors.json'), JSON.stringify(consoleErrors, null, 1));
  process.stdout.write(`done: ${report.length} captures, ${consoleErrors.length} console errors -> ${OUT}\n`);
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
