// The signed-in dashboard's testbed (S3). Serves apps/inspector/build at its mount with the CSP durable-server
// computes, answers every API call from the stateful mock in mocks.mjs (saasMock), and walks every signed-out and
// signed-in page: screenshots at 1440 and 390 in light and dark, and asserts no horizontal overflow, no CSP
// violation, no console error, and that the dashboard sent what the server requires (CSRF on mutations, the
// workspace header on flat routes only, no Authorization header). Also checks the three boot modes.
//
//   cd apps/inspector && npm run build
//   node testbed/saas.mjs build <out-dir> <port> [--quick | --axe] [--only=<name>]
//
// --axe runs a WCAG 2.1 AA scan of every page (needs AXE_PATH=<axe-core/axe.min.js>).
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import * as M from './mocks.mjs';

const { chromium } = await import('playwright');

const args = process.argv.slice(2);
const [BUILD_ARG, OUT, PORT_ARG] = args;
const FLAGS = new Set(args.slice(3).filter((a) => !a.startsWith('--only=')));
const ONLY = args.find((a) => a.startsWith('--only='))?.slice(7) ?? null;
const BUILD = path.resolve(BUILD_ARG ?? '');
const PORT = Number(PORT_ARG);
if (!BUILD_ARG || !OUT || !(PORT > 1024 && PORT < 65536)) {
  console.error('usage: node testbed/saas.mjs <build> <out> <port> [--quick|--axe] [--only=name]');
  process.exit(2);
}
fs.mkdirSync(OUT, { recursive: true });

function uiCsp(html) {
  let hashes = '';
  let rest = html;
  for (;;) {
    const start = rest.indexOf('<script>');
    if (start < 0) break;
    const body = rest.slice(start + '<script>'.length);
    const end = body.indexOf('</script>');
    if (end < 0) break;
    hashes += ` 'sha256-${crypto.createHash('sha256').update(Buffer.from(body.slice(0, end), 'utf8')).digest('base64')}'`;
    rest = body.slice(end + '</script>'.length);
  }
  return `default-src 'none'; script-src 'self'${hashes}; style-src 'self' 'unsafe-inline'; img-src 'self' data:; font-src 'self'; connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'`;
}
const INDEX = fs.readFileSync(path.join(BUILD, 'index.html'), 'utf8');
const CSP = uiCsp(INDEX);
const MOUNT = INDEX.match(/assets: "([^"]*)"/)?.[1] || '/app';
const TYPES = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.svg': 'image/svg+xml', '.woff2': 'font/woff2', '.json': 'application/json', '.txt': 'text/plain' };
const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://x');
  const headers = { 'content-security-policy': CSP, 'x-content-type-options': 'nosniff', 'referrer-policy': 'no-referrer', 'x-frame-options': 'DENY' };
  if (!url.pathname.startsWith(MOUNT)) { res.writeHead(404).end(); return; }
  let rel = decodeURIComponent(url.pathname.slice(MOUNT.length)) || '/';
  if (rel.endsWith('/')) rel += 'index.html';
  const file = path.join(BUILD, path.normalize(rel));
  if (file.startsWith(path.resolve(BUILD)) && fs.existsSync(file) && fs.statSync(file).isFile()) {
    res.writeHead(200, { ...headers, 'content-type': TYPES[path.extname(file)] ?? 'application/octet-stream' });
    fs.createReadStream(file).pipe(res);
  } else res.writeHead(200, { ...headers, 'content-type': 'text/html' }).end(INDEX);
});
await new Promise((r) => server.listen(PORT, '127.0.0.1', r));
console.log(`static server pid ${process.pid} on ${PORT}, mount ${MOUNT}; CSP script hashes: ${(CSP.match(/sha256-/g) ?? []).length}`);

const browser = await chromium.launch();
const failures = [];
const shots = [];
const base = `http://127.0.0.1:${PORT}${MOUNT}/`;

async function context({ theme, width, mock = {} }) {
  const ctx = await browser.newContext({ viewport: { width, height: 900 }, deviceScaleFactor: width === 390 ? 2 : 1, colorScheme: 'light' });
  await ctx.addInitScript(([t]) => {
    if (t) localStorage.setItem('theme', t);
    window.__csp = [];
    document.addEventListener('securitypolicyviolation', (e) => window.__csp.push(`${e.violatedDirective} ${e.blockedURI}`));
  }, [theme === 'dark' ? 'dark' : null]);
  await ctx.grantPermissions(['clipboard-read', 'clipboard-write'], { origin: `http://127.0.0.1:${PORT}` });
  const page = await ctx.newPage();
  page.__errors = [];
  page.on('console', (m) => { if (m.type() === 'error') page.__errors.push(m.text()); });
  page.on('pageerror', (e) => page.__errors.push(String(e)));
  const api = M.saasMock(mock);
  await page.route((url) => !url.pathname.startsWith(MOUNT), async (route) => {
    const req = route.request();
    const url = new URL(req.url());
    let body = null;
    try { body = req.postData() ? JSON.parse(req.postData()) : null; } catch { body = null; }
    const r = api.handle({ method: req.method(), path: url.pathname, query: url.searchParams, headers: req.headers(), body });
    if (r.body === null) return route.fulfill({ status: r.status, body: '' });
    return route.fulfill({ status: r.status, contentType: 'application/json', headers: { 'cache-control': 'no-store', ...(r.headers ?? {}) }, body: JSON.stringify(r.body) });
  });
  return { ctx, page, api };
}

async function health(page, api, tag) {
  const csp = await page.evaluate(() => window.__csp);
  if (csp.length) failures.push(`${tag}: CSP violations: ${csp.join('; ')}`);
  const errs = page.__errors.filter((e) => !/status of (4\d\d|503)/.test(e));
  if (errs.length) failures.push(`${tag}: console errors: ${errs.join(' | ')}`);
  if (api.state.violations.length) failures.push(`${tag}: request contract: ${[...new Set(api.state.violations)].join(' | ')}`);
}

async function snap(page, name, width, { full = true } = {}) {
  await page.waitForTimeout(250);
  const file = `${OUT}/${name}.png`;
  await page.screenshot({ path: file, fullPage: full });
  shots.push(file);
  const sw = await page.evaluate(() => document.scrollingElement.scrollWidth);
  if (sw > width) failures.push(`${name}: horizontal overflow ${sw} > ${width}`);
}

async function go(page, hash) {
  await page.goto(`${base}${hash}`, { waitUntil: 'networkidle' });
}

const T = M.TOKEN;
// name, hash, mock options, what to wait for, then optional actions (each may snap extra states)
const PAGES = [
  // signed out
  { name: 'sign-in', hash: '#/sign-in', mock: { signedIn: false }, wait: 'h1:has-text("Sign in")' },
  { name: 'sign-in-wrong', hash: '#/sign-in', mock: { signedIn: false }, wait: '#email', act: async (p) => {
    await p.fill('#email', 'ada@example.com'); await p.fill('#password', 'not the password'); await p.click('button[type=submit]');
    await p.waitForSelector('.banner.error');
  } },
  { name: 'sign-up', hash: '#/sign-up', mock: { signedIn: false }, wait: '#email', act: async (p) => {
    await p.fill('#email', 'ada@'); await p.fill('#password', 'short'); await p.click('button[type=submit]');
    await p.waitForSelector('.field-hint.bad');
  } },
  { name: 'verify-sent', hash: '#/sign-up', mock: { signedIn: false }, wait: '#email', act: async (p) => {
    await p.fill('#email', 'new.person@example.com'); await p.fill('#password', 'a long enough passphrase'); await p.click('button[type=submit]');
    await p.waitForSelector('h1:has-text("Check your inbox")');
  } },
  { name: 'verify', hash: `#/verify?token=${T}`, mock: { signedIn: false }, wait: 'h1:has-text("Confirm your email")', act: async (p, w, api) => {
    if (api.state.calls.some((c) => c.path === '/auth/verify')) throw new Error('verify was POSTed on load');
  } },
  { name: 'verify-wrong', hash: `#/verify?token=${T}`, mock: { signedIn: false }, wait: '#password', act: async (p) => {
    await p.fill('#password', 'not the sign-up password'); await p.click('button[type=submit]'); await p.waitForSelector('#password-hint.bad');
  } },
  { name: 'verify-ok', hash: `#/verify?token=${T}`, mock: { signedIn: false }, wait: '#password', act: async (p, w, api) => {
    await p.fill('#password', M.PASSWORD); await p.click('button[type=submit]');
    await p.waitForSelector('h1:has-text("Sign in")'); await p.waitForSelector('.banner.notice:has-text("Email verified")');
    const call = api.state.calls.find((c) => c.path === '/auth/verify');
    if (!call || call.headers['authorization']) throw new Error('verify must POST without an Authorization header');
    const where = await p.evaluate(() => location.hash);
    if (where.includes('token')) throw new Error(`the token stayed in the address bar: ${where}`);
  } },
  { name: 'verify-gone', hash: `#/verify?token=${M.DEAD_TOKEN}`, mock: { signedIn: false }, wait: '#password', act: async (p) => {
    await p.fill('#password', M.PASSWORD); await p.click('button[type=submit]'); await p.waitForSelector('h1:has-text("no longer valid")');
  } },
  { name: 'forgot', hash: '#/forgot', mock: { signedIn: false }, wait: '#email' },
  { name: 'forgot-sent', hash: '#/forgot', mock: { signedIn: false }, wait: '#email', act: async (p) => {
    await p.fill('#email', 'ada@example.com'); await p.click('button[type=submit]'); await p.waitForSelector('h1:has-text("Check your inbox")');
  } },
  { name: 'reset', hash: `#/reset?token=${T}`, mock: { signedIn: false }, wait: '#password' },
  { name: 'reset-done', hash: `#/reset?token=${T}`, mock: { signedIn: false }, wait: '#password', act: async (p) => {
    await p.fill('#password', 'a brand new passphrase'); await p.click('button[type=submit]');
    await p.waitForSelector('h1:has-text("Sign in")'); await p.waitForSelector('.banner.notice:has-text("Password changed")');
    const where = await p.evaluate(() => location.hash);
    if (where !== '#/sign-in') throw new Error(`reset should end at sign-in (got ${where})`);
  } },
  { name: 'reset-gone', hash: `#/reset?token=${M.DEAD_TOKEN}`, mock: { signedIn: false }, wait: '#password', act: async (p) => {
    await p.fill('#password', 'a brand new passphrase'); await p.click('button[type=submit]'); await p.waitForSelector('h1:has-text("no longer valid")');
  } },
  { name: 'accept-invite-signed-out', hash: `#/accept-invite?token=${M.INVITE_TOKEN}`, mock: { signedIn: false }, wait: 'h1:has-text("invited")' },
  { name: 'boot-error', hash: '#/overview', mock: { bootStatus: 503 }, wait: 'h1:has-text("cannot reach")' },
  // signed in
  { name: 'first-workspace', hash: '#/overview', mock: { workspaces: 'none' }, wait: '#first-ws-title' },
  { name: 'verify-wall', hash: '#/overview', mock: { workspaces: 'none', verified: false }, wait: '#wall-title' },
  { name: 'overview-setup', hash: '#/overview', mock: { fresh: true, workspaces: 'one' }, wait: '#ob-title' },
  { name: 'overview-setup-fallback', hash: '#/overview', mock: { fresh: true, workspaces: 'one', onboardingRoute: false }, wait: '#ob-title' },
  { name: 'overview', hash: '#/overview', mock: {}, wait: '.chart svg .ylab' },
  { name: 'switcher', hash: '#/overview', mock: {}, wait: '.chart svg .ylab', full: false, act: async (p, w) => {
    if (w === 390) await p.getByRole('button', { name: 'Menu', exact: true }).click();
    await p.locator('.ws-btn').click(); await p.waitForSelector('.pop');
  } },
  { name: 'account-menu', hash: '#/overview', mock: {}, wait: '.chart svg .ylab', full: false, act: async (p, w) => {
    if (w === 390) await p.getByRole('button', { name: 'Menu', exact: true }).click();
    await p.locator('.acct-btn').click(); await p.waitForSelector('#acct-menu');
    await p.locator('#acct-menu').scrollIntoViewIfNeeded();
  } },
  { name: 'runs', hash: '#/runs', mock: {}, wait: 'table.runlist tbody tr' },
  { name: 'run', hash: '#/runs/10416', mock: {}, wait: '.tl li' },
  { name: 'keys', hash: '#/keys', mock: {}, wait: '.rows li' },
  { name: 'keys-created', hash: '#/keys', mock: {}, wait: '.rows li', full: false, act: async (p) => {
    await p.fill('#key-name', 'payments-worker'); await p.click('form button[type=submit]'); await p.waitForSelector('dialog[open] .copy-value');
  } },
  { name: 'keys-viewer', hash: '#/keys', mock: { role: 'viewer', workspaces: 'one' }, wait: '.rows li' },
  { name: 'handlers', hash: '#/handlers', mock: {}, wait: '.rows li' },
  { name: 'handlers-test', hash: '#/handlers', mock: {}, wait: '.rows li', act: async (p) => {
    const row = p.locator('.rows li', { hasText: 'send-receipt' });
    await row.getByRole('button', { name: 'Test call' }).click();
    await row.locator('form button[type=submit]').click();
    await p.waitForSelector('.test-result');
  } },
  { name: 'handlers-empty', hash: '#/handlers', mock: { fresh: true, workspaces: 'one' }, wait: '.register' },
  { name: 'workflows', hash: '#/workflows', mock: {}, wait: '.rows li' },
  { name: 'workflows-empty', hash: '#/workflows', mock: { fresh: true, workspaces: 'one' }, wait: '#sample-title' },
  { name: 'workflow', hash: `#/workflows/${M.WF_NIGHTLY}`, mock: {}, wait: '#versions-title' },
  { name: 'usage', hash: '#/usage', mock: {}, wait: '.meters li' },
  { name: 'team', hash: '#/team', mock: {}, wait: '.rows li' },
  { name: 'team-viewer', hash: '#/team', mock: { role: 'viewer', workspaces: 'one' }, wait: '.rows li' },
  { name: 'settings', hash: '#/settings', mock: {}, wait: '#sess-title' },
  { name: 'accept-invite', hash: `#/accept-invite?token=${M.INVITE_TOKEN}`, mock: {}, wait: 'h1:has-text("Join")' },
  { name: 'accept-invite-gone', hash: `#/accept-invite?token=${M.DEAD_INVITE}`, mock: {}, wait: 'h1:has-text("Join")', act: async (p) => {
    await p.getByRole('button', { name: /^Accept/ }).click(); await p.waitForSelector('h1:has-text("no longer valid")');
  } },
  { name: 'team-limit', hash: '#/team', mock: {}, wait: '.rows li', full: false, act: async (p) => {
    await p.fill('#inv-email', 'new.colleague@example.com'); await p.click('form button[type=submit]'); await p.waitForSelector('.banner.warn:has-text("plan_limit")');
  } },
  { name: 'team-invited', hash: '#/team', mock: {}, wait: '.rows li', full: false, act: async (p) => {
    const ken = p.locator('.rows li', { hasText: 'ken@example.com' });
    await ken.getByRole('button', { name: 'Revoke' }).click();
    await ken.getByRole('button', { name: 'Revoke', exact: true }).click();
    await p.waitForSelector('text=No pending invites');
    await p.fill('#inv-email', 'new.colleague@example.com'); await p.click('form button[type=submit]'); await p.waitForSelector('dialog[open] .copy-value');
    const v = await p.locator('dialog[open] .copy-value').textContent();
    if (!v?.includes('/app/#/accept-invite?token=dri_')) throw new Error(`invite link is not a dashboard link: ${v}`);
  } },
];

const quick = FLAGS.has('--quick');
const list = ONLY ? PAGES.filter((p) => p.name === ONLY || p.name.startsWith(`${ONLY}`)) : PAGES;

try {
  if (FLAGS.has('--axe')) {
    if (!process.env.AXE_PATH) { console.error('--axe needs AXE_PATH=<path to axe-core/axe.min.js>'); process.exit(2); }
    const AXE = fs.readFileSync(process.env.AXE_PATH, 'utf8');
    let total = 0;
    let scanned = 0;
    for (const theme of ['light', 'dark']) for (const width of [1440, 390]) for (const pg of list) {
      const { ctx, page, api } = await context({ theme, width, mock: pg.mock });
      await go(page, pg.hash);
      await page.waitForSelector(pg.wait, { timeout: 10_000 });
      if (pg.act) await pg.act(page, width, api);
      await page.waitForTimeout(300);
      const r = await page.evaluate(async (src) => {
        if (!window.axe) (0, eval)(src);
        const res = await window.axe.run(document, { runOnly: { type: 'tag', values: ['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'] } });
        return res.violations.map((v) => `${v.id} (${v.impact}): ${v.nodes.length} × ${v.nodes.slice(0, 3).map((n) => n.target.join(' ')).join(' | ')}`);
      }, AXE);
      scanned++;
      total += r.length;
      if (r.length) console.log(`${theme}-${width} ${pg.name}:\n  ${r.join('\n  ')}`);
      await ctx.close();
    }
    console.log(`axe: ${scanned} page states scanned, ${total} violation group(s)`);
    process.exitCode = total ? 4 : 0;
  } else if (FLAGS.has('--behaviour')) {
    await behaviour();
  } else {
    for (const theme of quick ? ['light'] : ['light', 'dark']) for (const width of quick ? [1440] : [1440, 390]) {
      for (const pg of list) {
        const tag = `${theme}-${width}-${pg.name}`;
        const { ctx, page, api } = await context({ theme, width, mock: pg.mock });
        try {
          await go(page, pg.hash);
          await page.waitForSelector(pg.wait, { timeout: 10_000 });
          if (pg.act) await pg.act(page, width, api);
          await snap(page, tag, width, { full: pg.full !== false });
          await health(page, api, tag);
        } catch (e) {
          failures.push(`${tag}: ${String(e).split('\n')[0]}`);
          await page.screenshot({ path: `${OUT}/FAILED-${tag}.png`, fullPage: true }).catch(() => {});
        }
        await ctx.close();
      }
    }
    if (!quick && !ONLY && !FLAGS.has('--behaviour')) await behaviour();
  }
} finally {
  await browser.close();
  server.close();
}

async function behaviour() {
  // the three boot modes: 404 → compat (the key console), 401 → sign-in, 200 → the dashboard
  {
    const { ctx, page, api } = await context({ theme: 'light', width: 1440, mock: { bootStatus: 404 } });
    await go(page, '#/keys');
    await page.waitForSelector('#read-key, .start, .keys', { timeout: 10_000 }).catch(() => {});
    const where = await page.evaluate(() => location.hash);
    const keysCard = await page.locator('text=Keys for this tab').count();
    console.log(`boot 404: landed on ${where}; compat key card ${keysCard ? 'shown' : 'missing'}`);
    if (where !== '#/' || !keysCard) failures.push(`boot 404 should be the compat console at #/ (got ${where}, key card ${keysCard})`);
    await ctx.close();
    void api;
  }
  {
    const { ctx, page } = await context({ theme: 'light', width: 1440, mock: { signedIn: false } });
    await go(page, '#/runs/10416');
    await page.waitForSelector('h1:has-text("Sign in")');
    console.log(`boot 401: landed on ${await page.evaluate(() => location.hash)}`);
    // sign in → back to the run the visitor asked for
    await page.fill('#email', 'ada@example.com');
    await page.fill('#password', M.PASSWORD);
    await page.click('button[type=submit]');
    await page.waitForSelector('.tl li', { timeout: 10_000 }).catch(() => {});
    const after = await page.evaluate(() => location.hash);
    console.log(`after sign-in: ${after}`);
    if (after !== '#/runs/10416') failures.push(`sign-in should return to #/runs/10416 (got ${after})`);
    await ctx.close();
  }
  {
    // CSRF + workspace header: create a key, register a handler, Run now, cancel a run; then sign out
    const { ctx, page, api } = await context({ theme: 'light', width: 1440, mock: {} });
    await go(page, '#/keys');
    await page.waitForSelector('.rows li');
    await page.fill('#key-name', 'contract-check');
    await page.click('form button[type=submit]');
    await page.waitForSelector('dialog[open] .copy-value');
    await page.getByRole('button', { name: 'I have copied it' }).click();
    await go(page, '#/handlers');
    await page.waitForSelector('.rows li');
    await page.fill('#h-name', 'refund');
    await page.fill('#h-url', 'https://pay.example.com/steps/refund');
    await page.click('.register button[type=submit]');
    await page.waitForSelector('.rows li:has-text("refund")');
    await go(page, `#/workflows/${M.WF_DEMO}`);
    await page.waitForSelector('#versions-title');
    await page.getByRole('button', { name: /^Run now/ }).first().click();
    await page.waitForSelector('.tl li', { timeout: 10_000 }).catch(() => {});
    const landed = await page.evaluate(() => location.hash);
    if (landed !== '#/runs/10416') failures.push(`Run now should land on the run's timeline (got ${landed})`);
    const muts = api.state.calls.filter((c) => c.method !== 'GET');
    const missing = muts.filter((c) => !c.headers['x-durable-csrf']);
    console.log(`contract: ${muts.length} mutations, ${missing.length} without CSRF; ${api.state.violations.length} violations`);
    if (missing.length) failures.push(`mutations without CSRF: ${missing.map((c) => `${c.method} ${c.path}`).join(', ')}`);
    const reads = api.state.calls.filter((c) => c.method === 'GET' && !c.path.startsWith('/auth') && !c.path.startsWith('/me'));
    const csrfOnRead = reads.filter((c) => c.headers['x-durable-csrf']);
    if (csrfOnRead.length) failures.push(`reads carrying CSRF: ${csrfOnRead.map((c) => c.path).join(', ')}`);
    await health(page, api, 'contract');
    // switch workspace: flat routes follow the selection
    await go(page, '#/runs');
    await page.waitForSelector('table.runlist tbody tr');
    const before = api.state.calls.length;
    await page.locator('.ws-btn').click();
    await page.locator('.pop .item', { hasText: 'Staging' }).click();
    await page.waitForTimeout(800);
    const after = api.state.calls.slice(before).filter((c) => c.path === '/jobs');
    const ok = after.length > 0 && after.every((c) => c.headers['x-durable-workspace'] === M.WS_STAGE);
    console.log(`switch: ${after.length} /jobs request(s) after switching, all to Staging: ${ok}`);
    if (!ok) failures.push('after switching, /jobs did not follow the selected workspace');
    // sign out
    if (await page.getByRole('button', { name: 'Menu', exact: true }).isVisible()) await page.getByRole('button', { name: 'Menu', exact: true }).click();
    await page.locator('.acct-btn').click();
    await page.locator('#acct-menu button', { hasText: 'Sign out' }).click();
    await page.waitForSelector('h1:has-text("Sign in")');
    console.log(`sign out: ${await page.evaluate(() => location.hash)}`);
    await ctx.close();
  }
  {
    // a session that ends mid-visit (a 401 from /auth/session after a refused call) lands on sign-in with a note
    const { ctx, page, api } = await context({ theme: 'light', width: 1440, mock: {} });
    await go(page, '#/keys');
    await page.waitForSelector('.rows li');
    api.state.signedIn = false;
    await page.click('.rows li .btn:has-text("Revoke") >> nth=0');
    await page.getByRole('button', { name: 'Revoke', exact: true }).first().click();
    await page.waitForSelector('h1:has-text("Sign in")', { timeout: 10_000 }).catch(() => {});
    const note = await page.locator('.banner.notice').first().textContent().catch(() => null);
    console.log(`session ended: ${await page.evaluate(() => location.hash)}; note: ${note?.trim()}`);
    if (!note?.includes('session ended')) failures.push('an ended session should land on sign-in with a note');
    await ctx.close();
  }
}

console.log(`${shots.length} screenshots in ${OUT}`);
if (failures.length) { console.error(`FAILURES (${failures.length}):\n  ${failures.join('\n  ')}`); process.exit(4); }
if (!FLAGS.has('--axe')) console.log('all checks pass');
