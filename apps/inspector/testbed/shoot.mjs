// The dashboard's testbed (compat mode: GET /auth/session answers 404 here): serves apps/inspector/build at its mount with the CSP durable-server computes
// (inspector_csp), mocks the API (GET /metrics, /events, /jobs/{id}/events and the older routes) with Playwright
// routes, shoots every page at 1440 and 390 in light and dark, an older server (404 on the new routes) and no
// keys, and asserts: no horizontal overflow, no CSP violations, no console errors, the font loads, focus lands on
// "No" after Compact, polling pauses while hidden and resumes once, a 429's Retry-After is honoured, a 503 keeps
// the last good data on screen, and a slow server never sees two requests of one resource at once.
//
//   cd apps/inspector && npm run build
//   node testbed/shoot.mjs build <out-dir> <port> [--quick | --behaviour | --keys | --axe]
//
// --keys walks the keyboard paths; --axe runs a WCAG 2.1 AA scan (needs AXE_PATH=<axe-core/axe.min.js>).
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import * as M from './mocks.mjs';

const { chromium } = await import('playwright');

const [BUILD_ARG, OUT, PORT_ARG, FLAG] = process.argv.slice(2);
const BUILD = path.resolve(BUILD_ARG ?? '');
const PORT = Number(PORT_ARG);
if (!BUILD_ARG || !OUT || !(PORT > 1024 && PORT < 65536)) { console.error('usage: node testbed/shoot.mjs <build> <out> <port> [--quick|--behaviour|--keys|--axe]'); process.exit(2); }
fs.mkdirSync(OUT, { recursive: true });

function inspectorCsp(html) {
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
const CSP = inspectorCsp(INDEX);
// the build's mount (paths.base: /app by default, DURABLE_UI_BASE at build time)
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
console.log(`static server pid ${process.pid} on ${PORT}; CSP script hashes: ${(CSP.match(/sha256-/g) ?? []).length}`);

// mode: 'new' (the contract), 'old' (the new routes 404 without an envelope, like an older durable-server)
async function mockApi(page, { mode = 'new', keys = true, log = null, script = null } = {}) {
  await page.route(/\/(jobs|health|ready|admin|metrics|events)(\/|\?|$)/, async (route) => {
    const req = route.request();
    const url = new URL(req.url());
    const p = url.pathname;
    log?.push({ p, at: Date.now() });
    const auth = req.headers()['authorization'] ?? '';
    const json = (status, body, headers = {}) => route.fulfill({ status, contentType: 'application/json', headers: { 'cache-control': 'no-store', ...headers }, body: JSON.stringify(body) });
    if (script) { const r = script(p, url); if (r) return json(r.status, r.body, r.headers); }
    if (p === '/health') return json(200, { ok: true });
    if (p === '/ready') return json(200, { ready: true, code: null });
    const isApi = auth === `Bearer ${M.API_KEY}`;
    const isRead = isApi || auth === `Bearer ${M.READ_KEY}`;
    const isNew = p === '/metrics' || p === '/events' || /^\/jobs\/\d+\/events$/.test(p);
    if (isNew && mode === 'old') return route.fulfill({ status: 404, body: '' });
    if (!keys || !isRead) return json(401, { error: 'unauthorized', code: 'unauthorized' });
    if (p === '/metrics') {
      const w = url.searchParams.get('window') ?? '1h';
      if (!['15m', '1h', '6h', '24h'].includes(w)) return json(400, { error: 'invalid window', code: 'invalid_window' });
      return json(200, M.metrics(w));
    }
    if (p === '/events') return json(200, M.feed(Number(url.searchParams.get('limit') ?? 50)));
    if (p === '/jobs' && req.method() === 'GET') {
      const order = url.searchParams.get('order');
      return json(200, { runs: order === 'asc' ? [...M.RUNS_OUT()].reverse() : M.RUNS_OUT(), next_before: order === 'desc' ? '10408' : null, next_after: order === 'asc' ? '10419' : null });
    }
    if (p === '/jobs' && req.method() === 'POST') return isApi ? json(200, { run_id: '10419' }) : json(403, { error: 'forbidden', code: 'forbidden' });
    let m = p.match(/^\/jobs\/(\d+)$/);
    if (m) { const r = M.RUNS.find((x) => x.id === Number(m[1])); return r ? json(200, M.runOut(r)) : json(404, { error: 'no such run', code: 'not_found' }); }
    m = p.match(/^\/jobs\/(\d+)\/events$/);
    if (m) { const t = M.timeline(Number(m[1])); return t ? json(200, t) : json(404, { error: 'no such run', code: 'not_found' }); }
    m = p.match(/^\/jobs\/(\d+)\/spec$/);
    if (m) return isApi ? json(200, { spec: M.spec(Number(m[1])) }) : json(403, { error: 'forbidden', code: 'forbidden' });
    m = p.match(/^\/jobs\/(\d+)\/cancel$/);
    if (m) return json(200, { cancelled: Number(m[1]) });
    if (p === '/admin/diagnostics') return isApi ? json(200, M.DIAG) : json(403, { error: 'forbidden', code: 'forbidden' });
    if (p === '/admin/compact') return json(200, { generation: 15 });
    if (p === '/admin/backup') return json(200, { backup: JSON.parse(req.postData() ?? '{}').name });
    return route.fulfill({ status: 404, body: '' });
  });
}

const browser = await chromium.launch();
const failures = [];
const shots = [];
const base = `http://127.0.0.1:${PORT}${MOUNT}/`;

async function context({ theme, width, keys = true, mode = 'new', log = null, script = null }) {
  const ctx = await browser.newContext({ viewport: { width, height: 900 }, deviceScaleFactor: width === 390 ? 2 : 1, colorScheme: 'light' });
  await ctx.addInitScript(([rk, ak, t, k]) => {
    if (k) { sessionStorage.setItem('durable-read-key', rk); sessionStorage.setItem('durable-api-key', ak); }
    if (t) localStorage.setItem('theme', t);
    window.__csp = [];
    document.addEventListener('securitypolicyviolation', (e) => window.__csp.push(`${e.violatedDirective} ${e.blockedURI}`));
  }, [M.READ_KEY, M.API_KEY, theme === 'dark' ? 'dark' : null, keys]);
  const page = await ctx.newPage();
  page.__errors = [];
  page.on('console', (m) => { if (m.type() === 'error') page.__errors.push(m.text()); });
  page.on('pageerror', (e) => page.__errors.push(String(e)));
  await mockApi(page, { mode, keys, log, script });
  return { ctx, page };
}

async function health(page, tag) {
  const csp = await page.evaluate(() => window.__csp);
  if (csp.length) failures.push(`${tag}: CSP violations: ${csp.join('; ')}`);
  const errs = page.__errors.filter((e) => !/status of (4\d\d|503)|401|403|404/.test(e));
  if (errs.length) failures.push(`${tag}: console errors: ${errs.join(' | ')}`);
}

async function snap(page, name, { full = true, width } = {}) {
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

const quick = FLAG === '--quick';
if (FLAG === '--keys') {
  const out = [];
  const { ctx, page } = await context({ theme: 'light', width: 1440 });
  await go(page, '#/');
  await page.waitForSelector('.chart svg .ylab');
  await page.keyboard.press('Tab');
  out.push(['first tab stop', await page.evaluate(() => document.activeElement?.textContent?.trim())]);
  await page.keyboard.press('Enter');
  out.push(['after Enter', await page.evaluate(() => document.activeElement?.id)]);
  await page.locator('.plot').first().focus();
  await page.keyboard.press('ArrowLeft');
  await page.keyboard.press('ArrowLeft');
  out.push(['chart tooltip shown', await page.locator('.tip').count()]);
  out.push(['chart live text', await page.locator('.chart .sr[aria-live]').first().textContent()]);
  await go(page, '#/runs/10416');
  await page.waitForSelector('.gantt .plot');
  await page.locator('.gantt .plot').focus();
  await page.keyboard.press('Home');
  out.push(['gantt live text', await page.locator('.gantt ~ .sr, .gantt .sr[aria-live]').first().textContent()]);
  // the window picker: arrow keys move the choice
  await go(page, '#/');
  await page.getByRole('radio', { name: '1h' }).focus();
  await page.keyboard.press('ArrowRight');
  out.push(['window after ArrowRight', await page.evaluate(() => document.activeElement?.textContent?.trim() + ' checked=' + document.activeElement?.getAttribute('aria-checked'))]);
  await ctx.close();
  const m = await context({ theme: 'light', width: 390 });
  await go(m.page, '#/runs');
  const menu = m.page.getByRole('button', { name: 'Menu' });
  await menu.focus();
  await m.page.keyboard.press('Enter');
  out.push(['menu expanded', await menu.getAttribute('aria-expanded')]);
  await m.page.keyboard.press('Escape');
  out.push(['menu after Escape', `${await menu.getAttribute('aria-expanded')} focus=${await m.page.evaluate(() => document.activeElement?.textContent?.trim())}`]);
  await m.ctx.close();
  for (const [k, v] of out) console.log(`${k}: ${v}`);
  await browser.close(); server.close(); process.exit(0);
}
if (FLAG === '--axe') {
  // WCAG 2.1 AA scan of every page in both themes at both widths
  if (!process.env.AXE_PATH) { console.error('--axe needs AXE_PATH=<path to axe-core/axe.min.js>'); process.exit(2); }
  const AXE = fs.readFileSync(process.env.AXE_PATH, 'utf8');
  let total = 0;
  for (const theme of ['light', 'dark']) for (const width of [1440, 390]) {
    const { ctx, page } = await context({ theme, width });
    for (const hash of ['#/', '#/runs', '#/runs/10416', '#/runs/10410', '#/handlers', '#/submit', '#/admin', '#/settings']) {
      await go(page, hash);
      await page.waitForTimeout(700);
      const r = await page.evaluate(async (src) => {
        if (!window.axe) (0, eval)(src);
        const res = await window.axe.run(document, { runOnly: { type: 'tag', values: ['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'] } });
        return res.violations.map((v) => `${v.id} (${v.impact}): ${v.nodes.length} × ${v.nodes.slice(0, 2).map((n) => n.target.join(' ')).join(' | ')}`);
      }, AXE);
      total += r.length;
      if (r.length) console.log(`${theme}-${width} ${hash}:\n  ${r.join('\n  ')}`);
    }
    await ctx.close();
  }
  console.log(`axe: ${total} violation group(s)`);
  await browser.close(); server.close(); process.exit(total ? 4 : 0);
}
if (FLAG === '--keys') {
  const out = [];
  const { ctx, page } = await context({ theme: 'light', width: 1440 });
  await go(page, '#/');
  await page.waitForSelector('.chart svg .ylab');
  await page.keyboard.press('Tab');
  out.push(['first tab stop', await page.evaluate(() => document.activeElement?.textContent?.trim())]);
  await page.keyboard.press('Enter');
  out.push(['after Enter', await page.evaluate(() => document.activeElement?.id)]);
  await page.locator('.plot').first().focus();
  await page.keyboard.press('ArrowLeft');
  await page.keyboard.press('ArrowLeft');
  out.push(['chart tooltip shown', await page.locator('.tip').count()]);
  out.push(['chart live text', await page.locator('.chart .sr[aria-live]').first().textContent()]);
  await go(page, '#/runs/10416');
  await page.waitForSelector('.gantt .plot');
  await page.locator('.gantt .plot').focus();
  await page.keyboard.press('Home');
  out.push(['gantt live text', await page.locator('.gantt ~ .sr, .gantt .sr[aria-live]').first().textContent()]);
  // the window picker: arrow keys move the choice
  await go(page, '#/');
  await page.getByRole('radio', { name: '1h' }).focus();
  await page.keyboard.press('ArrowRight');
  out.push(['window after ArrowRight', await page.evaluate(() => document.activeElement?.textContent?.trim() + ' checked=' + document.activeElement?.getAttribute('aria-checked'))]);
  await ctx.close();
  const m = await context({ theme: 'light', width: 390 });
  await go(m.page, '#/runs');
  const menu = m.page.getByRole('button', { name: 'Menu' });
  await menu.focus();
  await m.page.keyboard.press('Enter');
  out.push(['menu expanded', await menu.getAttribute('aria-expanded')]);
  await m.page.keyboard.press('Escape');
  out.push(['menu after Escape', `${await menu.getAttribute('aria-expanded')} focus=${await m.page.evaluate(() => document.activeElement?.textContent?.trim())}`]);
  await m.ctx.close();
  for (const [k, v] of out) console.log(`${k}: ${v}`);
  await browser.close(); server.close(); process.exit(0);
}
if (FLAG === '--axe') {
  // WCAG 2.1 AA scan of every page in both themes at both widths
  if (!process.env.AXE_PATH) { console.error('--axe needs AXE_PATH=<path to axe-core/axe.min.js>'); process.exit(2); }
  const AXE = fs.readFileSync(process.env.AXE_PATH, 'utf8');
  let total = 0;
  for (const theme of ['light', 'dark']) for (const width of [1440, 390]) {
    const { ctx, page } = await context({ theme, width });
    for (const hash of ['#/', '#/runs', '#/runs/10416', '#/runs/10410', '#/handlers', '#/submit', '#/admin', '#/settings']) {
      await go(page, hash);
      await page.waitForTimeout(700);
      const r = await page.evaluate(async (src) => {
        if (!window.axe) (0, eval)(src);
        const res = await window.axe.run(document, { runOnly: { type: 'tag', values: ['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'] } });
        return res.violations.map((v) => `${v.id} (${v.impact}): ${v.nodes.length} × ${v.nodes.slice(0, 2).map((n) => n.target.join(' ')).join(' | ')}`);
      }, AXE);
      total += r.length;
      if (r.length) console.log(`${theme}-${width} ${hash}:\n  ${r.join('\n  ')}`);
    }
    await ctx.close();
  }
  console.log(`axe: ${total} violation group(s)`);
  await browser.close(); server.close(); process.exit(total ? 4 : 0);
}
if (FLAG === '--nokeys') {
  const nk = await context({ theme: 'light', width: 1440, keys: false });
  nk.page.on('request', (r) => console.log('req', r.url()));
  nk.page.on('requestfinished', (r) => console.log('done', r.url()));
  nk.page.on('requestfailed', (r) => console.log('FAILED', r.url(), r.failure()?.errorText));
  await nk.page.goto(`${base}#/`, { waitUntil: 'load' });
  await nk.page.waitForTimeout(4000);
  console.log(nk.page.__errors);
  await browser.close(); server.close(); process.exit(0);
}
const behaviourOnly = FLAG === '--behaviour';
if (behaviourOnly) console.log('behaviour checks only');
try {
  if (!behaviourOnly) for (const theme of quick ? ['light'] : ['light', 'dark']) {
    for (const width of quick ? [1440] : [1440, 390]) {
      const tag = `${theme}-${width}`;
      const { ctx, page } = await context({ theme, width });
      await go(page, '#/');
      await page.waitForSelector('.chart svg .ylab', { timeout: 10000 });
      await page.waitForSelector('.feed li', { timeout: 10000 });
      const font = await page.evaluate(async () => { await document.fonts.ready; return [...document.fonts].some((f) => f.family.includes('Google Sans Flex') && f.status === 'loaded'); });
      if (!font) failures.push(`${tag}: Google Sans Flex not loaded`);
      await snap(page, `${tag}-overview`, { width });
      // a tooltip: hover the throughput chart's busiest stretch
      const hit = page.locator('#throughput-title').locator('xpath=ancestor::section').locator('.hits rect').nth(width === 390 ? 20 : 45);
      await hit.hover();
      await page.waitForSelector('.tip');
      await page.locator('#throughput-title').scrollIntoViewIfNeeded();
      await snap(page, `${tag}-overview-tooltip`, { full: false, width });
      await page.mouse.move(2, 2);
      // window 6h shows the pre-start region
      if (width === 1440) {
        await page.getByRole('radio', { name: '6h' }).click();
        await page.waitForTimeout(400);
        await snap(page, `${tag}-overview-6h`, { full: false, width });
        await page.getByRole('radio', { name: '1h' }).click();
      }
      // table view of a chart
      if (width === 1440 && theme === 'light') {
        await page.getByRole('button', { name: 'Show Outcomes as a table' }).click();
        await snap(page, `${tag}-overview-table`, { full: false, width });
      }

      await go(page, '#/runs');
      await page.waitForSelector('table.runlist tbody tr');
      await snap(page, `${tag}-runs`, { width });
      if (width === 390) {
        const m = await page.evaluate(() => {
          const btn = [...document.querySelectorAll('table.runlist td button.danger')].at(-1);
          const r = btn ? btn.getBoundingClientRect() : null;
          const keys = [...document.querySelectorAll('table.runlist td.key')].map((el) => ({ text: el.textContent, sw: el.scrollWidth, cw: el.clientWidth }));
          const ids = [...document.querySelectorAll('table.runlist td.id')].map((el) => ({ text: el.textContent.trim(), sw: el.scrollWidth, cw: el.clientWidth }));
          return { iw: window.innerWidth, box: r ? { l: r.left, r: r.right } : null, keys, ids };
        });
        if (!m.box) failures.push(`${tag}: no cancel button`);
        else if (m.box.l < 0 || m.box.r > m.iw) failures.push(`${tag}: cancel escapes viewport`);
        const cut = m.keys.filter((k) => k.text.length < 20 && k.sw > k.cw);
        if (cut.length) failures.push(`${tag}: short keys truncated: ${cut.map((k) => k.text).join(', ')}`);
        const cutIds = m.ids.filter((k) => k.sw > k.cw);
        if (cutIds.length) failures.push(`${tag}: ids truncated`);
        // the menu
        await page.getByRole('button', { name: 'Menu' }).click();
        await snap(page, `${tag}-menu`, { full: false, width });
        await page.keyboard.press('Escape');
      }
      await page.getByRole('button', { name: /^Done/ }).click();
      await page.getByPlaceholder('Search keys on this page').fill('2026');
      await snap(page, `${tag}-runs-filtered`, { full: false, width });

      await go(page, '#/runs/10416');
      await page.waitForSelector('.tl li');
      await page.getByRole('button', { name: /^Reveal spec/ }).first().click();
      await page.waitForSelector('.step-list li');
      await snap(page, `${tag}-run-done`, { width });
      if (width === 1440) {
        await page.locator('.gantt .hits rect').nth(3).hover();
        await page.waitForSelector('.gantt .tip');
        await page.locator('#gantt-title').scrollIntoViewIfNeeded();
        await snap(page, `${tag}-run-gantt-tooltip`, { full: false, width });
        await page.mouse.move(2, 2);
      }
      await go(page, '#/runs/10418');
      await page.waitForSelector('.tl li');
      await snap(page, `${tag}-run-live`, { width });
      if (theme === 'light') {
        await go(page, '#/runs/10410');
        await page.waitForSelector('.tl li');
        await snap(page, `${tag}-run-truncated`, { width });
        await go(page, '#/runs/10411');
        await page.waitForSelector('.tl li');
        await snap(page, `${tag}-run-compacted`, { full: false, width });
        await go(page, '#/runs/10413');
        await page.waitForSelector('.tl li');
        await snap(page, `${tag}-run-failed`, { width });
      }

      await go(page, '#/handlers');
      await page.waitForSelector('.handler');
      await snap(page, `${tag}-handlers`, { width });

      await go(page, '#/submit');
      await page.getByLabel('Operation key').fill('deploy-2026-11-24');
      await page.getByLabel('Step 0 name').fill('build');
      await page.getByLabel('Step 0 handler').fill('ci-runner');
      await page.getByRole('button', { name: 'Add step' }).click();
      await page.getByLabel('Step 1 name').fill('deploy');
      await page.getByLabel('Step 1 handler').fill('deployer');
      await page.getByLabel('Input').fill('{"env":"prod"}');
      await snap(page, `${tag}-submit`, { width });
      await page.locator('form button[type=submit]').click();
      await page.waitForSelector('.success');
      await snap(page, `${tag}-submit-accepted`, { full: false, width });

      await go(page, '#/admin');
      await page.waitForSelector('.tiles');
      await snap(page, `${tag}-admin`, { width });
      await page.getByRole('button', { name: 'Compact', exact: true }).click();
      const focused = await page.evaluate(() => document.activeElement?.textContent?.trim());
      if (focused !== 'No') failures.push(`${tag}: after Compact, focus is on ${JSON.stringify(focused)}, not "No"`);
      await page.keyboard.press('Enter'); // answers No
      const back = await page.evaluate(() => document.activeElement?.textContent?.trim());
      if (back !== 'Compact') failures.push(`${tag}: after No, focus is on ${JSON.stringify(back)}, not Compact`);
      await page.getByRole('button', { name: 'Backup', exact: true }).click();
      const f2 = await page.evaluate(() => document.activeElement?.textContent?.trim());
      if (f2 !== 'No') failures.push(`${tag}: after Backup, focus is on ${JSON.stringify(f2)}`);
      await page.locator('.confirm').scrollIntoViewIfNeeded();
      await snap(page, `${tag}-admin-confirm`, { full: false, width });

      await go(page, '#/settings');
      await page.waitForSelector('#read-key');
      await snap(page, `${tag}-settings`, { width });
      await health(page, tag);
      await ctx.close();

      // an older server: the new routes answer 404 without an envelope
      if (!quick) {
        const old = await context({ theme, width, mode: 'old' });
        await go(old.page, '#/');
        await old.page.waitForSelector('.banner.warn');
        await old.page.waitForSelector('table.runlist tbody tr');
        await snap(old.page, `${tag}-old-server-overview`, { width });
        await go(old.page, '#/runs/10416');
        await old.page.waitForSelector('.banner.warn');
        await snap(old.page, `${tag}-old-server-run`, { width });
        if (theme === 'light') {
          await go(old.page, '#/handlers');
          await old.page.waitForSelector('.banner.warn');
          await snap(old.page, `${tag}-old-server-handlers`, { full: false, width });
        }
        await health(old.page, `${tag}-old`);
        await old.ctx.close();
        // no keys
        const nk = await context({ theme, width, keys: false });
        await go(nk.page, '#/');
        await nk.page.waitForSelector('.start');
        await snap(nk.page, `${tag}-nokeys`, { full: false, width });
        await health(nk.page, `${tag}-nokeys`);
        await nk.ctx.close();
      }
    }
  }

  if (!quick) {
    // behaviour: polling every ~5 s while visible, none while hidden
    const log = [];
    const { ctx, page } = await context({ theme: 'light', width: 1440, log });
    await go(page, '#/');
    await page.waitForSelector('.chart svg .ylab');
    const t0 = Date.now();
    await page.waitForTimeout(11_500);
    const visible = log.filter((x) => x.p === '/metrics' && x.at >= t0).length;
    await page.evaluate(() => {
      Object.defineProperty(document, 'visibilityState', { configurable: true, get: () => 'hidden' });
      document.dispatchEvent(new Event('visibilitychange'));
    });
    const t1 = Date.now();
    await page.waitForTimeout(11_500);
    const hidden = log.filter((x) => x.p === '/metrics' && x.at >= t1 + 300).length;
    const t2 = Date.now();
    await page.evaluate(() => {
      Object.defineProperty(document, 'visibilityState', { configurable: true, get: () => 'visible' });
      document.dispatchEvent(new Event('visibilitychange'));
    });
    await page.waitForTimeout(800);
    const resumed = log.filter((x) => x.p === '/metrics' && x.at >= t2).length;
    console.log(`polling: ${visible} /metrics requests in 11.5 s visible, ${hidden} while hidden, ${resumed} at once on return`);
    if (visible < 2 || visible > 3) failures.push(`polling: ${visible} requests in 11.5 s visible (want 2-3)`);
    if (hidden !== 0) failures.push(`polling: ${hidden} requests while hidden (want 0)`);
    if (resumed !== 1) failures.push(`polling: ${resumed} requests on becoming visible (want 1)`);
    // pause button
    await page.getByRole('button', { name: 'Pause live updates' }).click();
    const t3 = Date.now();
    await page.waitForTimeout(6_000);
    const paused = log.filter((x) => x.p === '/metrics' && x.at >= t3 + 300).length;
    const word = await page.locator('.live .status').textContent();
    console.log(`pause: ${paused} requests in 6 s; status reads ${JSON.stringify(word)}`);
    if (paused !== 0) failures.push(`pause: ${paused} requests while paused`);
    await snap(page, 'light-1440-paused', { full: false, width: 1440 });
    await health(page, 'behaviour');
    await ctx.close();

    // 429 with Retry-After: 9 → the next /metrics request waits at least ~9 s
    const log2 = [];
    let refused = false;
    const c2 = await context({
      theme: 'light', width: 1440, log: log2,
      script: (p) => {
        if (p === '/metrics' && !refused) { refused = true; return { status: 429, body: { error: 'rate limited', code: 'rate_limited' }, headers: { 'retry-after': '9' } }; }
        return null;
      },
    });
    await go(c2.page, '#/');
    await c2.page.waitForTimeout(12_000);
    const ms = log2.filter((x) => x.p === '/metrics').map((x) => x.at);
    const gap = ms.length > 1 ? ms[1] - ms[0] : null;
    console.log(`429: first two /metrics requests ${gap} ms apart (Retry-After 9 s)`);
    if (gap === null || gap < 8_800) failures.push(`429: next request after ${gap} ms, want ≥ 9000`);
    await c2.ctx.close();

    // 503 busy after a good answer: the charts keep the last good data on screen and say so
    let calls = 0;
    const c4 = await context({
      theme: 'light', width: 1440,
      script: (p) => (p === '/metrics' && ++calls > 1 ? { status: 503, body: { error: 'the server is busy; retry shortly', code: 'busy' } } : null),
    });
    await go(c4.page, '#/');
    await c4.page.waitForSelector('.chart svg .ylab');
    await c4.page.waitForSelector('.banner.warn', { timeout: 9000 });
    const kept = await c4.page.locator('.chart svg path[d^="M"]').count();
    const banner = await c4.page.locator('.banner.warn').first().textContent();
    console.log(`503: ${kept} chart paths still drawn; banner: ${banner?.trim()}`);
    if (kept < 4) failures.push(`503: the last good data left the screen (${kept} paths)`);
    await snap(c4.page, 'light-1440-stale-503', { full: false, width: 1440 });
    await health(c4.page, '503');
    await c4.ctx.close();

    // one in flight per resource: a slow /metrics (7 s) never overlaps itself
    const log3 = [];
    let inflight = 0;
    let maxInflight = 0;
    const c3 = await context({ theme: 'light', width: 1440, log: log3 });
    await c3.page.route(/\/metrics(\?|$)/, async (route) => {
      inflight++; maxInflight = Math.max(maxInflight, inflight);
      await new Promise((r) => setTimeout(r, 7_000));
      inflight--;
      await route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(M.metrics('1h')) });
    });
    await go(c3.page, '#/');
    await c3.page.waitForTimeout(16_000);
    console.log(`slow server: at most ${maxInflight} /metrics request(s) in flight at once`);
    if (maxInflight !== 1) failures.push(`slow server: ${maxInflight} concurrent /metrics requests`);
    await c3.ctx.close();
  }
} finally {
  await browser.close();
  server.close();
}
console.log(`${shots.length} screenshots in ${OUT}`);
if (failures.length) { console.error(`FAILURES:\n  ${failures.join('\n  ')}`); process.exit(4); }
console.log('all checks pass');
