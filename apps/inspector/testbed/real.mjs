// The dashboard against a REAL durable-server: a build with the observability API (expect "new") or an older one
// without it (expect "old": the pages must say so and keep working). Starts a handler fixture and the server on
// PORT and PORT+1 (default 58723), seeds runs (ok, flaky with retries, scheduled, cancelled), loads every page
// at 1440 and 390 and screenshots it; stops only the two processes it started.
//   node testbed/real.mjs <durable-server binary> <ui build dir> <out-dir> <label> <new|old>
import { spawn, spawnSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
const { chromium } = await import('playwright');

const [SRV, UI, OUT, LABEL, EXPECT] = process.argv.slice(2);
const PORT = Number(process.env.PORT ?? 58723), HPORT = PORT + 1;
const API_KEY = 'a'.repeat(64), READ_KEY = 'b'.repeat(64);
const API = `http://127.0.0.1:${PORT}`;
const dir = fs.mkdtempSync(path.join(path.dirname(OUT), 'real-journal-'));
fs.mkdirSync(OUT, { recursive: true });
const kids = [];
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const handler = spawn('node', ['-e', `
  let n = 0;
  require('node:http').createServer((req, res) => {
    let b = ''; req.on('data', (c) => (b += c));
    req.on('end', () => {
      n++;
      const flaky = req.url.startsWith('/flaky');
      const slow = req.url.startsWith('/slow');
      const fail = flaky && n % 3 !== 0;
      setTimeout(() => res.writeHead(fail ? 500 : 200, { 'content-type': 'application/json' }).end(fail ? '{"error":"boom"}' : '{"out":null}'), slow ? 400 : 5);
    });
  }).listen(${HPORT}, '127.0.0.1');`], { stdio: 'ignore' });
kids.push(handler);
// A build with the S2 SSRF guard refuses loopback handler URLs unless told this is a fixture; older builds have no flag.
const ssrfGuard = /--allow-local-handlers/.test(spawnSync(SRV, ['--help'], { encoding: 'utf8' }).stdout ?? '');
const srv = spawn(SRV, ['--journal', `${dir}/journal`, '--listen', `127.0.0.1:${PORT}`, '--handler', `step=http://127.0.0.1:${HPORT}/step`,
  '--handler', `flaky=http://127.0.0.1:${HPORT}/flaky`, '--handler', `slow=http://127.0.0.1:${HPORT}/slow`, '--ui-dir', UI, '--backup-root', `${dir}/backups`,
  ...(ssrfGuard ? ['--allow-local-handlers'] : [])],
  { stdio: ['ignore', 'ignore', 'pipe'], env: { ...process.env, DURABLE_API_KEY: API_KEY, DURABLE_READ_KEY: READ_KEY } });
kids.push(srv);
let stderr = '';
srv.stderr.on('data', (d) => (stderr += d));
console.log(`pids: server ${srv.pid}, handler ${handler.pid}`);
const failures = [];
try {
  for (let i = 0; i < 60; i++) {
    try { if ((await fetch(`${API}/ready`)).status === 200) break; } catch {}
    if (srv.exitCode !== null) throw new Error(`server exited ${srv.exitCode}: ${stderr}`);
    await sleep(200);
  }
  const post = (op, steps, extra = {}) => fetch(`${API}/jobs`, { method: 'POST', headers: { 'content-type': 'application/json', authorization: `Bearer ${API_KEY}` },
    body: JSON.stringify({ op_key: op, spec: { version: 1, steps: steps.map(([name, handler]) => ({ name, handler })), input_b64: 'aGVsbG8=' }, ...extra }) }).then((r) => r.json());
  for (let i = 0; i < 12; i++) await post(`real/ok-${i}`, [['fetch', 'step'], ['render', 'slow'], ['send', 'step']]);
  for (let i = 0; i < 4; i++) await post(`real/flaky-${i}`, [['prepare', 'step'], ['push', 'flaky']]);
  await post('real/scheduled', [['later', 'step']], { not_before: Date.now() + 3_600_000 });
  const c = await post('real/cancel-me', [['later', 'step']], { not_before: Date.now() + 3_600_000 });
  await fetch(`${API}/jobs/${c.run_id}/cancel`, { method: 'POST', headers: { authorization: `Bearer ${API_KEY}` } });
  await sleep(6000);

  const browser = await chromium.launch();
  for (const width of [1440, 390]) {
    const ctx = await browser.newContext({ viewport: { width, height: 900 }, deviceScaleFactor: width === 390 ? 2 : 1 });
    await ctx.addInitScript(([rk, ak]) => { sessionStorage.setItem('durable-read-key', rk); sessionStorage.setItem('durable-api-key', ak); window.__csp = [];
      document.addEventListener('securitypolicyviolation', (e) => window.__csp.push(`${e.violatedDirective} ${e.blockedURI}`)); }, [READ_KEY, API_KEY]);
    const page = await ctx.newPage();
    const errors = [];
    page.on('pageerror', (e) => errors.push(String(e)));
    page.on('console', (m) => { if (m.type() === 'error' && !/status of 404/.test(m.text())) errors.push(m.text()); });
    const shot = async (name) => { await page.waitForTimeout(400); await page.screenshot({ path: `${OUT}/${LABEL}-${width}-${name}.png`, fullPage: true });
      const sw = await page.evaluate(() => document.scrollingElement.scrollWidth); if (sw > width) failures.push(`${LABEL}-${width}-${name}: overflow ${sw}`); };
    await page.goto(`${API}/inspector/#/`, { waitUntil: 'load' });
    if (EXPECT === 'new') {
      await page.waitForSelector('.chart svg .ylab', { timeout: 10000 });
      await page.waitForSelector('.feed li', { timeout: 10000 });
    } else {
      await page.waitForSelector('.banner.warn', { timeout: 10000 });
      await page.waitForSelector('table.runlist tbody tr', { timeout: 10000 });
      const picker = await page.locator('[role=radiogroup][aria-label="Time window"]').count();
      if (picker) failures.push(`${LABEL}: the time window still shows against a server without /metrics`);
    }
    await shot('overview');
    await page.goto(`${API}/inspector/#/runs`, { waitUntil: 'load' });
    await page.waitForSelector('table.runlist tbody tr');
    await shot('runs');
    // the flaky run: retries on a real timeline
    const flaky = await (await fetch(`${API}/jobs?order=asc&limit=50`, { headers: { authorization: `Bearer ${READ_KEY}` } })).json();
    const f = flaky.runs.find((r) => r.key === 'real/flaky-1');
    await page.goto(`${API}/inspector/#/runs/${f.id}`, { waitUntil: 'load' });
    await page.waitForSelector(EXPECT === 'new' ? '.tl li' : '.banner.warn', { timeout: 10000 });
    await shot('run-flaky');
    await page.goto(`${API}/inspector/#/handlers`, { waitUntil: 'load' });
    await page.waitForSelector(EXPECT === 'new' ? '.handler' : '.banner.warn', { timeout: 10000 });
    await shot('handlers');
    await page.goto(`${API}/inspector/#/admin`, { waitUntil: 'load' });
    await page.waitForSelector('.tiles');
    await shot('admin');
    const csp = await page.evaluate(() => window.__csp);
    if (csp.length) failures.push(`${LABEL}-${width}: CSP ${csp.join('; ')}`);
    if (errors.length) failures.push(`${LABEL}-${width}: errors ${errors.join(' | ')}`);
    await ctx.close();
  }
  await browser.close();
  // the server's own CSP header, for the record
  const h = await fetch(`${API}/inspector/`);
  console.log(`CSP from the server: ${h.headers.get('content-security-policy')?.slice(0, 120)}…`);
  const m = await fetch(`${API}/metrics?window=15m`, { headers: { authorization: `Bearer ${READ_KEY}` } });
  console.log(`GET /metrics → ${m.status}`);
} finally {
  for (const k of kids) { try { k.kill('SIGTERM'); } catch {} }
  await sleep(500);
  for (const k of kids) { if (k.exitCode === null && k.signalCode === null) { try { k.kill('SIGKILL'); } catch {} } }
  fs.rmSync(dir, { recursive: true, force: true });
}
if (failures.length) { console.error(`FAILURES:\n  ${failures.join('\n  ')}`); process.exit(4); }
console.log('real-server checks pass');
