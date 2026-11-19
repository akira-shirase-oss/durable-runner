// capture.mjs — screenshot the inspector at 1440 and 390, light and dark.
// Self-contained: spawns durable-server + a handler fixture, seeds a few
// runs, shoots every tab, cleans up. Run OUTSIDE the sandbox (Chromium).
//
//   npm i -D playwright && npx playwright install chromium   # once
//   node apps/inspector/capture.mjs --out perf/ui/after [--port=58538] [--hport=58539]
//
// For "before" shots, build the pre-change UI and run again with --out perf/ui/before.
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = path.dirname(path.dirname(path.dirname(fileURLToPath(import.meta.url))));
const SRV = `${ROOT}/target/release/durable-server`;
const args = {};
{
  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith('--')) continue;
    const eq = a.indexOf('=');
    if (eq > 0) args[a.slice(2, eq)] = a.slice(eq + 1);
    else if (i + 1 < argv.length && !argv[i + 1].startsWith('--')) args[a.slice(2)] = argv[++i];
    else args[a.slice(2)] = 'true';
  }
}
const UI_DIR = path.resolve(args['ui-dir'] ?? `${ROOT}/apps/inspector/build`);
// An explicit --ui-dir means a before/other build: capture the views
// that exist (the old UI lacks the detail/submit/admin panels) and skip
// the layout assertions, which assert the NEW layout's contract.
const BEFORE_MODE = args['ui-dir'] !== undefined;
const PORT = Number(args.port ?? 53344);
const HPORT = Number(args.hport ?? 53345);
const OUT = path.resolve(args.out ?? 'perf/ui/shots');
const API_KEY = 'a'.repeat(64);
const READ_KEY = 'b'.repeat(64);
const API = `http://127.0.0.1:${PORT}`;

let chromium;
try {
  ({ chromium } = await import('playwright'));
} catch {
  console.error('needs playwright: npm i -D playwright && npx playwright install chromium');
  process.exit(2);
}

const kids = new Set();
const track = (p) => (kids.add(p), p);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
function portFree(port) {
  return new Promise((res) => {
    const s = net.connect({ host: '127.0.0.1', port });
    s.once('connect', () => { s.destroy(); res(false); });
    s.once('error', () => res(true));
  });
}
for (const p of [PORT, HPORT]) {
  if (!(await portFree(p))) { console.error(`port ${p} is busy — pick others with --port/--hport`); process.exit(3); }
}

const dir = fs.mkdtempSync(`${os.tmpdir()}/dr-ui-`);
const handler = track(spawn('node', ['-e', `
  require('node:http').createServer((req,res)=>{let b='';req.on('data',c=>b+=c);
    req.on('end',()=>res.writeHead(200,{'content-type':'application/json'}).end('{"out":null}'))})
  .listen(${HPORT},'127.0.0.1');`], { stdio: 'ignore' }));
const srv = track(spawn(SRV, [
  '--journal', `${dir}/journal`, '--listen', `127.0.0.1:${PORT}`,
  '--handler', `step=http://127.0.0.1:${HPORT}/step`,
  '--ui-dir', UI_DIR, '--backup-root', `${dir}/backups`,
], { stdio: 'ignore', env: { ...process.env, DURABLE_API_KEY: API_KEY, DURABLE_READ_KEY: READ_KEY } }));

try {
  for (let i = 0; i < 50; i++) {
    try { if ((await fetch(`${API}/ready`)).status === 200) break; } catch {}
    if (srv.exitCode !== null) throw new Error(`server exited ${srv.exitCode}`);
    await sleep(200);
  }
  // Seed a spread of states: done, queued, cancelled.
  const post = (op) => fetch(`${API}/jobs`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', authorization: `Bearer ${API_KEY}` },
    body: JSON.stringify({ op_key: op, spec: { version: 1, steps: [{ name: 'only', handler: 'step' }], input_b64: 'aGVsbG8gd29ybGQ=' } }),
  });
  for (let i = 0; i < 8; i++) await post(`ui-seed-${i}`);
  await fetch(`${API}/jobs`, { method: 'POST', headers: { 'content-type': 'application/json', authorization: `Bearer ${API_KEY}` },
    body: JSON.stringify({ op_key: 'ui-scheduled', not_before: Date.now() + 86400000, spec: { version: 1, steps: [{ name: 'later', handler: 'step' }], input_b64: 'bGF0ZXI=' } }) });
  const first = await post('ui-cancelled').then((r) => r.json());
  await fetch(`${API}/jobs/${first.run_id}/cancel`, { method: 'POST', headers: { authorization: `Bearer ${API_KEY}` } });
  await sleep(1200); // let the eight echo runs finish

  fs.mkdirSync(OUT, { recursive: true });
  const browser = await chromium.launch();
  const shots = [];
  const layoutFailures = [];
  for (const theme of ['dark', 'light']) {
    for (const width of [1440, 390]) {
      const ctx = await browser.newContext({ viewport: { width, height: 900 } });
      await ctx.addInitScript(
        ([rk, ak, t]) => {
          sessionStorage.setItem('durable-read-key', rk);
          sessionStorage.setItem('durable-api-key', ak);
          localStorage.setItem('durable-theme', t);
          document.documentElement.dataset.theme = t;
        },
        [READ_KEY, API_KEY, theme],
      );
      const page = await ctx.newPage();
      const base = `${API}/inspector/`;
      await page.goto(base, { waitUntil: 'networkidle' });
      try { await page.waitForSelector('table tbody tr', { timeout: 10000 }); } catch {}
      const snap = async (name) => {
        const file = `${OUT}/${theme}-${width}-${name}.png`;
        await page.screenshot({ path: file, fullPage: false });
        shots.push(file);
      };
      await snap('runs');
      // The phone layout contract: no horizontal overflow, and the row
      // action stays inside the viewport (the seeded scheduled run is
      // queued, so its cancel button must be there).
      if (width === 390 && !BEFORE_MODE) {
        const m = await page.evaluate(() => {
          const doc = document.scrollingElement;
          const btn = [...document.querySelectorAll('table.runlist td button.danger')].at(-1);
          const r = btn ? btn.getBoundingClientRect() : null;
          const keys = [...document.querySelectorAll('table.runlist td.key')].map((el) => ({
            text: el.textContent, sw: el.scrollWidth, cw: el.clientWidth,
          }));
          const ids = [...document.querySelectorAll('table.runlist td.id')].map((el) => ({
            text: el.textContent.trim(), sw: el.scrollWidth, cw: el.clientWidth,
          }));
          return { sw: doc.scrollWidth, iw: window.innerWidth, box: r ? { l: r.left, r: r.right } : null, keys, ids };
        });
        if (m.sw > m.iw) layoutFailures.push(`${theme}-390: scrollWidth ${m.sw} > innerWidth ${m.iw}`);
        if (!m.box) layoutFailures.push(`${theme}-390: no cancel button rendered on the runs table`);
        else if (m.box.l < 0 || m.box.r > m.iw)
          layoutFailures.push(`${theme}-390: cancel button at [${m.box.l.toFixed(0)},${m.box.r.toFixed(0)}] escapes ${m.iw}px`);
        // Keys are the run's identity: short ones must never truncate.
        const cut = m.keys.filter((k) => k.text.length < 20 && k.sw > k.cw);
        if (cut.length)
          layoutFailures.push(`${theme}-390: ${cut.length} key(s) under 20 chars truncated: ${cut.map((k) => JSON.stringify(k.text)).join(', ')}`);
        // The id column sizes from the widest id on the page (idChWidth):
        // nothing id-shaped may ever clip.
        const cutIds = m.ids.filter((k) => k.sw > k.cw);
        if (cutIds.length)
          layoutFailures.push(`${theme}-390: ${cutIds.length} id(s) truncated: ${cutIds.map((k) => k.text).join(', ')}`);
      }
      const views = [
        ['detail', async () => {
          await page.locator('table tbody tr td .link').first().click();
          await page.waitForSelector('.panel', { timeout: 4000 });
        }],
        ['submit', async () => {
          await page.locator('nav.tabs button', { hasText: 'submit' }).click();
        }],
        ['admin', async () => {
          await page.locator('nav.tabs button', { hasText: 'admin' }).click();
          await page.waitForSelector('.panel dl', { timeout: 4000 });
        }],
      ];
      for (const [name, go] of views) {
        try {
          await go();
          await snap(name);
        } catch (e) {
          if (!BEFORE_MODE) throw e;
          console.log(`${theme}-${width}-${name}: view not present in this build — skipped`);
        }
      }
      await ctx.close();
    }
  }
  await browser.close();
  console.log(shots.map((s) => `shot: ${s}`).join('\n'));
  if (layoutFailures.length) {
    console.error(`layout failures:\n  ${layoutFailures.join('\n  ')}`);
    process.exit(4);
  }
  console.log(BEFORE_MODE
    ? `before build (--ui-dir=${args['ui-dir']}): captured the views that exist; layout checks skipped`
    : '390px layout checks: pass');
} finally {
  for (const k of kids) { try { k.kill('SIGKILL'); } catch {} }
  fs.rmSync(dir, { recursive: true, force: true });
}
