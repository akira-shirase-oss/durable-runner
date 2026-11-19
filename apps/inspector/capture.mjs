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
for (const a of process.argv.slice(2).filter((a) => a.startsWith('--'))) {
  const [k, v] = a.slice(2).split('=');
  args[k] = v;
}
const UI_DIR = path.resolve(args['ui-dir'] ?? `${ROOT}/apps/inspector/build`);
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
      await page.waitForSelector('table tbody tr', { timeout: 10000 });
      const snap = async (name) => {
        const file = `${OUT}/${theme}-${width}-${name}.png`;
        await page.screenshot({ path: file, fullPage: false });
        shots.push(file);
      };
      await snap('runs');
      await page.locator('table tbody tr td .link').first().click();
      await page.waitForSelector('.panel', { timeout: 5000 });
      await snap('detail');
      await page.locator('nav.tabs button', { hasText: 'submit' }).click();
      await snap('submit');
      await page.locator('nav.tabs button', { hasText: 'admin' }).click();
      await page.waitForSelector('.panel dl', { timeout: 5000 });
      await snap('admin');
      await ctx.close();
    }
  }
  await browser.close();
  console.log(shots.map((s) => `shot: ${s}`).join('\n'));
} finally {
  for (const k of kids) { try { k.kill('SIGKILL'); } catch {} }
  fs.rmSync(dir, { recursive: true, force: true });
}
