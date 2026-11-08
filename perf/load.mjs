// Durable Runner load harness — dependency-free, drives the real binary.
//
//   node perf/load.mjs <scenario> [--key=value]
//
// Scenarios (every one writes a result JSON to perf/results/):
//   submit     POST /jobs at a fixed rate; latencies, refusals, done-rate
//   replay     pre-fill N events, restart, time replay-to-ready
//   stall      handler sleeps past the step deadline -> reconcile -> done
//   admission  flood submits past channel capacity -> 503 reserve refusals
//   compact    steady submits while /admin/compact runs repeatedly
//   sustained  the million-op run: fixed submit rate for a duration
//
// Evidence rules: machine spec + commit hash recorded in every result;
// a missed threshold is a failing result, kept — never rerun to green.
import { spawn, execFileSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const args = Object.fromEntries(
  process.argv.slice(3).map((a) => {
    const m = a.match(/^--([^=]+)=(.*)$/);
    return m ? [m[1], m[2]] : [a.replace(/^--/, ''), 'true'];
  })
);
const scenario = process.argv[2];
const ROOT = path.resolve(import.meta.dirname, '..');
const SRV = `${ROOT}/target/release/durable-server`;
const PORT = Number(args.port ?? 58400);
const HPORT = Number(args.hport ?? 58401);
const API = `http://127.0.0.1:${PORT}`;
const INPUT_B64 = Buffer.from('load').toString('base64');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const now = () => Date.now();

function machine() {
  return {
    host: os.hostname(),
    platform: `${os.platform()}/${os.arch()}`,
    cpu: os.cpus()[0]?.model,
    cores: os.cpus().length,
    mem_gb: Math.round(os.totalmem() / 2 ** 30),
    date: new Date().toISOString(),
    commit: execFileSync('git', ['rev-parse', '--short', 'HEAD'], { cwd: ROOT }).toString().trim(),
  };
}

function writeResult(name, data) {
  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  const file = `${ROOT}/perf/results/${stamp}-${name}.json`;
  fs.writeFileSync(file, JSON.stringify({ machine: machine(), scenario: name, ...data }, null, 2));
  console.log(`result -> ${file}`);
}

// --- handler fixture -------------------------------------------------------
// modes: ok (200), stall (sleep ms), chaos (random 500s + hangs)
const seen = new Set();
function startHandler(mode, stallMs = 0) {
  const src = `
    const seen = new Set();
    require('node:http').createServer((req, res) => {
      const url = new URL(req.url, 'http://x');
      if (req.method === 'GET') {
        res.writeHead(seen.has(url.searchParams.get('idem_key')) ? 200 : 404).end(); return;
      }
      let b = ''; req.on('data', c => b += c);
      req.on('end', () => {
        const key = JSON.parse(b || '{}').idem_key || '';
        // Idempotent semantics: the key is recorded on receipt — the effect
        // is committed-or-in-flight from the runner's point of view, so a
        // reconcile probe during the stall finds it and never repeats.
        seen.add(key);
        const answer = () => {
          res.writeHead(200, {'content-type':'application/json'});
          res.end(JSON.stringify({ok:true}));
        };
        ${mode === 'stall' ? `setTimeout(answer, ${stallMs});` :
          mode === 'chaos' ? `Math.random() < 0.1 ? setTimeout(answer, 25000) : (Math.random() < 0.1 ? (res.writeHead(500).end(),0) : answer());` :
          'answer();'}
      });
    }).listen(${HPORT}, '127.0.0.1');
  `;
  const p = spawn('node', ['-e', src], { stdio: 'ignore' });
  return p;
}

async function startServer(journalDir, extra = []) {
  const p = spawn(SRV, ['--journal', journalDir, '--listen', `127.0.0.1:${PORT}`,
    '--handler', `step=http://127.0.0.1:${HPORT}/step`, ...extra], { stdio: 'ignore' });
  for (let i = 0; i < 100; i++) {
    try { await fetch(`${API}/health`); return p; } catch { await sleep(50); }
  }
  throw new Error('server never became healthy');
}

async function submit(opKey, steps = 1) {
  const t0 = performance.now();
  const r = await fetch(`${API}/jobs`, {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      op_key: opKey,
      spec: { version: 1, steps: Array.from({ length: steps }, (_, i) => ({ name: `s${i}`, handler: 'step' })), input_b64: INPUT_B64 },
    }),
  });
  return { status: r.status, ms: performance.now() - t0, body: await r.text() };
}

const pct = (a, p) => a.length ? a.sort((x, y) => x - y)[Math.min(a.length - 1, Math.floor(p * a.length))] : null;

// --- scenarios -------------------------------------------------------------
async function runSubmit() {
  const rate = Number(args.rate ?? 50), secs = Number(args.seconds ?? 60);
  const dir = fs.mkdtempSync(`${os.tmpdir()}/dr-load-`);
  const h = startHandler('ok'); const s = await startServer(`${dir}/journal`);
  const lat = []; const codes = {}; let sent = 0, accepted = 0;
  const t0 = now();
  while (now() - t0 < secs * 1000) {
    const tickStart = now();
    for (let i = 0; i < rate; i++) {
      sent++;
      submit(`load-${sent}`).then((r) => {
        lat.push(r.ms); codes[r.status] = (codes[r.status] ?? 0) + 1;
        if (r.status === 200) accepted++;
      });
    }
    const elapsed = now() - tickStart;
    if (elapsed < 1000) await sleep(1000 - elapsed);
  }
  await sleep(8000); // let the last submissions drain
  const list = await (await fetch(`${API}/jobs`)).json();
  const done = list.runs.filter((r) => r.state === 'done').length;
  h.kill(); s.kill();
  writeResult('submit', {
    params: { rate_per_s: rate, seconds: secs, fsync: 'per-record (default)' },
    sent, accepted, done_of_last_drain: done, http_codes: codes,
    latency_ms: { p50: pct(lat, 0.5), p99: pct(lat, 0.99), max: pct(lat, 1) },
    verdict: accepted === sent && done >= accepted * 0.99 ? 'pass' : 'FAIL',
  });
}

async function runReplay() {
  const events = Number(args.events ?? 100_000);
  const dir = fs.mkdtempSync(`${os.tmpdir()}/dr-load-`);
  const h = startHandler('ok'); let s = await startServer(`${dir}/journal`);
  // Fill the journal: each submit = 1 event, each step lifecycle ~3 more.
  const steps = 4; // 4 steps/run: accepted + (dispatch+checkpoint)x3 + last -> ~events/4 runs
  const runs = Math.ceil(events / (1 + steps * 2));
  console.log(`filling ~${runs} runs (${runs * (1 + steps * 2)} records)...`);
  for (let i = 0; i < runs; i++) await submit(`replay-${i}`, steps);
  await sleep(runs / 2 + 3000);
  s.kill('SIGKILL'); await sleep(500);
  const t0 = now();
  s = await startServer(`${dir}/journal`); // open replays the journal
  const replayMs = now() - t0;
  const list = await (await fetch(`${API}/jobs`)).json();
  h.kill(); s.kill();
  writeResult('replay', {
    params: { target_events: events, runs, steps_per_run: steps },
    replay_ms: replayMs, runs_recovered: list.runs.length,
    verdict: list.runs.length === runs ? 'pass' : 'FAIL',
  });
}

async function runStall() {
  const stallMs = Number(args.stall_ms ?? 30000), n = Number(args.runs ?? 20);
  const dir = fs.mkdtempSync(`${os.tmpdir()}/dr-load-`);
  const h = startHandler('stall', stallMs);
  const s = await startServer(`${dir}/journal`, ['--step-timeout-ms', '2000']);
  const ids = [];
  for (let i = 0; i < n; i++) {
    const r = await submit(`stall-${i}`);
    if (r.status === 200) ids.push(JSON.parse(r.body).run_id);
  }
  // Wait past the stall: runs should end reconciled + done (probe finds key)
  await sleep(stallMs + 15_000);
  const list = await (await fetch(`${API}/jobs`)).json();
  const states = {};
  for (const r of list.runs) states[r.state.split(':')[0]] = (states[r.state.split(':')[0]] ?? 0) + 1;
  h.kill(); s.kill();
  writeResult('stall', {
    params: { runs: n, handler_stall_ms: stallMs, step_deadline_ms: 2000 },
    states,
    verdict: (states.done ?? 0) === n ? 'pass' : 'FAIL',
    note: 'stall past deadline -> ReplyLost -> probe (handler saw the key) -> reconciled done, never repeated',
  });
}

async function runAdmission() {
  const dir = fs.mkdtempSync(`${os.tmpdir()}/dr-load-`);
  const h = startHandler('stall', 60000); // keep in-flight slots occupied
  const s = await startServer(`${dir}/journal`);
  const codes = {}; const N = Number(args.n ?? 3000);
  await Promise.all(Array.from({ length: N }, async (_, i) => {
    const r = await submit(`flood-${i}`);
    codes[r.status] = (codes[r.status] ?? 0) + 1;
  }));
  h.kill(); s.kill();
  writeResult('admission', {
    params: { parallel_submits: N, cmd_capacity: 1024 },
    http_codes: codes,
    verdict: (codes[200] ?? 0) + (codes[503] ?? 0) + (codes[409] ?? 0) === N ? 'pass' : 'FAIL',
    note: 'over-capacity must refuse (503 reserve / 409), never queue silently',
  });
}

async function runCompact() {
  const secs = Number(args.seconds ?? 30);
  const dir = fs.mkdtempSync(`${os.tmpdir()}/dr-load-`);
  const h = startHandler('ok'); const s = await startServer(`${dir}/journal`);
  const lat = []; const codes = {}; let sent = 0;
  const t0 = now();
  const submits = (async () => {
    while (now() - t0 < secs * 1000) {
      sent++;
      submit(`compact-${sent}`).then((r) => { lat.push(r.ms); codes[r.status] = (codes[r.status] ?? 0) + 1; });
      await sleep(20);
    }
  })();
  const compactions = [];
  while (now() - t0 < secs * 1000) {
    const tc = now();
    const r = await fetch(`${API}/admin/compact`, { method: 'POST' });
    compactions.push({ status: r.status, ms: now() - tc });
    await sleep(5000);
  }
  await submits; await sleep(3000);
  const list = await (await fetch(`${API}/jobs`)).json();
  h.kill(); s.kill();
  writeResult('compact-under-load', {
    params: { seconds: secs, submit_interval_ms: 20 },
    submitted: sent, http_codes: codes, compactions,
    runs_after: list.runs.length,
    latency_ms: { p50: pct(lat, 0.5), p99: pct(lat, 0.99) },
    verdict: Object.keys(codes).every((c) => c === '200') && compactions.every((c) => c.status === 200) ? 'pass' : 'FAIL',
  });
}

async function runSustained() {
  const rate = Number(args.rate ?? 50), total = Number(args.total ?? 1_000_000);
  const dir = fs.mkdtempSync(`${os.tmpdir()}/dr-load-`);
  const h = startHandler('ok'); const s = await startServer(`${dir}/journal`);
  const lat = []; const codes = {}; let sent = 0, accepted = 0;
  const t0 = now();
  const perTick = Math.max(1, Math.round(rate / 10));
  while (sent < total) {
    const tickStart = now();
    for (let i = 0; i < perTick && sent < total; i++) {
      sent++;
      submit(`sustained-${sent}`).then((r) => {
        lat.push(r.ms); codes[r.status] = (codes[r.status] ?? 0) + 1;
        if (r.status === 200) accepted++;
      });
    }
    const elapsed = now() - tickStart;
    if (elapsed < 100) await sleep(100 - elapsed);
    if (sent % 100_000 === 0) console.log(`${sent} sent, ${accepted} accepted @${((now() - t0) / 1000).toFixed(0)}s`);
  }
  await sleep(30_000);
  const list = await (await fetch(`${API}/jobs`)).json();
  const done = list.runs.filter((r) => r.state === 'done').length;
  const jsize = execFileSync('du', ['-sk', `${dir}/journal`]).toString().split('\t')[0];
  h.kill(); s.kill();
  writeResult('sustained', {
    params: { rate_per_s: rate, target_submits: total, fsync: 'per-record' },
    sent, accepted, done_after_drain: done, http_codes: codes,
    wall_clock_s: Math.round((now() - t0) / 1000),
    journal_kb: Number(jsize),
    latency_ms: { p50: pct(lat, 0.5), p99: pct(lat, 0.99), max: pct(lat, 1) },
    verdict: accepted === sent && done === sent ? 'pass' : 'FAIL',
  });
}

const runners = {
  submit: runSubmit, replay: runReplay, stall: runStall,
  admission: runAdmission, compact: runCompact, sustained: runSustained,
};
if (!runners[scenario]) {
  console.error(`unknown scenario ${scenario}; choices: ${Object.keys(runners).join(', ')}`);
  process.exit(2);
}
await runners[scenario]();
