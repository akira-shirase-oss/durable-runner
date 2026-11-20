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
//   workspaces S2: N workspaces, own keys, split submits + isolation
//              (needs DATABASE_URL; skips visibly without one)
//
// Flags: --smoke picks the small local sizes (perf/battery.sh runs these);
// the full sizes are what .github/workflows/load.yml runs on CI. --p99=MS
// moves the declared latency bound. A scenario exits non-zero when its
// verdict is FAIL so a battery can gate on it.
//
// Evidence rules: machine spec + commit hash recorded in every result;
// a missed threshold is a failing result, kept — never rerun to green.
import { spawn, spawnSync, execFileSync } from 'node:child_process';
import fs from 'node:fs';
import net from 'node:net';
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
const PORT = Number(args.port ?? 53340); // not 58400: that is Service Monitor's default port, and a harness must never collide with a running monitor
const HPORT = Number(args.hport ?? 53341);
const API = `http://127.0.0.1:${PORT}`;
const INPUT_B64 = Buffer.from('load').toString('base64');
// Mutating and admin routes need the API key; the spawned server reads it
// from DURABLE_API_KEY, so one random key per harness run serves both.
const API_KEY = process.env.DURABLE_API_KEY ?? (await import('node:crypto')).randomBytes(32).toString('hex');
const AUTH = { authorization: `Bearer ${API_KEY}` };
const SMOKE = 'smoke' in args;
// The declared p99 bound for every scenario that measures latency. The
// pre-group-commit regime failed the gate at p99 24-30 s; the current code
// measures ~40 ms at 100 submits/s, so 2 s is 50x headroom over the
// measurement and still catches that regression.
const P99_MS = Number(args.p99 ?? 2000);

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const now = () => Date.now();

function machine() {
  return {
    host: process.env.PERF_HOST_LABEL ?? "maintainer-laptop", // never the real hostname: results are public
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
  return data;
}

// A verdict is computed from named checks, so a result says exactly which
// bound it missed. Misses are kept, never rerun to green.
function verdictOf(checks) {
  const failures = checks.filter(([, ok]) => !ok).map(([label]) => label);
  return {
    verdict: failures.length ? 'FAIL' : 'pass',
    ...(failures.length ? { failures } : {}),
  };
}

// --- spawned children ------------------------------------------------------
// Every spawned child is recorded in a pidfile AND tracked in memory:
//   - on ANY exit path the harness SIGKILLs its children and removes their
//     pidfiles (SIGKILL on the harness itself skips this — the pidfiles stay);
//   - on startup it reaps whatever its own leftover pidfiles name: the pid
//     is killed only if its current command line still matches the recorded
//     one (a reused pid is never touched). Then the port pre-flight runs.
// A crashed run's orphan otherwise answers the next run's /health with the
// old run's API key — 401s and resets that read as a product failure.
const PIDDIR = `${os.tmpdir()}/durable-load.pids`;
fs.mkdirSync(PIDDIR, { recursive: true });
const kids = new Set();
function pidfile(pid) { return `${PIDDIR}/${pid}.json`; }
function track(p, match, command, port) {
  fs.writeFileSync(pidfile(p.pid), JSON.stringify({
    pid: p.pid, match, command, port, started_at: new Date().toISOString(),
  }));
  kids.add(p);
  p.on('exit', () => { kids.delete(p); fs.rmSync(pidfile(p.pid), { force: true }); });
  return p;
}
function alive(pid) {
  try { process.kill(pid, 0); return true; }
  catch (e) { return e.code !== 'ESRCH'; } // EPERM: exists, we may not signal it
}
function cmdline(pid) {
  // spawnSync: ps exits 1 for a dead pid and is denied outright under a
  // sandbox (EPERM) — either way we just get no match.
  const r = spawnSync('ps', ['-p', String(pid), '-o', 'command=']);
  return r.error ? '' : (r.stdout || '').toString();
}
function portHeldBy(port, pid) {
  const r = spawnSync('lsof', ['-nP', `-iTCP:${port}`, '-sTCP:LISTEN']);
  if (r.error) throw r.error; // lsof denied/missing: cannot verify
  return (r.stdout || '').toString().split('\n')
    .some((l) => l.trim().split(/\s+/)[1] === String(pid));
}
// True when the pidfile may be deleted: the pid is gone, or proven ours and
// killed. False when it lives but cannot be verified — never kill a stranger,
// never drop the only record of our own stray child.
function verifyAndReap(rec) {
  if (!alive(rec.pid)) return true;
  let ours = cmdline(rec.pid).includes(rec.match);
  if (!ours && rec.port != null) {
    try { ours = portHeldBy(rec.port, rec.pid); } catch { /* cannot verify here */ }
  }
  if (!ours) return false;
  try {
    process.kill(rec.pid, 'SIGKILL');
  } catch (e) {
    // ESRCH: raced, already gone. EPERM (sandboxed signals): our orphan but
    // unreachable — say so and keep the pidfile so every run keeps flagging it.
    if (e.code !== 'ESRCH') {
      console.error(`stale child ${rec.pid} (${rec.command}) is ours but could not be killed: ${e.code}`);
      return false;
    }
  }
  return true;
}
function reapStale() {
  for (const f of fs.readdirSync(PIDDIR).filter((f) => f.endsWith('.json'))) {
    let rec;
    try { rec = JSON.parse(fs.readFileSync(`${PIDDIR}/${f}`, 'utf8')); }
    catch { fs.rmSync(`${PIDDIR}/${f}`, { force: true }); continue; }
    if (verifyAndReap(rec)) fs.rmSync(`${PIDDIR}/${f}`, { force: true });
  }
}
process.on('exit', () => {
  for (const k of kids) {
    try { k.kill('SIGKILL'); } catch { /* already gone */ }
  }
});
for (const sig of ['SIGINT', 'SIGTERM']) {
  process.on(sig, () => process.exit(sig === 'SIGINT' ? 130 : 143));
}
reapStale();

// Something is already listening on a port the harness wants? Stop rather
// than run against it — see above.
function portFree(port) {
  return new Promise((res) => {
    const s = net.connect(port, '127.0.0.1');
    s.once('connect', () => { s.end(); res(false); });
    s.once('error', () => res(true));
  });
}
async function assertFree(port, what) {
  if (!(await portFree(port))) {
    console.error(`${what} port ${port} already has a listener — a stale process from an earlier run? pick another --port/--hport`);
    process.exit(3);
  }
}
await assertFree(PORT, 'server');
await assertFree(HPORT, 'handler');

// --- handler fixture -------------------------------------------------------
// modes: ok (200), stall (sleep ms), chaos (random 500s + hangs)
const seen = new Set();
function startHandler(mode, stallMs = 0) {
  const src = `
    const DRLOAD = 'drload-handler-${HPORT}'; // in the command line: stale-reap marker
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
  const p = track(
    spawn('node', ['-e', src], { stdio: 'ignore' }),
    `drload-handler-${HPORT}`,
    `node -e <handler fixture> on 127.0.0.1:${HPORT}`,
    HPORT,
  );
  return p;
}

async function startServer(journalDir, extra = []) {
  // The battery measures the engine's bounds, not the per-address
  // limiter's (that has its own tests): the request-rate path stays on at
  // 100k/s so it is exercised, but never trips; the per-peer conn cap is
  // off so the admission flood can queue sockets the kernel will allow.
  const argv = ['--journal', journalDir, '--listen', `127.0.0.1:${PORT}`,
    '--handler', `step=http://127.0.0.1:${HPORT}/step`,
    // The fixture handler lives on loopback — the S2-5 opt-out, always
    // visible in argv.
    '--allow-local-handlers',
    '--rate-limit-per-addr', '100000', '--max-conns-per-addr', '0', ...extra];
  const p = track(
    spawn(SRV, argv, { stdio: 'ignore', env: { ...process.env, DURABLE_API_KEY: API_KEY } }),
    journalDir, // unique per run (mkdtemp): matches only a child that serves this journal
    [SRV, ...argv].join(' '),
    PORT,
  );
  for (let i = 0; i < 100; i++) {
    // If the child died (EADDRINUSE, a bad flag) while something still
    // answers /health, the listener is not ours — say so, don't run on it.
    if (p.exitCode !== null) throw new Error(`durable-server exited (code ${p.exitCode}) before becoming healthy`);
    try { await fetch(`${API}/health`); return p; } catch { await sleep(50); }
  }
  throw new Error('server never became healthy');
}

async function submit(opKey, steps = 1, auth = AUTH) {
  const t0 = performance.now();
  try {
    const r = await fetch(`${API}/jobs`, {
      method: 'POST', headers: { 'content-type': 'application/json', ...auth },
      body: JSON.stringify({
        op_key: opKey,
        spec: { version: 1, steps: Array.from({ length: steps }, (_, i) => ({ name: `s${i}`, handler: 'step' })), input_b64: INPUT_B64 },
      }),
    });
    return { status: r.status, ms: performance.now() - t0, body: await r.text() };
  } catch (e) {
    // A refused or reset connection is backpressure at the TCP layer (the
    // kernel's accept queue is full): record it under its own code so a
    // flood shows kernel refusals next to the server's HTTP 503s instead of
    // crashing the harness with no result at all.
    const code = e.cause?.code ?? e.code ?? 'error';
    return { status: `tcp:${code}`, ms: performance.now() - t0, body: '' };
  }
}

// Run counts come from the bounded diagnostics (counts by state, never
// rows): GET /jobs is paged, and paging 1M runs to count them would be the
// unbounded read the paging exists to prevent.
async function diag() {
  return (await fetch(`${API}/admin/diagnostics`, { headers: AUTH })).json();
}

const pct = (a, p) => a.length ? a.sort((x, y) => x - y)[Math.min(a.length - 1, Math.floor(p * a.length))] : null;

// --- scenarios -------------------------------------------------------------
async function runSubmit() {
  const rate = Number(args.rate ?? (SMOKE ? 20 : 50)), secs = Number(args.seconds ?? (SMOKE ? 10 : 60));
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
  const done = (await diag()).states.done ?? 0;
  h.kill(); s.kill();
  const p99 = pct(lat, 0.99);
  const r = writeResult('submit', {
    params: { rate_per_s: rate, seconds: secs, fsync: 'per-record (default)', smoke: SMOKE },
    sent, accepted, done_of_last_drain: done, http_codes: codes,
    latency_ms: { p50: pct(lat, 0.5), p99, max: pct(lat, 1) },
    thresholds: { accepted: 'every submit (in-capacity rate)', done: '>=99% of accepted', p99_ms: `<${P99_MS}` },
    ...verdictOf([
      ['every submit accepted', accepted === sent],
      ['>=99% drained to done', done >= accepted * 0.99],
      [`p99 < ${P99_MS}ms`, p99 !== null && p99 < P99_MS],
    ]),
  });
  return r.verdict === 'pass';
}

async function runReplay() {
  const events = Number(args.events ?? (SMOKE ? 10_000 : 100_000));
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
  const recovered = (await diag()).live_runs;
  h.kill(); s.kill();
  const r = writeResult('replay', {
    params: { target_events: events, runs, steps_per_run: steps, smoke: SMOKE },
    replay_ms: replayMs, runs_recovered: recovered,
    thresholds: { recovered: 'every filled run', replay_ms: '<30000' },
    ...verdictOf([
      ['every filled run recovered', recovered === runs],
      ['replay under 30 s', replayMs < 30_000],
    ]),
  });
  return r.verdict === 'pass';
}

async function runStall() {
  const stallMs = Number(args.stall_ms ?? (SMOKE ? 6_000 : 30_000)), n = Number(args.runs ?? (SMOKE ? 5 : 20));
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
  const states = (await diag()).states;
  h.kill(); s.kill();
  const r = writeResult('stall', {
    params: { runs: n, handler_stall_ms: stallMs, step_deadline_ms: 2000, smoke: SMOKE },
    states,
    thresholds: { done: 'every run reconciled to done' },
    ...verdictOf([['every run reconciled to done', (states.done ?? 0) === n]]),
    note: 'stall past deadline -> ReplyLost -> probe (handler saw the key) -> reconciled done, never repeated',
  });
  return r.verdict === 'pass';
}

async function runAdmission() {
  const dir = fs.mkdtempSync(`${os.tmpdir()}/dr-load-`);
  const h = startHandler('stall', 60000); // keep in-flight slots occupied
  const s = await startServer(`${dir}/journal`);
  const codes = {}; const N = Number(args.n ?? (SMOKE ? 1_500 : 3_000));
  await Promise.all(Array.from({ length: N }, async (_, i) => {
    const r = await submit(`flood-${i}`);
    codes[r.status] = (codes[r.status] ?? 0) + 1;
  }));
  h.kill(); s.kill();
  const honest = (codes[200] ?? 0) + (codes[503] ?? 0) + (codes[409] ?? 0);
  // tcp:* codes are kernel-level refusals (accept queue full): bounded
  // backpressure too, recorded separately from the server's chosen 503s.
  const tcp = Object.keys(codes)
    .filter((k) => k.startsWith('tcp:'))
    .reduce((a, k) => a + codes[k], 0);
  const r = writeResult('admission', {
    params: { parallel_submits: N, cmd_capacity: 1024, smoke: SMOKE },
    http_codes: codes, tcp_refusals: tcp,
    thresholds: { codes: 'only 200/409/503 or a tcp refusal', refusals: 'a bound is actually hit' },
    ...verdictOf([
      ['every answer is an honest code or a tcp refusal', honest + tcp === N],
      // Either layer may refuse first: the server's queue bound (503) or
      // the kernel's accept queue (tcp reset) — macOS's is shallow enough
      // that it usually wins locally; CI shows the split.
      ['the flood hit a bound (503s or tcp refusals)', (codes[503] ?? 0) + tcp > 0],
    ]),
    note: 'over-capacity must refuse (503 reserve / 409), never queue silently',
  });
  return r.verdict === 'pass';
}

async function runCompact() {
  const secs = Number(args.seconds ?? (SMOKE ? 15 : 30));
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
    const r = await fetch(`${API}/admin/compact`, { method: 'POST', headers: AUTH });
    compactions.push({ status: r.status, ms: now() - tc });
    await sleep(5000);
  }
  await submits; await sleep(3000);
  const runsAfter = (await diag()).live_runs;
  h.kill(); s.kill();
  const p99 = pct(lat, 0.99);
  const r = writeResult('compact-under-load', {
    params: { seconds: secs, submit_interval_ms: 20, smoke: SMOKE },
    submitted: sent, http_codes: codes, compactions,
    runs_after: runsAfter,
    latency_ms: { p50: pct(lat, 0.5), p99 },
    thresholds: { submits: 'all 200', compactions: 'all 200', p99_ms: `<${P99_MS}` },
    ...verdictOf([
      ['every submit accepted', Object.keys(codes).every((c) => c === '200')],
      ['every compaction succeeded', compactions.every((c) => c.status === 200)],
      [`p99 < ${P99_MS}ms`, p99 !== null && p99 < P99_MS],
    ]),
  });
  return r.verdict === 'pass';
}

async function runSustained() {
  const rate = Number(args.rate ?? (SMOKE ? 50 : 100)), total = Number(args.total ?? (SMOKE ? 5_000 : 1_000_000));
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
  const done = (await diag()).states.done ?? 0;
  const jsize = execFileSync('du', ['-sk', `${dir}/journal`]).toString().split('\t')[0];
  h.kill(); s.kill();
  const p99 = pct(lat, 0.99);
  const r = writeResult('sustained', {
    params: { rate_per_s: rate, target_submits: total, fsync: 'per-record', smoke: SMOKE },
    sent, accepted, done_after_drain: done, http_codes: codes,
    wall_clock_s: Math.round((now() - t0) / 1000),
    journal_kb: Number(jsize),
    latency_ms: { p50: pct(lat, 0.5), p99, max: pct(lat, 1) },
    thresholds: { accepted: 'every submit', done: 'every accepted run', p99_ms: `<${P99_MS}` },
    ...verdictOf([
      ['every submit accepted', accepted === sent],
      ['every accepted run finished', done === sent],
      [`p99 < ${P99_MS}ms`, p99 !== null && p99 < P99_MS],
    ]),
  });
  return r.verdict === 'pass';
}

// The S2 battery: N workspaces share one server, each authenticated by its
// own minted key; submits split across workspaces and every workspace's
// listing is re-paged to count exactly its own runs. Needs DATABASE_URL —
// without one the server has no account graph and the scenario skips
// visibly rather than running compat mode and calling it workspace load.
async function runWorkspaces() {
  const backend = process.env.DATABASE_URL ? 'postgres' : process.env.DURABLE_ACCOUNTS_BACKEND;
  if (!backend) {
    console.error('workspaces scenario needs DATABASE_URL or DURABLE_ACCOUNTS_BACKEND=memory; skipping — this is a skip, not a pass');
    return true;
  }
  const n = Number(args.workspaces ?? 2), per = Number(args.per ?? (SMOKE ? 200 : 25_000));
  const dir = fs.mkdtempSync(`${os.tmpdir()}/dr-load-`);
  const h = startHandler('ok'); const s = await startServer(`${dir}/journal`);
  // Mint one admin key per workspace through the ops routes.
  const keys = [];
  for (let i = 0; i < n; i++) {
    const w = await (await fetch(`${API}/admin/workspaces`, {
      method: 'POST', headers: { 'content-type': 'application/json', ...AUTH },
      body: JSON.stringify({ name: `load-ws-${i}` }),
    })).json();
    const k = await (await fetch(`${API}/admin/workspaces/${w.public_id}/keys`, {
      method: 'POST', headers: { 'content-type': 'application/json', ...AUTH },
      body: JSON.stringify({ name: 'load', permission: 'admin' }),
    })).json();
    keys.push({ public_id: w.public_id, auth: { authorization: `Bearer ${k.key}` } });
  }
  // S2-4: handler names resolve per workspace — `step` exists only as a
  // ws-0 flag registration, so every workspace registers it through the
  // route before its submits can admit.
  for (const ws of keys) {
    const r = await fetch(`${API}/handlers/step`, {
      method: 'PUT', headers: { 'content-type': 'application/json', ...ws.auth },
      body: JSON.stringify({ url: `http://127.0.0.1:${HPORT}/step` }),
    });
    if (r.status !== 200) {
      console.error(`handler registration failed: ${r.status} ${await r.text()}`);
      h.kill(); s.kill(); return false;
    }
  }
  // Registration churn concurrent with dispatch lookups: a second name
  // flaps register→delete under the first workspace's key while the
  // submit burst runs — the registry is rebuilt per workspace, never
  // blocked.
  let churnDone = false; let churnOps = 0;
  const churn = (async () => {
    while (!churnDone) {
      const put = await fetch(`${API}/handlers/churn`, {
        method: 'PUT', headers: { 'content-type': 'application/json', ...keys[0].auth },
        body: JSON.stringify({ url: `http://127.0.0.1:${HPORT}/step` }),
      });
      const del = await fetch(`${API}/handlers/churn`, { method: 'DELETE', headers: keys[0].auth });
      if (put.status !== 200 || del.status !== 200) {
        console.error(`churn broke: put=${put.status} del=${del.status}`); return;
      }
      churnOps++; await sleep(5);
    }
  })();
  // Submit `per` runs per workspace, same op_key prefix in each — dedup is
  // per (workspace, key), so identical keys in two workspaces are two runs.
  const lat = []; const codes = {}; let sent = 0;
  const t0 = now();
  for (let i = 0; i < per; i++) {
    for (const ws of keys) {
      sent++;
      submit(`shared-${i}`, 1, ws.auth).then((r) => {
        lat.push(r.ms); codes[r.status] = (codes[r.status] ?? 0) + 1;
      });
    }
    if (i % 100 === 0) await sleep(50);
  }
  await sleep(15_000); // drain
  churnDone = true; await churn;
  // Page each workspace's list: it must contain exactly its `per` runs and
  // no id another workspace owns.
  const owned = new Map(); // ws -> Set(run ids)
  const pageCounts = {};
  for (const [w, ws] of keys.entries()) {
    const ids = new Set(); let after = null; let pages = 0;
    for (;;) {
      const q = after == null ? '' : `&after=${after}`;
      const page = await (await fetch(`${API}/jobs?limit=1000${q}`, { headers: ws.auth })).json();
      for (const r of page.runs ?? []) ids.add(r.id);
      pages++;
      if (page.next_after == null) break;
      after = page.next_after;
    }
    owned.set(w, ids); pageCounts[ws.public_id] = { runs: ids.size, pages };
  }
  const disjoint = [...owned.values()].every((ids, i) =>
    [...owned.values()].every((o, j) => i === j || [...ids].every((id) => !o.has(id))));
  // No-oracle spot check: workspace 0's first run read under workspace 1's key.
  const foreign = [...owned.get(0)][0];
  const probe = await fetch(`${API}/jobs/${foreign}`, { headers: keys[1].auth });
  const done = (await diag()).states.done ?? 0;
  h.kill(); s.kill();
  const p99 = pct(lat, 0.99);
  const r = writeResult('workspaces', {
    params: { workspaces: n, submits_per_workspace: per, account_backend: backend, smoke: SMOKE },
    sent, http_codes: codes, per_workspace: pageCounts, churn_registrations: churnOps,
    done_after_drain: done,
    latency_ms: { p50: pct(lat, 0.5), p99, max: pct(lat, 1) },
    thresholds: {
      accepted: 'every submit', sized: `${per} runs per workspace`,
      disjoint: 'no run id shared across workspaces', oracle: 'foreign run id → 404',
      done: 'every accepted run', p99_ms: `<${P99_MS}`,
    },
    ...verdictOf([
      ['every submit accepted', Object.keys(codes).every((c) => c === '200')],
      ['every workspace lists exactly its own runs', [...owned.values()].every((ids) => ids.size === per)],
      ['run ids disjoint across workspaces', disjoint],
      ['a foreign run id answers 404', probe.status === 404],
      ['every accepted run finished', done >= sent * 0.99],
      [`p99 < ${P99_MS}ms`, p99 !== null && p99 < P99_MS],
    ]),
  });
  return r.verdict === 'pass';
}

const runners = {
  submit: runSubmit, replay: runReplay, stall: runStall,
  admission: runAdmission, compact: runCompact, sustained: runSustained,
  workspaces: runWorkspaces,
};
if (!runners[scenario]) {
  console.error(`unknown scenario ${scenario}; choices: ${Object.keys(runners).join(', ')}`);
  process.exit(2);
}
const ok = await runners[scenario]();
process.exit(ok === false ? 1 : 0);
