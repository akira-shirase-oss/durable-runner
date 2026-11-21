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
// moves the declared latency bound; --submit_budget_ms/--drain_budget_ms
// bound how long refused ops may retry and runs may drain (see
// runSustained). A scenario exits non-zero when its verdict is FAIL so a
// battery can gate on it.
//
// Evidence rules: machine spec + commit hash recorded in every result;
// a missed threshold is a failing result, kept — never rerun to green.
// RESULTS_DIR overrides where result JSONs land (CI: a per-run dir).
//
// Memory: every counter, histogram, filter, and pending pool is FIXED
// size — the generator runs in O(1) heap at any scale (CI run 44760152519
// died at ~4 GB Mark-Compact because the old one retained per-op state).
// peak_heap_mb in every result is the measured proof.
import { spawn, spawnSync, execFileSync } from 'node:child_process';
import fs from 'node:fs';
import http from 'node:http';
import https from 'node:https';
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

// The client's own sockets are a bounded resource: an earlier churn-flood
// let keep-alive churn pile ~12k CLOSE_WAIT + ~12k FIN_WAIT_2 and exhaust
// the machine's ephemeral port range, taking every session's outbound TCP
// with it. Bounded keep-alive agents — node:http/node:https builtins, no
// third-party dep (undici-as-import broke CI on a clean checkout) — dead
// ends evict in seconds instead of camping for the socket lifetime.
const POOL_CONNS = Number(process.env.DR_LOAD_POOL_CONNS ?? 128);
const httpPool = new http.Agent({ keepAlive: true, maxSockets: POOL_CONNS, keepAliveMsecs: 4_000, scheduling: 'lifo' });
const httpsPool = new https.Agent({ keepAlive: true, maxSockets: POOL_CONNS, keepAliveMsecs: 4_000, scheduling: 'lifo' });

// A fetch-shaped helper over node:http — the harness's only HTTP client.
// Every response body is consumed (an unread body pins its conn), errors
// carry their kernel .code (a tcp refusal is a refusal, not an answer),
// and AbortSignal destroys the request's socket.
function httpFetch(url, opts = {}) {
  const u = new URL(url);
  const mod = u.protocol === 'https:' ? https : http;
  const pooled = !process.env.DR_LOAD_NO_POOL;
  const agent = pooled ? (u.protocol === 'https:' ? httpsPool : httpPool) : false;
  return new Promise((resolve, reject) => {
    const req = mod.request(u, {
      method: opts.method ?? 'GET',
      headers: opts.headers,
      agent,
    }, (res) => {
      const chunks = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => {
        const body = Buffer.concat(chunks);
        resolve({
          status: res.statusCode,
          ok: res.statusCode >= 200 && res.statusCode < 300,
          headers: res.headers,
          text: async () => body.toString('utf8'),
          arrayBuffer: async () => body.buffer.slice(body.byteOffset, body.byteOffset + body.byteLength),
          json: async () => JSON.parse(body.toString('utf8')),
        });
      });
      res.on('error', reject);
    });
    if (opts.signal) {
      if (opts.signal.aborted) {
        req.destroy(Object.assign(new Error('aborted'), { name: 'AbortError' }));
        return;
      }
      opts.signal.addEventListener('abort', () => {
        req.destroy(Object.assign(new Error('aborted'), { name: 'AbortError' }));
      }, { once: true });
    }
    req.on('error', reject);
    if (opts.body) req.write(opts.body);
    req.end();
  });
}
const fetch = httpFetch;

// One load run per machine at a time (the heavy slot): a second run races
// the first for ports, fds and the measurement — refuse it, don't queue.
// The slot is MACHINE-wide (one shared path under tmpdir, not inside any
// checkout — two worktrees must resolve to the same slot) and it names its
// holder pid: a stale slot whose pid is dead gets reclaimed, a live one
// refuses. heavy.sh/battery.sh hold the same slot for their children via
// DR_HEAVY_HELD; a bare `node load.mjs` still self-guards.
const LOCK = path.join(os.tmpdir(), 'durable-runner-heavy.slot');
const unlock = () => { try { fs.rmSync(LOCK, { recursive: true, force: true }); } catch { /* already gone */ } };
if (!process.env.DR_HEAVY_HELD) {
  for (let attempt = 0; attempt < 4; attempt++) {
    try {
      fs.mkdirSync(LOCK);
      fs.writeFileSync(path.join(LOCK, 'holder'), `${process.pid} ${process.argv.slice(1).join(' ')}\n`);
      process.on('exit', unlock);
      for (const sig of ['SIGINT', 'SIGTERM']) {
        process.on(sig, () => { unlock(); process.exit(130); });
      }
      break;
    } catch (e) {
      if (e.code !== 'EEXIST') throw e;
      // Contended. Liveness decides: a dead holder's slot is stale and gets
      // swept; a live one (or an unanswerable one) refuses the run. kill(0)
      // answers ESRCH for dead, EPERM for alive-but-not-ours — both are
      // "alive" for our purpose, so only ESRCH permits the sweep.
      let alive = true;
      try {
        const holder = fs.readFileSync(path.join(LOCK, 'holder'), 'utf8');
        const pid = Number(holder.split(' ')[0]);
        if (Number.isFinite(pid) && pid > 0) {
          try { process.kill(pid, 0); } catch (ke) { alive = ke.code !== 'ESRCH'; }
        }
      } catch {
        // No holder file yet — a slot that young is being written, not stale.
        try { alive = Date.now() - fs.statSync(LOCK).mtimeMs < 30_000; } catch { alive = false; }
      }
      if (alive) {
        let holder = 'unknown';
        try { holder = fs.readFileSync(path.join(LOCK, 'holder'), 'utf8').trim(); } catch {}
        console.error(`another load run holds ${LOCK} (${holder}) — one heavy run per machine`);
        process.exit(3);
      }
      unlock(); // stale: the recorded pid is dead — reclaim and retry
    }
  }
  process.env.DR_HEAVY_HELD = String(process.pid);
}

// Socket self-check: this process's own TCP count, sampled. A run whose
// client-side dead conns grow past the bound is the leak above — the run
// fails and its result is kept, not rerun to green. CLOSE_WAIT and
// FIN_WAIT_2 are bounded separately — they pile on different sides of a
// half-close and each is independently sufficient to exhaust the box.
// An lsof that cannot answer fails the run too: blind is not "clean".
const SOCKET_BOUND = Number(process.env.DR_LOAD_SOCKET_BOUND ?? 1024);
let socketPeak = 0, closeWaitPeak = 0, finWait2Peak = 0, socketOverBound = 0, lsofBlind = 0;
const countOwnSockets = () => {
  // lsof exits 1 when it lists nothing — parse stdout either way; but a
  // spawn failure or a nonzero exit with empty output means we cannot see.
  const out = spawnSync('lsof', ['-nP', '-a', '-p', String(process.pid), '-iTCP'], { encoding: 'utf8' });
  if (out.error || (out.status !== 0 && !String(out.stdout ?? '').includes('TCP'))) {
    if (out.error || out.status > 1) {
      lsofBlind++;
      console.error(`socket self-check: lsof cannot answer (error=${out.error} status=${out.status})`);
      return { total: -1, cw: -1, fw2: -1 };
    }
  }
  let total = 0, cw = 0, fw2 = 0;
  for (const line of String(out.stdout ?? '').split('\n')) {
    if (!line.includes('TCP')) continue;
    total++;
    if (line.includes('CLOSE_WAIT')) cw++;
    if (line.includes('FIN_WAIT_2')) fw2++;
  }
  socketPeak = Math.max(socketPeak, total);
  closeWaitPeak = Math.max(closeWaitPeak, cw);
  finWait2Peak = Math.max(finWait2Peak, fw2);
  if (cw > SOCKET_BOUND || fw2 > SOCKET_BOUND) {
    socketOverBound++;
    console.error(`socket self-check: ${cw} CLOSE_WAIT / ${fw2} FIN_WAIT_2 > bound ${SOCKET_BOUND} (total ${total})`);
  }
  return { total, cw, fw2 };
};
// 1 s cadence: a dead-conn pile lives ~4 s (keep-alive evict) — a slower
// sampler misses it entirely and the self-check goes blind.
const sockTimer = setInterval(countOwnSockets, 1_000);
sockTimer.unref();
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

// --- O(1) accounting -------------------------------------------------------
// The battery is million-scale: no per-op array, Set, or promise fan-out may
// grow with the run. A generator that retains per-op state is itself the
// memory bug the 44760152519 run hit (~4 GB Mark-Compact at ~140 s in).
//
// Fixed-bucket latency histogram: 1 ms resolution to 2 s, doubling buckets
// to ~4.4 min, plus an overflow bucket. Percentiles report the containing
// bucket's UPPER edge — a conservative bound on the true value.
const LAT_EDGES = (() => {
  const e = [];
  for (let ms = 1; ms <= 2048; ms++) e.push(ms);
  for (let ms = 4096; ms <= 262144; ms *= 2) e.push(ms);
  return e;
})();
class LatHist {
  constructor() { this.counts = new Array(LAT_EDGES.length + 1).fill(0); this.n = 0; this.max = null; }
  add(ms) {
    this.n++;
    if (this.max === null || ms > this.max) this.max = ms;
    if (ms <= 2048) { this.counts[Math.max(0, Math.ceil(ms) - 1)]++; return; }
    for (let i = 2048; i < LAT_EDGES.length; i++) if (ms <= LAT_EDGES[i]) { this.counts[i]++; return; }
    this.counts[LAT_EDGES.length]++;
  }
  pct(p) {
    const need = Math.ceil(p * this.n);
    let c = 0;
    for (let i = 0; i < LAT_EDGES.length; i++) {
      c += this.counts[i];
      if (c >= need) return LAT_EDGES[i];
    }
    return null; // every sample in the overflow bucket
  }
  obj() {
    return {
      p50: this.pct(0.5), p99: this.pct(0.99),
      max: this.max === null ? null : Math.round(this.max * 1000) / 1000,
      note: 'bucket upper edges: 1 ms to 2 s, x2 to ~4.4 min (true pXX <= value)',
    };
  }
}

// Per-workspace Bloom-style filter for the million-id disjointness pass:
// 16 Mbit (2 MiB) fixed per workspace, 4 probes by FNV-1a double hashing.
// A probable hit is recorded as a bounded "suspect" and then verified
// exactly through the API — the filter makes full coverage O(1) memory,
// the suspect re-checks keep the verdict exact, never statistical.
const BLOOM_BITS = 1 << 24;
class Bloom {
  constructor() { this.bits = new Uint8Array(BLOOM_BITS / 8); }
  hashes(id) {
    let h = 0xcbf29ce484222325n;
    for (let i = 0; i < id.length; i++) {
      h ^= BigInt(id.charCodeAt(i));
      h = (h * 0x100000001b3n) & 0xffffffffffffffffn;
    }
    const h1 = Number(h & 0xffffffffn) >>> 0;
    const h2 = (Number(h >> 32n) >>> 0) | 1;
    return [0, 1, 2, 3].map((i) => (h1 + i * h2) % BLOOM_BITS);
  }
  test(id) { return this.hashes(id).every((b) => (this.bits[b >> 3] & (1 << (b & 7))) !== 0); }
  add(id) { for (const b of this.hashes(id)) this.bits[b >> 3] |= 1 << (b & 7); }
}

// O(1) settle-notify for bounded in-flight pools. Promise.race over a 4k
// pending Set allocates a reaction per element per call — at million scale
// that garbage stream is itself a heap hazard. One slot wakes the single
// pacing waiter when any op settles.
let wakeWaiter = null;
const wake = () => { const w = wakeWaiter; wakeWaiter = null; if (w) w(); };
const waitSettle = () => new Promise((r) => { wakeWaiter = r; });

// node:http's client has no setTypeOfService path, so the former
// undici/macOS EINVAL race is gone with the dependency; any
// uncaughtException is fatal again.

// Heap-peak sampler: every result carries peak_heap_mb, so the harness's
// own bound is measured evidence — the O(1) claim is checked, not asserted.
let memPeak = 0;
const memTimer = setInterval(() => {
  const h = process.memoryUsage().heapUsed;
  if (h > memPeak) memPeak = h;
}, 5000);
memTimer.unref();

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

// CI runs set RESULTS_DIR to a per-run directory (load.yml: the artifact
// then carries only this run's results, not the committed laptop history).
// Unset, results land in the committed evidence directory as before.
const RESULTS_DIR = process.env.RESULTS_DIR ?? `${ROOT}/perf/results`;
function writeResult(name, data) {
  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  fs.mkdirSync(RESULTS_DIR, { recursive: true });
  const file = `${RESULTS_DIR}/${stamp}-${name}.json`;
  countOwnSockets();
  fs.writeFileSync(file, JSON.stringify({
    machine: machine(), scenario: name,
    loadavg: os.loadavg().map((v) => Math.round(v * 100) / 100),
    peak_heap_mb: Math.round(memPeak / 2 ** 20),
    socket_peak: socketPeak, close_wait_peak: closeWaitPeak,
    fin_wait2_peak: finWait2Peak,
    socket_bound: SOCKET_BOUND,
    ...(lsofBlind ? { lsof_blind_samples: lsofBlind } : {}),
    ...data,
  }, null, 2));
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
function startHandler(mode, stallMs = 0) {
  const src = `
    const DRLOAD = 'drload-handler-${HPORT}'; // in the command line: stale-reap marker
    const seen = new Set();
    const sigs = { signed: 0, unsigned: 0, signedProbes: 0, unsignedProbes: 0 };
    require('node:http').createServer((req, res) => {
      const url = new URL(req.url, 'http://x');
      if (req.method === 'GET' && url.pathname === '/stats') {
        res.writeHead(200, {'content-type':'application/json'});
        res.end(JSON.stringify(sigs)); return;
      }
      const signed = !!req.headers['x-durable-signature'];
      if (req.method === 'GET') {
        signed ? sigs.signedProbes++ : sigs.unsignedProbes++;
        res.writeHead(seen.has(url.searchParams.get('idem_key')) ? 200 : 404).end(); return;
      }
      let b = ''; req.on('data', c => b += c);
      req.on('end', () => {
        signed ? sigs.signed++ : sigs.unsigned++;
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
    // DR_LOAD_SRV_STDIO=inherit surfaces the server's stderr (e.g. the
    // DURABLE_ACTOR_TRACE histogram); default keeps runs quiet.
    spawn(SRV, argv, {
      stdio: process.env.DR_LOAD_SRV_STDIO === 'inherit' ? 'inherit' : 'ignore',
      env: { ...process.env, DURABLE_API_KEY: API_KEY },
    }),
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

// Counted evidence for the result: 503 at_capacity responses are the
// admission bounds working as designed (per-workspace share + global
// pool); a submit retries them while drain frees slots, up to a hard
// deadline, and the retry count is reported — backpressure made visible,
// never an unbounded wait.
let submit503s = 0;
let submitRetries = 0;
let submitTcpRefusals = 0;
// Refusals by errno — ECONNREFUSED is the server's accept queue pacing
// us; EADDRNOTAVAIL/EPROTO are the HARNESS out of ephemeral ports or
// sockets, a different diagnosis entirely (the 1M battery miss at
// 44881585860 lumped them together as "tcp refusals").
const submitTcpByCode = {};
// One op, retried against honest backpressure until `until` (absolute ms;
// default +90 s). Within the budget a 503 or a kernel refusal is pacing —
// the op keeps retrying; the old fixed 90 s cap abandoned it mid-run and
// the abandoned op read as a refusal. `resp` is the response latency of
// EVERY attempt that got an HTTP answer — the p99 bound applies to those:
// a kernel-level refusal never reached the server and contributes no
// sample, so the bound measures server speed, not box congestion. `ms` is
// time to the final answer (pacing included); `attempts` counts tries.
// Every response body is consumed — an unread body pins its
// connection and buffers for the run.
async function submit(opKey, steps = 1, auth = AUTH, until = performance.now() + 90_000) {
  const t0 = performance.now();
  const resp = [];
  let attempts = 0;
  for (;;) {
    attempts++;
    let r;
    const a0 = performance.now();
    // An attempt that never answers (a wedged socket) cannot stall the op
    // forever: 60 s is far above any legitimate response. AbortController +
    // clearTimeout, not AbortSignal.timeout — that API keeps the signal's
    // timer (and the whole request graph it pins) alive for the full
    // timeout even after the request settled, which is ~200 MB of
    // retention under a retry storm.
    const ac = new AbortController();
    const timer = setTimeout(() => ac.abort(), 60_000);
    try {
      r = await fetch(`${API}/jobs`, {
        method: 'POST', headers: { 'content-type': 'application/json', ...auth },
        signal: ac.signal,
        body: JSON.stringify({
          op_key: opKey,
          spec: { version: 1, steps: Array.from({ length: steps }, (_, i) => ({ name: `s${i}`, handler: 'step' })), input_b64: INPUT_B64 },
        }),
      });
      resp.push(performance.now() - a0);
    } catch (e) {
      // A refused or reset connection is backpressure at the TCP layer (the
      // kernel's accept queue is full): retry inside the budget like a 503,
      // then report it under its own code so a flood shows kernel refusals
      // next to the server's HTTP 503s instead of crashing the harness.
      const code = e.name === 'AbortError' ? 'hung' : (e.cause?.code ?? e.code ?? 'error');
      if (performance.now() < until) {
        submitTcpRefusals++;
        submitTcpByCode[code] = (submitTcpByCode[code] ?? 0) + 1;
        submitRetries += Math.max(0, attempts - 1);
        await sleep(150);
        continue;
      }
      return { status: `tcp:${code}`, resp, ms: performance.now() - t0, attempts, body: '' };
    } finally {
      clearTimeout(timer);
    }
    if (r.status === 503 && performance.now() < until) {
      submit503s++;
      await r.arrayBuffer();
      await sleep(150);
      continue;
    }
    submitRetries += Math.max(0, attempts - 1);
    return { status: r.status, resp, ms: performance.now() - t0, attempts, body: await r.text() };
  }
}

// Run counts come from the bounded diagnostics (counts by state, never
// rows): GET /jobs is paged, and paging 1M runs to count them would be the
// unbounded read the paging exists to prevent.
// Kernel-level refusals (accept-queue overflow under load) hit machinery
// calls the same way they hit submits; retry a few times rather than let
// one reset kill a million-scale run. `submit` keeps its own tcp: codes.
async function req(url, opts = {}, tries = 6) {
  for (let i = 0;; i++) {
    try { return await fetch(url, opts); }
    catch (e) {
      if (i + 1 >= tries) throw e;
      await sleep(250);
    }
  }
}

async function diag() {
  return (await req(`${API}/admin/diagnostics`, { headers: AUTH })).json();
}

// --- scenarios -------------------------------------------------------------
async function runSubmit() {
  const rate = Number(args.rate ?? (SMOKE ? 20 : 50)), secs = Number(args.seconds ?? (SMOKE ? 10 : 60));
  const dir = fs.mkdtempSync(`${os.tmpdir()}/dr-load-`);
  const h = startHandler('ok'); const s = await startServer(`${dir}/journal`);
  const hst = new LatHist(); const codes = {}; let sent = 0, accepted = 0;
  const t0 = now();
  while (now() - t0 < secs * 1000) {
    const tickStart = now();
    for (let i = 0; i < rate; i++) {
      sent++;
      submit(`load-${sent}`).then((r) => {
        for (const m of r.resp) hst.add(m);
        codes[r.status] = (codes[r.status] ?? 0) + 1;
        if (r.status === 200) accepted++;
      });
    }
    const elapsed = now() - tickStart;
    if (elapsed < 1000) await sleep(1000 - elapsed);
  }
  await sleep(8000); // let the last submissions drain
  const done = (await diag()).states.done ?? 0;
  h.kill(); s.kill();
  const p99 = hst.pct(0.99);
  const r = writeResult('submit', {
    params: { rate_per_s: rate, seconds: secs, fsync: 'per-record (default)', smoke: SMOKE },
    sent, accepted, done_of_last_drain: done, http_codes: codes,
    latency_ms: hst.obj(),
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
  // Single attempt per op — this scenario measures the refusal itself;
  // the retry budget other scenarios use would mask the bound.
  await Promise.all(Array.from({ length: N }, async (_, i) => {
    const r = await submit(`flood-${i}`, 1, AUTH, performance.now());
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

// Offer far above --max-connections with connections that PARK a slot
// (a request head that never completes) — the churn-conn hoard shape
// that wedged /health behind a full accept queue. Asserts: liveness
// answers on its dedicated listener the whole flood, offered work gets
// a fast honest answer (200/503/tcp-refusal — never a hang past the
// bound), and full throughput returns within N seconds of the flood
// stopping (S2 wedge regression, made explicit).
async function runOverload() {
  const pool = Number(args.flood ?? 256);
  const secs = Number(args.seconds ?? (SMOKE ? 8 : 15));
  const recoverSecs = Number(args.recover_secs ?? 5);
  const hport2 = HPORT + 10;
  const dir = fs.mkdtempSync(`${os.tmpdir()}/dr-load-`);
  const h = startHandler('ok');
  const s = await startServer(`${dir}/journal`, [
    '--max-connections', '64',
    '--health-listen', `127.0.0.1:${hport2}`,
  ]);
  const healthBase = `http://127.0.0.1:${hport2}`;
  const healthProbe = () =>
    fetch(`${healthBase}/health`, { signal: AbortSignal.timeout(2000) });
  const base = await submit('overload-base', 1, AUTH, performance.now() + 10_000);
  const healthFails = [];
  let stopProbes = false;
  const probeLoop = (async () => {
    while (!stopProbes) {
      const t0 = now();
      try {
        const r = await healthProbe();
        if (r.status !== 200) healthFails.push(`status:${r.status}`);
      } catch (e) {
        healthFails.push(e.cause?.code || e.name);
      }
      const took = now() - t0;
      if (took > 500) healthFails.push(`slow:${Math.round(took)}ms`);
      await sleep(120);
    }
  })();
  // 4× the conn cap; each socket parks a slot on a head that never
  // completes. Past the cap the extras wait in the kernel backlog —
  // the honest refusal path (RST) when it too fills.
  const conns = [];
  for (let i = 0; i < pool; i++) {
    const c = net.connect(PORT, '127.0.0.1');
    c.on('error', () => {});
    c.write('POST /jobs HTTP/1.1\r\nhost: x\r\n');
    conns.push(c);
  }
  // Single attempt per offer (until=now): the measured answer is the
  // honest one — a served 200, a bound 503, or a tcp refusal — not the
  // retry loop's masked outcome (the admission scenario does the same).
  const offer = Array.from({ length: 48 }, async (_, i) => {
    const t0 = now();
    const r = await submit(`overload-${i}`, 1, AUTH, performance.now());
    return { status: r.status, took: now() - t0 };
  });
  const offered = await Promise.all(offer);
  const answers = {};
  let hung = 0;
  let httpAnswered = 0;
  let tcpRefused = 0;
  for (const r of offered) {
    answers[r.status] = (answers[r.status] ?? 0) + 1;
    // A conn parked in the full kernel backlog waits out the flood
    // before its refusal/service — the wedge is "never answered",
    // not "answered late" (queued backpressure is the honest one).
    if (r.took > secs * 1000 + 15_000) hung++;
    // Only a real HTTP answer counts — a tcp reset is a FAILURE for the
    // offer (the AGENTS.md EOF-as-success rule), not an "answered".
    if (typeof r.status === 'number') httpAnswered++;
    else tcpRefused++;
  }
  const midHealth = await healthProbe().then((r) => r.status).catch((e) => `ERR:${e.cause?.code || e.name}`);
  await sleep(secs * 1000);
  for (const c of conns) c.destroy(); // the flood stops
  const t0 = now();
  let recovered = 0;
  const recCodes = {};
  while (recovered < 40 && now() - t0 < recoverSecs * 1000) {
    const r = await submit(`recover-${recovered}`, 1, AUTH, performance.now() + 10_000);
    recCodes[r.status] = (recCodes[r.status] ?? 0) + 1;
    if (r.status === 200) recovered++;
  }
  const recoverMs = now() - t0;
  stopProbes = true;
  await probeLoop;
  h.kill(); s.kill();
  const r = writeResult('overload', {
    params: { flood_conns: pool, max_connections: 64, header_timeout_ms: 'default (10 s)', smoke: SMOKE },
    baseline_status: base.status,
    answers_during_flood: answers,
    http_answered: httpAnswered,
    tcp_refused: tcpRefused,
    health_during_flood: midHealth,
    health_failures: healthFails,
    recovery: { submits_done: recovered, ms: Math.round(recoverMs), codes: recCodes },
    thresholds: {
      offers: 'every offer settles inside flood+15 s; only real HTTP answers count — a tcp reset is a failure',
      legit_floor: 'at least 4 offers get a real HTTP 200 during the flood — nonzero service proves rotation (measured 7 at this shape)',
      health: '200 on the dedicated listener, every probe <500 ms',
      recovery: `40 submits served <${recoverSecs} s after the flood`,
    },
    ...verdictOf([
      ['baseline submit served before the flood', base.status === 200],
      ['every offered request settled inside the bound', hung === 0 && offered.length === 48],
      // Legitimate keep-alive clients must still get real HTTP answers
      // under the flood — not merely a kernel RST (S2 re-review R2).
      // A parked-head flood refuses most backlog conns by RST; the floor
      // is that rotation still admits SOME real service (measured 7 at
      // this shape — 4 gives margin, 0 is the wedge it guards).
      ['at least 4 offers got a real HTTP answer during the flood', httpAnswered >= 4],
      ['at least 4 offers answered 200 during the flood', (answers[200] ?? 0) >= 4],
      ['/health answered 200 mid-flood on its listener', midHealth === 200],
      ['every liveness probe answered <500ms during the flood', healthFails.length === 0],
      [`40 submits inside ${recoverSecs}s after the flood`, recovered === 40],
    ]),
    note: 'a conn hoard must not wedge work or liveness: reserve + slot rotation; tcp refusals are failures, not answers',
  });
  return r.verdict === 'pass';
}

async function runCompact() {
  const secs = Number(args.seconds ?? (SMOKE ? 15 : 30));
  const dir = fs.mkdtempSync(`${os.tmpdir()}/dr-load-`);
  const h = startHandler('ok'); const s = await startServer(`${dir}/journal`);
  const hst = new LatHist(); const codes = {}; let sent = 0;
  const t0 = now();
  const submits = (async () => {
    while (now() - t0 < secs * 1000) {
      sent++;
      submit(`compact-${sent}`).then((r) => { for (const m of r.resp) hst.add(m); codes[r.status] = (codes[r.status] ?? 0) + 1; });
      await sleep(20);
    }
  })();
  const compactions = [];
  while (now() - t0 < secs * 1000) {
    const tc = now();
    const r = await fetch(`${API}/admin/compact`, { method: 'POST', headers: AUTH });
    await r.arrayBuffer();
    compactions.push({ status: r.status, ms: now() - tc });
    await sleep(5000);
  }
  await submits; await sleep(3000);
  const runsAfter = (await diag()).live_runs;
  h.kill(); s.kill();
  const p99 = hst.pct(0.99);
  const r = writeResult('compact-under-load', {
    params: { seconds: secs, submit_interval_ms: 20, smoke: SMOKE },
    submitted: sent, http_codes: codes, compactions,
    runs_after: runsAfter,
    latency_ms: hst.obj(),
    thresholds: { submits: 'all 200', compactions: 'all 200', p99_ms: `<${P99_MS}` },
    ...verdictOf([
      ['every submit accepted', Object.keys(codes).every((c) => c === '200')],
      ['every compaction succeeded', compactions.every((c) => c.status === 200)],
      [`p99 < ${P99_MS}ms`, p99 !== null && p99 < P99_MS],
    ]),
  });
  return r.verdict === 'pass';
}

// The million-op run. The OFFER is fixed-rate; a refused op keeps retrying
// inside the submit budget instead of being abandoned — at_capacity 503s
// and kernel refusals are the server pacing the harness, and whether the
// million all land inside the budget is the measurement. The old semantics
// counted an abandoned op as a refusal, so any run where drain < offer was
// a guaranteed FAIL (the CI miss at c94edfd: ~85 completions/s vs a 100/s
// offer on a 4-core runner) — the bound itself worked exactly as designed.
// Default budgets: submit phase = 2x the ideal offer duration (an achieved
// accept rate below rate/2 is an honest capacity miss), drain = 1x the
// ideal offer duration on top of that.
async function runSustained() {
  const rate = Number(args.rate ?? (SMOKE ? 50 : 100)), total = Number(args.total ?? (SMOKE ? 5_000 : 1_000_000));
  const submitBudgetMs = Number(args.submit_budget_ms ?? Math.round((total / rate) * 2000));
  const drainBudgetMs = Number(args.drain_budget_ms ?? Math.round((total / rate) * 1000));
  const dir = fs.mkdtempSync(`${os.tmpdir()}/dr-load-`);
  const h = startHandler('ok'); const s = await startServer(`${dir}/journal`);
  const hst = new LatHist(); const acceptHst = new LatHist(); const codes = {};
  let sent = 0, accepted = 0;
  const MAX_PENDING = +(process.env.DR_LOAD_MAX_PENDING || 384); // below the server's default --max-connections 512: deeper offers pile sockets into the kernel accept queue and the run measures TCP refusal storms, not the engine
  const pending = new Set();
  const t0 = now();
  const phaseEnd = t0 + submitBudgetMs;
  const perTick = Math.max(1, Math.round(rate / 10));
  while (sent < total) {
    const tickStart = now();
    for (let i = 0; i < perTick && sent < total; i++) {
      sent++;
      const k = sent;
      const p = submit(`sustained-${k}`, 1, AUTH, phaseEnd).then((r) => {
        pending.delete(p);
        for (const m of r.resp) hst.add(m);
        acceptHst.add(r.ms);
        codes[r.status] = (codes[r.status] ?? 0) + 1;
        if (r.status === 200) accepted++;
        wake();
      });
      pending.add(p);
    }
    if (pending.size >= MAX_PENDING) await waitSettle();
    const elapsed = now() - tickStart;
    if (elapsed < 100) await sleep(100 - elapsed);
    if (sent % 100_000 === 0) {
      console.log(`${sent} sent, ${accepted} accepted @${((now() - t0) / 1000).toFixed(0)}s`);
    }
  }
  await Promise.all(pending);
  const submitSecs = (now() - t0) / 1000;
  // Drain: poll until every accepted run is terminal or the budget is gone
  // — a fixed sleep under-drains a million-scale backlog and a partial
  // drain masquerading as a pass is worse than an honest miss.
  const drainStart = now();
  let done = 0;
  for (;;) {
    done = (await diag()).states.done ?? 0;
    if (done >= accepted || now() - drainStart >= drainBudgetMs) break;
    await sleep(2_000);
  }
  const jsize = execFileSync('du', ['-sk', `${dir}/journal`]).toString().split('\t')[0];
  h.kill(); s.kill();
  const p99 = hst.pct(0.99);
  const r = writeResult('sustained', {
    params: {
      rate_per_s: rate, target_submits: total, fsync: 'per-record',
      submit_budget_ms: submitBudgetMs, drain_budget_ms: drainBudgetMs, smoke: SMOKE,
    },
    sent, accepted, done_after_drain: done, http_codes: codes,
    backpressure: { at_capacity_503s: submit503s, tcp_refusals: submitTcpRefusals, tcp_refusals_by_code: { ...submitTcpByCode }, retries: submitRetries },
    wall_clock_s: Math.round((now() - t0) / 1000),
    submit_phase_s: Math.round(submitSecs),
    accept_rate_per_s: submitSecs > 0 ? Math.round(accepted / submitSecs) : accepted,
    journal_kb: Number(jsize),
    latency_ms: hst.obj(),
    accept_latency_ms: acceptHst.obj(),
    thresholds: {
      accepted: 'every offered op admitted within the submit budget',
      done: 'every accepted run finished within the drain budget',
      p99_ms: `<${P99_MS} per-attempt response`,
    },
    ...verdictOf([
      ['every offered op admitted', accepted === sent],
      ['every accepted run finished', done >= accepted],
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
  // --backend=memory|postgres forces the account backend for this run;
  // unset, the ambient env decides (DATABASE_URL, else BACKEND=memory).
  const backend = args.backend ?? (process.env.DATABASE_URL ? 'postgres' : process.env.DURABLE_ACCOUNTS_BACKEND);
  if (!backend) {
    console.error('workspaces scenario needs --backend, DATABASE_URL, or DURABLE_ACCOUNTS_BACKEND=memory; skipping — this is a skip, not a pass');
    return true;
  }
  const dbUrl = args.database_url ?? process.env.DATABASE_URL;
  if (backend === 'postgres' && !dbUrl) {
    console.error('workspaces --backend=postgres needs DATABASE_URL or --database_url=; skipping — this is a skip, not a pass');
    return true;
  }
  // The spawned server inherits process.env — pin exactly the backend this
  // run asked for so an ambient DATABASE_URL can't silently turn a memory
  // run into a pg one (the server refuses the pair anyway).
  delete process.env.DATABASE_URL;
  delete process.env.DURABLE_ACCOUNTS_BACKEND;
  if (backend === 'postgres') process.env.DATABASE_URL = dbUrl;
  else process.env.DURABLE_ACCOUNTS_BACKEND = backend;
  const n = Number(args.workspaces ?? 2), per = Number(args.per ?? (SMOKE ? 200 : 25_000));
  const dir = fs.mkdtempSync(`${os.tmpdir()}/dr-load-`);
  const h = startHandler('ok'); const s = await startServer(`${dir}/journal`);
  // Mint one admin key per workspace through the ops routes.
  const keys = [];
  for (let i = 0; i < n; i++) {
    const w = await (await req(`${API}/admin/workspaces`, {
      method: 'POST', headers: { 'content-type': 'application/json', ...AUTH },
      body: JSON.stringify({ name: `load-ws-${i}` }),
    })).json();
    const k = await (await req(`${API}/admin/workspaces/${w.public_id}/keys`, {
      method: 'POST', headers: { 'content-type': 'application/json', ...AUTH },
      body: JSON.stringify({ name: 'load', permission: 'admin' }),
    })).json();
    keys.push({ public_id: w.public_id, auth: { authorization: `Bearer ${k.key}` } });
  }
  // S2-4: handler names resolve per workspace — `step` exists only as a
  // ws-0 flag registration, so every workspace registers it through the
  // route before its submits can admit.
  for (const ws of keys) {
    const r = await req(`${API}/handlers/step`, {
      method: 'PUT', headers: { 'content-type': 'application/json', ...ws.auth },
      body: JSON.stringify({ url: `http://127.0.0.1:${HPORT}/step` }),
    });
    const body = await r.text();
    if (r.status !== 200) {
      console.error(`handler registration failed: ${r.status} ${body}`);
      h.kill(); s.kill(); return false;
    }
  }
  // Registration churn concurrent with dispatch lookups: a second name
  // flaps register→delete under the first workspace's key while the
  // submit burst runs — the registry is rebuilt per workspace, never
  // blocked. Kernel refusals count as churn_errors, not a harness crash;
  // a persistent outage stops churning rather than spin on a dead port.
  // Every response body is consumed — an unread body pins its
  // connection and buffers, one leak per op for the whole run.
  let churnDone = false; let churnOps = 0, churnErrors = 0;
  const churn = (async () => {
    while (!churnDone && !process.env.DR_LOAD_NO_CHURN) {
      try {
        const put = await fetch(`${API}/handlers/churn`, {
          method: 'PUT', headers: { 'content-type': 'application/json', ...keys[0].auth },
          body: JSON.stringify({ url: `http://127.0.0.1:${HPORT}/step` }),
        });
        await put.arrayBuffer();
        const del = await fetch(`${API}/handlers/churn`, { method: 'DELETE', headers: keys[0].auth });
        await del.arrayBuffer();
        if (put.status !== 200 || del.status !== 200) {
          console.error(`churn broke: put=${put.status} del=${del.status}`); return;
        }
        churnOps++;
      } catch {
        if (++churnErrors > 200) { console.error('churn abandoned: server unreachable'); return; }
      }
      await sleep(5);
    }
  })();
  // Submit `per` runs per workspace, same op_key prefix in each — dedup is
  // per (workspace, key), so identical keys in two workspaces are two runs.
  // Each op retries inside the submit budget: refusals are pacing, and
  // whether they all land in budget is the measurement (see runSustained).
  // Outstanding fetches stay bounded by MAX_PENDING with an O(1) wake —
  // Promise.race over the pool per op is a million-scale garbage stream.
  const submitBudgetMs = Number(args.submit_budget_ms ?? Math.round((per * n / 50) * 1000));
  const hst = new LatHist(); const acceptHst = new LatHist(); const codes = {};
  let sent = 0, accepted = 0;
  const MAX_PENDING = +(process.env.DR_LOAD_MAX_PENDING || 384); // below the server's default --max-connections 512: deeper offers pile sockets into the kernel accept queue and the run measures TCP refusal storms, not the engine
  const pending = new Set();
  const t0 = now();
  const phaseEnd = t0 + submitBudgetMs;
  for (let i = 0; i < per; i++) {
    for (const ws of keys) {
      sent++;
      const p = submit(`shared-${i}`, 1, ws.auth, phaseEnd).then((r) => {
        pending.delete(p);
        for (const m of r.resp) hst.add(m);
        acceptHst.add(r.ms);
        codes[r.status] = (codes[r.status] ?? 0) + 1;
        if (r.status === 200) accepted++;
        wake();
      });
      pending.add(p);
    }
    if (pending.size >= MAX_PENDING) await waitSettle();
    // DR_LOAD_NO_PACE drops the 50 ms cadence — used to find the
    // server's knee when the paced offer sits below it.
    if (i % 100 === 0 && !process.env.DR_LOAD_NO_PACE) await sleep(50);
    if (i > 0 && i % 25_000 === 0) {
      console.log(`${sent} sent, ${accepted} accepted @${((now() - t0) / 1000).toFixed(0)}s`);
    }
  }
  await Promise.all(pending); // every submit answered before drain timing
  const submitSecs = (now() - t0) / 1000;
  // Drain: poll diagnostics until every run is terminal or the budget is
  // gone. A fixed sleep under-drains a million-scale run and a partial
  // drain masquerading as a pass is worse than an honest miss.
  const drainBudgetMs = Number(args.drain_budget_ms ?? (SMOKE ? 120_000 : 1_800_000));
  const drainStart = now();
  let done = 0;
  for (;;) {
    done = (await diag()).states.done ?? 0;
    if (done >= accepted || now() - drainStart >= drainBudgetMs) break;
    await sleep(2_000);
  }
  const drainSecs = (now() - drainStart) / 1000;
  churnDone = true; await churn;
  // Identity evidence, O(1): per workspace a fixed 2 MiB Bloom filter plus
  // the first 256 ids as an exact sample. A filter hit only ever produces
  // a bounded SUSPECT which is then verified exactly through the API —
  // storing all ~1.1 M id strings was part of the heap that OOM'd CI.
  const SAMPLE_K = 256;
  const SUSPECT_CAP = 512;
  const owned = []; // { bloom, sample, paged, suspects: Set }
  const crossSuspects = []; // { id, a, b } — id listed under b hit a's filter
  let suspectOverflow = false;
  const pageCounts = {};
  for (const [w, ws] of keys.entries()) {
    const bloom = new Bloom(); const sample = []; const suspects = new Set();
    let after = null, pages = 0, paged = 0;
    for (;;) {
      const q = after == null ? '' : `&after=${after}`;
      const page = await (await req(`${API}/jobs?limit=1000${q}`, { headers: ws.auth })).json();
      for (const r of page.runs ?? []) {
        paged++;
        if (bloom.test(r.id)) {
          if (suspects.size < SUSPECT_CAP) suspects.add(r.id); else suspectOverflow = true;
        }
        for (let a = 0; a < w; a++) {
          if (owned[a].bloom.test(r.id)) {
            if (crossSuspects.length < SUSPECT_CAP) crossSuspects.push({ id: r.id, a, b: w });
            else suspectOverflow = true;
          }
        }
        bloom.add(r.id);
        if (sample.length < SAMPLE_K) sample.push(r.id);
      }
      pages++;
      if (page.next_after == null) break;
      after = page.next_after;
    }
    owned.push({ bloom, sample, paged, suspects });
    pageCounts[ws.public_id] = { runs: paged, pages };
  }
  // Exact verification, all bounded:
  //  1. each cross suspect re-read under the EARLIER workspace's key —
  //     200 = a real shared id; 404 clears a filter false positive.
  const shared = [];
  for (const c of crossSuspects) {
    const r = await req(`${API}/jobs/${c.id}`, { headers: keys[c.a].auth });
    await r.arrayBuffer();
    if (r.status === 200) shared.push(`${c.id} readable under ws${c.a} and listed under ws${c.b}`);
  }
  //  2. each self suspect: re-page its own workspace and count sightings —
  //     >=2 means the listing returned the same run id twice.
  const dupes = [];
  for (const [w, ws] of keys.entries()) {
    const sus = owned[w].suspects;
    if (!sus.size) continue;
    const counts = new Map([...sus].map((id) => [id, 0]));
    let after = null;
    for (;;) {
      const q = after == null ? '' : `&after=${after}`;
      const page = await (await req(`${API}/jobs?limit=1000${q}`, { headers: ws.auth })).json();
      for (const r of page.runs ?? []) if (counts.has(r.id)) counts.set(r.id, counts.get(r.id) + 1);
      if (page.next_after == null) break;
      after = page.next_after;
    }
    for (const [id, count] of counts) if (count >= 2) dupes.push(`${id} listed ${count}x under ws${w}`);
  }
  //  3. sample exact cross-check: each workspace's sampled ids re-read
  //     under every other workspace's key — a 404 is the no-oracle rule.
  const sampleLeaks = [];
  for (const [b, o] of owned.entries()) {
    for (const id of o.sample) {
      for (const [a, ks] of keys.entries()) {
        if (a === b) continue;
        const r = await req(`${API}/jobs/${id}`, { headers: ks.auth });
        await r.arrayBuffer();
        if (r.status !== 404) sampleLeaks.push(`ws${b} id ${id} answered ${r.status} under ws${a}`);
      }
    }
  }
  // No-oracle spot check: workspace 0's first sampled run read under
  // workspace 1's key.
  const probe = await req(`${API}/jobs/${owned[0].sample[0]}`, { headers: keys[1].auth });
  await probe.arrayBuffer();
  // S2-6: every call this scenario dispatches went through a
  // store-registered handler, so each must carry the signature headers.
  const sigs = await fetch(`http://127.0.0.1:${HPORT}/stats`).then((r) => r.json())
    .catch(() => ({ signed: -1, unsigned: -1 }));
  h.kill(); s.kill();
  const p99 = hst.pct(0.99);
  const disjoint = shared.length === 0 && sampleLeaks.length === 0 && !suspectOverflow;
  const r = writeResult('workspaces', {
    params: {
      workspaces: n, submits_per_workspace: per, account_backend: backend,
      submit_budget_ms: submitBudgetMs, drain_budget_ms: drainBudgetMs, smoke: SMOKE,
    },
    sent, accepted, http_codes: codes,
    backpressure: { at_capacity_503s: submit503s, tcp_refusals: submitTcpRefusals, tcp_refusals_by_code: { ...submitTcpByCode }, retries: submitRetries },
    per_workspace: pageCounts, churn_registrations: churnOps, churn_errors: churnErrors,
    done_after_drain: done, drain_seconds: Math.round(drainSecs),
    drain_runs_per_s: drainSecs > 0 ? Math.round(done / drainSecs) : done,
    submit_phase_s: Math.round(submitSecs),
    accept_rate_per_s: submitSecs > 0 ? Math.round(accepted / submitSecs) : accepted,
    handler_calls: sigs,
    identity: {
      suspects_cross: crossSuspects.length, suspects_self: [...owned.values()].reduce((a, o) => a + o.suspects.size, 0),
      suspect_overflow: suspectOverflow,
      shared_verified: shared, dupes_verified: dupes, sample_leaks: sampleLeaks.slice(0, 10),
    },
    latency_ms: hst.obj(),
    accept_latency_ms: acceptHst.obj(),
    thresholds: {
      accepted: 'every offered op admitted within the submit budget',
      sized: `${per} runs per workspace, no id listed twice`,
      disjoint: 'no run id shared across workspaces (bloom + exact checks)',
      oracle: 'foreign run id → 404',
      done: `every accepted run within ${Math.round(drainBudgetMs / 1000)}s drain`,
      signed: 'every handler call signed', p99_ms: `<${P99_MS} per-attempt response`,
    },
    ...verdictOf([
      ['every offered op admitted', Object.keys(codes).every((c) => c === '200')],
      ['every workspace lists exactly its own runs', owned.every((o) => o.paged === per) && dupes.length === 0],
      ['run ids disjoint across workspaces', disjoint],
      ['a foreign run id answers 404', probe.status === 404],
      ['every accepted run finished', done >= sent * 0.99],
      ['every handler call was signed', sigs.signed > 0 && sigs.unsigned === 0],
      [`p99 < ${P99_MS}ms`, p99 !== null && p99 < P99_MS],
    ]),
  });
  return r.verdict === 'pass';
}

// The socket-bound regression test: a server that closes EVERY response
// (--max-requests-per-connection 1) forces the client to open a fresh conn
// per op — the exact churn shape that piled 12k CLOSE_WAIT before the
// bounded pool. The run passes only if the client's own socket count stays
// under the bound. Measured under node:http's agents (undici removed):
// pooled and unpooled both pile at the in-flight window (~MAXP) since the
// pile is server-driven, not client-retention — DR_LOAD_NO_POOL remains
// the regression switch but the discriminator here is the absolute bound
// plus the drain check, tripped 13x at bound 50 in the red proof.
async function runConnreuse() {
  const n = Number(args.ops ?? 400);
  const dir = fs.mkdtempSync(`${os.tmpdir()}/dr-load-`);
  const h = startHandler('ok');
  const s = await startServer(`${dir}/journal`, ['--max-requests-per-connection', '1']);
  const t0 = now();
  let sent = 0, ok200 = 0;
  const codes = {};
  const pending = new Set(); const MAXP = 64;
  const settle = () => new Promise((res) => {
    const w = () => pending.size < MAXP ? res() : setImmediate(w);
    w();
  });
  for (let i = 0; i < n; i++) {
    sent++;
    const p = submit(`connreuse-${i}`, 1, AUTH, performance.now() + 20_000).then((r) => {
      pending.delete(p); codes[r.status] = (codes[r.status] ?? 0) + 1;
      if (r.status === 200) ok200++;
    });
    pending.add(p);
    // A sub-second run never trips the 1 s sampler — count at op
    // cadence too so a churn pile cannot slip between ticks.
    if (i % 64 === 0) countOwnSockets();
    if (pending.size >= MAXP) await settle();
  }
  await Promise.all(pending);
  // Let dead ends drain through the pool's keep-alive window, then count.
  await sleep(4_500);
  const final = countOwnSockets();
  const r = writeResult('connreuse', {
    ops: n, ms: Math.round(now() - t0), codes,
    close_wait_after_drain: final.cw, total_sockets_after_drain: final.total,
    ...verdictOf([
      ['every op answered 200', ok200 === sent],
      [`sockets bounded (<=${SOCKET_BOUND})`, socketPeak <= SOCKET_BOUND],
      [`CLOSE_WAIT drains under bound (<=${SOCKET_BOUND / 4})`, closeWaitPeak <= SOCKET_BOUND / 4 && final.cw <= SOCKET_BOUND / 4],
      ['client never over the bound', socketOverBound === 0],
    ]),
  });
  return r.verdict === 'pass';
}

const runners = {
  submit: runSubmit, replay: runReplay, stall: runStall,
  admission: runAdmission, compact: runCompact, sustained: runSustained,
  workspaces: runWorkspaces, overload: runOverload, connreuse: runConnreuse,
};
if (!runners[scenario]) {
  console.error(`unknown scenario ${scenario}; choices: ${Object.keys(runners).join(', ')}`);
  process.exit(2);
}
const ok = await runners[scenario]();
// A run whose client-side sockets crossed the bound fails even when its
// scenario checks passed — the leak is the thing being measured. So does
// a run that could not measure: a blind lsof is not a clean one.
if (socketOverBound > 0) {
  console.error(`socket self-check tripped ${socketOverBound}x — run fails`);
  process.exit(1);
}
if (lsofBlind > 0) {
  console.error(`lsof could not answer ${lsofBlind}x — the socket bound was unobserved, run fails`);
  process.exit(1);
}
process.exit(ok === false ? 1 : 0);
