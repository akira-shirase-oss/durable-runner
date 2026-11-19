// restart_ready.mjs — restart-to-ready: how long an upgrade's restart
// takes before the server answers /ready, at a journal of N queued runs.
//
//   node perf/restart_ready.mjs --runs=100000 [--runs=300000 ...]
//       [--port=58536] [--hport=58537]
//
// The upgrade is a stop plus a start plus a replay: the server must not
// answer ready before the journal's state is rebuilt, so this is the
// number the deploy plan quotes as expected unavailability. The fill is
// the in-process probe (engine submits, no HTTP); the measured restart
// is the release binary. A missed threshold is kept — never rerun to
// green. Result JSONs land in perf/results/.
import { spawn, spawnSync, execFileSync } from 'node:child_process';
import fs from 'node:fs';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const SRV = `${ROOT}/target/release/durable-server`;
const API_KEY = process.env.DURABLE_API_KEY ?? (await import('node:crypto')).randomBytes(32).toString('hex');
const args = {};
for (const a of process.argv.slice(2).filter((a) => a.startsWith('--'))) {
  const [k, v] = a.slice(2).split('=');
  if (args[k] === undefined) args[k] = v;
  else if (Array.isArray(args[k])) args[k].push(v);
  else args[k] = [args[k], v];
}
const PORT = Number(args.port ?? 53342);
const HPORT = Number(args.hport ?? 53343);
const SIZES = (Array.isArray(args.runs) ? args.runs : [args.runs ?? '100000']).map(Number);
const API = `http://127.0.0.1:${PORT}`;
const READY_TARGET_MS = 5_000;

// --- spawned children (same discipline as load.mjs) --------------------
const PIDDIR = `${os.tmpdir()}/durable-load.pids`;
fs.mkdirSync(PIDDIR, { recursive: true });
const kids = new Set();
function pidfile(pid) { return `${PIDDIR}/${pid}.json`; }
function track(p, match, command, port) {
  fs.writeFileSync(pidfile(p.pid), JSON.stringify({ pid: p.pid, match, command, port, started_at: new Date().toISOString() }));
  kids.add(p);
  p.on('exit', () => { kids.delete(p); fs.rmSync(pidfile(p.pid), { force: true }); });
  p.unref(); // children are reaped by the exit hook; they must not hold node open
  return p;
}
function alive(pid) {
  try { process.kill(pid, 0); return true; } catch (e) { return e.code !== 'ESRCH'; }
}
function cmdline(pid) {
  const r = spawnSync('ps', ['-p', String(pid), '-o', 'command=']);
  return r.error ? '' : (r.stdout || '').toString();
}
function portHeldBy(port, pid) {
  const r = spawnSync('lsof', ['-nP', `-iTCP:${port}`, '-sTCP:LISTEN']);
  if (r.error) throw r.error;
  return (r.stdout || '').toString().split('\n').some((l) => l.trim().split(/\s+/)[1] === String(pid));
}
function verifyAndReap(rec) {
  if (!alive(rec.pid)) return true;
  let ours = cmdline(rec.pid).includes(rec.match);
  if (!ours && rec.port != null) {
    try { ours = portHeldBy(rec.port, rec.pid); } catch { /* cannot verify here */ }
  }
  if (!ours) return false;
  try { process.kill(rec.pid, 'SIGKILL'); }
  catch (e) {
    if (e.code !== 'ESRCH') {
      console.error(`stale child ${rec.pid} (${rec.command}) is ours but could not be killed: ${e.code}`);
      return false;
    }
  }
  return true;
}
for (const f of fs.readdirSync(PIDDIR).filter((f) => f.endsWith('.json'))) {
  let rec;
  try { rec = JSON.parse(fs.readFileSync(`${PIDDIR}/${f}`, 'utf8')); }
  catch { fs.rmSync(`${PIDDIR}/${f}`, { force: true }); continue; }
  if (verifyAndReap(rec)) fs.rmSync(`${PIDDIR}/${f}`, { force: true });
}
process.on('exit', () => { for (const k of kids) { try { k.kill('SIGKILL'); } catch {} } });
for (const sig of ['SIGINT', 'SIGTERM']) {
  process.on(sig, () => process.exit(sig === 'SIGINT' ? 130 : 143));
}

function portFree(port) {
  return new Promise((resolve) => {
    const s = net.connect({ host: '127.0.0.1', port });
    s.once('connect', () => { s.destroy(); resolve(false); });
    s.once('error', () => resolve(true));
  });
}
for (const [port, what] of [[PORT, 'server'], [HPORT, 'handler']]) {
  if (!(await portFree(port))) {
    console.error(`${what} port ${port} already has a listener — a stale process? pick another --port/--hport`);
    process.exit(3);
  }
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
function machine() {
  return {
    host: process.env.PERF_HOST_LABEL || 'maintainer-laptop',
    platform: `${os.platform()}/${os.arch()}`,
    cpu: os.cpus()[0]?.model ?? 'unknown',
    cores: os.cpus().length,
    mem_gb: Math.round(os.totalmem() / 2 ** 30),
    date: new Date().toISOString(),
    commit: execFileSync('git', ['rev-parse', '--short', 'HEAD'], { cwd: ROOT }).toString().trim(),
  };
}
function writeResult(name, extra) {
  const r = { machine: machine(), scenario: `restart-ready-${name}`, ...extra };
  const file = `${ROOT}/perf/results/${new Date().toISOString().replace(/[:.]/g, '-')}-${r.scenario}.json`;
  fs.writeFileSync(file, JSON.stringify(r, null, 1));
  console.log(`result -> ${file}`);
  return r;
}

function startHandler() {
  const src = `
    const DRLOAD = 'drload-handler-${HPORT}';
    require('node:http').createServer((req, res) => {
      let b=''; req.on('data',c=>b+=c); req.on('end',()=>{
        res.writeHead(200,{'content-type':'application/json'}).end('{"out":null}');
      });
    }).listen(${HPORT}, '127.0.0.1');
  `;
  return track(spawn('node', ['-e', src], { stdio: 'ignore' }), `drload-handler-${HPORT}`, `node -e <handler fixture> on 127.0.0.1:${HPORT}`, HPORT);
}

// Fill `dir` with `runs` queued runs through the engine probe (in-process
// submits, no HTTP — the same path restart_probe_open measures).
function fill(dir, runs) {
  const t0 = Date.now();
  const env = {
    ...process.env,
    PROBE_DIR: dir,
    PROBE_RUNS: String(runs),
    PATH: `/opt/homebrew/opt/rustup/bin:${process.env.PATH}`,
  };
  const r = spawnSync('cargo', ['test', '--release', '-p', 'durable-engine', '--', '--ignored', 'restart_probe_fill', '--nocapture'],
    { env, cwd: ROOT, encoding: 'utf8', stdio: ['ignore', 'pipe', 'inherit'] });
  if (r.status !== 0) throw new Error(`probe fill failed (exit ${r.status})`);
  console.log((r.stdout || '').split('\n').filter((l) => l.includes('FILLED')).join('\n'));
  return (Date.now() - t0) / 1000;
}

async function runSize(runs) {
  const dir = fs.mkdtempSync(`${os.tmpdir()}/dr-restart-`);
  const journal = `${dir}/journal`;
  const h = startHandler();
  const fill_s = fill(journal, runs);
  // The upgrade: a stopped server starts over the filled journal. Time
  // from spawn to the first /ready=200 — that is the unavailability a
  // deploy's restart costs at this size.
  const errLog = `${dir}/server.err.log`;
  const s = track(
    spawn(SRV, ['--journal', journal, '--listen', `127.0.0.1:${PORT}`,
      '--handler', `step=http://127.0.0.1:${HPORT}/step`,
      '--rate-limit-per-addr', '100000', '--max-conns-per-addr', '0',
      '--max-live-runs', String(runs * 2), '--max-index-bytes', '8589934592'],
      { stdio: ['ignore', 'ignore', fs.openSync(errLog, 'w')], env: { ...process.env, DURABLE_API_KEY: API_KEY } }),
    journal, `durable-server --journal ${journal}`, PORT,
  );
  const t0 = performance.now();
  let ready_ms = null;
  while (performance.now() - t0 < 300_000) {
    if (s.exitCode !== null) throw new Error(`durable-server exited (${s.exitCode}) before ready`);
    try {
      const res = await fetch(`${API}/ready`, { signal: AbortSignal.timeout(2000) });
      if (res.status === 200) { ready_ms = Math.round(performance.now() - t0); break; }
    } catch { /* not listening yet */ }
    await sleep(25);
  }
  if (ready_ms === null) throw new Error('server never became ready in 5 minutes');
  let profile = null;
  try {
    profile = fs.readFileSync(errLog, 'utf8').split('\n').find((l) => l.includes('PROFILE')) ?? null;
    if (profile) console.log(profile);
  } catch { /* no log */ }
  const jmib = Math.round(
    Number(execFileSync('du', ['-sk', journal]).toString().split('\t')[0]) / 1024,
  );
  s.kill('SIGTERM'); // the drain path: exit within drain_timeout_secs
  for (let i = 0; i < 200 && s.exitCode === null; i++) await sleep(200);
  if (s.exitCode === null) s.kill('SIGKILL');
  const r = writeResult(`${runs}`, {
    params: { runs, fill: 'restart_probe_fill (queued runs)', smoke: false },
    fill_s: Math.round(fill_s * 10) / 10,
    ready_ms,
    journal_mib: jmib,
    profile,
    thresholds: { ready_ms: `<${READY_TARGET_MS} (the upgrade-unavailability target)` },
    ...verdict(runs, ready_ms),
  });
  return r.verdict === 'pass';
}
function verdict(runs, ready_ms) {
  const pass = ready_ms < READY_TARGET_MS;
  return { verdict: pass ? 'pass' : 'fail', checks: [[`ready < ${READY_TARGET_MS}ms at ${runs} runs`, pass, `${ready_ms}ms`]] };
}

for (const n of SIZES) {
  console.log(`=== restart-to-ready at ${n} runs`);
  await runSize(n);
}
