// Mock data shaped like a real server's answers to GET /metrics, /events and /jobs/{id}/events, plus the older
// endpoints. Deterministic: every 10-second bucket is generated from its own timestamp, so the four
// windows agree with each other. A busy spell 35-55 minutes ago, a failing handler (cdn-push) 40-45 minutes ago,
// retries, lost replies reconciled, and a server that started 5 hours ago (6h/24h windows show the gap).
export const API_KEY = 'a'.repeat(64);
export const READ_KEY = 'b'.repeat(64);
// like the real server: `now` is the request time, and the last bucket is the one in progress (partial)
export const NOW = Date.now();
export const SINCE = NOW - 5 * 3_600_000;
const MIN = 60_000;

function rng(seed) {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}
const pois = (r, mean) => { // small Poisson-ish
  let n = 0, p = Math.exp(-mean), s = p, u = r();
  while (u > s && n < 500) { n++; p *= mean / n; s += p; }
  return n;
};

function bucket10(t) {
  const full = bucket10Full(t);
  if (t + 10_000 <= NOW) return full;
  // the bucket in progress: counters so far (partial), gauges as last sampled
  const f = Math.max(0, (NOW - t) / 10_000);
  const out = { ...full };
  for (const k of ['accepted', 'deduplicated', 'refused', 'completed', 'failed', 'cancelled', 'dispatched', 'steps_ok', 'steps_failed', 'replies_lost', 'reconciled']) out[k] = Math.floor(full[k] * f);
  if (!out.dispatched) out.step_ms = { p50: null, p95: null, max: null };
  return out;
}

function bucket10Full(t) {
  if (t < SINCE) return { t, accepted: 0, deduplicated: 0, refused: 0, completed: 0, failed: 0, cancelled: 0, dispatched: 0, steps_ok: 0, steps_failed: 0, replies_lost: 0, reconciled: 0, live_runs: null, active_runs: null, in_flight: null, step_ms: { p50: null, p95: null, max: null } };
  const r = rng(Math.floor(t / 10_000));
  const ago = (NOW - t) / MIN;
  const daily = 1 + 0.35 * Math.sin(((t / MIN) % 240) / 240 * 2 * Math.PI);
  const busy = ago > 35 && ago < 55 ? 2.6 + 0.6 * Math.sin((ago - 35) / 20 * Math.PI) : 1;
  const outage = ago > 40 && ago < 45.5;
  const accepted = pois(r, 4.2 * daily * busy);
  const deduplicated = pois(r, 0.25 * busy);
  const refused = ago > 41 && ago < 44 ? pois(r, 2.5) : 0;
  const failed = outage ? pois(r, 1.3) : r() < 0.03 ? 1 : 0;
  const cancelled = r() < 0.025 ? 1 : 0;
  const completed = Math.max(0, pois(r, 4.0 * daily * (busy > 1 ? busy * 0.92 : 1)) - (outage ? 2 : 0));
  const dispatched = Math.round((completed + failed) * 2.6 + pois(r, 1.2) + (outage ? 6 : 0));
  const steps_failed = outage ? pois(r, 4.5) : pois(r, 0.12);
  const lost = (Math.abs(ago - 12.3) < 0.09 || Math.abs(ago - 41.2) < 0.09 || Math.abs(ago - 43.6) < 0.09 || Math.abs(ago - 140) < 0.09) ? 1 : 0;
  const reconciled = (Math.abs(ago - 12.1) < 0.09 || Math.abs(ago - 41.0) < 0.09 || Math.abs(ago - 43.4) < 0.09 || Math.abs(ago - 139.8) < 0.09) ? 1 : 0;
  const steps_ok = Math.max(0, dispatched - steps_failed - lost);
  const live = Math.round(3100 + (t - SINCE) / (5 * 3_600_000) * 300 + (busy - 1) * 40);
  const active = Math.round(22 + (busy - 1) * 60 + r() * 8 + (outage ? 40 : 0));
  const p50 = Math.round(105 + r() * 30 + (busy - 1) * 25 + (outage ? 160 : 0));
  const p95 = Math.round(380 + r() * 160 + (busy - 1) * 260 + (outage ? 1600 : 0));
  return {
    t, accepted, deduplicated, refused, completed, failed, cancelled, dispatched, steps_ok, steps_failed,
    replies_lost: lost, reconciled, live_runs: live, active_runs: active, in_flight: Math.min(active, Math.round(3 + r() * 6 + (busy - 1) * 4)),
    step_ms: dispatched ? { p50, p95, max: Math.round(p95 * (1.6 + r())) } : { p50: null, p95: null, max: null },
  };
}

const COUNTERS = ['accepted', 'deduplicated', 'refused', 'completed', 'failed', 'cancelled', 'dispatched', 'steps_ok', 'steps_failed', 'replies_lost', 'reconciled'];
function bucket60(t) {
  const parts = Array.from({ length: 6 }, (_, i) => bucket10(t + i * 10_000)).filter((p) => p.t <= NOW);
  const out = { t };
  for (const k of COUNTERS) out[k] = parts.reduce((a, p) => a + p[k], 0);
  for (const k of ['live_runs', 'active_runs', 'in_flight']) out[k] = [...parts].reverse().find((p) => p[k] !== null)?.[k] ?? null;
  const pick = (k) => { const v = parts.map((p) => p.step_ms[k]).filter((x) => x !== null); return v.length ? Math.max(...v) : null; };
  out.step_ms = { p50: pick('p50'), p95: pick('p95'), max: pick('max') };
  return out;
}

const WIN = { '15m': [15 * MIN, 10_000], '1h': [60 * MIN, 10_000], '6h': [360 * MIN, 60_000], '24h': [1440 * MIN, 60_000] };
export function metrics(window) {
  const [w, b] = WIN[window];
  const end = Math.floor(NOW / b) * b;
  const points = [];
  for (let t = end - w + b; t <= end; t += b) points.push(b === 10_000 ? bucket10(t) : bucket60(t));
  const totals = {};
  for (const k of COUNTERS) totals[k] = points.reduce((a, p) => a + p[k], 0);
  const p50s = points.map((p) => p.step_ms.p50).filter((x) => x !== null).sort((a, z) => a - z);
  const p95s = points.map((p) => p.step_ms.p95).filter((x) => x !== null).sort((a, z) => a - z);
  const maxs = points.map((p) => p.step_ms.max).filter((x) => x !== null);
  totals.step_ms = p50s.length ? { p50: p50s[Math.floor(p50s.length / 2)], p95: p95s[Math.floor(p95s.length * 0.9)], max: Math.max(...maxs) } : { p50: null, p95: null, max: null };
  const scale = w / (60 * MIN);
  const H = (name, calls, failRate, lost, p50, p95, max, okAgo, failAgo) => ({
    name, calls: Math.round(calls * scale), ok: Math.round(calls * scale * (1 - failRate)) - lost, failed: Math.round(calls * scale * failRate), replies_lost: lost,
    step_ms: { p50, p95, max }, last_ok_at: NOW - okAgo, last_failure_at: failAgo === null ? null : NOW - failAgo,
  });
  const recent = window === '15m';
  return {
    now: NOW, since: SINCE, window_ms: w, bucket_ms: b, points, totals,
    states: { queued: 3120, running: 28, retry_waiting: 12, reconciling: 2, done: 18234, failed: 41, cancelled: 9 },
    handlers: [
      H('cdn-push', 2140, recent ? 0.004 : 0.071, recent ? 0 : 2, 210, 1840, 6100, 4_000, recent ? 9 * MIN : 41 * MIN),
      H('chat-hook', 1650, 0, 0, 64, 180, 420, 9_000, null),
      H('ci-runner', 380, 0.008, 0, 2400, 7800, 14200, 70_000, 22 * MIN),
      H('deployer', 290, 0.003, 1, 820, 2600, 5200, 140_000, 3 * 3_600_000),
      H('exporter', 2210, 0.002, 1, 150, 520, 1900, 3_000, 52 * MIN),
      H('renderer', 2190, 0.011, 0, 330, 1250, 3900, 2_000, 6 * MIN),
    ].map((h) => (h.calls ? h : { ...h, ok: 0 })),
  };
}

// --- runs and their timelines -----------------------------------------------------------------------------
export const RUNS = [
  { id: 10418, key: 'nightly-export-2026-11-24', state: 'running:step:2:attempt:2', steps: 4, finished_at: null },
  { id: 10417, key: 'deploy-2026-11-24', state: 'reconciling:step:1', steps: 3, finished_at: null },
  { id: 10416, key: 'digest-2026-11-24', state: 'done', steps: 3, finished_at: NOW - 60_000 },
  { id: 10415, key: 'invoice-sync-0193', state: 'retry_waiting:step:0:attempt:3', steps: 2, finished_at: null },
  { id: 10414, key: 'thumbnails-batch-17', state: 'queued', steps: 1, finished_at: null },
  { id: 10413, key: 'webhook-replay-acme', state: 'failed:step:2', steps: 3, finished_at: NOW - 300_000 },
  { id: 10412, key: 'backup-nightly-1', state: 'cancelled', steps: 1, finished_at: NOW - 900_000 },
  { id: 10411, key: 'customer-export/acme-corp/2026-11-24T03:00:00Z/full-snapshot-with-attachments', state: 'done', steps: 5, finished_at: NOW - 3_600_000 },
  { id: 10410, key: 'search-reindex-eu', state: 'done', steps: 2, finished_at: NOW - 4_000_000 },
  { id: 10409, key: 'ui-scheduled', state: 'queued', steps: 1, finished_at: null },
  { id: 9, key: 'ui-seed-0', state: 'done', steps: 1, finished_at: NOW - 9_000_000 },
];
/** A run as the server answers it: the id is a string. */
export const runOut = (r) => ({ ...r, id: String(r.id) });
export const RUNS_OUT = () => RUNS.map(runOut);
export const SPECS = {
  10418: [['fetch', 'exporter'], ['render', 'renderer'], ['publish', 'cdn-push'], ['notify', 'chat-hook']],
  10417: [['build', 'ci-runner'], ['deploy', 'deployer'], ['notify', 'chat-hook']],
  10416: [['collect', 'exporter'], ['render', 'renderer'], ['send', 'chat-hook']],
};
export function spec(id) {
  const steps = (SPECS[id] ?? [['only', 'exporter']]).map(([name, handler]) => ({ name, handler }));
  return { version: 1, steps, input_b64: 'eyJ0ZW5hbnQiOiJhY21lIiwiZGF5IjoiMjAyNi0xMS0yNCJ9' };
}

function seq(start, list) {
  let t = start;
  return list.map(([dt, kind, extra = {}]) => { t += dt; return { at: t, kind, ...extra }; });
}
const D = (step, attempt, at0 = 0) => ['dispatched', { step, attempt, deadline: 10_000 }];
export function timeline(id) {
  const run = RUNS.find((r) => r.id === id);
  if (!run) return null;
  let events = [];
  let complete = true;
  let reason = null;
  const fix = (ev) => ev.map((e) => (e.kind === 'dispatched' ? { ...e, deadline: e.at + 10_000 } : e));
  switch (id) {
    case 10418:
      events = seq(NOW - 95_000, [
        [0, 'accepted', { not_before: NOW - 95_000 }], [40, ...D(0, 1)], [380, 'checkpointed', { step: 0 }],
        [0, ...D(1, 1)], [2600, 'checkpointed', { step: 1 }], [0, ...D(2, 1)], [10_000, 'step_failed', { step: 2 }],
        [4_200, ...D(2, 2)],
      ]);
      break;
    case 10417:
      events = seq(NOW - 48_000, [[0, 'accepted'], [60, ...D(0, 1)], [21_400, 'checkpointed', { step: 0 }], [40, ...D(1, 1)], [9_000, 'reply_lost', { step: 1 }]]);
      break;
    case 10416:
      events = seq(NOW - 72_400, [
        [0, 'accepted', { not_before: NOW - 72_400 }], [50, ...D(0, 1)], [120, 'checkpointed', { step: 0 }],
        [0, ...D(1, 1)], [2000, 'step_failed', { step: 1 }], [2100, ...D(1, 2)], [1400, 'checkpointed', { step: 1 }],
        [0, ...D(2, 1)], [600, 'reply_lost', { step: 2 }], [200, 'reconciled', { step: 2, effect_found: true }],
        [0, 'finished', { outcome: 'done' }],
      ]);
      break;
    case 10415:
      events = seq(NOW - 31_000, [[0, 'accepted'], [30, ...D(0, 1)], [2200, 'step_failed', { step: 0 }], [2000, ...D(0, 2)], [10_000, 'step_failed', { step: 0 }]]);
      break;
    case 10414:
      events = [{ at: NOW - 8_000, kind: 'accepted', not_before: NOW - 8_000 }];
      break;
    case 10413:
      events = seq(NOW - 300_000 - 64_000, [
        [0, 'accepted'], [40, ...D(0, 1)], [310, 'checkpointed', { step: 0 }], [30, ...D(1, 1)], [900, 'checkpointed', { step: 1 }],
        [30, ...D(2, 1)], [1200, 'step_failed', { step: 2 }], [2000, ...D(2, 2)], [1100, 'step_failed', { step: 2 }],
        [4000, ...D(2, 3)], [10_000, 'step_failed', { step: 2 }], [8000, ...D(2, 4)], [1300, 'step_failed', { step: 2 }],
        [16_000, ...D(2, 5)], [10_000, 'step_failed', { step: 2 }], [0, 'finished', { outcome: 'failed' }],
      ]);
      break;
    case 10412:
      events = seq(NOW - 900_000 - 12_000, [[0, 'accepted'], [40, ...D(0, 1)], [6_000, 'cancel_requested'], [5_900, 'finished', { outcome: 'cancelled' }]]);
      break;
    case 10411:
      complete = false; reason = 'compacted';
      events = seq(NOW - 3_600_000 - 9_000, [[0, ...D(3, 1)], [4100, 'checkpointed', { step: 3 }], [30, ...D(4, 1)], [4800, 'checkpointed', { step: 4 }], [0, 'finished', { outcome: 'done' }]]);
      break;
    case 10410: {
      complete = false; reason = 'truncated';
      const list = [[0, 'accepted'], [40, ...D(0, 1)], [200, 'checkpointed', { step: 0 }]];
      for (let a = 1; a <= 48; a++) list.push([a === 1 ? 30 : 1500, ...D(1, a)], [400 + (a % 5) * 60, a === 48 ? 'checkpointed' : 'step_failed', { step: 1 }]);
      list.push([0, 'finished', { outcome: 'done' }]);
      const all = seq(NOW - 4_000_000 - 95_000, list);
      events = [...all.slice(0, 16), ...all.slice(-80)];
      break;
    }
    case 10409:
      events = [{ at: NOW - 600_000, kind: 'accepted', not_before: NOW + 86_400_000 }];
      break;
    case 9:
      complete = false; reason = 'evicted'; events = [];
      break;
  }
  // run ids cross the API as strings (S2's opaque public ids); the page compares them as strings
  return { run: String(id), key: run.key, state: run.state, complete, reason, events: fix(events) };
}

export function feed(limit) {
  const all = [];
  for (const r of RUNS) {
    const tl = timeline(r.id);
    for (const e of tl.events) all.push({ ...e, run: String(r.id), key: r.key });
  }
  // background traffic from other runs in the last 10 minutes
  const rr = rng(7);
  for (let i = 0; i < 80; i++) {
    const id = 10300 + Math.floor(rr() * 100);
    const at = NOW - Math.floor(rr() * 600_000);
    const kinds = ['accepted', 'dispatched', 'checkpointed', 'checkpointed', 'finished', 'dispatched'];
    const kind = kinds[Math.floor(rr() * kinds.length)];
    all.push({ at, run: String(id), key: id % 7 === 0 ? null : `orders-sync-${id}`, kind, ...(kind === 'finished' ? { outcome: 'done' } : kind === 'accepted' ? {} : { step: Math.floor(rr() * 3), ...(kind === 'dispatched' ? { attempt: 1, deadline: at + 10_000 } : {}) }) });
  }
  all.sort((a, b) => b.at - a.at);
  return { since: SINCE, events: all.filter((e) => e.at <= NOW).slice(0, limit) };
}

export const DIAG = {
  generation: 14, records_written: 1204318, live_runs: 3412, active_runs: 28, index_bytes: 2846720, dedup_keys: 9120,
  states: { cancelled: 9, done: 18234, failed: 41, queued: 3120, reconciling: 2, retry_waiting: 12, running: 28 },
  space_ok: true, errors: { no_space: 0, stale_reply: 3 },
};

// --- the signed-in API (S3), stateful ------------------------------------------------------------------------
// One mock server per browser context: accounts, the session (held here — the testbed is one visitor), workspaces,
// members, invites, keys, handlers, usage, onboarding and workflows, shaped like docs/S3-PLAN.md and, where the code
// on devin/dr-s3 differs, like the code (DELETE /me/sessions, {current_password,new_password}, token_gone 410,
// {"status":"sent"} on resend/forgot; at 1f9f409: the workspace list from GET /workspaces while /auth/session
// carries [], keys as {public_id, name, prefix, permission, revoked, last_used_at_ms, created_by} minted with
// {name, permission}, members nested {account:{public_id,email}, role, since}, invite tokens dri_ + 64 hex with
// an invite_url that is the API path, unknown invites 404, test calls that require input_b64). It also checks what the dashboard sends: CSRF on every
// mutation, x-durable-workspace on flat routes only, never an Authorization header — every breach is logged in
// `violations` so the testbed can fail on it.
export const PASSWORD = 'correct horse battery';
export const TOKEN = `drt_${'A'.repeat(43)}`;
export const DEAD_TOKEN = `drt_${'B'.repeat(43)}`;
export const INVITE_TOKEN = `dri_${'7c'.repeat(32)}`;
export const DEAD_INVITE = `dri_${'00'.repeat(32)}`;
export const WS_PROD = '0193a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b';
export const WS_STAGE = '0193a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5c';
export const WF_DEMO = '0193b000-0000-7000-8000-00000000d0e0';
export const WF_NIGHTLY = '0193b000-0000-7000-8000-00000000d0e1';
const FLAT = ['/jobs', '/metrics', '/events', '/handlers', '/handler-secret', '/diagnostics', '/whoami'];
const isFlat = (p) => FLAT.some((f) => p === f || p.startsWith(`${f}/`));
const hex = (n, seed) => Array.from({ length: n }, (_, i) => ((seed * 31 + i * 7) % 16).toString(16)).join('');

export function saasMock({
  signedIn = true,
  verified = true,
  workspaces = 'two',
  role = 'owner',
  onboardingRoute = true,
  fresh = false,
  bootStatus = null,
  unsupported = [],
} = {}) {
  const s = {
    signedIn,
    csrf: 'drc_' + hex(64, 3),
    me: { public_id: '0193a000-0000-7000-8000-0000000000a1', email: 'ada@example.com', verified },
    violations: [],
    calls: [],
    ws:
      workspaces === 'none'
        ? []
        : [
            { public_id: WS_PROD, name: 'Production', role },
            ...(workspaces === 'two' ? [{ public_id: WS_STAGE, name: 'Staging', role: 'viewer' }] : []),
          ],
    keys: fresh
      ? []
      : [
          { public_id: '0193d000-0000-7000-8000-000000000001', name: 'ci-deploy', prefix: 'drk_7f3a9c1', permission: 'write', revoked: false, last_used_at_ms: NOW - 4 * 60_000, created_by: '0193a000-0000-7000-8000-0000000000a1' },
          { public_id: '0193d000-0000-7000-8000-000000000002', name: 'grafana-read', prefix: 'drk_b21c04e', permission: 'read', revoked: false, last_used_at_ms: null, created_by: '0193a000-0000-7000-8000-0000000000a2' },
          { public_id: '0193d000-0000-7000-8000-000000000003', name: 'old-laptop', prefix: 'dr_91ee2d0', permission: 'write', revoked: true, last_used_at_ms: NOW - 20 * 86_400_000, created_by: null },
        ],
    handlers: fresh
      ? []
      : [
          { name: 'builtin:echo', url: 'builtin:echo', health: { last_test_at: NOW - 3_600_000, last_test_status: 'ok', last_test_latency_ms: 1, p50_ms: 1, p95_ms: 2 } },
          { name: 'charge-card', url: 'https://pay.example.com/steps/charge', health: { last_test_at: NOW - 600_000, last_test_status: 'ok', last_test_latency_ms: 84, p50_ms: 92, p95_ms: 310 } },
          { name: 'send-receipt', url: 'https://mail.example.com/hooks/durable/receipt', health: { last_test_at: NOW - 86_400_000, last_test_status: 'failed', last_test_latency_ms: 412, p50_ms: 140, p95_ms: 980 } },
          { name: 'cdn-push', url: 'https://cdn.example.com/purge', health: null },
        ],
    members: [
      { account: { public_id: '0193a000-0000-7000-8000-0000000000a1', email: 'ada@example.com' }, role, since: NOW - 30 * 86_400_000 },
      { account: { public_id: '0193a000-0000-7000-8000-0000000000a2', email: 'grace@example.com' }, role: 'admin', since: NOW - 12 * 86_400_000 },
      { account: { public_id: '0193a000-0000-7000-8000-0000000000a3', email: 'linus.torvalds.very.long.address@example-company-with-a-long-domain.com' }, role: 'member', since: NOW - 2 * 86_400_000 },
      { account: { public_id: '0193a000-0000-7000-8000-0000000000a4', email: 'margaret@example.com' }, role: 'viewer', since: NOW - 86_400_000 },
    ],
    invites: [{ public_id: '0193e000-0000-7000-8000-000000000001', email: 'ken@example.com', role: 'member', expires_at: NOW + 13 * 86_400_000 }],
    testCalls: 0,
    sessions: [
      { id: '0193c000-0000-7000-8000-000000000001', current: true, created_at_ms: NOW - 2 * 3_600_000, last_seen_at_ms: NOW - 5_000, user_agent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0 Safari/537.36', ip: '203.0.113.7' },
      { id: '0193c000-0000-7000-8000-000000000002', current: false, created_at_ms: NOW - 6 * 86_400_000, last_seen_at_ms: NOW - 86_400_000, user_agent: 'Mozilla/5.0 (iPhone; CPU iPhone OS 18_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Mobile/15E148 Safari/604.1', ip: '198.51.100.24' },
      { id: '0193c000-0000-7000-8000-000000000003', current: false, created_at_ms: NOW - 20 * 86_400_000, last_seen_at_ms: NOW - 9 * 86_400_000, user_agent: 'curl/8.9.1', ip: '192.0.2.10' },
    ],
    workflows: fresh
      ? []
      : [
          { public_id: WF_DEMO, name: 'demo', current_version: 1, updated_at_ms: NOW - 2 * 3_600_000, last_run: { pubid: '10416', at: NOW - 72_400, state: 'done' } },
          { public_id: WF_NIGHTLY, name: 'nightly-export', current_version: 3, updated_at_ms: NOW - 5 * 86_400_000, last_run: { pubid: '10418', at: NOW - 95_000, state: 'running:step:2:attempt:2' } },
        ],
    versions: {
      [WF_DEMO]: [{ version: 1, spec: { version: 1, steps: [{ name: 'echo', handler: 'builtin:echo' }], input_b64: 'eyJoZWxsbyI6IndvcmxkIn0=' }, created_at_ms: NOW - 2 * 3_600_000 }],
      [WF_NIGHTLY]: [
        { version: 3, spec: { version: 1, steps: [{ name: 'fetch', handler: 'charge-card' }, { name: 'render', handler: 'cdn-push' }, { name: 'notify', handler: 'send-receipt' }], input_b64: '' }, created_at_ms: NOW - 5 * 86_400_000 },
        { version: 2, spec: { version: 1, steps: [{ name: 'fetch', handler: 'charge-card' }, { name: 'render', handler: 'cdn-push' }], input_b64: '' }, created_at_ms: NOW - 9 * 86_400_000 },
        { version: 1, spec: { version: 1, steps: [{ name: 'fetch', handler: 'charge-card' }], input_b64: '' }, created_at_ms: NOW - 14 * 86_400_000 },
      ],
    },
  };
  const wsOf = (pid) => s.ws.find((w) => w.public_id === pid) ?? null;
  const roleIn = (pid) => wsOf(pid)?.role ?? null;
  const rank = { viewer: 0, member: 1, admin: 2, owner: 3 };
  const atLeast = (pid, r) => rank[roleIn(pid)] >= rank[r];
  const E = (status, code, error, extra = {}) => ({ status, body: { error, code, ...extra } });
  const ok = (body, status = 200) => ({ status, body });
  const gone = () => E(410, 'token_gone', 'this link is no longer valid — request a fresh one');

  function usage() {
    return {
      plan: 'free',
      limits: { jobs_per_min: 60, live_runs: 500, in_flight: 8, handlers: 16, max_input_bytes: 65536, retention_days: 7, members: 5, api_keys: 10, workflows: 20, test_calls_per_hour: 30, workspaces_per_account: 3, retained_bytes: 16_777_216 },
      used: { jobs_this_minute: 7, live_runs: 41, in_flight: 3, handlers: s.handlers.length, api_keys: s.keys.filter((k) => !k.revoked).length, members: s.members.length + s.invites.length, workflows: s.workflows.length, test_calls_hour: 24 + s.testCalls },
      windows: { minute_ends_at: NOW + 38_000, hour_ends_at: NOW + 22 * 60_000 },
    };
  }

  function handle({ method, path, query, headers, body }) {
    s.calls.push({ method, path, headers });
    const mutation = method !== 'GET' && method !== 'HEAD';
    const has = (h) => headers[h] !== undefined;
    if (has('authorization')) s.violations.push(`${method} ${path}: sent an Authorization header in signed-in mode`);
    const wsHeader = headers['x-durable-workspace'];
    if (path.startsWith('/workspaces/') && wsHeader !== undefined) s.violations.push(`${method} ${path}: x-durable-workspace on a path-scoped route`);
    if (unsupported.some((u) => path.startsWith(u))) return { status: 404, body: null };

    // --- open auth routes
    if (path === '/auth/session') {
      if (bootStatus) return { status: bootStatus, body: bootStatus === 404 ? null : { error: 'store down', code: 'accounts_unavailable' } };
      if (!s.signedIn) return E(401, 'unauthenticated', 'no session');
      // the code's bootstrap carries an empty list; GET /workspaces is where the memberships are
      return ok({ account: s.me, csrf: s.csrf, workspaces: [] });
    }
    if (path === '/auth/login') {
      if (body?.password !== PASSWORD) return E(401, 'invalid_credentials', 'invalid credentials');
      s.signedIn = true;
      return ok({ account: s.me, csrf: s.csrf });
    }
    if (path === '/auth/signup') {
      if (!body?.email || String(body.password ?? '').length < 12) return E(400, 'bad_input', 'password must be at least 12 characters');
      return ok({ status: 'verification_sent' }, 202);
    }
    // verify takes the sign-up password with the token (pre-registration takeover fix): missing → 400, a token
    // whose pending password does not match → 403 invalid_credentials, a dead token → 410
    if (path === '/auth/verify') {
      if (typeof body?.password !== 'string' || body.password === '') return E(400, 'bad_input', 'password is required');
      if (body?.token !== TOKEN) return gone();
      if (body.password !== PASSWORD) return E(403, 'invalid_credentials', 'token and password do not match');
      s.signedIn = false; // every session of the account ends; no cookie is set
      return ok({ verified: true });
    }
    // reset sets no cookie: the visitor signs in afresh
    if (path === '/auth/reset') {
      if (body?.token !== TOKEN) return gone();
      s.signedIn = false;
      return ok({ reset: true });
    }
    if (path === '/auth/resend-verification' || path === '/auth/forgot') return ok({ status: 'sent' }, 202);

    // --- everything else needs the session
    if (!s.signedIn) return E(401, 'unauthenticated', 'no session');
    if (mutation && headers['x-durable-csrf'] !== s.csrf) {
      s.violations.push(`${method} ${path}: mutation without the CSRF token`);
      return E(403, 'csrf_failed', 'cross-site request refused');
    }
    if (path === '/auth/logout') {
      s.signedIn = false;
      return { status: 204, body: null };
    }
    if (path === '/me') return ok({ account: s.me });
    if (path === '/me/sessions' && method === 'GET') return ok({ sessions: s.sessions });
    if (path === '/me/sessions' && method === 'DELETE') {
      const n = s.sessions.filter((x) => !x.current).length;
      s.sessions = s.sessions.filter((x) => x.current);
      return ok({ revoked: n });
    }
    let m = path.match(/^\/me\/sessions\/([^/]+)$/);
    if (m && method === 'DELETE') {
      const before = s.sessions.length;
      s.sessions = s.sessions.filter((x) => x.id !== m[1] || x.current);
      return s.sessions.length < before ? { status: 204, body: null } : E(404, 'not_found', 'no such session');
    }
    if (path === '/me/password') return body?.current_password === PASSWORD ? ok({ changed: true, csrf: s.csrf }) : E(403, 'invalid_credentials', 'current password does not match');
    if (path === '/invites/accept') {
      if (!s.me.verified) return E(403, 'email_unverified', "verify the account's email first");
      if (body?.token !== INVITE_TOKEN) return E(404, 'not_found', 'no such invite');
      const w = { public_id: '0193a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5d', name: 'Acme Payments', role: 'member' };
      s.ws.push(w);
      return ok({ workspace: w });
    }
    if (path === '/invites/decline') return { status: 204, body: null };
    if (path === '/workspaces' && method === 'GET') return ok({ workspaces: s.ws });
    if (path === '/workspaces' && method === 'POST') {
      if (!s.me.verified) return E(403, 'email_unverified', 'verify your email first');
      if (s.ws.length >= 3) return E(429, 'plan_limit', 'plan limit', { limit: 'workspaces_per_account', plan: 'free', used: 3, cap: 3 });
      const w = { public_id: `0193a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a6${s.ws.length}`, name: body.name, role: 'owner', plan: 'free' };
      s.ws.push(w);
      return ok({ public_id: w.public_id, name: w.name, role: 'owner', handler_secret: hex(64, 9) });
    }

    // --- /workspaces/{pid}/… : the workspace comes from the path
    m = path.match(/^\/workspaces\/([^/]+)(\/.*)?$/);
    if (m) {
      const pid = decodeURIComponent(m[1]);
      const rest = m[2] ?? '';
      if (!wsOf(pid)) return E(404, 'not_found', 'no such workspace');
      if (mutation && !s.me.verified) return E(403, 'email_unverified', 'verify your email first');
      const need = (r) => (atLeast(pid, r) ? null : E(403, 'forbidden', 'your role cannot do this'));
      if (rest === '') {
        if (method === 'GET') return ok({ ...wsOf(pid), created_at_ms: NOW - 30 * 86_400_000 });
        if (method === 'PATCH') { const f = need('admin'); if (f) return f; wsOf(pid).name = body.name; return ok(wsOf(pid)); }
        if (method === 'DELETE') {
          const f = need('owner'); if (f) return f;
          if (body?.confirm !== `delete ${wsOf(pid).name}`) return E(400, 'bad_input', 'confirm phrase does not match');
          s.ws = s.ws.filter((w) => w.public_id !== pid);
          return { status: 204, body: null };
        }
      }
      if (rest === '/keys') {
        if (method === 'GET') return ok({ keys: s.keys });
        const f = need('admin'); if (f) return f;
        if (!['write', 'read', 'admin'].includes(body?.permission)) return E(400, 'bad_input', `permission must be write or read, not ${JSON.stringify(body?.permission)}`);
        if (s.keys.filter((k) => !k.revoked).length >= 10) return E(429, 'plan_limit', 'plan limit', { limit: 'api_keys', plan: 'free', used: 10, cap: 10 });
        const secret = 'drk_' + hex(64, 5);
        const row = { public_id: `0193d000-0000-7000-8000-0000000001${String(s.keys.length).padStart(2, '0')}`, name: body.name, prefix: secret.slice(0, 11), permission: body.permission === 'read' ? 'read' : 'write', revoked: false, last_used_at_ms: null, created_by: s.me.public_id };
        s.keys.unshift(row);
        return ok({ ...row, key: secret });
      }
      m = rest.match(/^\/keys\/([^/]+)(\/rotate)?$/);
      if (m) {
        const f = need('admin'); if (f) return f;
        const k = s.keys.find((x) => x.public_id === decodeURIComponent(m[1]));
        if (!k || k.revoked) return E(404, 'not_found', 'no such key');
        k.revoked = true;
        if (m[2]) {
          const secret = 'drk_' + hex(64, 6);
          const row = { ...k, public_id: k.public_id.slice(0, -2) + 'f0', prefix: secret.slice(0, 11), last_used_at_ms: null, revoked: false, created_by: s.me.public_id };
          s.keys.unshift(row);
          return ok({ ...row, key: secret });
        }
        return { status: 204, body: null };
      }
      if (rest === '/members' && method === 'GET') return ok({ members: s.members });
      if (rest === '/members' && method === 'POST') {
        const f = need('admin'); if (f) return f;
        if (s.invites.some((i) => i.email === body.email)) return E(409, 'invite_pending', 'a live invite already covers this address');
        if (s.members.some((x) => x.account.email === body.email)) return E(409, 'already_member', 'this account is already a member');
        if (s.members.length + s.invites.length >= 5) return E(429, 'plan_limit', 'plan limit', { limit: 'members', plan: 'free', used: 5, cap: 5 });
        const inv = { public_id: `0193e000-0000-7000-8000-0000000000${String(s.invites.length + 2).padStart(2, '0')}`, email: body.email, role: body.role, expires_at: NOW + 14 * 86_400_000 };
        s.invites.push(inv);
        // the code answers its own API path, not a dashboard link; the dashboard builds the link from the token
        return ok({ invite: inv, invite_url: `/invites/accept?token=${INVITE_TOKEN}` });
      }
      m = rest.match(/^\/members\/([^/]+)$/);
      if (m) {
        const f = need('admin'); if (f) return f;
        const mem = s.members.find((x) => x.account.public_id === decodeURIComponent(m[1]));
        if (!mem) return E(404, 'not_found', 'no such member');
        if (mem.role === 'owner') return E(409, 'last_owner', 'the owner cannot be removed or demoted');
        if (method === 'PATCH') { mem.role = body.role; return ok({ account: { public_id: mem.account.public_id }, role: body.role }); }
        s.members = s.members.filter((x) => x !== mem);
        return { status: 204, body: null };
      }
      if (rest === '/invites') return ok({ invites: s.invites });
      m = rest.match(/^\/invites\/([^/]+)$/);
      if (m) { const f = need('admin'); if (f) return f; s.invites = s.invites.filter((i) => i.public_id !== decodeURIComponent(m[1])); return { status: 204, body: null }; }
      if (rest === '/usage') return ok(usage());
      if (rest === '/onboarding') {
        if (!onboardingRoute) return { status: 404, body: null };
        return ok({ steps: [
          { id: 'handler', done: s.handlers.length > 0, detail: s.handlers[0]?.name ?? null },
          { id: 'test_call', done: s.handlers.some((h) => h.health?.last_test_status === 'ok'), detail: null },
          { id: 'api_key', done: s.keys.some((k) => !k.revoked), detail: null },
          { id: 'first_run', done: !fresh, detail: null },
        ] });
      }
      if (rest === '/workflows' && method === 'GET') return ok({ workflows: s.workflows });
      if (rest === '/workflows' && method === 'POST') {
        const f = need('member'); if (f) return f;
        if (s.workflows.some((w) => w.name === body.name)) return E(409, 'name_taken', 'name taken');
        const bad = body.spec.steps.find((st) => !s.handlers.some((h) => h.name === st.handler));
        if (bad) return E(409, 'unregistered_handler', `no handler named ${bad.handler}`);
        const id = `0193b000-0000-7000-8000-00000000d1${String(s.workflows.length).padStart(2, '0')}`;
        s.workflows.unshift({ public_id: id, name: body.name, current_version: 1, updated_at_ms: Date.now(), last_run: null });
        s.versions[id] = [{ version: 1, spec: body.spec, created_at_ms: Date.now() }];
        return ok({ public_id: id, name: body.name, current_version: 1 });
      }
      m = rest.match(/^\/workflows\/([^/]+)(\/run|\/runs)?$/);
      if (m) {
        const id = decodeURIComponent(m[1]);
        const wf = s.workflows.find((w) => w.public_id === id);
        if (!wf) return E(404, 'not_found', 'no such workflow');
        if (m[2] === '/runs') return ok({ runs: [
          { run_id: '10416', submitted_at_ms: NOW - 72_400, state: 'done', finished_at_ms: NOW - 60_000 },
          { run_id: '10413', submitted_at_ms: NOW - 364_000, state: 'failed:step:2', finished_at_ms: NOW - 300_000 },
          { run_id: '10412', submitted_at_ms: NOW - 912_000, state: 'cancelled', finished_at_ms: NOW - 900_000 },
        ] });
        if (m[2] === '/run') {
          const f = need('member'); if (f) return f;
          if (!headers['idempotency-key']) s.violations.push('Run now without an Idempotency-Key');
          wf.last_run = { pubid: '10416', at: Date.now(), state: 'queued' };
          return ok({ run_id: '10416', version: wf.current_version });
        }
        if (method === 'GET') return ok({ ...wf, versions: s.versions[id] ?? [] });
        if (method === 'PATCH') {
          const f = need('member'); if (f) return f;
          if (body.name) wf.name = body.name;
          if (body.spec) { wf.current_version += 1; s.versions[id].unshift({ version: wf.current_version, spec: body.spec, created_at_ms: Date.now() }); }
          wf.updated_at_ms = Date.now();
          return ok(wf);
        }
        if (method === 'DELETE') { const f = need('member'); if (f) return f; s.workflows = s.workflows.filter((w) => w !== wf); return { status: 204, body: null }; }
      }
      return { status: 404, body: null };
    }

    // --- flat routes: the workspace comes from x-durable-workspace
    if (isFlat(path)) {
      if (!wsHeader) { s.violations.push(`${method} ${path}: flat route without x-durable-workspace`); return E(400, 'workspace_required', 'pick a workspace'); }
      if (!wsOf(wsHeader)) return E(403, 'forbidden', 'not a member');
      if (mutation && !s.me.verified) return E(403, 'email_unverified', 'verify your email first');
      const write = atLeast(wsHeader, 'member');
      if (path === '/handlers' && method === 'GET') return ok({ handlers: s.handlers });
      m = path.match(/^\/handlers\/([^/]+)(\/test)?$/);
      if (m) {
        if (!write) return E(403, 'forbidden', 'your role cannot do this');
        const name = decodeURIComponent(m[1]);
        if (m[2]) {
          if (typeof body?.input_b64 !== 'string') return E(422, 'bad_json', 'malformed JSON');
          if (++s.testCalls > 30) return E(429, 'test_call_limited', 'test calls are bounded to 30 per workspace per hour');
          const h = s.handlers.find((x) => x.name === name);
          if (!h) return E(404, 'not_found', 'no such handler in this workspace');
          if (name === 'send-receipt') return ok({ status: 'failed', http_status: 500, latency_ms: 412, reply_excerpt_b64: Buffer.from('{"error":"template receipt-v2 not found"}').toString('base64') });
          h.health = { ...(h.health ?? {}), last_test_at: Date.now(), last_test_status: 'ok', last_test_latency_ms: 84 };
          return ok({ status: 'ok', http_status: 200, latency_ms: name === 'builtin:echo' ? 1 : 84, reply_excerpt_b64: body?.input_b64 ?? '' });
        }
        if (method === 'PUT') {
          if (name === 'builtin:echo' && body?.url && body.url !== 'builtin:echo') return E(400, 'bad_handler_url', 'builtin:echo registers with url "builtin:echo" or an empty url');
          if (String(body?.url ?? '').includes('127.0.0.1')) return E(400, 'url_refused', 'private or local targets are refused');
          const h = s.handlers.find((x) => x.name === name);
          if (h) h.url = body.url; else s.handlers.push({ name, url: body.url, health: null });
          return ok({ name, url: body.url });
        }
        if (method === 'DELETE') { s.handlers = s.handlers.filter((x) => x.name !== name); return ok({ deleted: name }); }
      }
      if (path === '/handler-secret') return atLeast(wsHeader, 'admin') ? ok({ secret: hex(64, 11) }) : E(403, 'forbidden', 'your role cannot do this');
      if (path === '/diagnostics') return ok({ live_runs: fresh ? 0 : 41, active_runs: fresh ? 0 : 6, states: fresh ? {} : { queued: 2, running: 3, retry_waiting: 1, done: 31, failed: 3, cancelled: 1 } });
      if (path === '/metrics') return ok(metrics(query.get('window') ?? '1h'));
      if (path === '/events') return ok(feed(Number(query.get('limit') ?? 50)));
      if (path === '/jobs' && method === 'GET') return ok(fresh ? { runs: [], next_before: null } : { runs: RUNS_OUT(), next_before: query.get('order') === 'desc' ? '10408' : null, next_after: null });
      if (path === '/jobs' && method === 'POST') return write ? ok({ run_id: '10414' }) : E(403, 'forbidden', 'your role cannot do this');
      m = path.match(/^\/jobs\/(\d+)$/);
      if (m) { const r = RUNS.find((x) => x.id === Number(m[1])); return r ? ok(runOut(r)) : E(404, 'not_found', 'no such run'); }
      m = path.match(/^\/jobs\/(\d+)\/events$/);
      if (m) { const t = timeline(Number(m[1])); return t ? ok(t) : E(404, 'not_found', 'no such run'); }
      m = path.match(/^\/jobs\/(\d+)\/spec$/);
      if (m) return write ? ok({ spec: spec(Number(m[1])) }) : E(403, 'forbidden', 'your role cannot do this');
      m = path.match(/^\/jobs\/(\d+)\/cancel$/);
      if (m) return write ? ok({ cancelled: Number(m[1]) }) : E(403, 'forbidden', 'your role cannot do this');
    }
    if (path === '/health') return ok({ ok: true });
    if (path === '/ready') return ok({ ready: true, code: null });
    return { status: 404, body: null };
  }
  return { state: s, handle };
}
