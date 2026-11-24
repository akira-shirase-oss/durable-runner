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
  return { run: id, key: run.key, state: run.state, complete, reason, events: fix(events) };
}

export function feed(limit) {
  const all = [];
  for (const r of RUNS) {
    const tl = timeline(r.id);
    for (const e of tl.events) all.push({ ...e, run: r.id, key: r.key });
  }
  // background traffic from other runs in the last 10 minutes
  const rr = rng(7);
  for (let i = 0; i < 80; i++) {
    const id = 10300 + Math.floor(rr() * 100);
    const at = NOW - Math.floor(rr() * 600_000);
    const kinds = ['accepted', 'dispatched', 'checkpointed', 'checkpointed', 'finished', 'dispatched'];
    const kind = kinds[Math.floor(rr() * kinds.length)];
    all.push({ at, run: id, key: id % 7 === 0 ? null : `orders-sync-${id}`, kind, ...(kind === 'finished' ? { outcome: 'done' } : kind === 'accepted' ? {} : { step: Math.floor(rr() * 3), ...(kind === 'dispatched' ? { attempt: 1, deadline: at + 10_000 } : {}) }) });
  }
  all.sort((a, b) => b.at - a.at);
  return { since: SINCE, events: all.filter((e) => e.at <= NOW).slice(0, limit) };
}

export const DIAG = {
  generation: 14, records_written: 1204318, live_runs: 3412, active_runs: 28, index_bytes: 2846720, dedup_keys: 9120,
  states: { cancelled: 9, done: 18234, failed: 41, queued: 3120, reconciling: 2, retry_waiting: 12, running: 28 },
  space_ok: true, errors: { no_space: 0, stale_reply: 3 },
};
