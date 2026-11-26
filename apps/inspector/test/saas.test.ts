import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
	allowances, can, deriveOnboarding, deviceName, finishedFromStates, isSample, listOf, meters, normHandler, normInvite, normKey, normMe, normMember,
	normOnboarding, normSession, normWorkflow, normWorkflowDetail, normWorkflowRun, normWorkspace, onboardingDone, STEP_ORDER, toMs,
} from '../src/lib/saas.ts';

test('roles: the matrix, used to hide controls', () => {
	assert.ok(can('viewer', 'read') && !can('viewer', 'write'));
	assert.ok(can('member', 'write') && !can('member', 'admin'));
	assert.ok(can('admin', 'admin') && !can('admin', 'owner'));
	assert.ok(can('owner', 'owner'));
	assert.ok(!can(null, 'read'));
});

test('lists: a bare array or an object holding one', () => {
	assert.deepEqual(listOf([1, 2], 'keys'), [1, 2]);
	assert.deepEqual(listOf({ keys: [3] }, 'keys'), [3]);
	assert.deepEqual(listOf({ keys: 'x' }, 'keys'), []);
	assert.deepEqual(listOf(null, 'keys'), []);
});

test('timestamps: ms numbers, numeric strings and ISO dates', () => {
	assert.equal(toMs(1_795_000_000_000), 1_795_000_000_000);
	assert.equal(toMs('1795000000000'), 1_795_000_000_000);
	assert.equal(toMs('2026-11-24T10:00:00Z'), Date.UTC(2026, 10, 24, 10));
	assert.equal(toMs('soon'), null);
	assert.equal(toMs(null), null);
	assert.equal(toMs(Number.NaN), null);
});

test('session bootstrap parts', () => {
	assert.deepEqual(normMe({ public_id: 'a', email: 'ada@example.com', verified: true }), { public_id: 'a', email: 'ada@example.com', verified: true });
	assert.equal(normMe({}), null);
	assert.deepEqual(normWorkspace({ public_id: 'w1', name: 'Prod', role: 'admin', plan: 'free' }), { public_id: 'w1', name: 'Prod', role: 'admin', plan: 'free' });
	assert.deepEqual(normWorkspace({ public_id: 'w2', role: 'superuser' }), { public_id: 'w2', name: 'Untitled', role: 'viewer', plan: 'free' }, 'an unknown role reads as the least');
	assert.equal(normWorkspace({ name: 'no id' }), null);
});

test('keys: id or public_id, admin reads as write, _ms or ISO times, revoked flag or time', () => {
	const k = normKey({ public_id: 'k1', name: 'ci', prefix: 'dr_ab12', scope: 'write', created_at_ms: 5, last_used_at: null, revoked: false });
	assert.deepEqual(k, { id: 'k1', name: 'ci', prefix: 'dr_ab12', scope: 'write', createdAt: 5, lastUsedAt: null, revoked: false, createdBy: null });
	assert.equal(normKey({ id: 7, permission: 'admin' })?.scope, 'write');
	assert.equal(normKey({ id: 7, permission: 'admin' })?.id, '7');
	assert.equal(normKey({ id: 'k', scope: 'read', revoked_at: '2026-11-24T10:00:00Z' })?.revoked, true);
	assert.equal(normKey({ name: 'no id' }), null);
});

test('members, invites, sessions', () => {
	assert.deepEqual(normMember({ account: { public_id: 'a1', email: 'ada@example.com' }, role: 'owner', since: '2026-11-24T10:00:00Z' }), {
		accountId: 'a1', email: 'ada@example.com', role: 'owner', since: Date.UTC(2026, 10, 24, 10),
	});
	assert.deepEqual(normMember({ account_public_id: 'a2', email: 'b@example.com', role: 'member', since_ms: 7 }), { accountId: 'a2', email: 'b@example.com', role: 'member', since: 7 }, 'the store row is flat');
	assert.equal(normMember({ role: 'owner' }), null);
	assert.deepEqual(normInvite({ public_id: 'i1', email: 'b@example.com', role: 'viewer', expires_at_ms: 9 }), { id: 'i1', email: 'b@example.com', role: 'viewer', expiresAt: 9 });
	assert.deepEqual(normSession({ id: 's1', current: true, created_at_ms: 1, last_seen_at_ms: 2, user_agent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X) Firefox/140.0', ip: '203.0.113.7' }), {
		id: 's1', current: true, createdAt: 1, lastSeenAt: 2, userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X) Firefox/140.0', ip: '203.0.113.7',
	});
	assert.equal(deviceName('Mozilla/5.0 (Macintosh; Intel Mac OS X) Firefox/140.0'), 'Firefox on macOS');
	assert.equal(deviceName('Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) AppleWebKit Version/18 Mobile Safari/604.1'), 'Safari on iOS');
	assert.equal(deviceName('curl/8.9.1'), 'curl');
	assert.equal(deviceName(null), 'Unknown device');
});

test('handlers: health fields, flag source, the sample', () => {
	const h = normHandler({ name: 'charge', url: 'https://pay.example.com/step', health: { last_test_status: 'ok', last_test_latency_ms: 84, p50_ms: 90, p95_ms: 310, last_test_at: 5 } });
	assert.deepEqual(h?.health, { lastTestAt: 5, lastTestStatus: 'ok', lastTestLatencyMs: 84, p50: 90, p95: 310 });
	assert.equal(normHandler({ name: 'step', source: 'flag' })?.health, null);
	assert.ok(isSample({ name: 'builtin:echo', url: null }));
	assert.ok(isSample({ name: 'echo', url: 'builtin:echo' }));
	assert.ok(!isSample({ name: 'charge', url: 'https://x' }));
});

test('workflows: list rows and detail with versions newest first', () => {
	assert.deepEqual(normWorkflow({ public_id: 'w', name: 'demo', current_version: 2, updated_at: 3, last_run: { pubid: 'WBwXuaDUPWc', at: 4, state: 'done' } }), {
		id: 'w', name: 'demo', currentVersion: 2, updatedAt: 3, lastRun: { runId: 'WBwXuaDUPWc', at: 4, state: 'done' },
	});
	const d = normWorkflowDetail({ public_id: 'w', name: 'demo', versions: [{ version: 1, spec: { steps: [{ name: 'a', handler: 'x' }], input_b64: '' } }, { version: 2, spec: { steps: [] } }] });
	assert.deepEqual(d?.versions.map((v) => v.version), [2, 1]);
	assert.equal(d?.currentVersion, 2);
	assert.deepEqual(d?.versions[1]?.spec.steps, [{ name: 'a', handler: 'x' }]);
});

test('usage meters pair each meter with its limit and grade it', () => {
	const m = meters({ plan: 'free', limits: { api_keys: 10, members: 5, handlers: 16, jobs_per_min: 60 }, used: { api_keys: 10, members: 4, handlers: 1, jobs_this_minute: 0 } });
	const by = Object.fromEntries(m.map((x) => [x.id, x]));
	assert.equal(by.api_keys?.level, 'full');
	assert.equal(by.members?.level, 'warn');
	assert.equal(by.handlers?.level, 'ok');
	assert.equal(by.jobs_per_min?.pct, 0);
	assert.equal(by.workflows?.level, 'unknown', 'a number the server did not send is unknown, never 0');
	assert.deepEqual(allowances({ limits: { max_input_bytes: 65536, retention_days: 7 } }).map((a) => a.id), ['max_input_bytes', 'retention_days']);
});

test('onboarding from the server: page order, unknown steps dropped, missing = not done', () => {
	const s = normOnboarding({ steps: [{ id: 'first_run', done: true }, { id: 'handler', done: true, detail: 'charge' }, { id: 'bogus', done: true }] });
	assert.deepEqual(s.map((x) => x.id), [...STEP_ORDER]);
	assert.deepEqual(s.map((x) => x.done), [false, true, false, true]);
	assert.equal(s[1]?.detail, 'charge');
	assert.ok(!onboardingDone(s));
});

test('onboarding from the server\'s four facts: only true counts', () => {
	const s = normOnboarding({ has_key: true, has_handler: true, test_call_ok: false, first_job_finished: 'yes' });
	assert.deepEqual(s.map((x) => x.id), [...STEP_ORDER]);
	assert.deepEqual(s.map((x) => x.done), [true, true, false, false]);
	assert.ok(onboardingDone(normOnboarding({ has_key: true, has_handler: true, test_call_ok: true, first_job_finished: true })));
});

test('workflows as the server answers them: last run, versions, and a detail with only the latest spec', () => {
	const row = { public_id: 'w', name: 'demo', version: 2, archived: false, created_at_ms: 1, updated_at_ms: 5, last_run: { id: 'WBwXuaDUPWc', state: 'done', version: 2, op_key: 'k', created_at_ms: 9 } };
	assert.deepEqual(normWorkflow(row), { id: 'w', name: 'demo', currentVersion: 2, updatedAt: 5, lastRun: { runId: 'WBwXuaDUPWc', at: 9, state: 'done' } });
	assert.equal(normWorkflow({ ...row, last_run: null })?.lastRun, null);
	const spec = { version: 1, steps: [{ name: 'a', handler: 'x' }], input_b64: 'aA==' };
	const full = normWorkflowDetail({ workflow: row, spec, versions: [{ version: 2, spec, created_at_ms: 5 }, { version: 1, spec: { steps: [] }, created_at_ms: 1 }] });
	assert.deepEqual(full?.versions.map((v) => [v.version, v.createdAt]), [[2, 5], [1, 1]]);
	const latestOnly = normWorkflowDetail({ workflow: row, spec });
	assert.deepEqual(latestOnly?.versions, [{ version: 2, spec: { steps: [{ name: 'a', handler: 'x' }], input_b64: 'aA==' }, createdAt: 5 }]);
	assert.deepEqual(normWorkflowRun({ id: 'WBwXuaDUPWc', state: 'running:step:1:attempt:1', version: 2, op_key: 'k', created_at_ms: 9 }), {
		runId: 'WBwXuaDUPWc', submittedAt: 9, state: 'running:step:1:attempt:1', finishedAt: null,
	});
});

test('onboarding from facts: done only when a fact proves it', () => {
	const none = deriveOnboarding({ keys: null, handlers: null, finishedRuns: null, testOkSeen: false });
	assert.ok(none.every((x) => !x.done));
	const key = { id: 'k', name: 'ci', prefix: 'dr_1', scope: 'write' as const, createdAt: 1, lastUsedAt: null, revoked: false, createdBy: null };
	const all = deriveOnboarding({
		keys: [{ ...key, revoked: true }, key],
		handlers: [{ name: 'builtin:echo', url: 'builtin:echo', source: null, health: { lastTestAt: 1, lastTestStatus: 'ok', lastTestLatencyMs: 1, p50: null, p95: null } }],
		finishedRuns: 1,
		testOkSeen: false,
	});
	assert.ok(onboardingDone(all));
	const revokedOnly = deriveOnboarding({ keys: [{ ...key, revoked: true }], handlers: [], finishedRuns: 0, testOkSeen: true });
	assert.deepEqual(revokedOnly.map((x) => x.done), [false, false, true, false]);
	assert.equal(finishedFromStates({ done: 2, failed: 1, running: 4 }), 3);
	assert.equal(finishedFromStates(undefined), null);
});

test('keys carry who minted them (S3-5 created_by)', () => {
	assert.equal(normKey({ public_id: 'k', permission: 'write', created_by: 'acc-1' })?.createdBy, 'acc-1');
	assert.equal(normKey({ public_id: 'k' })?.createdBy, null);
});

test('invite links: the token moves into the dashboard fragment; anything else stays as given', async () => {
	const { inviteLink } = await import('../src/lib/saas.ts');
	const tok = `dri_${'a'.repeat(64)}`;
	assert.equal(inviteLink(`/invites/accept?token=${tok}`, 'https://run.example.com/app/'), `https://run.example.com/app/#/accept-invite?token=${tok}`);
	assert.equal(inviteLink(`https://run.example.com/app#/accept-invite?token=${tok}`, 'https://run.example.com/app/'), `https://run.example.com/app/#/accept-invite?token=${tok}`);
	assert.equal(inviteLink('https://elsewhere.example.com/x', 'https://run.example.com/app/'), 'https://elsewhere.example.com/x');
});
