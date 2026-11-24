import assert from 'node:assert/strict';
import { test } from 'node:test';
import { copyFor, isFlatWorkspaceRoute, isMutation, parseEnvelope, planLimitCopy, sessionHeaders, type SessionCred } from '../src/lib/http.ts';

const cred: SessionCred = { kind: 'session', csrf: 'drc_abc', ws: '0193a1b2-0000-7000-8000-000000000001' };

test('CSRF rides every mutation and no read', () => {
	for (const m of ['POST', 'PUT', 'PATCH', 'DELETE', 'post']) assert.equal(sessionHeaders(m, '/workspaces/x/keys', cred)['x-durable-csrf'], 'drc_abc', m);
	for (const m of ['GET', 'HEAD', undefined]) assert.equal(sessionHeaders(m, '/workspaces/x/keys', cred)['x-durable-csrf'], undefined, String(m));
	assert.ok(isMutation('DELETE') && !isMutation(undefined));
});

test('the workspace header goes to flat routes only; /workspaces/{pid}/… take it from the path', () => {
	for (const p of ['/jobs', '/jobs?order=desc', '/jobs/WBwXuaDUPWc/events', '/metrics?window=1h', '/events?limit=30', '/handlers', '/handlers/builtin%3Aecho/test', '/handler-secret', '/diagnostics', '/whoami'])
		assert.equal(sessionHeaders('GET', p, cred)['x-durable-workspace'], cred.ws, p);
	for (const p of ['/workspaces/x/keys', '/workspaces', '/auth/session', '/me/sessions', '/invites/accept', '/jobsx', '/handlersx'])
		assert.equal(sessionHeaders('GET', p, cred)['x-durable-workspace'], undefined, p);
	assert.ok(isFlatWorkspaceRoute('/jobs/1/cancel'));
	assert.ok(!isFlatWorkspaceRoute('/admin/diagnostics'));
});

test('no selected workspace, no header; never an Authorization header', () => {
	const h = sessionHeaders('POST', '/jobs', { kind: 'session', csrf: 'drc_abc', ws: null });
	assert.deepEqual(h, { 'x-durable-csrf': 'drc_abc' });
	assert.deepEqual(sessionHeaders('POST', '/auth/login', { kind: 'session', csrf: '', ws: null }), {});
	for (const p of ['/jobs', '/workspaces/x']) assert.equal(Object.keys(sessionHeaders('POST', p, cred)).some((k) => k.toLowerCase() === 'authorization'), false);
});

test('the envelope: message, code and the extra fields (plan_limit)', () => {
	assert.deepEqual(parseEnvelope({ error: 'no', code: 'plan_limit', limit: 'api_keys', cap: 10 }), { message: 'no', code: 'plan_limit', extra: { limit: 'api_keys', cap: 10 } });
	assert.deepEqual(parseEnvelope(null), { message: null, code: null, extra: {} });
	assert.deepEqual(parseEnvelope([1]), { message: null, code: null, extra: {} });
	assert.deepEqual(parseEnvelope({ error: '', code: 7 }), { message: null, code: null, extra: {} });
});

test('copy: known codes read as sentences; unknown ones keep the server message', () => {
	assert.match(copyFor('email_unverified', 'x'), /Verify your email/);
	assert.equal(copyFor('something_new', 'the server said so'), 'the server said so');
	assert.equal(copyFor(null, 'fallback'), 'fallback');
});

test('plan_limit copy names the limit, the cap and the way out', () => {
	assert.equal(
		planLimitCopy({ limit: 'api_keys', plan: 'free', used: 10, cap: 10 }, null),
		'The Free plan allows 10 API keys. Revoke one you no longer need.',
	);
	assert.match(planLimitCopy({ limit: 'jobs_per_min', plan: 'free', cap: 60 }, 7_200), /allows 60 jobs per minute\..*Retry in 8 s\.$/);
	assert.match(planLimitCopy({ limit: 'members', cap: 1 }, null), /allows 1 member\./);
	assert.match(planLimitCopy({ limit: 'mystery' }, null), /^The Free plan's limit on mystery is reached\./);
	assert.equal(planLimitCopy({ limit: 'workspaces' }, null), "The Free plan's limit on workspaces you own is reached. Delete one you no longer need.");
	assert.equal(planLimitCopy({ limit: 'invites' }, null), "The Free plan's limit on invites per day is reached. Revoke a pending invite or try again tomorrow.");
	assert.match(planLimitCopy({}, 0), /^The Free plan's limit is reached\. Free up room/);
});
