import assert from 'node:assert/strict';
import { afterEach, test } from 'node:test';
import { ApiError, createKey, listRuns, login, onSessionLost, probeSession, revokeKey, type SessionCred } from '../src/lib/api.ts';

// The transport, with fetch stubbed: what a signed-in request carries, and how answers become values or errors.
type Seen = { url: string; init: RequestInit };
const seen: Seen[] = [];
const real = globalThis.fetch;
function answer(status: number, body: unknown, headers: Record<string, string> = {}) {
	globalThis.fetch = (async (url: string, init: RequestInit = {}) => {
		seen.push({ url, init });
		return new Response(body === null ? null : JSON.stringify(body), { status, headers: { 'content-type': 'application/json', ...headers } });
	}) as typeof fetch;
}
const hdr = (s: Seen) => s.init.headers as Record<string, string>;
const cred: SessionCred = { kind: 'session', csrf: 'drc_tok', ws: 'ws-1' };
afterEach(() => {
	globalThis.fetch = real;
	seen.length = 0;
	onSessionLost(null);
});

test('a signed-in mutation on a path route: CSRF, cookie, no workspace header, no Authorization', async () => {
	answer(200, { key: 'drk_x' });
	assert.deepEqual(await createKey(cred, 'ws-1', 'ci', 'write'), { key: 'drk_x' });
	const s = seen[0]!;
	assert.equal(s.url, '/workspaces/ws-1/keys');
	assert.equal(s.init.method, 'POST');
	assert.equal(s.init.credentials, 'same-origin');
	assert.equal(hdr(s)['x-durable-csrf'], 'drc_tok');
	assert.equal(hdr(s)['x-durable-workspace'], undefined);
	assert.equal(hdr(s)['authorization'], undefined);
	assert.equal(hdr(s)['content-type'], 'application/json');
	assert.deepEqual(JSON.parse(String(s.init.body)), { name: 'ci', permission: 'write' }, 'the server field is permission');
});

test('a signed-in read on a flat route: the workspace header, no CSRF', async () => {
	answer(200, { runs: [] });
	await listRuns(cred, 'desc');
	assert.equal(hdr(seen[0]!)['x-durable-workspace'], 'ws-1');
	assert.equal(hdr(seen[0]!)['x-durable-csrf'], undefined);
});

test('compat: the pasted key as a bearer, exactly as before', async () => {
	answer(200, { runs: [] });
	await listRuns('k'.repeat(64), 'desc');
	assert.equal(hdr(seen[0]!)['authorization'], `Bearer ${'k'.repeat(64)}`);
	assert.equal(hdr(seen[0]!)['x-durable-csrf'], undefined);
});

test('204 is a value (undefined), not a JSON error', async () => {
	answer(204, null);
	assert.equal(await revokeKey(cred, 'ws-1', 'k1'), undefined);
});

test('a plan_limit 429 keeps its code, numbers and Retry-After', async () => {
	answer(429, { error: 'plan limit', code: 'plan_limit', limit: 'api_keys', plan: 'free', used: 10, cap: 10 }, { 'retry-after': '7' });
	const e = await createKey(cred, 'ws-1', 'x', 'read').then(() => null, (err: unknown) => err);
	assert.ok(e instanceof ApiError);
	assert.equal(e.status, 429);
	assert.equal(e.code, 'plan_limit');
	assert.deepEqual(e.extra, { limit: 'api_keys', plan: 'free', used: 10, cap: 10 });
	assert.equal(e.retryAfterMs, 7000);
});

test('a signed-in 401 asks the account to re-check the session; a refused login does not', async () => {
	let lost = 0;
	onSessionLost(() => lost++);
	answer(401, { error: 'no session', code: 'unauthenticated' });
	await listRuns(cred, 'desc').catch(() => {});
	assert.equal(lost, 1);
	answer(401, { error: 'invalid credentials', code: 'invalid_credentials' });
	const e = await login('a@b.c', 'wrong password!').then(() => null, (err: unknown) => err);
	assert.ok(e instanceof ApiError && e.code === 'invalid_credentials');
	assert.equal(lost, 1, 'a wrong password is not a lost session');
});

test('probeSession returns the status that decides the mode, with or without a body', async () => {
	answer(404, null);
	assert.deepEqual(await probeSession(), { status: 404, body: null, code: null });
	answer(503, { error: 'down', code: 'accounts_unavailable' });
	assert.equal((await probeSession()).code, 'accounts_unavailable');
});
