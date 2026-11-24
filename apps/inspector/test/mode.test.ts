import assert from 'node:assert/strict';
import { test } from 'node:test';
import { hashParam, modeFromStatus, redirectFor, safeNext, segmentOf, tokenShapeOk } from '../src/lib/mode.ts';

test('GET /auth/session decides the mode: 200 signed in, 401 signed out, 404 compat, anything else an error', () => {
	assert.equal(modeFromStatus(200), 'signed-in');
	assert.equal(modeFromStatus(401), 'signed-out');
	assert.equal(modeFromStatus(404), 'compat');
	for (const s of [0, 403, 500, 502, 503, 204]) assert.equal(modeFromStatus(s), 'error', String(s));
});

test('route segments', () => {
	assert.equal(segmentOf('/'), '');
	assert.equal(segmentOf('/runs/[id]'), 'runs');
	assert.equal(segmentOf('/accept-invite'), 'accept-invite');
	assert.equal(segmentOf(null), null);
});

test('signed out: only the auth pages show; everything else goes to sign-in', () => {
	for (const r of ['/sign-in', '/sign-up', '/verify', '/verify-sent', '/forgot', '/reset', '/accept-invite']) assert.equal(redirectFor('signed-out', r), null, r);
	for (const r of ['/', '/overview', '/runs', '/runs/[id]', '/keys', '/settings', '/admin', '/submit']) assert.equal(redirectFor('signed-out', r), '#/sign-in', r);
	assert.equal(redirectFor('signed-out', null), null, 'not-found stays not-found');
});

test('signed in: the root, the compat-only pages and the sign-in pages go to the overview; mailed links stay', () => {
	for (const r of ['/', '/admin', '/submit', '/sign-in', '/sign-up', '/forgot', '/verify-sent']) assert.equal(redirectFor('signed-in', r), '#/overview', r);
	for (const r of ['/overview', '/runs', '/runs/[id]', '/workflows', '/workflows/[id]', '/handlers', '/keys', '/usage', '/team', '/settings', '/verify', '/reset', '/accept-invite'])
		assert.equal(redirectFor('signed-in', r), null, r);
});

test('compat: the key console as before; the signed-in pages and auth pages are not there', () => {
	for (const r of ['/', '/runs', '/runs/[id]', '/handlers', '/submit', '/admin', '/settings']) assert.equal(redirectFor('compat', r), null, r);
	for (const r of ['/overview', '/workflows', '/keys', '/usage', '/team', '/sign-in', '/verify', '/accept-invite']) assert.equal(redirectFor('compat', r), '#/', r);
});

test('booting and error never redirect', () => {
	assert.equal(redirectFor('booting', '/keys'), null);
	assert.equal(redirectFor('error', '/'), null);
});

test('safeNext: only in-app hash routes, never an auth page (except an invite) or an escape', () => {
	assert.equal(safeNext('#/runs/WBwXuaDUPWc'), '#/runs/WBwXuaDUPWc');
	assert.equal(safeNext('#/accept-invite?token=drt_x'), '#/accept-invite?token=drt_x');
	assert.equal(safeNext('#/sign-in'), null);
	assert.equal(safeNext('#/verify?token=drt_x'), null);
	assert.equal(safeNext('#//evil.example'), null);
	assert.equal(safeNext('https://evil.example/#/runs'), null);
	assert.equal(safeNext('#/runs"><script>'), null);
	assert.equal(safeNext('javascript:alert(1)'), null);
	assert.equal(safeNext(null), null);
	assert.equal(safeNext(`#/${'a'.repeat(600)}`), null);
});

test('hashParam reads the fragment query; tokenShapeOk checks the drt_ shape', () => {
	const tok = `drt_${'A'.repeat(43)}`;
	assert.equal(hashParam(`#/verify?token=${tok}`, 'token'), tok);
	assert.equal(hashParam('#/verify', 'token'), null);
	assert.equal(hashParam('#/verify?token=', 'token'), null);
	assert.equal(hashParam('#/reset?a=1&token=drt_b', 'token'), 'drt_b');
	assert.ok(tokenShapeOk(tok));
	assert.ok(!tokenShapeOk(`drt_${'A'.repeat(42)}`));
	assert.ok(!tokenShapeOk(`drs_${'A'.repeat(43)}`));
	assert.ok(!tokenShapeOk(`drt_${'A'.repeat(42)}=`));
	assert.ok(!tokenShapeOk(null));
});

test('invite tokens: the server mints dri_ + 64 hex; the drt_ mail shape is accepted too', async () => {
	const { inviteTokenShapeOk } = await import('../src/lib/mode.ts');
	assert.ok(inviteTokenShapeOk(`dri_${'0f'.repeat(32)}`));
	assert.ok(inviteTokenShapeOk(`drt_${'A'.repeat(43)}`));
	assert.ok(!inviteTokenShapeOk(`dri_${'0F'.repeat(32)}`), 'lower-case hex only');
	assert.ok(!inviteTokenShapeOk(`dri_${'0'.repeat(63)}`));
	assert.ok(!inviteTokenShapeOk(null));
});
