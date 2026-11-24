import assert from 'node:assert/strict';
import { test } from 'node:test';
import { NAV, parseRunId, runHref, sectionOf } from '../src/lib/routes.ts';

test('route ids map to their nav section', () => {
	assert.equal(sectionOf('/'), 'overview');
	assert.equal(sectionOf('/runs'), 'runs');
	assert.equal(sectionOf('/runs/[id]'), 'runs');
	assert.equal(sectionOf('/handlers'), 'handlers');
	assert.equal(sectionOf('/submit'), 'submit');
	assert.equal(sectionOf('/admin'), 'admin');
	assert.equal(sectionOf('/settings'), 'settings');
	assert.equal(sectionOf(null), 'overview');
	assert.equal(sectionOf('/nowhere'), 'overview');
});

test('every nav link is a hash route, and only overview + handlers are monitoring pages', () => {
	for (const n of NAV) assert.match(n.href, /^#\//);
	assert.deepEqual(NAV.filter((n) => n.monitoring).map((n) => n.section), ['overview', 'handlers']);
});

test('run ids: digits only, safe integers, never NaN', () => {
	assert.equal(parseRunId('10'), 10);
	assert.equal(parseRunId(' 10418 '), 10418);
	assert.equal(parseRunId('#42'), 42);
	assert.equal(parseRunId('0'), 0);
	assert.equal(parseRunId('-1'), null);
	assert.equal(parseRunId('1e3'), null);
	assert.equal(parseRunId('10abc'), null);
	assert.equal(parseRunId(''), null);
	assert.equal(parseRunId(undefined), null);
	assert.equal(parseRunId('99999999999999999'), null); // past MAX_SAFE_INTEGER
	assert.equal(runHref(10), '#/runs/10');
});
