import assert from 'node:assert/strict';
import { test } from 'node:test';
import { effective, migrateStored, parseChoice, toggled } from '../src/lib/theme.ts';

test('the toggle flips the scheme and follows the device again when it lands on the device scheme', () => {
	// device light, following it: pressing stores dark; pressing again is light = the device → follow
	assert.equal(toggled('system', false), 'dark');
	assert.equal(toggled('dark', false), 'system');
	// device dark
	assert.equal(toggled('system', true), 'light');
	assert.equal(toggled('light', true), 'system');
	// a stored choice equal to the device (set in Settings) flips to the other scheme
	assert.equal(toggled('light', false), 'dark');
	assert.equal(toggled('dark', true), 'light');
});

test('the scheme in effect', () => {
	assert.equal(effective('system', true), 'dark');
	assert.equal(effective('system', false), 'light');
	assert.equal(effective('light', true), 'light');
	assert.equal(parseChoice('dark'), 'dark');
	assert.equal(parseChoice('blue'), 'system');
	assert.equal(parseChoice(null), 'system');
});

test('the old durable-theme key migrates once', () => {
	assert.equal(migrateStored('dark', null, false), 'dark'); // a real choice against the device: kept
	assert.equal(migrateStored('dark', null, true), null); // same as the device: follow it
	assert.equal(migrateStored('light', null, true), 'light');
	assert.equal(migrateStored('light', 'dark', true), 'dark'); // a new choice wins
	assert.equal(migrateStored(null, null, true), null);
	assert.equal(migrateStored('sepia', null, true), null);
});
