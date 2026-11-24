import assert from 'node:assert/strict';
import { test } from 'node:test';
import { share, stateView } from '../src/lib/state.ts';

test('every state the server reports maps to its chip, label and detail', () => {
	assert.deepEqual(stateView('queued'), { kind: 'queued', label: 'Queued', detail: '' });
	assert.deepEqual(stateView('running:step:1:attempt:2'), { kind: 'running', label: 'Running', detail: 'step 1 · attempt 2' });
	assert.deepEqual(stateView('retry_waiting:step:0:attempt:3'), {
		kind: 'retry_waiting',
		label: 'Retry waiting',
		detail: 'step 0 · attempt 3',
	});
	assert.deepEqual(stateView('reconciling:step:4'), { kind: 'reconciling', label: 'Reconciling', detail: 'step 4' });
	assert.deepEqual(stateView('done'), { kind: 'done', label: 'Done', detail: '' });
	assert.deepEqual(stateView('failed:step:2'), { kind: 'failed', label: 'Failed', detail: 'step 2' });
	assert.deepEqual(stateView('cancelled'), { kind: 'cancelled', label: 'Cancelled', detail: '' });
});

test('an unknown kind or an unexpected tail is shown as sent, never dropped', () => {
	assert.deepEqual(stateView('paused'), { kind: 'paused', label: 'paused', detail: '' });
	assert.deepEqual(stateView('running:step:1:attempt'), { kind: 'running', label: 'Running', detail: 'step:1:attempt' });
	assert.deepEqual(stateView('running::'), { kind: 'running', label: 'Running', detail: ':' });
	assert.deepEqual(stateView(''), { kind: '', label: '', detail: '' });
});

test('diagnostics bars: proportional, visible when non-zero, never NaN', () => {
	assert.equal(share(50, 100), 50);
	assert.equal(share(100, 100), 100);
	assert.equal(share(1, 10_000), 4); // tiny but non-zero stays visible
	assert.equal(share(0, 100), 0);
	assert.equal(share(0, 0), 0); // every state empty
	assert.equal(share(5, 0), 0);
	assert.equal(share(200, 100), 100); // clamped
});
