import assert from 'node:assert/strict';
import { test } from 'node:test';
import { backoffDelay, MAX_BACKOFF_MS, nextDelay, parseRetryAfter, POLL_MS } from '../src/lib/poll.ts';

test('errors double the wait, capped, never below the base', () => {
	assert.equal(backoffDelay(POLL_MS, 0), 5_000);
	assert.equal(backoffDelay(POLL_MS, 1), 10_000);
	assert.equal(backoffDelay(POLL_MS, 2), 20_000);
	assert.equal(backoffDelay(POLL_MS, 3), 40_000);
	assert.equal(backoffDelay(POLL_MS, 4), MAX_BACKOFF_MS);
	assert.equal(backoffDelay(POLL_MS, 1000), MAX_BACKOFF_MS);
	assert.equal(backoffDelay(POLL_MS, -3), 5_000);
	assert.equal(backoffDelay(POLL_MS, Number.NaN), 5_000);
});

test('Retry-After: delta-seconds or an HTTP date; junk is ignored', () => {
	const now = Date.UTC(2026, 10, 24, 15, 0, 0);
	assert.equal(parseRetryAfter('7', now), 7_000);
	assert.equal(parseRetryAfter(' 1 ', now), 1_000);
	assert.equal(parseRetryAfter('0', now), 0);
	assert.equal(parseRetryAfter(new Date(now + 30_000).toUTCString(), now), 30_000);
	assert.equal(parseRetryAfter(new Date(now - 30_000).toUTCString(), now), 0);
	assert.equal(parseRetryAfter('soon', now), null);
	assert.equal(parseRetryAfter('-5', now), null);
	assert.equal(parseRetryAfter('', now), null);
	assert.equal(parseRetryAfter(null, now), null);
	assert.equal(parseRetryAfter('99999999', now), 86_400_000); // a day at most
});

test('the next poll honours the server over the backoff', () => {
	assert.equal(nextDelay(POLL_MS, 0, null), 5_000);
	assert.equal(nextDelay(POLL_MS, 1, 30_000), 30_000); // 429 asked for longer
	assert.equal(nextDelay(POLL_MS, 3, 1_000), 40_000); // the streak is longer than the ask
});
