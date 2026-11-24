import assert from 'node:assert/strict';
import { test } from 'node:test';
import { fmtAgo, fmtApproxMs, fmtBytes, fmtClock, fmtClockMs, fmtCompact, fmtDateTime, fmtInt, fmtMs, fmtMsTick, fmtOffset, fmtPct, fmtRate } from '../src/lib/format.ts';

test('counts: full under 10,000, compact above, never "10.0K"', () => {
	assert.equal(fmtInt(1204318), '1,204,318');
	assert.equal(fmtInt(null), '—');
	assert.equal(fmtInt(Number.NaN), '—');
	assert.equal(fmtCompact(9999), '9,999');
	assert.equal(fmtCompact(10_000), '10K');
	assert.equal(fmtCompact(12_940), '12.9K');
	assert.equal(fmtCompact(184_000), '184K');
	assert.equal(fmtCompact(3_412_000), '3.4M');
	assert.equal(fmtCompact(2_000_000_000), '2B');
	assert.equal(fmtCompact(undefined), '—');
});

test('durations pick a unit that reads at a glance', () => {
	assert.equal(fmtMs(0), '0 ms');
	assert.equal(fmtMs(0.4), '<1 ms');
	assert.equal(fmtMs(120), '120 ms');
	assert.equal(fmtMs(999.6), '1000 ms');
	assert.equal(fmtMs(1200), '1.2 s');
	assert.equal(fmtMs(1250), '1.25 s');
	assert.equal(fmtMs(12_400), '12.4 s');
	assert.equal(fmtMs(125_000), '2 min 05 s');
	assert.equal(fmtMs(3 * 3_600_000 + 12 * 60_000), '3 h 12 min');
	assert.equal(fmtMs(52 * 3_600_000), '2 d 4 h');
	assert.equal(fmtMs(null), '—');
	assert.equal(fmtMs(-500), '−500 ms');
	assert.equal(fmtOffset(1200), '+1.2 s');
	assert.equal(fmtOffset(-5), '+0 ms');
});

test('latency axis ticks are short', () => {
	assert.equal(fmtMsTick(0), '0');
	assert.equal(fmtMsTick(2.5), '2.5ms');
	assert.equal(fmtMsTick(250), '250ms');
	assert.equal(fmtMsTick(1500), '1.5s');
	assert.equal(fmtMsTick(20_000), '20s');
	assert.equal(fmtMsTick(120_000), '2m');
	assert.equal(fmtMsTick(70_000), '1m 10s'); // a Gantt offset between whole minutes
	assert.equal(fmtMsTick(3_600_000), '1h');
});

test('percentages never round a real failure away', () => {
	assert.equal(fmtPct(0.9823), '98.2%');
	assert.equal(fmtPct(1), '100%');
	assert.equal(fmtPct(0), '0%');
	assert.equal(fmtPct(0.0004), '<0.1%');
	assert.equal(fmtPct(0.9996), '>99.9%');
	assert.equal(fmtPct(0.5), '50%');
	assert.equal(fmtPct(null), '—');
	assert.equal(fmtPct(0.12345, 2), '12.35%');
});

test('rates per minute', () => {
	assert.equal(fmtRate(0), '0/min');
	assert.equal(fmtRate(0.4), '0.4/min');
	assert.equal(fmtRate(12.44), '12.4/min');
	assert.equal(fmtRate(40), '40/min');
	assert.equal(fmtRate(1240), '1,240/min');
	assert.equal(fmtRate(25_000), '25K/min');
});

test('ago: the live indicator and feed wording', () => {
	assert.equal(fmtAgo(400), 'just now');
	assert.equal(fmtAgo(3_200), '3 s ago');
	assert.equal(fmtAgo(59_999), '59 s ago');
	assert.equal(fmtAgo(4 * 60_000 + 5_000), '4 min ago');
	assert.equal(fmtAgo(2 * 3_600_000), '2 h ago');
	assert.equal(fmtAgo(72 * 3_600_000), '3 d ago');
	assert.equal(fmtAgo(Number.NaN), 'just now');
});

test('clock times are local, 24-hour, zero-padded', () => {
	const t = new Date(2026, 10, 24, 9, 5, 7, 50).getTime();
	assert.equal(fmtClock(t), '09:05:07');
	assert.equal(fmtClock(t, false), '09:05');
	assert.equal(fmtClockMs(t), '09:05:07.050');
	assert.equal(fmtDateTime(t, t), 'Nov 24, 09:05:07');
	assert.equal(fmtDateTime(t, new Date(2027, 0, 2).getTime()), 'Nov 24 2026, 09:05:07');
});

test('bytes in binary units', () => {
	assert.equal(fmtBytes(512), '512 B');
	assert.equal(fmtBytes(2_846_720), '2.71 MiB');
	assert.equal(fmtBytes(1536), '1.5 KiB');
	assert.equal(fmtBytes(null), '—');
});

test('latency percentiles read as approximate', () => {
	assert.equal(fmtApproxMs(1), '≈1 ms');
	assert.equal(fmtApproxMs(1200), '≈1.2 s');
	assert.equal(fmtApproxMs(null), '—');
});
