// Number, duration and time formatting for the dashboard. Pure: every function takes its inputs (including "now")
// as arguments, so the tests pin them. Counts use en-US grouping everywhere so a column reads the same on every
// machine; clock times are the viewer's local time.

const INT = new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 });

/** A count in full: 1,204,318. Non-finite → "—". */
export function fmtInt(n: number | null | undefined): string {
	if (n === null || n === undefined || !Number.isFinite(n)) return '—';
	return INT.format(Math.round(n));
}

/** A count for a tile or an axis: in full under 10,000, compact above (12.9K, 3.4M, 1.2B); never "10.0K". */
export function fmtCompact(n: number | null | undefined): string {
	if (n === null || n === undefined || !Number.isFinite(n)) return '—';
	const a = Math.abs(n);
	if (a < 10_000) return fmtInt(n);
	const [div, unit] = a >= 1e9 ? [1e9, 'B'] : a >= 1e6 ? [1e6, 'M'] : [1e3, 'K'];
	const v = n / div;
	const digits = Math.abs(v) >= 100 ? 0 : 1;
	const s = v.toFixed(digits).replace(/\.0$/, '');
	return `${s}${unit}`;
}

/** A duration: "<1 ms", "120 ms", "1.25 s", "12.4 s", "2 min 05 s", "3 h 12 min", "2 d 4 h". null → "—". */
export function fmtMs(ms: number | null | undefined): string {
	if (ms === null || ms === undefined || !Number.isFinite(ms)) return '—';
	if (ms < 0) return `−${fmtMs(-ms)}`;
	if (ms < 1) return ms === 0 ? '0 ms' : '<1 ms';
	if (ms < 1000) return `${Math.round(ms)} ms`;
	if (ms < 10_000) return `${trim((ms / 1000).toFixed(2))} s`;
	if (ms < 60_000) return `${trim((ms / 1000).toFixed(1))} s`;
	const s = Math.round(ms / 1000);
	if (s < 3600) return `${Math.floor(s / 60)} min ${pad2(s % 60)} s`;
	const m = Math.round(s / 60);
	if (m < 24 * 60) return `${Math.floor(m / 60)} h ${pad2(m % 60)} min`;
	const h = Math.round(m / 60);
	return `${Math.floor(h / 24)} d ${h % 24} h`;
}

/** A latency for an axis tick: shorter than fmtMs ("250ms", "1.5s", "2m"). */
export function fmtMsTick(ms: number): string {
	if (ms === 0) return '0';
	if (ms < 1000) return `${trim(ms.toFixed(ms < 10 ? 1 : 0))}ms`;
	if (ms < 60_000) return `${trim((ms / 1000).toFixed(ms < 10_000 ? 1 : 0))}s`;
	if (ms < 3_600_000) return `${trim((ms / 60_000).toFixed(1))}m`;
	return `${trim((ms / 3_600_000).toFixed(1))}h`;
}

/** A ratio as a percentage: 0.9823 → "98.2%". Tiny but non-zero never rounds to 0, near-one never rounds to 100. */
export function fmtPct(ratio: number | null | undefined, digits = 1): string {
	if (ratio === null || ratio === undefined || !Number.isFinite(ratio)) return '—';
	if (ratio <= 0) return '0%';
	if (ratio >= 1) return '100%';
	const floor = 10 ** -digits;
	const pct = ratio * 100;
	if (pct < floor) return `<${floor.toFixed(digits)}%`;
	if (pct > 100 - floor) return `>${(100 - floor).toFixed(digits)}%`;
	return `${trim(pct.toFixed(digits))}%`;
}

/** A rate per minute: "0", "0.4/min", "12.4/min", "1,240/min". */
export function fmtRate(perMin: number | null | undefined): string {
	if (perMin === null || perMin === undefined || !Number.isFinite(perMin)) return '—';
	if (perMin === 0) return '0/min';
	if (perMin < 100) return `${trim(perMin.toFixed(1))}/min`;
	return `${fmtCompact(perMin)}/min`;
}

/** How long ago, for the live indicator and the activity feed: "just now", "3 s ago", "4 min ago", "2 h ago". */
export function fmtAgo(deltaMs: number): string {
	if (!Number.isFinite(deltaMs) || deltaMs < 1000) return 'just now';
	const s = Math.floor(deltaMs / 1000);
	if (s < 60) return `${s} s ago`;
	const m = Math.floor(s / 60);
	if (m < 60) return `${m} min ago`;
	const h = Math.floor(m / 60);
	if (h < 48) return `${h} h ago`;
	return `${Math.floor(h / 24)} d ago`;
}

/** Local wall-clock time, 24-hour: "15:42" or "15:42:07". */
export function fmtClock(t: number, seconds = true): string {
	const d = new Date(t);
	const hm = `${pad2(d.getHours())}:${pad2(d.getMinutes())}`;
	return seconds ? `${hm}:${pad2(d.getSeconds())}` : hm;
}

/** Local wall-clock time with milliseconds, for a run's timeline: "15:42:07.050". */
export function fmtClockMs(t: number): string {
	return `${fmtClock(t)}.${String(new Date(t).getMilliseconds()).padStart(3, '0')}`;
}

const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

/** A local date and time: "Nov 24, 15:42:07"; the year is added when it is not `now`'s year. */
export function fmtDateTime(t: number, now: number): string {
	const d = new Date(t);
	const y = d.getFullYear() === new Date(now).getFullYear() ? '' : ` ${d.getFullYear()}`;
	return `${MONTHS[d.getMonth()]} ${d.getDate()}${y}, ${fmtClock(t)}`;
}

/** An offset from a run's first event: "+0 ms", "+1.20 s", "+2 min 05 s". */
export function fmtOffset(ms: number): string {
	return `+${fmtMs(Math.max(0, ms))}`;
}

/** Bytes in binary units: "512 B", "2.7 MiB". */
export function fmtBytes(n: number | null | undefined): string {
	if (n === null || n === undefined || !Number.isFinite(n)) return '—';
	if (n < 1024) return `${fmtInt(n)} B`;
	const units = ['KiB', 'MiB', 'GiB', 'TiB'];
	let v = n / 1024;
	let i = 0;
	while (v >= 1024 && i < units.length - 1) {
		v /= 1024;
		i++;
	}
	return `${trim(v.toFixed(v < 10 ? 2 : 1))} ${units[i]}`;
}

function pad2(n: number): string {
	return String(n).padStart(2, '0');
}

function trim(s: string): string {
	return s.includes('.') ? s.replace(/\.?0+$/, '') : s;
}
