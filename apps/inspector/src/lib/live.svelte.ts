// Live data: every polled resource of the current page, refreshed every 5 s while the tab is visible and the
// viewer has not paused. Each resource schedules its next request only after the last one answered (so one is
// ever in flight), backs off on errors (5 → 10 → 20 → 40 → 60 s), waits at least as long as a 429's Retry-After,
// and stops asking when the answer cannot change by retrying: a 404 without an error envelope (an older server
// without the route) or a missing key (a new key restarts it).
import { untrack } from 'svelte';
import { session } from './session.svelte.ts';
import { ApiError } from './api.ts';
import { nextDelay, POLL_MS } from './poll.ts';

export type ResourceStatus = 'idle' | 'loading' | 'ok' | 'error' | 'unsupported' | 'needs-key' | 'gone';

class LiveController {
	/** The viewer pressed pause. */
	paused = $state(false);
	/** The tab is visible (Page Visibility API). Hidden tabs never poll. */
	visible = $state(true);
	/** A clock for "updated 3 s ago": ticks each second while visible. */
	now = $state(Date.now());
	/** The newest successful answer of any live resource. */
	lastUpdate = $state<number | null>(null);
	/** Live resources whose last request failed (not counting unsupported routes or missing keys). */
	failing = $state(0);
	/** When the slowest failing resource retries next (ms since epoch), for the indicator. */
	retryAt = $state<number | null>(null);

	#resources = new Set<Resource<unknown>>();
	#tick: ReturnType<typeof setInterval> | null = null;
	#installed = false;

	install(): () => void {
		if (this.#installed) return () => {};
		this.#installed = true;
		this.visible = document.visibilityState !== 'hidden';
		const onVisibility = () => {
			this.visible = document.visibilityState !== 'hidden';
			if (this.visible) {
				this.now = Date.now();
				this.#startTick();
				for (const r of this.#resources) r.wake();
			} else {
				this.#stopTick();
				for (const r of this.#resources) r.sleep();
			}
		};
		document.addEventListener('visibilitychange', onVisibility);
		if (this.visible) this.#startTick();
		return () => {
			document.removeEventListener('visibilitychange', onVisibility);
			this.#stopTick();
			this.#installed = false;
		};
	}

	setPaused(p: boolean) {
		this.paused = p;
		for (const r of this.#resources) (p ? r.sleep() : r.wake());
	}

	register(r: Resource<unknown>) {
		this.#resources.add(r);
	}

	unregister(r: Resource<unknown>) {
		this.#resources.delete(r);
		this.recount();
	}

	noteUpdate(at: number) {
		if (this.lastUpdate === null || at > this.lastUpdate) this.lastUpdate = at;
	}

	recount() {
		let n = 0;
		let wait: number | null = null;
		for (const r of this.#resources) {
			if (r.live && untrack(() => r.status) === 'error') {
				n++;
				const w = r.retryAt;
				if (w !== null) wait = Math.max(wait ?? 0, w);
			}
		}
		this.failing = n;
		this.retryAt = wait;
	}

	#startTick() {
		if (this.#tick) return;
		this.#tick = setInterval(() => (this.now = Date.now()), 1000);
	}

	#stopTick() {
		if (this.#tick) clearInterval(this.#tick);
		this.#tick = null;
	}
}

export const live = new LiveController();

export interface ResourceOptions {
	/** Poll interval, or a function read before each scheduling (a finished run stops polling); 0 fetches on
	 *  demand only. Default 5 s. */
	interval?: number | (() => number);
	/** Counts toward the live indicator (default true). */
	live?: boolean;
	/** A 404 `not_found` means the thing is not there (a run id from another workspace, an expired run): stop
	 *  asking and do not count it as failing. The page shows its own not-found state. */
	goneOn404?: boolean;
}

/** One polled read. Pages hand it to `poll()`, which starts it with the component and refetches (abandoning a
 *  request in flight for the old parameters) whenever `deps` or the keys change. */
export class Resource<T> {
	data = $state.raw<T | null>(null);
	error = $state.raw<ApiError | null>(null);
	status = $state<ResourceStatus>('idle');
	updatedAt = $state<number | null>(null);
	readonly live: boolean;
	readonly #goneOn404: boolean;
	/** When the next retry is due while failing (ms since epoch). */
	retryAt: number | null = null;

	#load: (signal: AbortSignal) => Promise<T>;
	#interval: () => number;
	#timer: ReturnType<typeof setTimeout> | null = null;
	#abort: AbortController | null = null;
	#errors = 0;
	#retryAfter: number | null = null;
	#active = false;
	#gen = 0;

	constructor(load: (signal: AbortSignal) => Promise<T>, opts: ResourceOptions = {}) {
		this.#load = load;
		const iv = opts.interval ?? POLL_MS;
		this.#interval = typeof iv === 'function' ? iv : () => iv;
		this.live = opts.live ?? true;
		this.#goneOn404 = opts.goneOn404 ?? false;
	}

	/** First load still pending (no data yet). */
	get loading(): boolean {
		return this.data === null && (this.status === 'loading' || this.status === 'idle');
	}

	/** Register with the live controller; the first request is the caller's `refresh()` (see `poll`). */
	start() {
		if (this.#active) return;
		this.#active = true;
		live.register(this as Resource<unknown>);
	}

	stop() {
		this.#active = false;
		this.#clear();
		this.#abort?.abort();
		this.#abort = null;
		this.#gen++;
		live.unregister(this as Resource<unknown>);
	}

	/** Fetch now, unless a request is already in flight; `force` abandons that request first (its parameters
	 *  changed). Paused or hidden, a forced refresh still runs once: the viewer asked for it. */
	async refresh(force = false): Promise<void> {
		if (!this.#active) return;
		if (this.#abort && !force) return;
		this.#abort?.abort();
		this.#clear();
		const ac = new AbortController();
		this.#abort = ac;
		const gen = ++this.#gen;
		if (untrack(() => this.data) === null) this.status = 'loading';
		try {
			const v = await this.#load(ac.signal);
			if (gen !== this.#gen) return;
			this.data = v;
			this.error = null;
			this.status = 'ok';
			this.updatedAt = Date.now();
			this.#errors = 0;
			this.#retryAfter = null;
			this.retryAt = null;
			if (this.live) live.noteUpdate(this.updatedAt);
		} catch (e) {
			if (gen !== this.#gen || ac.signal.aborted) return;
			const err = e instanceof ApiError ? e : new ApiError(e instanceof Error ? e.message : String(e), 0);
			this.error = err;
			if (err.unsupported) this.status = 'unsupported';
			else if (this.#goneOn404 && err.status === 404 && err.code === 'not_found') this.status = 'gone';
			else if (err.needsKey) this.status = 'needs-key';
			else {
				this.status = 'error';
				this.#errors++;
				this.#retryAfter = err.retryAfterMs;
			}
		} finally {
			if (gen === this.#gen) {
				this.#abort = null;
				this.#schedule();
				live.recount();
			}
		}
	}

	/** Paused or hidden: no new request (one in flight finishes). */
	sleep() {
		this.#clear();
		this.retryAt = null;
	}

	/** Visible and unpaused again: fetch now. */
	wake() {
		if (!this.#active || this.#abort) return;
		const s = untrack(() => this.status);
		if (s === 'unsupported' || s === 'needs-key' || s === 'gone') return;
		void this.refresh();
	}

	#schedule() {
		this.#clear();
		this.retryAt = null;
		const base = untrack(() => this.#interval());
		if (!this.#active || base <= 0 || live.paused || !live.visible) return;
		const s = untrack(() => this.status);
		if (s === 'unsupported' || s === 'needs-key' || s === 'gone') return;
		const delay = nextDelay(base, this.#errors, this.#retryAfter);
		if (s === 'error') this.retryAt = Date.now() + delay;
		this.#timer = setTimeout(() => void this.refresh(), delay);
	}

	#clear() {
		if (this.#timer) clearTimeout(this.#timer);
		this.#timer = null;
	}
}

/** Run `res` for the life of the calling component: start and stop with it, and refresh at once whenever the
 *  values `deps` reads change (the time window, a cursor, a run id) or a key is saved or forgotten. Call during
 *  component initialisation. */
export function poll<T>(res: Resource<T>, deps: () => unknown = () => null) {
	$effect(() => {
		res.start();
		return () => res.stop();
	});
	$effect(() => {
		deps();
		void session.keysVersion;
		untrack(() => void res.refresh(true));
	});
}
