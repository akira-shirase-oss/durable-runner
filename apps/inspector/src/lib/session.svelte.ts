// The tab's session: the two keys (sessionStorage, through api.ts exactly as before: D-053), the time window
// the monitoring pages show, which observability routes this server serves, and one-line notices. Pages ask it
// for credentials (`reader`, `writer`) and whether a change is allowed (`canWrite`): in compat mode those are the
// pasted keys; signed in, they are the session (account.svelte.ts) and the role in the selected workspace.
import { forgetKeys, loadKey, saveKey, type Cred, type KeyKind, type Window } from './api.ts';
import { account } from './account.svelte.ts';
import { parseWindow } from './series.ts';

const WINDOW_KEY = 'durable-window';

class Session {
	readKey = $state('');
	apiKey = $state('');
	/** Bumped on every key change: polled resources that were refused for want of a key try again. */
	keysVersion = $state(0);
	window = $state<Window>('1h');
	/** Whether this server serves GET /metrics (null until a page asked): the time window is hidden without it. */
	metricsSupported = $state<boolean | null>(null);
	/** A success notice for the page ("run 10 cancelled"); pages show it in a banner. */
	notice = $state<string | null>(null);

	init() {
		this.readKey = loadKey('read');
		this.apiKey = loadKey('api');
		try {
			this.window = parseWindow(localStorage.getItem(WINDOW_KEY)) ?? '1h';
		} catch {
			/* storage blocked: the default window */
		}
	}

	/** The key for reads: the read key, else the API key (which may also read). */
	get readerKey(): string {
		return this.readKey || this.apiKey;
	}

	/** Signed in to the SaaS dashboard (else the compat key console). */
	get signedIn(): boolean {
		return account.mode === 'signed-in';
	}

	/** Credentials for reads. */
	get reader(): Cred {
		return this.signedIn ? account.cred : this.readerKey;
	}

	/** Credentials for changes (submit, cancel, the spec). */
	get writer(): Cred {
		return this.signedIn ? account.cred : this.apiKey;
	}

	/** Whether changes are possible: the API key is pasted, or the role is member or above (and verified). */
	get canWrite(): boolean {
		return this.signedIn ? account.can('write') : !!this.apiKey;
	}

	/** What a disabled change control says it needs. */
	get writeNeed(): string {
		if (!this.signedIn) return 'needs the API key';
		return account.verified ? 'needs the member role' : 'needs a verified email';
	}

	setKey(kind: KeyKind, value: string) {
		const v = value.trim();
		saveKey(kind, v);
		if (kind === 'read') this.readKey = v;
		else this.apiKey = v;
		this.keysVersion++;
	}

	forget() {
		forgetKeys();
		this.readKey = '';
		this.apiKey = '';
		this.keysVersion++;
		this.notice = 'Keys forgotten: nothing is held for this tab';
	}

	setWindow(w: Window) {
		this.window = w;
		try {
			localStorage.setItem(WINDOW_KEY, w);
		} catch {
			/* a per-viewer convenience only */
		}
	}
}

export const session = new Session();
