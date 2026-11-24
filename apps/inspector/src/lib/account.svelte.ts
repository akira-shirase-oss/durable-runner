// The signed-in account, reactive: which mode this server gave us (mode.ts), who is signed in, the CSRF token,
// the workspaces and the one this tab has selected. The selection lives in sessionStorage (D-062), so two tabs
// can look at two workspaces; requests to /workspaces/{pid}/… carry it in the path, flat routes in
// x-durable-workspace (http.ts). The session itself is the HttpOnly cookie: nothing here can read or keep it.
import { ApiError, listMyWorkspaces, login, logout, onSessionLost, probeSession, type SessionCred } from './api.ts';
import { modeFromStatus, type Mode } from './mode.ts';
import { can, listOf, normMe, normWorkspace, type Action, type Me, type Role, type WorkspaceRef } from './saas.ts';

const WS_KEY = 'durable-workspace';
const NEXT_KEY = 'durable-next';

function loadPid(): string | null {
	try {
		return sessionStorage.getItem(WS_KEY);
	} catch {
		return null;
	}
}

function savePid(pid: string | null) {
	try {
		if (pid) sessionStorage.setItem(WS_KEY, pid);
		else sessionStorage.removeItem(WS_KEY);
	} catch {
		/* storage blocked: the selection lasts for this page view */
	}
}

class Account {
	mode = $state<Mode>('booting');
	me = $state<Me | null>(null);
	csrf = $state('');
	workspaces = $state<WorkspaceRef[]>([]);
	pid = $state<string | null>(null);
	/** Why boot could not decide (network error, 5xx, accounts_unavailable). */
	bootError = $state<ApiError | null>(null);
	/** A one-line message for the sign-in page ("your session ended", "password changed, sign in again"). */
	notice = $state<string | null>(null);
	/** Bumped on every workspace switch: pages keyed on it start fresh, so one workspace's numbers never show
	 *  under another's name. */
	generation = $state(0);
	/** This tab saw a handler test call answer ok (onboarding counts the server's answer as a fact). */
	testOkSeen = $state(false);
	/** The address just signed up with, for the verify-sent page's Resend (memory only, never stored). */
	pendingEmail = $state('');

	#rechecking: Promise<void> | null = null;
	#lastRecheck = 0;

	get current(): WorkspaceRef | null {
		return this.workspaces.find((w) => w.public_id === this.pid) ?? null;
	}

	get role(): Role | null {
		return this.current?.role ?? null;
	}

	get verified(): boolean {
		return this.me?.verified === true;
	}

	/** Whether the selected workspace lets this account do `action` (always false while unverified, except read). */
	can(action: Action): boolean {
		if (action !== 'read' && !this.verified) return false;
		return can(this.role, action);
	}

	get cred(): SessionCred {
		return { kind: 'session', csrf: this.csrf, ws: this.pid };
	}

	/** Decide the mode: GET /auth/session. */
	async boot(): Promise<void> {
		onSessionLost(() => void this.recheck());
		this.bootError = null;
		try {
			const r = await probeSession();
			const m = modeFromStatus(r.status);
			if (m === 'signed-in') await this.#apply(r.body);
			else if (m === 'error') this.bootError = new ApiError(`GET /auth/session answered ${r.status}`, r.status, r.code);
			if (m !== 'signed-in') this.#clear();
			this.mode = m;
		} catch (e) {
			this.bootError = e instanceof ApiError ? e : new ApiError(String(e), 0);
			this.mode = 'error';
		}
	}

	/** Re-read the session (after a workspace was created or an invite accepted, or a request answered 401). */
	async refresh(): Promise<void> {
		const r = await probeSession();
		const m = modeFromStatus(r.status);
		if (m === 'signed-in') {
			await this.#apply(r.body);
			this.mode = 'signed-in';
		} else if (m === 'signed-out') {
			this.#clear();
			this.mode = 'signed-out';
		} else if (m === 'error') {
			throw new ApiError(`GET /auth/session answered ${r.status}`, r.status, r.code);
		}
	}

	/** A signed-in request answered 401: the session may have ended (expired, revoked elsewhere, a password
	 *  change). At most one check in flight and one per 10 s; only a 401 from /auth/session itself signs out —
	 *  a route that does not take sessions yet must not sign anybody out. */
	recheck(): Promise<void> {
		if (this.mode !== 'signed-in') return Promise.resolve();
		if (this.#rechecking) return this.#rechecking;
		if (Date.now() - this.#lastRecheck < 10_000) return Promise.resolve();
		this.#lastRecheck = Date.now();
		this.#rechecking = (async () => {
			try {
				const r = await probeSession();
				if (r.status === 401) {
					this.#clear();
					this.notice = 'Your session ended. Sign in again.';
					this.mode = 'signed-out';
				} else if (r.status === 200) await this.#apply(r.body);
			} catch {
				/* the server did not answer: the pages already show their own errors */
			} finally {
				this.#rechecking = null;
			}
		})();
		return this.#rechecking;
	}

	async signIn(email: string, password: string): Promise<void> {
		const r = await login(email, password);
		if (typeof r.csrf === 'string') this.csrf = r.csrf;
		this.notice = null;
		// the login answer has no workspace list: the session bootstrap does
		await this.refresh();
		// accepted, yet no session: the browser dropped the cookie (__Host- cookies need HTTPS)
		if (this.mode !== 'signed-in') {
			throw new ApiError('signed in, but this browser did not keep the session cookie (it is Secure: the page must be served over HTTPS)', 0, 'cookie_refused');
		}
	}

	/** Sign out on the server first: a cookie the server still honours must not look signed out here. A 401
	 *  means it already ended, which is the same outcome. */
	async signOut(): Promise<void> {
		try {
			await logout(this.cred);
		} catch (e) {
			if (!(e instanceof ApiError && e.status === 401)) throw e;
		}
		this.#clear();
		this.notice = 'Signed out.';
		this.mode = 'signed-out';
	}

	/** After a password change or reset the server minted a fresh session: its new CSRF token. */
	setCsrf(token: string | undefined) {
		if (token) this.csrf = token;
	}

	select(pid: string) {
		if (pid === this.pid) return;
		this.pid = pid;
		savePid(pid);
		this.generation++;
	}

	/** Remember where a signed-out visitor was going, to return there after sign-in. */
	rememberNext(hash: string | null) {
		try {
			if (hash) sessionStorage.setItem(NEXT_KEY, hash);
			else sessionStorage.removeItem(NEXT_KEY);
		} catch {
			/* best effort */
		}
	}

	takeNext(): string | null {
		try {
			const v = sessionStorage.getItem(NEXT_KEY);
			sessionStorage.removeItem(NEXT_KEY);
			return v;
		} catch {
			return null;
		}
	}

	/** The session bootstrap, then the workspace list: GET /workspaces is the authority (the bootstrap's own
	 *  `workspaces` may be empty on servers that fill it elsewhere); without that route, the bootstrap's list. */
	async #apply(body: unknown) {
		const b = (typeof body === 'object' && body !== null ? body : {}) as Record<string, unknown>;
		this.me = normMe(b.account);
		if (typeof b.csrf === 'string') this.csrf = b.csrf;
		let raw: unknown = b.workspaces;
		try {
			raw = await listMyWorkspaces(this.cred);
		} catch {
			/* an older server, or a moment's outage: the bootstrap's list stands */
		}
		this.workspaces = listOf(raw, 'workspaces')
			.map(normWorkspace)
			.filter((w): w is WorkspaceRef => w !== null);
		const keep = this.pid ?? loadPid();
		const pick = this.workspaces.find((w) => w.public_id === keep) ?? this.workspaces[0] ?? null;
		if ((pick?.public_id ?? null) !== this.pid) {
			this.pid = pick?.public_id ?? null;
			this.generation++;
		}
		savePid(this.pid);
	}

	#clear() {
		this.me = null;
		this.csrf = '';
		this.workspaces = [];
		this.pid = null;
		this.testOkSeen = false;
	}
}

export const account = new Account();
