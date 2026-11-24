// Which dashboard this server gets, decided once at boot by GET /auth/session, and where a route may not be shown
// in that mode. Pure: the account store (account.svelte.ts) asks and the shell (+layout.svelte) redirects.
//
//   200 → signed-in: the SaaS dashboard, authenticated by the session cookie (+ x-durable-csrf on mutations)
//   401 → signed-out: the sign-in / sign-up pages, no sidebar
//   404 → compat: no account store on this server, the key-paste console exactly as before (self-hosters)
//   anything else (a network error, 5xx, accounts_unavailable) → an error with a retry; never a guessed mode

export type Mode = 'booting' | 'signed-in' | 'signed-out' | 'compat' | 'error';

export function modeFromStatus(status: number): Exclude<Mode, 'booting'> {
	if (status === 200) return 'signed-in';
	if (status === 401) return 'signed-out';
	if (status === 404) return 'compat';
	return 'error';
}

/** The signed-out pages (no sidebar). */
export const AUTH_SECTIONS = ['sign-in', 'sign-up', 'verify', 'verify-sent', 'forgot', 'reset', 'accept-invite'] as const;
export type AuthSection = (typeof AUTH_SECTIONS)[number];

/** Auth pages that still make sense with a session: a mailed link can be opened in a signed-in tab. */
const AUTH_ANY_MODE = new Set<string>(['verify', 'reset', 'accept-invite']);
/** Pages only the signed-in dashboard has. */
const SAAS_ONLY = new Set<string>(['overview', 'workflows', 'keys', 'usage', 'team']);
/** Pages only the compat console has: the key-paste Submit and the operator console (ops keys never work on the
 *  public origin in signed-in mode). */
const COMPAT_ONLY = new Set<string>(['submit', 'admin']);

/** The first segment of a SvelteKit route id ("/runs/[id]" → "runs", "/" → ""); null for a route that matched
 *  nothing. */
export function segmentOf(routeId: string | null | undefined): string | null {
	if (routeId === null || routeId === undefined) return null;
	return routeId.split('/').filter(Boolean)[0] ?? '';
}

export function isAuthSection(seg: string | null): seg is AuthSection {
	return seg !== null && (AUTH_SECTIONS as readonly string[]).includes(seg);
}

/** Where the shell must send a route in this mode, or null to show it. Unknown routes (null) are left to the
 *  not-found page. */
export function redirectFor(mode: Mode, routeId: string | null | undefined): string | null {
	const seg = segmentOf(routeId);
	if (seg === null || mode === 'booting' || mode === 'error') return null;
	if (mode === 'compat') {
		if (isAuthSection(seg) || SAAS_ONLY.has(seg)) return '#/';
		return null;
	}
	if (mode === 'signed-out') return isAuthSection(seg) ? null : '#/sign-in';
	// signed-in
	if (seg === '' || COMPAT_ONLY.has(seg)) return '#/overview';
	if (isAuthSection(seg) && !AUTH_ANY_MODE.has(seg)) return '#/overview';
	return null;
}

/** A route the shell may return to after sign-in: an in-app hash route, never an auth page or anything that
 *  could leave the app. Anything else is null (then the overview). */
export function safeNext(v: string | null | undefined): string | null {
	if (typeof v !== 'string' || v.length > 512) return null;
	if (!/^#\/[A-Za-z0-9/_\-?=&%.~:]*$/.test(v) || v.startsWith('#//')) return null;
	const seg = v.slice(2).split(/[/?]/)[0] ?? '';
	if (isAuthSection(seg) && seg !== 'accept-invite') return null;
	return v;
}

/** The value of `name` in the fragment's query (`#/verify?token=drt_…`): tokens travel in the fragment so mail
 *  prefetchers never see them. Null when absent or empty. */
export function hashParam(hash: string, name: string): string | null {
	const q = hash.indexOf('?');
	if (q < 0) return null;
	const v = new URLSearchParams(hash.slice(q + 1)).get(name);
	return v === null || v === '' ? null : v;
}

/** A mailed token's shape (`drt_` + 43 base64url characters). The server decides; this only stops a mangled
 *  paste from being posted. */
export function tokenShapeOk(t: string | null): t is string {
	return t !== null && /^drt_[A-Za-z0-9_-]{43}$/.test(t);
}
