// Form checks before a request is sent, pure. They mirror the server's rules so the page can say what is wrong
// next to the field; the server stays the authority (its refusal is shown when the two ever disagree).
// Each returns the message for the field, or null when the value may be sent.

/** The server's normalisation: trimmed, lower-cased, at most 254 characters, exactly one "@" (with something on
 *  both sides here, so a typo is caught before the round trip). */
export function emailError(v: string): string | null {
	const e = v.trim();
	if (e === '') return 'Enter your email address';
	if (e.length > 254) return 'An email address is at most 254 characters';
	const at = e.indexOf('@');
	if (at < 1 || at !== e.lastIndexOf('@') || at === e.length - 1 || /\s/.test(e)) return 'Enter an email address like name@example.com';
	return null;
}

export const PASSWORD_MIN = 12;
export const PASSWORD_MAX = 128;

/** 12–128 characters (counted as the server counts: Unicode scalar values), no composition rules (NIST). The
 *  server also refuses a small common-password list; that refusal comes back as its own message. */
export function passwordError(v: string): string | null {
	const n = [...v].length;
	if (n === 0) return 'Enter a password';
	if (n < PASSWORD_MIN) return `Use at least ${PASSWORD_MIN} characters (${n} so far)`;
	if (n > PASSWORD_MAX) return `Use at most ${PASSWORD_MAX} characters`;
	return null;
}

export function confirmError(password: string, again: string): string | null {
	if (again === '') return 'Type the password again';
	return again === password ? null : 'The two passwords differ';
}

/** Workspace, key, workflow and handler names: 1–128 characters, no control characters (the server's rule for
 *  handler names, applied to every name the dashboard sends). */
export function nameError(v: string, what = 'name'): string | null {
	const n = v.trim();
	if (n === '') return `Enter a ${what}`;
	if ([...n].length > 128) return `A ${what} is at most 128 characters`;
	// eslint-disable-next-line no-control-regex
	if (/[\u0000-\u001f\u007f]/.test(n)) return `A ${what} cannot contain control characters`;
	return null;
}

/** A handler URL: absolute http(s) (the server also refuses private and local targets — that refusal comes back
 *  from it). `builtin:echo`, the in-process sample handler, is the one non-HTTP target. */
export function handlerUrlError(v: string): string | null {
	const u = v.trim();
	if (u === '') return 'Enter the URL the runner should call';
	if (u === 'builtin:echo') return null;
	let url: URL;
	try {
		url = new URL(u);
	} catch {
		return 'Enter an absolute URL, like https://example.com/step';
	}
	if (url.protocol !== 'https:' && url.protocol !== 'http:') return 'The URL must start with https:// (or http://)';
	if (url.username || url.password) return 'The URL cannot carry a user name or password';
	if (u.length > 2048) return 'The URL is longer than 2048 characters';
	return null;
}

/** The phrase the delete dialog asks for (the server checks the same `{confirm}`). */
export function deletePhrase(name: string): string {
	return `delete ${name}`;
}

/** JSON input for a job or test call, as the base64 the API carries. Empty input is the empty string. Returns
 *  the error for the field instead when it is not JSON (inputs are JSON-encoded on the wire). */
export function inputToB64(text: string): { b64: string; error: null } | { b64: null; error: string } {
	const t = text.trim();
	if (t === '') return { b64: '', error: null };
	try {
		JSON.parse(t);
	} catch {
		return { b64: null, error: 'The input must be JSON, like {"name": "value"}' };
	}
	const bytes = new TextEncoder().encode(t);
	if (bytes.length > 64 * 1024) return { b64: null, error: 'The input is larger than 64 KiB' };
	let bin = '';
	for (const b of bytes) bin += String.fromCharCode(b);
	return { b64: btoa(bin), error: null };
}

/** A reply excerpt (base64) as text when it is UTF-8, else null (then the page shows the base64). */
export function b64ToText(b64: string | null | undefined): string | null {
	if (!b64) return null;
	try {
		const bin = atob(b64);
		const bytes = Uint8Array.from(bin, (c) => c.charCodeAt(0));
		return new TextDecoder('utf-8', { fatal: true }).decode(bytes);
	} catch {
		return null;
	}
}

/** A workflow's steps: at least one, at most 64, each with a name (1–128, unique) and a handler. Returns the
 *  first problem as `{index, field, message}` (index -1: the list itself), or null. */
export function stepsError(steps: { name: string; handler: string }[]): { index: number; field: 'name' | 'handler' | 'list'; message: string } | null {
	if (steps.length === 0) return { index: -1, field: 'list', message: 'Add at least one step' };
	if (steps.length > 64) return { index: -1, field: 'list', message: 'A workflow has at most 64 steps' };
	const seen = new Set<string>();
	for (const [i, s] of steps.entries()) {
		const n = nameError(s.name, 'step name');
		if (n) return { index: i, field: 'name', message: n };
		if (seen.has(s.name.trim())) return { index: i, field: 'name', message: 'Two steps have this name' };
		seen.add(s.name.trim());
		if (s.handler.trim() === '') return { index: i, field: 'handler', message: 'Pick the handler this step calls' };
	}
	return null;
}
