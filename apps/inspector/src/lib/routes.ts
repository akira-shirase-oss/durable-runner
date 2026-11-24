// The inspector's sections and their hash routes (#/, #/runs, #/runs/10, …), pure. SvelteKit's hash router does
// the matching; this module names the sections, maps a route id to its nav item and validates the run id param.

export type Section = 'overview' | 'runs' | 'handlers' | 'submit' | 'admin' | 'settings';

export interface NavItem {
	section: Section;
	label: string;
	href: string;
	/** a monitoring page: it shows the time-window picker */
	monitoring: boolean;
}

export const NAV: readonly NavItem[] = [
	{ section: 'overview', label: 'Overview', href: '#/', monitoring: true },
	{ section: 'runs', label: 'Runs', href: '#/runs', monitoring: false },
	{ section: 'handlers', label: 'Handlers', href: '#/handlers', monitoring: true },
	{ section: 'submit', label: 'Submit', href: '#/submit', monitoring: false },
	{ section: 'admin', label: 'Admin', href: '#/admin', monitoring: false },
	{ section: 'settings', label: 'Settings', href: '#/settings', monitoring: false },
];

/** The nav section for a SvelteKit route id ("/", "/runs", "/runs/[id]", …); unknown routes fall back to overview. */
export function sectionOf(routeId: string | null | undefined): Section {
	const first = (routeId ?? '/').split('/').filter(Boolean)[0] ?? '';
	const hit = NAV.find((n) => n.section === first);
	return hit ? hit.section : 'overview';
}

/** A run id from a route param or a "jump to run" field: the server's opaque public id (11 base64url
 *  characters), or a legacy all-digit id the default workspace still accepts (an optional leading "#" or spaces
 *  are forgiven). Anything else is null. The id is passed back to the server as-is: only it can decode it. */
export function parseRunId(s: string | null | undefined): string | null {
	const v = (s ?? '').trim().replace(/^#/, '');
	if (/^[A-Za-z0-9_-]{11}$/.test(v)) return v;
	if (/^\d{1,16}$/.test(v) && Number.isSafeInteger(Number(v))) return v;
	return null;
}

export function runHref(id: string): string {
	return `#/runs/${encodeURIComponent(id)}`;
}
