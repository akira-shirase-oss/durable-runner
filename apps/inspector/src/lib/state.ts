/** How the inspector shows a run state the server reports, e.g.
 *  `running:step:1:attempt:2`: the kind before the first `:` picks the chip's
 *  colour (it is also the row's `data-state`), the label names it, and the
 *  rest is shown beside it as `step 1 · attempt 2`. Nothing is dropped: an
 *  unknown kind keeps its own name and an unexpected tail is shown as sent. */
export interface StateView {
	kind: string;
	label: string;
	detail: string;
}

const LABELS: Record<string, string> = {
	queued: 'Queued',
	running: 'Running',
	retry_waiting: 'Retry waiting',
	reconciling: 'Reconciling',
	done: 'Done',
	failed: 'Failed',
	cancelled: 'Cancelled',
};

export function stateView(state: string): StateView {
	const cut = state.indexOf(':');
	const kind = cut < 0 ? state : state.slice(0, cut);
	const rest = cut < 0 ? '' : state.slice(cut + 1);
	const parts = rest ? rest.split(':') : [];
	let detail = rest;
	if (parts.length > 0 && parts.length % 2 === 0 && parts.every((p) => p !== '')) {
		const pairs: string[] = [];
		for (let i = 0; i < parts.length; i += 2) pairs.push(`${parts[i]} ${parts[i + 1]}`);
		detail = pairs.join(' · ');
	}
	return { kind, label: LABELS[kind] ?? kind, detail };
}

/** A diagnostics bar's width in percent of the largest count: at least 4 so
 *  a non-zero count stays visible, 0 for zero, never NaN when all are 0. */
export function share(n: number, max: number): number {
	if (!(n > 0) || !(max > 0)) return 0;
	return Math.min(100, Math.max(4, (n / max) * 100));
}
