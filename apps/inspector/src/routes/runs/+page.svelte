<script lang="ts">
	// Runs: the run list in either order with cursor paging (D-053), state filter chips and a key search over the
	// runs on this page (the API pages by id, so filters apply to the loaded page and say so), jump to a run id,
	// cancel, and a live refresh of the page every 5 s.
	import { goto } from '$app/navigation';
	import Icon from '../../lib/Icon.svelte';
	import Notice from '../../lib/ui/Notice.svelte';
	import RunsTable from '../../lib/ui/RunsTable.svelte';
	import { cancelRun, listRuns } from '../../lib/api.ts';
	import { fmtInt } from '../../lib/format.ts';
	import { live, poll, Resource } from '../../lib/live.svelte.ts';
	import { STATE_ORDER } from '../../lib/overview.ts';
	import { parseRunId, runHref } from '../../lib/routes.ts';
	import { session } from '../../lib/session.svelte.ts';
	import { stateView } from '../../lib/state.ts';

	type Order = 'asc' | 'desc';
	const PAGE = 50;

	let order = $state<Order>('desc');
	let cursor = $state<string | null>(null);
	let trail = $state<(string | null)[]>([]);
	let filter = $state<string>('all');
	let search = $state('');
	let jump = $state('');
	let jumpError = $state<string | null>(null);
	let actionError = $state<string | null>(null);

	const res = new Resource((signal) => listRuns(session.reader, order, cursor, PAGE, signal));
	poll(res, () => [order, cursor]);

	const runs = $derived(res.data?.runs ?? []);
	const next = $derived(res.data ? ((order === 'desc' ? res.data.next_before : res.data.next_after) ?? null) : null);
	const counts = $derived.by(() => {
		const c: Record<string, number> = {};
		for (const r of runs) {
			const k = stateView(r.state).kind;
			c[k] = (c[k] ?? 0) + 1;
		}
		return c;
	});
	const kinds = $derived([...STATE_ORDER, ...Object.keys(counts).filter((k) => !STATE_ORDER.includes(k))]);
	const shown = $derived(
		runs.filter(
			(r) =>
				(filter === 'all' || stateView(r.state).kind === filter) &&
				(search.trim() === '' || r.key.toLowerCase().includes(search.trim().toLowerCase())),
		),
	);
	const filtering = $derived(filter !== 'all' || search.trim() !== '');

	function setOrder(o: Order) {
		if (o === order) return;
		order = o;
		cursor = null;
		trail = [];
	}

	function forward() {
		if (next === null) return;
		trail = [...trail, cursor];
		cursor = next;
	}

	function back() {
		if (trail.length === 0) return;
		cursor = trail.at(-1) ?? null;
		trail = trail.slice(0, -1);
	}

	async function cancel(id: string) {
		try {
			await cancelRun(id, session.writer);
			session.notice = `Run ${id} cancelled. A step already sent to its handler cannot be undone.`;
			actionError = null;
			void res.refresh(true);
		} catch (e) {
			actionError = e instanceof Error ? e.message : String(e);
		}
	}

	function go(e: SubmitEvent) {
		e.preventDefault();
		const id = parseRunId(jump);
		if (id === null) {
			jumpError = 'A run id is the 11-character id from the submit answer, like WBwXuaDUPWc';
			return;
		}
		jumpError = null;
		void goto(runHref(id));
	}
</script>

<div class="page">
	{#if session.notice}
		<p class="banner notice"><Icon name="check" size={16} /><span class="msg">{session.notice}</span><button class="btn ghost sm act" onclick={() => (session.notice = null)}>Dismiss</button></p>
	{/if}
	{#if actionError}
		<p class="banner error"><Icon name="alert" size={16} /><span class="msg">{actionError}</span><button class="btn ghost sm act" onclick={() => (actionError = null)}>Dismiss</button></p>
	{/if}
	<Notice status={res.status} error={res.error} what="The run list" route="GET /jobs" stale={res.updatedAt} retry={() => res.refresh(true)} />

	<section class="card" aria-labelledby="runs-title">
		<div class="card-head toolbar">
			<h2 id="runs-title" class="sr">Runs</h2>
			<div class="find">
				<label class="search">
					<Icon name="search" size={15} />
					<span class="sr">Search keys on this page</span>
					<input type="search" placeholder="Search keys on this page" bind:value={search} autocomplete="off" spellcheck="false" />
				</label>
				{#if search}<button class="btn ghost sm" onclick={() => (search = '')}>Clear</button>{/if}
			</div>
			<form class="jump" onsubmit={go}>
				<label>
					<span class="sr">Jump to run id</span>
					<input class="mono" placeholder="Run id" bind:value={jump} aria-invalid={jumpError ? 'true' : undefined} aria-describedby={jumpError ? 'jump-error' : undefined} />
				</label>
				<button class="btn sm" type="submit">Open<Icon name="arrow-right" size={14} /></button>
			</form>
			<div class="seg order" role="group" aria-label="Order">
				<button aria-pressed={order === 'desc'} onclick={() => setOrder('desc')}>Newest first</button>
				<button aria-pressed={order === 'asc'} onclick={() => setOrder('asc')}>Oldest first</button>
			</div>
		</div>
		{#if jumpError}<p class="jump-error" id="jump-error" role="alert">{jumpError}</p>{/if}
		<div class="filters" role="group" aria-label="Filter by state (this page)">
			<button class="fchip" aria-pressed={filter === 'all'} onclick={() => (filter = 'all')}>All<span class="n">{fmtInt(runs.length)}</span></button>
			{#each kinds as k (k)}
				<button class="fchip" data-state={k} aria-pressed={filter === k} onclick={() => (filter = filter === k ? 'all' : k)}>
					<i class="dot" aria-hidden="true"></i>{stateView(k).label}<span class="n">{fmtInt(counts[k] ?? 0)}</span>
				</button>
			{/each}
		</div>
		<p class="scope">
			{#if filtering}
				Showing {fmtInt(shown.length)} of the {fmtInt(runs.length)} runs on this page. Search and filters apply to the loaded page only; page on for more.
			{:else}
				Page {trail.length + 1} · {fmtInt(runs.length)} runs, {order === 'desc' ? 'newest' : 'oldest'} first · refreshes every 5 s. Search and filters apply to this page.
			{/if}
		</p>

		{#if res.loading}
			<p class="empty">Loading runs…</p>
		{:else if runs.length === 0 && res.status === 'ok'}
			{#if session.signedIn}
				<p class="empty">No runs yet. <a href="#/workflows">Run a workflow</a>, send one from the <a href="#/overview">setup checklist</a>, or submit with an API key (<span class="mono">POST /jobs</span>).</p>
			{:else}
				<p class="empty">No runs yet. <a href="#/submit">Submit one</a></p>
			{/if}
		{:else if shown.length === 0 && runs.length > 0}
			<p class="empty">No run on this page matches. <button class="link" onclick={() => { filter = 'all'; search = ''; }}>Clear filters</button></p>
		{:else if shown.length > 0}
			<div class="table-wrap" >
				<RunsTable
					runs={shown}
					now={live.now}
					canCancel={session.canWrite}
					oncancel={cancel}
					caption="Runs, page {trail.length + 1}, {order === 'desc' ? 'newest' : 'oldest'} first"
				/>
			</div>
		{/if}
		{#if runs.length > 0 || trail.length > 0}
			<nav class="pager" aria-label="Pages">
				<button class="btn sm" onclick={back} disabled={trail.length === 0}><Icon name="chevron-left" />Back</button>
				<span class="hint">Page {trail.length + 1}</span>
				<button class="btn sm" onclick={forward} disabled={next === null}>Forward<Icon name="chevron-right" /></button>
			</nav>
		{/if}
	</section>
</div>

<style>
	.toolbar {
		flex-wrap: wrap;
		align-items: center;
		justify-content: flex-start;
		gap: 8px 10px;
		padding-bottom: 12px;
	}
	.find {
		display: flex;
		flex: 1;
		gap: 6px;
		min-width: 220px;
		max-width: 420px;
	}
	.order {
		margin-left: auto;
	}
	.filters {
		display: flex;
		flex-wrap: wrap;
		gap: 6px;
		padding: 12px 18px 0;
		border-top: 1px solid var(--c-line);
	}
	.fchip {
		display: inline-flex;
		align-items: center;
		gap: 7px;
		height: 30px;
		padding: 0 10px;
		border: 1px solid var(--c-line-strong);
		border-radius: 999px;
		background: var(--c-card);
		color: var(--c-ink-soft);
		font: inherit;
		font-size: 13px;
		font-weight: 520;
		cursor: pointer;
		transition:
			background-color 150ms ease-out,
			border-color 150ms ease-out;
	}
	.fchip:hover {
		background: var(--c-hover);
	}
	.fchip[aria-pressed='true'] {
		border-color: var(--c-ink-soft);
		background: var(--c-ink);
		color: var(--c-card);
	}
	.fchip .n {
		color: var(--c-body);
		font-variant-numeric: tabular-nums;
	}
	.fchip[aria-pressed='true'] .n {
		color: inherit;
		opacity: 0.75;
	}
	.fchip .dot {
		width: 7px;
		height: 7px;
		border-radius: 50%;
		background: var(--c-state-queued);
	}
	.fchip[data-state='running'] .dot {
		background: var(--c-state-run);
	}
	.fchip[data-state='retry_waiting'] .dot {
		background: var(--c-state-retry);
	}
	.fchip[data-state='reconciling'] .dot {
		background: var(--c-state-probe);
	}
	.fchip[data-state='done'] .dot {
		background: var(--c-state-done);
	}
	.fchip[data-state='failed'] .dot {
		background: var(--c-danger);
	}
	.fchip[data-state='cancelled'] .dot {
		background: transparent;
		box-shadow: inset 0 0 0 1.5px var(--c-state-cancelled);
	}
	.search {
		position: relative;
		display: flex;
		flex: 1;
		align-items: center;
	}
	.search :global(svg) {
		position: absolute;
		left: 11px;
		color: var(--c-body);
		pointer-events: none;
	}
	.search input {
		width: 100%;
		min-height: 34px;
		padding: 5px 10px 5px 32px;
		font-size: 13.5px;
	}
	.search input::-webkit-search-cancel-button {
		-webkit-appearance: none;
		appearance: none;
	}
	.jump {
		display: flex;
		gap: 6px;
	}
	.jump input {
		width: 104px;
		min-height: 34px;
		padding: 5px 10px;
	}
	.jump-error {
		margin: -4px 18px 10px;
		color: var(--c-danger);
		font-size: 13px;
	}
	.scope {
		margin: 0;
		padding: 10px 18px 12px;
		color: var(--c-body);
		font-size: 13px;
	}
	.pager {
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 12px;
		padding: 12px 18px;
		border-top: 1px solid var(--c-line);
	}
	.pager .hint {
		font-size: 13.5px;
	}
	.empty a,
	.empty .link {
		color: var(--c-brand-deep);
	}
	@media (max-width: 600px) {
		.card-head.toolbar {
			padding: 14px 14px 10px;
		}
		.find {
			flex-basis: 100%;
			max-width: none;
		}
		.order {
			margin-left: 0;
		}
		.filters,
		.scope,
		.pager {
			padding-left: 14px;
			padding-right: 14px;
		}
	}
</style>
