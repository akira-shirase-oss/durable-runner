<script lang="ts">
	// What the Overview can still show against a server without GET /metrics: the diagnostics counters (with the
	// API key) and the latest runs (with any key). Polled like everything else.
	import Icon from '../Icon.svelte';
	import KpiTile from './KpiTile.svelte';
	import Notice from './Notice.svelte';
	import RunsTable from './RunsTable.svelte';
	import StateBars from './StateBars.svelte';
	import { ApiError, diagnostics, listRuns } from '../api.ts';
	import { fmtBytes, fmtCompact } from '../format.ts';
	import { live, poll, Resource } from '../live.svelte.ts';
	import { session } from '../session.svelte.ts';

	const diag = new Resource((signal) =>
		session.apiKey ? diagnostics(session.apiKey, signal) : Promise.reject(new ApiError('needs the API key', 401)),
	);
	poll(diag);
	const runs = new Resource((signal) => listRuns(session.readerKey, 'desc', null, 10, signal));
	poll(runs);
</script>

{#if diag.data}
	<dl class="kpis" aria-label="Diagnostics">
		<KpiTile label="Live runs" value={fmtCompact(diag.data.live_runs)} sub="held in the index" />
		<KpiTile label="Active runs" value={fmtCompact(diag.data.active_runs)} sub="queued, running or waiting" />
		<KpiTile label="Records written" value={fmtCompact(diag.data.records_written)} sub="journal generation {diag.data.generation}" />
		<KpiTile label="Index" value={fmtBytes(diag.data.index_bytes)} sub="{fmtCompact(diag.data.dedup_keys)} dedup keys" />
		<KpiTile label="Disk reserve" value={diag.data.space_ok ? 'OK' : 'Below reserve'} tone={diag.data.space_ok ? 'ok' : 'bad'} sub="admission refuses before exhaustion">
			{#snippet icon()}<Icon name="hard-drive" size={18} />{/snippet}
		</KpiTile>
	</dl>
{:else}
	<Notice status={diag.status} error={diag.error} what="Diagnostics" route="GET /admin/diagnostics" needsApi />
{/if}

<div class="grid two lower">
	<section class="card" aria-labelledby="legacy-states">
		<div class="card-head">
			<div>
				<h2 id="legacy-states">Runs by state</h2>
				<p class="sub">From diagnostics, now</p>
			</div>
		</div>
		<div class="card-body">
			{#if diag.data}<StateBars states={diag.data.states} />{:else}<p class="empty">Needs the API key</p>{/if}
		</div>
	</section>
	<section class="card" aria-labelledby="legacy-runs">
		<div class="card-head ruled">
			<div>
				<h2 id="legacy-runs">Latest runs</h2>
				<p class="sub">Newest first</p>
			</div>
			<a class="btn ghost sm" href="#/runs">All runs<Icon name="arrow-right" size={14} /></a>
		</div>
		{#if runs.data && runs.data.runs.length}
			<div class="table-wrap">
				<RunsTable runs={runs.data.runs} now={live.now} canCancel={false} caption="The ten newest runs" />
			</div>
		{:else if runs.data}
			<p class="empty">No runs yet</p>
		{:else}
			<div class="card-body"><Notice status={runs.status} error={runs.error} what="The run list" route="GET /jobs" /></div>
		{/if}
	</section>
</div>

<style>
	.kpis {
		display: grid;
		grid-template-columns: repeat(5, minmax(0, 1fr));
		gap: 12px;
		margin: 0;
	}
	@media (max-width: 1100px) {
		.kpis {
			grid-template-columns: repeat(2, minmax(0, 1fr));
		}
	}
	.lower {
		align-items: start;
	}
	.card-head .btn {
		text-decoration: none;
	}
</style>
