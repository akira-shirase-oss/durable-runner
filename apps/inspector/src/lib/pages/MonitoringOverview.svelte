<script lang="ts">
	// The monitoring overview (compat's #/ and the signed-in #/overview below the checklist): the window at a glance. KPI tiles with sparklines, throughput, outcomes, step latency, retries and
	// lost replies, runs by state now, the live activity feed and the handlers' health, all from GET /metrics and
	// GET /events, refreshed every 5 s while visible. Against a server without those routes it says so and shows
	// what the older API can: diagnostics and the latest runs.
	import Icon from '../Icon.svelte';
	import ChartCard from '../charts/ChartCard.svelte';
	import TimeChart, { type ChartSeries } from '../charts/TimeChart.svelte';
	import ActivityFeed from '../ui/ActivityFeed.svelte';
	import KpiTile from '../ui/KpiTile.svelte';
	import Notice from '../ui/Notice.svelte';
	import StateBars from '../ui/StateBars.svelte';
	import LegacyOverview from '../ui/LegacyOverview.svelte';
	import { ApiError, diagnostics, events, metrics } from '../api.ts';
	import { fmtAgo, fmtApproxMs, fmtClock, fmtCompact, fmtInt, fmtMsTick, fmtPct, fmtRate } from '../format.ts';
	import { live, poll, Resource } from '../live.svelte.ts';
	import { column, overviewModel } from '../overview.ts';
	import { peak, WINDOW_LABEL, type Bin } from '../series.ts';
	import { session } from '../session.svelte.ts';
	import { workspaceDiagnostics } from '../api.ts';
	import { untrack } from 'svelte';

	/** compat: the operator's server-wide view (disk reserve from /admin/diagnostics); signed in: one workspace */
	let { ops = true }: { ops?: boolean } = $props();

	const metricsRes = new Resource((signal) => metrics(session.reader, session.window, signal));
	poll(metricsRes, () => session.window);
	const feedRes = new Resource((signal) => events(session.reader, 30, signal));
	poll(feedRes);
	const diagRes = new Resource((signal) =>
		!ops ? Promise.reject(new ApiError('not on this page', 0)) : session.apiKey ? diagnostics(session.apiKey, signal) : Promise.reject(new ApiError('needs the API key', 401)),
		{ interval: () => (ops ? 5_000 : 0), live: untrack(() => ops) },
	);
	poll(diagRes);
	// the workspace's own counts (signed in): what "Runs by state" shows when metrics carry the whole server
	const wsDiag = new Resource((signal) => (ops ? Promise.reject(new ApiError('not on this page', 0)) : workspaceDiagnostics(session.reader, signal)), {
		interval: () => (ops ? 0 : 5_000),
		live: untrack(() => !ops),
	});
	poll(wsDiag);

	let gridWidth = $state(0);
	const maxBins = $derived.by(() => {
		const chart = gridWidth >= 900 ? gridWidth / 2 - 70 : gridWidth - 60;
		return Math.max(20, Math.min(120, Math.floor(chart / 6)));
	});

	const m = $derived(metricsRes.data);
	const o = $derived(m ? overviewModel(m, maxBins) : null);
	const bins = $derived(o?.bins ?? []);
	const win = $derived(WINDOW_LABEL[session.window]);
	const per = $derived(o ? `per ${o.binText}` : '');

	const col = (k: keyof Bin) => column(bins, k);
	// sparklines show finished bins only: the bin in progress is partial and would read as a drop
	const spark = (k: keyof Bin) => (bins.at(-1)?.partial ? col(k).slice(0, -1) : col(k));

	const throughput: ChartSeries[] = $derived([
		{ id: 'accepted', label: 'Accepted', color: 'var(--c-chart-1)', kind: 'area', values: col('accepted') },
		{ id: 'completed', label: 'Completed', color: 'var(--c-chart-ok)', kind: 'area', values: col('completed') },
	]);
	const outcomes: ChartSeries[] = $derived([
		{ id: 'completed', label: 'Completed', color: 'var(--c-chart-ok)', kind: 'bar', values: col('completed') },
		{ id: 'cancelled', label: 'Cancelled', color: 'var(--c-chart-neutral)', kind: 'bar', values: col('cancelled') },
		{ id: 'failed', label: 'Failed', color: 'var(--c-chart-fail)', kind: 'bar', values: col('failed') },
	]);
	const latency: ChartSeries[] = $derived([
		{ id: 'p50', label: 'p50', color: 'var(--c-chart-1)', kind: 'line', values: col('p50') },
		{ id: 'p95', label: 'p95', color: 'var(--c-chart-2)', kind: 'line', values: col('p95') },
	]);
	const retries: ChartSeries[] = $derived([
		{ id: 'failed', label: 'Failed attempts', color: 'var(--c-chart-retry)', kind: 'bar', values: col('steps_failed') },
		{ id: 'lost', label: 'Lost replies', color: 'var(--c-chart-lost)', kind: 'bar', values: col('replies_lost') },
	]);

	function busiest(values: (number | null)[], what: string): string {
		const p = peak(values);
		if (!p || p.v === 0) return '';
		return ` Busiest ${o?.binText ?? ''}: ${fmtInt(p.v)} ${what} at ${fmtClock(bins[p.i]!.t, false)}.`;
	}

	const handlers = $derived(
		[...(m?.handlers ?? [])].sort((a, b) => b.failed / Math.max(1, b.calls) - a.failed / Math.max(1, a.calls) || b.calls - a.calls).slice(0, 8),
	);
	const unsupported = $derived(metricsRes.status === 'unsupported');
	$effect(() => {
		if (metricsRes.status === 'ok') session.metricsSupported = true;
		else if (metricsRes.status === 'unsupported') session.metricsSupported = false;
	});
</script>

<div class="page">
	{#if session.notice}
		<p class="banner notice"><Icon name="check" size={16} /><span class="msg">{session.notice}</span><button class="btn ghost sm act" onclick={() => (session.notice = null)}>Dismiss</button></p>
	{/if}
	{#if !(metricsRes.status === 'needs-key' && !m && ops)}
		<Notice status={metricsRes.status} error={metricsRes.error} what="Metrics" route="GET /metrics" stale={metricsRes.updatedAt} retry={() => metricsRes.refresh(true)} />
	{/if}

	{#if unsupported}
		<LegacyOverview />
	{:else if metricsRes.status === 'needs-key' && !m && !ops}
		<!-- the refusal is in the banner above -->
	{:else if metricsRes.status === 'needs-key' && !m}
		<section class="card start" aria-labelledby="start-title">
			<span class="start-icon" aria-hidden="true"><Icon name="key" size={22} /></span>
			<h2 id="start-title">Add a key to see the dashboard</h2>
			<p class="hint">
				The read key shows the overview, runs, timelines and handlers; the API key also submits, cancels and
				administers. Both stay in this tab only.
			</p>
			<a class="btn brand sm" href="#/settings"><Icon name="settings" />Open Settings</a>
		</section>
	{:else}
		<dl class="kpis" aria-label="Key figures, {win}">
			<KpiTile
				label="Live runs"
				value={fmtCompact(o?.liveRuns)}
				sub={o?.liveRuns === null ? 'not sampled yet' : 'held in the index'}
				spark={spark('live_runs')}
				trend="Live runs over the {win}"
			/>
			<KpiTile
				label="Active runs"
				value={fmtCompact(o?.activeRuns)}
				sub={o ? `${fmtInt(o.inFlight)} handler call${o.inFlight === 1 ? '' : 's'} in flight` : ''}
				spark={spark('active_runs')}
				trend="Active runs over the {win}"
			/>
			<KpiTile
				label="Completed per min"
				value={o ? fmtRate(o.completedPerMin).replace('/min', '') : '—'}
				sub={o ? `${fmtRate(o.acceptedPerMin).replace('/min', '')} accepted per min` : ''}
				spark={spark('completed')}
				color="var(--c-chart-ok)"
				trend="Runs completed {per} over the {win}"
			/>
			<KpiTile
				label="Success rate"
				value={fmtPct(o?.success)}
				sub={m ? `${fmtInt(m.totals.failed)} failed of ${fmtInt(m.totals.completed + m.totals.failed)}` : ''}
				spark={o ? (bins.at(-1)?.partial ? o.successSeries.slice(0, -1) : o.successSeries) : undefined}
				color="var(--c-chart-ok)"
				tone={o && o.success !== null && o.success < 0.95 ? 'warn' : undefined}
				trend="Completed out of completed plus failed, {per}; cancelled runs are not counted"
			/>
			<KpiTile
				label="p95 step latency"
				value={fmtApproxMs(o?.p95)}
				sub={o ? `p50 ${fmtApproxMs(o.p50)}` : ''}
				spark={spark('p95')}
				color="var(--c-chart-2)"
				trend="Worst p95 {per} over the {win}"
			/>
			{#if !ops}
				<KpiTile
					label="Finished"
					value={m ? fmtCompact(m.totals.completed + m.totals.failed + m.totals.cancelled) : '—'}
					sub={m ? `${fmtInt(m.totals.failed)} failed, ${fmtInt(m.totals.cancelled)} cancelled · ${win}` : ''}
					tone={m && m.totals.failed > 0 ? 'warn' : undefined}
				/>
			{:else if diagRes.data}
				<KpiTile
					label="Disk reserve"
					value={diagRes.data.space_ok ? 'OK' : 'Below reserve'}
					tone={diagRes.data.space_ok ? 'ok' : 'bad'}
					sub="journal generation {diagRes.data.generation}"
				>
					{#snippet icon()}<Icon name="hard-drive" size={18} />{/snippet}
				</KpiTile>
			{:else}
				<KpiTile
					label="Disk reserve"
					value="—"
					sub={diagRes.status === 'needs-key' ? 'needs the API key' : diagRes.status === 'error' ? 'diagnostics unavailable' : 'loading…'}
					href={diagRes.status === 'needs-key' ? '#/settings' : undefined}
				/>
			{/if}
		</dl>

		<div class="grid two" bind:clientWidth={gridWidth}>
			<ChartCard id="throughput" title="Throughput" sub="Runs {per} · {win}" series={throughput} {bins} format={fmtInt}
				extraColumns={[{ label: 'Deduplicated', values: col('deduplicated') }, { label: 'Refused', values: col('refused') }]}>
				<TimeChart
					{bins}
					since={m?.since ?? null}
					series={throughput}
					label="Throughput: runs accepted and completed {per}"
					summary={m ? `${fmtInt(m.totals.accepted)} runs accepted and ${fmtInt(m.totals.completed)} completed over the ${win}; ${fmtInt(m.totals.deduplicated)} submits deduplicated and ${fmtInt(m.totals.refused)} refused.${busiest(col('accepted'), 'accepted')}` : 'Loading'}
					loading={metricsRes.loading}
					empty="No runs accepted or completed in the {win}"
					extra={(i) => [
						{ label: 'deduplicated', value: fmtInt(bins[i]?.deduplicated) },
						{ label: 'refused', value: fmtInt(bins[i]?.refused) },
					]}
				/>
			</ChartCard>
			<ChartCard id="outcomes" title="Outcomes" sub="Runs finished {per} · {win}" series={outcomes} {bins} format={fmtInt}>
				<TimeChart
					{bins}
					since={m?.since ?? null}
					series={outcomes}
					stacked
					label="Outcomes: runs completed, cancelled and failed {per}"
					summary={m ? `${fmtInt(m.totals.completed)} completed, ${fmtInt(m.totals.cancelled)} cancelled and ${fmtInt(m.totals.failed)} failed over the ${win}.${busiest(col('failed'), 'failed')}` : 'Loading'}
					loading={metricsRes.loading}
					empty="No run finished in the {win}"
				/>
			</ChartCard>
			<ChartCard id="latency" title="Step latency" sub="Dispatch to answer, worst bucket {per} · approximate" series={latency} {bins} format={fmtApproxMs}
				extraColumns={[{ label: 'max', values: col('max') }]}>
				<TimeChart
					{bins}
					since={m?.since ?? null}
					series={latency}
					format={fmtApproxMs}
					tick={fmtMsTick}
					label="Step latency: p50 and p95 {per}"
					summary={o ? `Step latency over the ${win}: p50 ${fmtApproxMs(o.p50)}, p95 ${fmtApproxMs(o.p95)}, max ${fmtApproxMs(o.max)}. Percentiles are histogram bucket bounds, so approximate.` : 'Loading'}
					loading={metricsRes.loading}
					empty="No step attempts in the {win}"
					extra={(i) => [{ label: 'max', value: fmtApproxMs(bins[i]?.max) }]}
				/>
			</ChartCard>
			<ChartCard id="retries" title="Retries and lost replies" sub="Failed attempts and replies lost {per} · {win}" series={retries} {bins} format={fmtInt}
				extraColumns={[{ label: 'Reconciled', values: col('reconciled') }, { label: 'Dispatched', values: col('dispatched') }]}>
				<TimeChart
					{bins}
					since={m?.since ?? null}
					series={retries}
					stacked
					label="Failed attempts and lost replies {per}"
					summary={m ? `${fmtInt(m.totals.steps_failed)} failed attempts and ${fmtInt(m.totals.replies_lost)} lost replies over the ${win}; ${fmtInt(m.totals.reconciled)} reconcile probes answered, out of ${fmtInt(m.totals.dispatched)} attempts dispatched.` : 'Loading'}
					loading={metricsRes.loading}
					empty="No failed attempts or lost replies in the {win}"
					extra={(i) => [
						{ label: 'reconciled', value: fmtInt(bins[i]?.reconciled) },
						{ label: 'dispatched', value: fmtInt(bins[i]?.dispatched) },
					]}
				/>
			</ChartCard>
		</div>

		<div class="grid two lower">
			<section class="card" aria-labelledby="states-title">
				<div class="card-head">
					<div>
						<h2 id="states-title">Runs by state</h2>
						<p class="sub">{ops ? 'Now, across the whole journal' : 'Now, in this workspace'}</p>
					</div>
					<a class="btn ghost sm" href="#/runs">Runs<Icon name="arrow-right" size={14} /></a>
				</div>
				<div class="card-body">
					{#if !ops && wsDiag.data}<StateBars states={wsDiag.data.states} />{:else if m}<StateBars states={m.states} />{:else}<p class="empty">{metricsRes.loading ? 'Loading…' : 'Unavailable'}</p>{/if}
				</div>
			</section>
			<section class="card feed-card" aria-labelledby="feed-title">
				<div class="card-head">
					<div>
						<h2 id="feed-title">Activity</h2>
						<p class="sub">The latest events across runs</p>
					</div>
				</div>
				<div class="card-body feed-body">
					{#if feedRes.status === 'unsupported' || feedRes.status === 'needs-key' || (feedRes.status === 'error' && !feedRes.data)}
						<Notice status={feedRes.status} error={feedRes.error} what="The activity feed" route="GET /events" />
					{:else if feedRes.data}
						<ActivityFeed events={feedRes.data.events} now={live.now} />
					{:else}
						<p class="empty">Loading…</p>
					{/if}
				</div>
			</section>
		</div>

		<section class="card" aria-labelledby="handlers-title">
			<div class="card-head ruled">
				<div>
					<h2 id="handlers-title">Handlers</h2>
					<p class="sub">Calls over the {win}, sorted by failure rate</p>
				</div>
				<a class="btn ghost sm" href="#/handlers">All handlers<Icon name="arrow-right" size={14} /></a>
			</div>
			{#if handlers.length === 0}
				<p class="empty">{m ? 'No handlers registered' : metricsRes.loading ? 'Loading…' : 'Unavailable'}</p>
			{:else}
				<div class="table-wrap">
					<table class="data">
						<thead>
							<tr>
								<th scope="col">Handler</th>
								<th scope="col" class="num">Calls</th>
								<th scope="col" class="num">Failure</th>
								<th scope="col" class="num lost-col">Lost replies</th>
								<th scope="col" class="num">p95 (approx.)</th>
								<th scope="col" class="last-col">Last failure, since start</th>
							</tr>
						</thead>
						<tbody>
							{#each handlers as h (h.name)}
								<tr>
									<th scope="row" class="mono-cell"><a class="hlink" href="#/handlers">{h.name}</a></th>
									<td class="num">{fmtInt(h.calls)}</td>
									<td class="num" class:bad={h.calls > 0 && h.failed / h.calls >= 0.01}>{h.calls ? fmtPct(h.failed / h.calls) : '—'}</td>
									<td class="num lost-col">{fmtInt(h.replies_lost)}</td>
									<td class="num">{fmtApproxMs(h.step_ms.p95)}</td>
									<td class="muted last-col">{h.last_failure_at ? fmtAgo(live.now - h.last_failure_at) : 'none'}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		</section>
	{/if}
</div>

<style>
	.start {
		display: flex;
		flex-direction: column;
		align-items: flex-start;
		gap: 10px;
		padding: 26px 26px 28px;
	}
	.start h2 {
		font-size: 19px;
	}
	.start .hint {
		max-width: 62ch;
		margin: 0;
		line-height: 1.55;
	}
	.start a {
		margin-top: 6px;
		text-decoration: none;
	}
	.start-icon {
		display: grid;
		place-items: center;
		width: 46px;
		height: 46px;
		border-radius: 14px;
		background: var(--c-brand-tint);
		color: var(--c-brand-deep);
	}
	.kpis {
		display: grid;
		grid-template-columns: repeat(6, minmax(0, 1fr));
		gap: 20px;
		margin: 0;
	}
	@media (max-width: 1320px) {
		.kpis {
			grid-template-columns: repeat(3, minmax(0, 1fr));
		}
	}
	@media (max-width: 620px) {
		.kpis {
			grid-template-columns: repeat(2, minmax(0, 1fr));
		}
	}
	.lower {
		align-items: start;
	}
	.feed-body {
		max-height: 380px;
		padding-top: 6px;
		overflow-y: auto;
	}
	@media (max-width: 600px) {
		.lost-col,
		.last-col {
			display: none;
		}
	}
	.hlink {
		color: var(--c-ink);
		text-decoration: none;
	}
	.hlink:hover {
		text-decoration: underline;
		text-underline-offset: 3px;
	}
	.card-head .btn {
		text-decoration: none;
	}
</style>
