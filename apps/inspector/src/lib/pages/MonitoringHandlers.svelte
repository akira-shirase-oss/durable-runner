<script lang="ts">
	// Handler traffic (compat's Handlers page, and the lower half of the signed-in one): one card per handler over the window: calls split by outcome, failure rate, latency
	// (p50, p95, max on one scale shared by every card, so they compare at a glance) and when it last answered
	// well and badly. The server aggregates per handler over the window (no per-handler time series), so the
	// whole-runner step latency over time sits on top for context. "Last ok" and "last failure" reach back to the
	// server's start, not just the window. Percentiles are histogram bucket bounds, so approximate.
	import Icon from '../Icon.svelte';
	import ChartCard from '../charts/ChartCard.svelte';
	import TimeChart, { type ChartSeries } from '../charts/TimeChart.svelte';
	import Notice from '../ui/Notice.svelte';
	import { metrics, type HandlerStats } from '../api.ts';
	import { fmtAgo, fmtApproxMs, fmtDateTime, fmtInt, fmtMsTick, fmtPct } from '../format.ts';
	import { live, poll, Resource } from '../live.svelte.ts';
	import { column, displayBins } from '../overview.ts';
	import { binLabel, niceScale, WINDOW_LABEL } from '../series.ts';
	import { session } from '../session.svelte.ts';

	const res = new Resource((signal) => metrics(session.reader, session.window, signal));
	poll(res, () => session.window);

	let width = $state(0);
	const m = $derived(res.data);
	const win = $derived(WINDOW_LABEL[session.window]);
	const db = $derived(m ? displayBins(m, Math.max(20, Math.min(120, Math.floor((width - 60) / 6)))) : null);
	const bins = $derived(db?.bins ?? []);
	const latency: ChartSeries[] = $derived([
		{ id: 'p50', label: 'p50', color: 'var(--c-chart-1)', kind: 'line', values: column(bins, 'p50') },
		{ id: 'p95', label: 'p95', color: 'var(--c-chart-2)', kind: 'line', values: column(bins, 'p95') },
	]);
	const handlers = $derived(m?.handlers ?? []);
	const scale = $derived(niceScale(Math.max(1, ...handlers.map((h) => h.step_ms.max ?? h.step_ms.p95 ?? 0)), 4, 1));

	function failure(h: HandlerStats): number | null {
		return h.calls > 0 ? h.failed / h.calls : null;
	}
	function pct(v: number | null): number {
		return v === null ? 0 : Math.min(100, (v / scale.max) * 100);
	}
	function health(h: HandlerStats): { tone: string; text: string } {
		const f = failure(h);
		if (h.calls === 0) return { tone: '', text: 'No calls' };
		if (f !== null && f >= 0.05) return { tone: 'bad', text: 'Failing' };
		if ((f ?? 0) > 0 || h.replies_lost > 0) return { tone: 'warn', text: 'Degraded' };
		return { tone: 'ok', text: 'Healthy' };
	}
	$effect(() => {
		if (res.status === 'ok') session.metricsSupported = true;
		else if (res.status === 'unsupported') session.metricsSupported = false;
	});
</script>

<div class="page" bind:clientWidth={width}>
	<Notice status={res.status} error={res.error} what="Handler statistics" route="GET /metrics" stale={res.updatedAt} retry={() => res.refresh(true)} />
	{#if res.status !== 'unsupported'}
		<ChartCard id="h-latency" title="Step latency, all handlers" sub="Dispatch to answer, worst bucket per {db ? binLabel(db.binMs) : '…'} · {win}" series={latency} {bins} format={fmtApproxMs}>
			<TimeChart
				{bins}
				since={m?.since ?? null}
				series={latency}
				format={fmtApproxMs}
				tick={fmtMsTick}
				height={170}
				label="Step latency across all handlers"
				summary={m ? `Across all handlers over the ${win}: p50 ${fmtApproxMs(m.totals.step_ms.p50)}, p95 ${fmtApproxMs(m.totals.step_ms.p95)}, max ${fmtApproxMs(m.totals.step_ms.max)}.` : 'Loading'}
				loading={res.loading}
				empty="No step attempts in the {win}"
			/>
		</ChartCard>

		<div class="section-head">
			<h2 class="section-title">{session.signedIn ? 'Traffic per handler' : 'Handlers'}</h2>
			<p class="sub">Calls and latency over the {win}; last ok and last failure since the server started{m ? ` (${fmtDateTime(m.since, live.now)})` : ''}.</p>
		</div>
		{#if m && handlers.length === 0}
			<p class="card empty">{session.signedIn ? 'No handler has been called in this window yet.' : 'No handlers are registered on this server'}{#if !session.signedIn} (<code>--handler name=url</code>){/if}.</p>
		{:else if !m}
			<p class="card empty">{res.loading ? 'Loading…' : 'Unavailable'}</p>
		{:else}
			<div class="cards">
				{#each handlers as h (h.name)}
					{@const st = health(h)}
					{@const f = failure(h)}
					<section class="card handler" aria-labelledby="h-{h.name}">
						<div class="hhead">
							<h2 id="h-{h.name}" class="name"><Icon name="webhook" size={16} /><span class="mono">{h.name}</span></h2>
							<span class="badge {st.tone}">{st.text}</span>
						</div>
						<dl class="nums">
							<div><dt>Calls</dt><dd>{fmtInt(h.calls)}</dd></div>
							<div><dt>Failure</dt><dd class:bad={(f ?? 0) > 0}>{fmtPct(f)}</dd></div>
							<div><dt>p50</dt><dd>{fmtApproxMs(h.step_ms.p50)}</dd></div>
							<div><dt>p95</dt><dd>{fmtApproxMs(h.step_ms.p95)}</dd></div>
						</dl>

						<div class="split" aria-hidden="true">
							{#if h.ok > 0}<i class="ok" style:flex-grow={h.ok}></i>{/if}
							{#if h.replies_lost > 0}<i class="lost" style:flex-grow={h.replies_lost}></i>{/if}
							{#if h.failed > 0}<i class="fail" style:flex-grow={h.failed}></i>{/if}
						</div>
						<ul class="split-legend">
							<li><i class="ok" aria-hidden="true"></i>{fmtInt(h.ok)} ok</li>
							<li><i class="lost" aria-hidden="true"></i>{fmtInt(h.replies_lost)} lost replies</li>
							<li><i class="fail" aria-hidden="true"></i>{fmtInt(h.failed)} failed</li>
						</ul>

						<div class="lat">
							<p class="lat-title">Latency over the window</p>
							<div class="lat-track" role="img" aria-label="Latency of {h.name}: p50 {fmtApproxMs(h.step_ms.p50)}, p95 {fmtApproxMs(h.step_ms.p95)}, max {fmtApproxMs(h.step_ms.max)}, on a scale to {fmtMsTick(scale.max)}">
								{#if h.step_ms.p95 !== null}<span class="p95" style:width="{pct(h.step_ms.p95)}%"></span>{/if}
								{#if h.step_ms.p50 !== null}<span class="p50" style:width="{pct(h.step_ms.p50)}%"></span>{/if}
								{#if h.step_ms.max !== null}<span class="max" style:left="{pct(h.step_ms.max)}%"></span>{/if}
							</div>
							<div class="lat-axis" aria-hidden="true">
								{#each scale.ticks as t (t)}<span style:left="{(t / scale.max) * 100}%">{fmtMsTick(t)}</span>{/each}
							</div>
							<ul class="lat-legend" aria-hidden="true">
								<li><i class="p50"></i>p50</li>
								<li><i class="p95"></i>p95</li>
								<li><i class="max"></i>max</li>
							</ul>
						</div>

						<dl class="last" aria-label="Since the server started">
							<div>
								<dt>Last ok</dt>
								<dd title={h.last_ok_at ? fmtDateTime(h.last_ok_at, live.now) : ''}>{h.last_ok_at ? fmtAgo(live.now - h.last_ok_at) : 'none since start'}</dd>
							</div>
							<div>
								<dt>Last failure</dt>
								<dd class:bad={!!h.last_failure_at} title={h.last_failure_at ? fmtDateTime(h.last_failure_at, live.now) : ''}>{h.last_failure_at ? fmtAgo(live.now - h.last_failure_at) : 'none since start'}</dd>
							</div>
						</dl>
					</section>
				{/each}
			</div>
		{/if}
	{/if}
</div>

<style>
	.cards {
		display: grid;
		grid-template-columns: repeat(auto-fill, minmax(320px, 1fr));
		gap: 16px;
	}
	.handler {
		display: flex;
		flex-direction: column;
		gap: 14px;
		padding: 16px 18px 16px;
	}
	.hhead {
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 10px;
	}
	.name {
		min-width: 0;
		font-size: 15px;
	}
	.name .mono {
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
	}
	.nums {
		display: grid;
		grid-template-columns: repeat(4, minmax(0, 1fr));
		gap: 8px;
		margin: 0;
	}
	.nums dt,
	.last dt {
		color: var(--c-body);
		font-size: 12.5px;
	}
	.nums dd {
		margin: 2px 0 0;
		font-size: 19px;
		font-weight: 560;
		letter-spacing: -0.015em;
		white-space: nowrap;
	}
	dd.bad {
		color: var(--c-danger);
	}
	.split {
		display: flex;
		gap: 2px;
		height: 8px;
		overflow: hidden;
		border-radius: 4px;
		background: var(--c-line);
	}
	.split i {
		flex-basis: 0;
		min-width: 3px;
	}
	i.ok {
		background: var(--c-chart-ok);
	}
	i.lost {
		background: var(--c-chart-lost);
	}
	i.fail {
		background: var(--c-chart-fail);
	}
	.split-legend,
	.lat-legend {
		display: flex;
		flex-wrap: wrap;
		gap: 4px 14px;
		margin: -6px 0 0;
		padding: 0;
		list-style: none;
		color: var(--c-ink-soft);
		font-size: 12.5px;
	}
	.split-legend li,
	.lat-legend li {
		display: inline-flex;
		align-items: center;
		gap: 6px;
	}
	.split-legend i {
		width: 9px;
		height: 9px;
		border-radius: 3px;
	}
	.lat-title {
		margin: 0 0 8px;
		color: var(--c-body);
		font-size: 12.5px;
	}
	.lat-track {
		position: relative;
		height: 14px;
		border-radius: 4px;
		background: var(--c-grid);
	}
	.lat-track span {
		position: absolute;
		top: 0;
		left: 0;
		height: 100%;
		border-radius: 0 4px 4px 0;
	}
	.lat-track .p95,
	.lat-legend .p95 {
		background: var(--c-chart-2);
	}
	.lat-track .p50,
	.lat-legend .p50 {
		background: var(--c-chart-1);
	}
	.lat-track .p50 {
		top: 3px;
		height: 8px;
		box-shadow: 2px 0 0 var(--c-card);
	}
	.lat-track .max {
		top: -3px;
		width: 2px;
		height: 20px;
		margin-left: -1px;
		border-radius: 1px;
		background: var(--c-ink-soft);
	}
	.lat-legend i {
		width: 12px;
		height: 8px;
		border-radius: 2px;
	}
	.lat-legend i.max {
		width: 2px;
		height: 12px;
		background: var(--c-ink-soft);
	}
	.lat-axis {
		position: relative;
		height: 16px;
		margin: 4px 0 10px;
		color: var(--c-body);
		font-size: 11px;
		font-variant-numeric: tabular-nums;
	}
	.lat-axis span {
		position: absolute;
		transform: translateX(-50%);
		white-space: nowrap;
	}
	.lat-axis span:first-child {
		transform: none;
	}
	.lat-axis span:last-child {
		transform: translateX(-100%);
	}
	.lat-legend {
		margin-top: 0;
	}
	.last {
		display: grid;
		grid-template-columns: 1fr 1fr;
		gap: 8px;
		margin: 0;
		padding-top: 12px;
		border-top: 1px solid var(--c-line);
	}
	.last dd {
		margin: 2px 0 0;
		font-size: 13.5px;
	}
	code {
		font-family: var(--font-mono);
		font-size: 0.92em;
	}
	@media (max-width: 600px) {
		.cards {
			grid-template-columns: minmax(0, 1fr);
		}
		.nums dd {
			font-size: 17px;
		}
	}
</style>
