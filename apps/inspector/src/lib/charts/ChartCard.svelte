<script lang="ts">
	// A chart's card: its title and what it measures, a legend (a swatch shaped like the mark: a block for areas
	// and bars, a stroke for lines), and a Table toggle that swaps the plot for the same numbers as a table, newest
	// first, so no value is reachable only by hovering.
	import type { Snippet } from 'svelte';
	import Icon from '../Icon.svelte';
	import { fmtClock } from '../format.ts';
	import type { ChartBin, ChartSeries } from './TimeChart.svelte';

	let {
		title,
		sub,
		id,
		series,
		bins,
		format,
		extraColumns = [],
		children,
		actions,
	}: {
		title: string;
		sub?: string;
		id: string;
		series: ChartSeries[];
		bins: ChartBin[];
		format: (v: number) => string;
		/** values shown in the table (and the tooltip) but not plotted */
		extraColumns?: { label: string; values: (number | null)[] }[];
		children: Snippet;
		actions?: Snippet;
	} = $props();

	let table = $state(false);

	const rows = $derived.by(() => {
		const out: { t: number; end: number; cells: string[] }[] = [];
		for (let i = bins.length - 1; i >= 0; i--) {
			const vals = [...series.map((s) => s.values[i] ?? null), ...extraColumns.map((c) => c.values[i] ?? null)];
			if (vals.every((v) => v === null)) continue;
			out.push({ t: bins[i]!.t, end: bins[i]!.end, cells: vals.map((v) => (v === null ? '—' : format(v))) });
		}
		return out;
	});
</script>

<section class="card chart-card" aria-labelledby="{id}-title">
	<div class="card-head">
		<div class="titles">
			<h2 id="{id}-title">{title}</h2>
			{#if sub}<p class="sub">{sub}</p>{/if}
		</div>
		<div class="tools">
			{@render actions?.()}
			<button class="btn ghost sm icon-sm" aria-pressed={table} title={table ? 'Show the chart' : 'Show as a table'} onclick={() => (table = !table)}>
				<Icon name={table ? 'chart' : 'table'} size={15} /><span class="sr">{table ? `Show the ${title} chart` : `Show ${title} as a table`}</span>
			</button>
		</div>
		{#if series.length > 1}
			<ul class="legend" aria-label="Legend">
				{#each series as s (s.id)}
					<li><i class="sw {s.kind}" style:--k={s.color}></i>{s.label}</li>
				{/each}
			</ul>
		{/if}
	</div>
	<div class="body">
		{#if table}
			<div class="table-scroll">
				{#if rows.length === 0}
					<p class="empty">No values in this window</p>
				{:else}
					<table class="data">
						<caption class="sr">{title}{sub ? `, ${sub}` : ''}, newest first</caption>
						<thead>
							<tr>
								<th scope="col">Interval</th>
								{#each series as s (s.id)}<th scope="col" class="num">{s.label}</th>{/each}
								{#each extraColumns as c (c.label)}<th scope="col" class="num">{c.label}</th>{/each}
							</tr>
						</thead>
						<tbody>
							{#each rows as r (r.t)}
								<tr>
									<th scope="row" class="when">{fmtClock(r.t)} – {fmtClock(r.end)}</th>
									{#each r.cells as c, i (i)}<td class="num">{c}</td>{/each}
								</tr>
							{/each}
						</tbody>
					</table>
				{/if}
			</div>
		{:else}
			{@render children()}
		{/if}
	</div>
</section>

<style>
	.chart-card {
		display: flex;
		flex-direction: column;
	}
	.card-head {
		align-items: flex-start;
		flex-wrap: nowrap;
		position: relative;
		display: grid;
		grid-template-columns: minmax(0, 1fr) auto;
		row-gap: 10px;
	}
	.titles {
		min-width: 0;
	}
	.tools {
		display: flex;
		align-items: center;
		gap: 4px;
		margin: -4px -6px 0 0;
	}
	.legend {
		grid-column: 1 / -1;
		display: flex;
		flex-wrap: wrap;
		gap: 4px 12px;
		margin: 0;
		padding: 0;
		list-style: none;
		color: var(--c-ink-soft);
		font-size: 12.5px;
	}
	.legend li {
		display: inline-flex;
		align-items: center;
		gap: 6px;
		white-space: nowrap;
	}
	.sw {
		display: block;
		width: 10px;
		height: 10px;
		border-radius: 3px;
		background: var(--k);
	}
	.sw.line {
		width: 14px;
		height: 3px;
		border-radius: 2px;
	}
	.icon-sm {
		width: 32px;
		padding: 0;
		color: var(--c-body);
	}
	.icon-sm[aria-pressed='true'] {
		background: var(--c-hover);
		color: var(--c-ink);
	}
	.body {
		padding: 12px 14px 12px 6px;
	}
	.table-scroll {
		max-height: 214px;
		margin: -4px -8px -4px 8px;
		overflow: auto;
		border-radius: 10px;
	}
	.table-scroll :global(table.data th),
	.table-scroll :global(table.data td) {
		padding: 6px 10px;
		font-size: 13px;
	}
	.table-scroll :global(thead th) {
		position: sticky;
		top: 0;
	}
	.when {
		color: var(--c-ink-soft);
		font-weight: 450;
		font-variant-numeric: tabular-nums;
	}
	.table-scroll .empty {
		padding: 24px;
	}
</style>
