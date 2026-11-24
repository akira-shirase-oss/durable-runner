<script lang="ts">
	// A list of runs: id (a link to the run), key, state chip with its detail, steps, finished, and Cancel while
	// the run can still be cancelled. On a phone each run is two rows: id + key + action, then state · steps ·
	// finished (the id column sized to the widest id on the page so keys align and ids never clip).
	import { goto } from '$app/navigation';
	import StateChip from './StateChip.svelte';
	import type { RunItem } from '../api.ts';
	import { fmtClock, fmtDateTime } from '../format.ts';
	import { idChWidth } from '../idch.ts';
	import { runHref } from '../routes.ts';
	import { cancellable, stateView } from '../state.ts';

	let {
		runs,
		now,
		canCancel,
		oncancel,
		caption,
	}: {
		runs: RunItem[];
		now: number;
		canCancel: boolean;
		oncancel?: (id: number) => void;
		caption: string;
	} = $props();

	const idCh = $derived(idChWidth(runs.map((r) => r.id)));

	function finished(t: number | null): string {
		if (!t) return '—';
		return now - t < 20 * 3_600_000 ? fmtClock(t) : fmtDateTime(t, now);
	}

	function rowClick(e: MouseEvent, id: number) {
		const t = e.target as HTMLElement;
		if (t.closest('a, button')) return;
		if (window.getSelection()?.toString()) return;
		void goto(runHref(id));
	}
</script>

<table class="data runlist" style:--id-ch="{idCh}ch">
	<caption class="sr">{caption}</caption>
	<thead>
		<tr>
			<th scope="col">ID</th>
			<th scope="col">Key</th>
			<th scope="col">State</th>
			<th scope="col" class="num">Steps</th>
			<th scope="col">Finished</th>
			<th scope="col"><span class="sr">Action</span></th>
		</tr>
	</thead>
	<tbody>
		{#each runs as r (r.id)}
			{@const sv = stateView(r.state)}
			<!-- the id link is the keyboard path; a click anywhere else on the row is a shortcut to it -->
			<!-- svelte-ignore a11y_click_events_have_key_events, a11y_no_noninteractive_element_interactions -->
			<tr onclick={(e) => rowClick(e, r.id)}>
				<td class="id"><a class="link" href={runHref(r.id)}>{r.id}</a></td>
				<td class="key">{r.key}</td>
				<td class="state"><StateChip state={r.state} /></td>
				<td class="num steps">{r.steps}</td>
				<td class="time">{finished(r.finished_at)}</td>
				<td class="act">
					{#if cancellable(r.state)}
						<button
							class="btn danger sm"
							disabled={!canCancel}
							title={canCancel ? `Cancel run ${r.id}` : 'Cancelling needs the API key'}
							onclick={() => oncancel?.(r.id)}>Cancel<span class="sr"> run {r.id}</span></button
						>
					{/if}
				</td>
				<td class="meta">
					<span class="chip" data-state={sv.kind}><i class="dot" aria-hidden="true"></i>{sv.label}</span>
					{#if sv.detail}{`${sv.detail} · `}{/if}{r.steps} {r.steps === 1 ? 'step' : 'steps'} · {finished(r.finished_at)}
				</td>
			</tr>
		{/each}
	</tbody>
</table>

<style>
	tbody tr {
		cursor: pointer;
	}
	td.id {
		width: 1%;
		font-family: var(--font-mono);
		font-size: 14px;
		font-weight: 600;
	}
	td.id .link {
		color: var(--c-brand-deep);
		font-variant-numeric: tabular-nums;
		text-decoration: none;
	}
	td.id .link:hover {
		text-decoration: underline;
		text-underline-offset: 3px;
	}
	.key {
		color: var(--c-ink-soft);
		font-family: var(--font-mono);
		font-size: 13.5px;
		white-space: normal !important;
		overflow-wrap: anywhere;
	}
	td.state {
		white-space: nowrap;
	}
	td.steps,
	td.time {
		color: var(--c-ink-soft);
		font-variant-numeric: tabular-nums;
	}
	td.act {
		width: 1%;
		padding-top: 6px !important;
		padding-bottom: 6px !important;
		text-align: right;
	}
	td.meta {
		display: none;
	}

	@media (max-width: 600px) {
		.runlist thead {
			display: none;
		}
		.runlist,
		.runlist tbody {
			display: block;
		}
		.runlist tr {
			display: grid;
			grid-template-columns: var(--id-ch, 4ch) minmax(0, 1fr) auto;
			column-gap: 10px;
			align-items: baseline;
			padding: 10px 14px;
			border-top: 1px solid var(--c-line);
			font-family: var(--font-mono);
			font-size: 14px;
		}
		.runlist td {
			display: block;
			width: auto;
			padding: 0 !important;
			border: 0 !important;
			line-height: 1.45;
		}
		.runlist td.id {
			grid-area: 1 / 1;
			overflow: hidden;
		}
		.runlist td.key {
			grid-area: 1 / 2;
			min-width: 0;
			overflow: hidden;
			text-overflow: ellipsis;
			white-space: nowrap !important;
			overflow-wrap: normal;
		}
		.runlist td.act {
			grid-column: 3;
			grid-row: 1 / 3;
			align-self: center;
		}
		.runlist td.state,
		.runlist td.steps,
		.runlist td.time {
			display: none;
		}
		.runlist td.meta {
			grid-area: 2 / 2;
			display: block;
			min-width: 0;
			margin-top: 6px;
			overflow: hidden;
			color: var(--c-body);
			font-family: var(--font-sans);
			font-size: 13px;
			text-overflow: ellipsis;
			white-space: nowrap;
		}
		.runlist td.meta .chip {
			height: 20px;
			margin-right: 4px;
			padding: 0 7px;
			font-size: 12px;
		}
	}
</style>
