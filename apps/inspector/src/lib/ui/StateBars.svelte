<script lang="ts">
	// Runs by state now: a thin segmented bar of the whole (decorative), then one labelled row per state with its
	// count and share. Colours are the state chips'.
	import { fmtInt, fmtPct } from '../format.ts';
	import { orderedStates } from '../overview.ts';
	import { share } from '../state.ts';
	import { stateView } from '../state.ts';

	let { states }: { states: Record<string, number> } = $props();
	const rows = $derived(orderedStates(states));
	const sum = $derived(rows.reduce((a, [, n]) => a + n, 0));
	const top = $derived(Math.max(0, ...rows.map(([, n]) => n)));
</script>

<div class="whole" aria-hidden="true">
	{#each rows as [s, n] (s)}
		{#if n > 0}<i data-state={s} style:flex-grow={n}></i>{/if}
	{/each}
</div>
<dl class="bars">
	{#each rows as [s, n] (s)}
		<div>
			<dt><i class="dot" data-state={s} aria-hidden="true"></i>{stateView(s).label}</dt>
			<dd class="track"><i data-state={s} style:width="{share(n, top)}%"></i></dd>
			<dd class="count">{fmtInt(n)}<span class="pct">{sum > 0 ? fmtPct(n / sum) : ''}</span></dd>
		</div>
	{/each}
</dl>

<style>
	.whole {
		display: flex;
		gap: 2px;
		height: 10px;
		margin: 2px 0 14px;
		overflow: hidden;
		border-radius: 5px;
		background: var(--c-line);
	}
	.whole i {
		flex-basis: 0;
		min-width: 3px;
		background: var(--c-state-queued);
	}
	.dot {
		flex: none;
		width: 8px;
		height: 8px;
		border-radius: 50%;
		background: var(--c-state-queued);
	}
	[data-state='running'] {
		background: var(--c-state-run) !important;
	}
	[data-state='retry_waiting'] {
		background: var(--c-state-retry) !important;
	}
	[data-state='reconciling'] {
		background: var(--c-state-probe) !important;
	}
	[data-state='done'] {
		background: var(--c-state-done) !important;
	}
	[data-state='failed'] {
		background: var(--c-danger) !important;
	}
	[data-state='cancelled'] {
		background: var(--c-line-strong) !important;
	}
	.dot[data-state='cancelled'] {
		background: transparent !important;
		box-shadow: inset 0 0 0 1.5px var(--c-state-cancelled);
	}
	.bars .count {
		display: inline-flex;
		justify-content: flex-end;
		gap: 8px;
		min-width: 6.5rem;
	}
	.pct {
		min-width: 3rem;
		color: var(--c-body);
		text-align: right;
	}
</style>
