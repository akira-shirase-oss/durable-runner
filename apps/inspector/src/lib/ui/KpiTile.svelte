<script lang="ts">
	// A stat tile: label, value (proportional figures), one line of context, and an optional sparkline of the
	// window (decorative; `trend` says it in words for screen readers).
	import type { Snippet } from 'svelte';
	import Sparkline from '../charts/Sparkline.svelte';

	let {
		label,
		value,
		sub,
		spark,
		color = 'var(--c-chart-1)',
		trend,
		tone,
		icon,
		href,
	}: {
		label: string;
		value: string;
		sub?: string;
		spark?: (number | null)[];
		color?: string;
		trend?: string;
		tone?: 'ok' | 'bad' | 'warn';
		icon?: Snippet;
		href?: string;
	} = $props();
</script>

<div class="tile">
	<dt>{label}</dt>
	<dd class="value {tone ?? ''}">{@render icon?.()}{value}</dd>
	{#if sub}<dd class="sub-line">{#if href}<a href={href}>{sub}</a>{:else}{sub}{/if}</dd>{/if}
	{#if spark}
		<dd class="spark"><Sparkline values={spark} {color} />{#if trend}<span class="sr">{trend}</span>{/if}</dd>
	{/if}
</div>

<style>
	.tile {
		display: flex;
		flex-direction: column;
		min-width: 0;
		padding: 18px 20px 16px;
		border-radius: 16px;
		background: var(--c-card);
		box-shadow:
			0 0 0 1px var(--c-bar-edge),
			0 1px 2px var(--c-tile-shadow);
	}
	dt {
		color: var(--c-body);
		font-size: 13px;
		font-weight: 480;
		white-space: nowrap;
		overflow: hidden;
		text-overflow: ellipsis;
	}
	dd {
		margin: 0;
	}
	.value {
		display: flex;
		align-items: center;
		gap: 7px;
		margin-top: 4px;
		font-size: 25px;
		font-weight: 560;
		letter-spacing: -0.02em;
		line-height: 1.15;
		white-space: nowrap;
	}
	.value.ok {
		color: var(--c-state-done-ink);
	}
	.value.bad {
		color: var(--c-danger);
	}
	.value.warn {
		color: var(--c-state-retry-ink);
	}
	.sub-line {
		margin-top: 3px;
		overflow: hidden;
		color: var(--c-body);
		font-size: 12.5px;
		text-overflow: ellipsis;
		white-space: nowrap;
	}
	.sub-line a {
		color: var(--c-brand-deep);
		text-underline-offset: 3px;
	}
	.spark {
		margin-top: auto;
		padding-top: 8px;
	}
</style>
